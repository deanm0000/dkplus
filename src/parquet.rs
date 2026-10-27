//! Streaming Parquet output without a DuckLake connection.
use crate::*;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::parquet::arrow::AsyncArrowWriter;
use datafusion::parquet::file::properties::EnabledStatistics;
use object_store::buffered::BufWriter;

pub const TARGET_ROWS: usize = 1_048_576;
pub const TARGET_BYTES: usize = 128 * 1024 * 1024;

#[derive(Debug, Clone)]
pub enum Grouping {
    None,
    /// Never split a key, even when that exceeds the size target.
    WholeKeys(String),
    /// Flush on key changes; large keys may span multiple row groups.
    SingleKey(String),
}

#[derive(Debug)]
pub struct WrittenFile {
    pub path: ObjPath,
    pub rows: i64,
    pub size_bytes: i64,
    pub footer_bytes: i64,
}

/// Input must be ascending by the grouping column and contain no null group keys.
/// The caller owns removal of a partial object if writing fails.
pub struct ParquetWriter {
    inner: OutputWriter,
    path: ObjPath,
}

impl ParquetWriter {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        path: ObjPath,
        schema: SchemaRef,
        grouping: Grouping,
    ) -> Result<Self> {
        let (column, single) = match &grouping {
            Grouping::None => (None, false),
            Grouping::WholeKeys(name) => (Some(name.as_str()), false),
            Grouping::SingleKey(name) => (Some(name.as_str()), true),
        };
        let cols = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                Ok(Column {
                    id: index as i64,
                    ty: types::catalog_type(field.data_type())?.into(),
                })
            })
            .collect::<Result<Vec<_>>>()?;
        let refs = cols.iter().collect::<Vec<_>>();
        let inner = OutputWriter::at_path(
            store,
            path.clone(),
            path.to_string(),
            schema,
            single,
            &refs,
            column,
        )?;
        Ok(Self { inner, path })
    }

    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.inner.write(batch).await
    }

    pub async fn finish(self) -> Result<WrittenFile> {
        let file = self.inner.finish().await?;
        Ok(WrittenFile {
            path: self.path,
            rows: file.rows,
            size_bytes: file.size,
            footer_bytes: file.footer,
        })
    }
}

pub(crate) fn properties(manual_groups: bool) -> Result<WriterProperties> {
    Ok(WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .set_statistics_enabled(EnabledStatistics::Chunk)
        .set_statistics_truncate_length(None)
        .set_max_row_group_row_count(if manual_groups { None } else { Some(65_536) })
        .set_max_row_group_bytes(None)
        .build())
}

pub(crate) struct Groups {
    pub(crate) node_only: bool,
    pub(crate) last_node: Option<String>,
    pub(crate) rows: usize,
    pub(crate) bytes: usize,
}

impl Groups {
    pub(crate) fn boundary(&mut self, node: &str) -> bool {
        let changed = self.last_node.as_deref().is_some_and(|last| last != node);
        let flush =
            changed && (self.node_only || self.rows >= TARGET_ROWS || self.bytes >= TARGET_BYTES);
        if flush {
            self.rows = 0;
            self.bytes = 0;
        }
        self.last_node = Some(node.to_string());
        flush
    }
}

pub(crate) struct OutputWriter {
    writer: AsyncArrowWriter<BufWriter>,
    store: Arc<dyn ObjectStore>,
    path: ObjPath,
    name: String,
    schema: SchemaRef,
    node_idx: Option<usize>,
    stats: Vec<FileColumnStats>,
    rows: i64,
    groups: Groups,
}

impl OutputWriter {
    pub(crate) fn new(
        work: &mut crate::repack::Work,
        schema: SchemaRef,
        node_only: bool,
        cols: &[&Column],
        row_group_col: Option<&str>,
    ) -> Result<Self> {
        let name = format!("ducklake-{}.parquet", Uuid::now_v7());
        let path = ObjPath::from(format!("{}{name}", work.prefix));
        work.outputs.push(path.clone());
        Self::at_path(
            work.store.clone(),
            path,
            name,
            schema,
            node_only,
            cols,
            row_group_col,
        )
    }

    fn at_path(
        store: Arc<dyn ObjectStore>,
        path: ObjPath,
        name: String,
        schema: SchemaRef,
        node_only: bool,
        cols: &[&Column],
        row_group_col: Option<&str>,
    ) -> Result<Self> {
        let node_idx = row_group_col
            .map(|name| schema.index_of(name))
            .transpose()?;
        if let Some(index) = node_idx
            && !types::supported(schema.field(index).data_type())
        {
            return Err(Error::UnsupportedType {
                data_type: schema.field(index).data_type().to_string(),
            });
        }
        let writer = AsyncArrowWriter::try_new_with_options(
            BufWriter::new(store.clone(), path.clone()),
            schema.clone(),
            ArrowWriterOptions::new()
                .with_properties(properties(true)?)
                .with_skip_arrow_metadata(true),
        )?;
        let stats = cols
            .iter()
            .map(|c| FileColumnStats {
                column_id: c.id,
                ty: c.ty.clone(),
                size_bytes: 0,
                value_count: 0,
                null_count: 0,
                min: None,
                max: None,
                contains_nan: if c.ty == "float64" || c.ty == "float32" {
                    Some(false)
                } else {
                    None
                },
            })
            .collect();
        Ok(Self {
            writer,
            store: store.clone(),
            path,
            name,
            schema,
            node_idx,
            stats,
            rows: 0,
            groups: Groups {
                node_only,
                last_node: None,
                rows: 0,
                bytes: 0,
            },
        })
    }

    pub(crate) async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.schema() != self.schema {
            fail!(InvalidInput, "output batch schema changed while writing");
        }
        for (i, stat) in self.stats.iter_mut().enumerate() {
            let (lo, hi, nan) = column_bounds(std::slice::from_ref(batch), i)?;
            stat.min = match (&stat.min, &lo) {
                (None, _) => lo,
                (_, None) => stat.min.take(),
                _ => widen(&stat.ty, stat.min.take(), &lo, false),
            };
            stat.max = match (&stat.max, &hi) {
                (None, _) => hi,
                (_, None) => stat.max.take(),
                _ => widen(&stat.ty, stat.max.take(), &hi, true),
            };
            stat.null_count += batch.column(i).null_count() as i64;
            stat.value_count += batch.num_rows() as i64;
            if let Some(nan) = nan {
                stat.contains_nan = Some(stat.contains_nan == Some(true) || nan);
            }
        }
        self.rows += batch.num_rows() as i64;
        let nodes = self
            .node_idx
            .map(|index| types::keys(batch.column(index)))
            .transpose()?;
        let mut start = 0;
        while start < batch.num_rows() {
            let node = nodes.as_ref().map_or("", |nodes| nodes[start].as_str());
            if self.node_idx.is_some()
                && self
                    .groups
                    .last_node
                    .as_deref()
                    .is_some_and(|last| last > node)
            {
                fail!(
                    InvalidInput,
                    "row-group keys must be sorted ascending across batches"
                );
            }
            if self.groups.boundary(node) {
                self.writer.flush().await?;
            }
            let mut end = start + 1;
            while end < batch.num_rows() && nodes.as_ref().is_none_or(|nodes| nodes[end] == node) {
                end += 1;
            }
            let mut pos = start;
            while pos < end {
                let n = if self.groups.node_only || self.node_idx.is_none() {
                    (end - pos).min(TARGET_ROWS - self.groups.rows)
                } else {
                    end - pos
                };
                self.writer.write(&batch.slice(pos, n)).await?;
                self.groups.rows += n;
                self.groups.bytes = self.writer.in_progress_size();
                pos += n;
                if (self.groups.node_only || self.node_idx.is_none())
                    && (self.groups.rows >= TARGET_ROWS || self.groups.bytes >= TARGET_BYTES)
                {
                    self.writer.flush().await?;
                    self.groups.rows = 0;
                    self.groups.bytes = 0;
                }
            }
            start = end;
        }
        Ok(())
    }

    pub(crate) async fn finish(self) -> Result<FileInfo> {
        let pq = self.writer.close().await?;
        let mut stats = self.stats;
        for (i, stat) in stats.iter_mut().enumerate() {
            stat.size_bytes = pq
                .row_groups()
                .iter()
                .map(|g| g.column(i).compressed_size())
                .sum();
        }
        let size = self.store.head(&self.path).await?.size;
        let trailer = self.store.get_range(&self.path, size - 8..size).await?;
        let footer = i64::from(u32::from_le_bytes(trailer[..4].try_into()?));
        Ok(FileInfo {
            name: self.name,
            rows: self.rows,
            size: i64::try_from(size)?,
            footer,
            stats,
        })
    }
}
