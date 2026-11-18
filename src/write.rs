//! Append and key-based replacement writes for non-window DuckLake tables.

use crate::*;
use datafusion::arrow::array::{
    Array, ArrayRef, Float32Array, Float64Array, Int64Array, StringArray,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::parquet::arrow::arrow_writer::ArrowWriterOptions;
use datafusion::parquet::basic::{Compression, ZstdLevel};
use datafusion::parquet::file::properties::WriterProperties;
use object_store::ObjectStoreExt;
use std::collections::{HashMap, HashSet};

const ROW_ID_FIELD: &str = "2147483540";
const SNAPSHOT_ID_FIELD: &str = "2147483539";

#[derive(Clone, Debug, PartialEq, Eq)]
struct Source {
    id: i64,
    path: String,
    relative: bool,
    rows: i64,
    row_id_start: Option<i64>,
    begin_snapshot: i64,
    format: String,
    mapping: Option<i64>,
    encryption: Option<String>,
    partial_max: Option<i64>,
    delete: Option<Delete>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Delete {
    id: i64,
    path: String,
    relative: bool,
    count: i64,
    format: String,
    encryption: Option<String>,
    partial_max: Option<i64>,
}

#[derive(Debug)]
struct NewDelete {
    source_id: i64,
    count: i64,
    name: String,
    size: i64,
    footer: i64,
}

struct Uploads<'a> {
    store: &'a Arc<dyn ObjectStore>,
    prefix: &'a str,
    paths: &'a mut Vec<ObjPath>,
}

impl Table {
    /// Append batches as one new DuckLake data file and snapshot.
    pub async fn insert(&self, batches: Vec<RecordBatch>) -> Result<LoadReport> {
        self.validate()?;
        let batches = validate_batches(batches)?;
        let mut client = connect(&self.cfg).await?;
        let (meta, snapshot) = {
            let tx = client
                .build_transaction()
                .isolation_level(IsolationLevel::RepeatableRead)
                .read_only(true)
                .start()
                .await?;
            let meta = table_meta(&tx, &self.schema, &self.table).await?;
            let snapshot = latest_snapshot(&tx).await?.0;
            tx.commit().await?;
            (meta, snapshot)
        };
        let columns = resolve_columns(&meta, &batches[0].schema())?;
        let (bytes, parquet) = write_parquet(&batches, &columns)?;
        let file = file_info(
            format!("ducklake-{}.parquet", Uuid::now_v7()),
            &bytes,
            &parquet,
            &batches,
            &columns,
        )?;
        let (store, prefix) = object_store_for(&self.cfg, &meta.root)?;
        let path = ObjPath::from(format!("{prefix}{}", file.name));
        if let Err(error) = store
            .put(&path, PutPayload::from(Bytes::from(bytes)))
            .await
            .context("uploading insert data file")
        {
            cleanup_if_unreferenced(self, &client, &meta, store.as_ref(), &path).await;
            return Err(error);
        }
        let result = commit_insert(self, &mut client, &meta, snapshot, &file).await;
        if result.is_err() {
            cleanup_if_unreferenced(self, &client, &meta, store.as_ref(), &path).await;
        }
        result
    }

    /// Replace existing rows with matching unique keys while retaining their
    /// unchanged data files and row identities.
    ///
    /// Duplicate incoming keys are rejected; null key components compare equal.
    /// Existing matching rows are hidden with appended positional delete files;
    /// active inline rows with matching keys receive the same end snapshot.
    pub async fn upsert(&self, batches: Vec<RecordBatch>) -> Result<LoadReport> {
        self.validate()?;
        let unique = self
            .unique_by_cols
            .as_deref()
            .filter(|columns| !columns.is_empty())
            .ok_or_else(|| Error::MissingRequiredSetting {
                table: self.to_string(),
                setting: "unique_by_cols",
            })?;
        let batches = validate_batches(batches)?;
        let mut client = connect(&self.cfg).await?;
        let (meta, snapshot, sources, inline_names) = {
            let tx = client
                .build_transaction()
                .isolation_level(IsolationLevel::RepeatableRead)
                .read_only(true)
                .start()
                .await?;
            let meta = table_meta(&tx, &self.schema, &self.table).await?;
            ensure_unpartitioned(&tx, meta.table_id).await?;
            let snapshot = latest_snapshot(&tx).await?.0;
            let sources = sources(&tx, meta.table_id).await?;
            let inline_names = inline_names(&tx, meta.table_id).await?;
            tx.commit().await?;
            (meta, snapshot, sources, inline_names)
        };
        let columns = resolve_columns(&meta, &batches[0].schema())?;
        let key_indices = unique
            .iter()
            .map(|name| {
                batches[0]
                    .schema()
                    .index_of(name)
                    .with_context(|| format!("unique key column {name:?} is missing from input"))
            })
            .collect::<Result<Vec<_>>>()?;
        let incoming_keys = incoming_keys(&batches, &key_indices)?;

        let (store, prefix) = object_store_for(&self.cfg, &meta.root)?;
        let mut uploaded = Vec::<ObjPath>::new();
        let data_file = async {
            let delete_files = prepare_deletes(
                self,
                &meta,
                &sources,
                &incoming_keys,
                unique,
                Uploads {
                    store: &store,
                    prefix: &prefix,
                    paths: &mut uploaded,
                },
            )
            .await?;
            let inline_matches =
                read_inline_matches(&client, &meta, &inline_names, unique, &incoming_keys).await?;
            let (bytes, parquet) = write_parquet(&batches, &columns)?;
            let data_file = file_info(
                format!("ducklake-{}.parquet", Uuid::now_v7()),
                &bytes,
                &parquet,
                &batches,
                &columns,
            )?;
            let data_path = ObjPath::from(format!("{prefix}{}", data_file.name));
            uploaded.push(data_path.clone());
            store
                .put(&data_path, PutPayload::from(Bytes::from(bytes)))
                .await
                .context("uploading upsert data file")?;
            Ok::<_, Error>((delete_files, inline_matches, data_file))
        }
        .await;
        let (delete_files, inline_matches, data_file) = match data_file {
            Ok(prepared) => prepared,
            Err(error) => {
                cleanup_paths(self, &client, &meta, store.as_ref(), &uploaded).await;
                return Err(error);
            }
        };
        let result = commit_upsert(
            self,
            &mut client,
            &meta,
            snapshot,
            &sources,
            &inline_names,
            &inline_matches,
            &incoming_keys,
            unique,
            &delete_files,
            &data_file,
        )
        .await;
        if result.is_err() {
            cleanup_paths(self, &client, &meta, store.as_ref(), &uploaded).await;
        }
        result
    }
}

fn validate_batches(batches: Vec<RecordBatch>) -> Result<Vec<RecordBatch>> {
    let batches = batches
        .into_iter()
        .filter(|batch| batch.num_rows() != 0)
        .collect::<Vec<_>>();
    if batches.is_empty() {
        fail!(InvalidInput, "no rows to write");
    }
    let schema = batches[0].schema();
    if batches.iter().skip(1).any(|batch| batch.schema() != schema) {
        fail!(InvalidInput, "load batch schemas differ");
    }
    Ok(batches)
}

async fn latest_snapshot(client: &impl GenericClient) -> Result<(i64, i64, i64, i64)> {
    let row = client
        .query_one(
            "SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
               FROM ducklake.ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1",
            &[],
        )
        .await
        .context("reading latest DuckLake snapshot")?;
    Ok((row.get(0), row.get(1), row.get(2), row.get(3)))
}

async fn ensure_unpartitioned(client: &impl GenericClient, table_id: i64) -> Result<()> {
    let partitioned: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM ducklake.ducklake_partition_info
                             WHERE table_id = $1 AND end_snapshot IS NULL)",
            &[&table_id],
        )
        .await?
        .get(0);
    if partitioned {
        fail!(
            UnsupportedFeature,
            "upsert does not support partitioned DuckLake tables"
        );
    }
    Ok(())
}

async fn sources(client: &impl GenericClient, table_id: i64) -> Result<Vec<Source>> {
    let rows = client
        .query(
            "SELECT f.data_file_id, f.path, f.path_is_relative, f.record_count,
                    f.row_id_start, f.begin_snapshot, f.file_format, f.mapping_id,
                    f.encryption_key, f.partial_max, d.delete_file_id, d.path,
                    d.path_is_relative, d.delete_count, d.format, d.encryption_key,
                    d.partial_max
               FROM ducklake.ducklake_data_file f
               LEFT JOIN ducklake.ducklake_delete_file d
                 ON d.table_id = f.table_id AND d.data_file_id = f.data_file_id
                AND d.end_snapshot IS NULL
              WHERE f.table_id = $1 AND f.end_snapshot IS NULL
              ORDER BY f.data_file_id",
            &[&table_id],
        )
        .await
        .context("reading active DuckLake data files")?;
    let mut out = Vec::with_capacity(rows.len());
    for row in rows {
        let id = row.get(0);
        if out.last().is_some_and(|source: &Source| source.id == id) {
            fail!(
                InvalidData,
                "multiple active delete files for data file {id}"
            );
        }
        let delete = row.get::<_, Option<i64>>(10).map(|delete_id| Delete {
            id: delete_id,
            path: row.get(11),
            relative: row.get::<_, Option<bool>>(12).unwrap_or(false),
            count: row.get(13),
            format: row.get(14),
            encryption: row.get(15),
            partial_max: row.get(16),
        });
        out.push(Source {
            id,
            path: row.get(1),
            relative: row.get::<_, Option<bool>>(2).unwrap_or(false),
            rows: row.get(3),
            row_id_start: row.get(4),
            begin_snapshot: row.get(5),
            format: row.get(6),
            mapping: row.get(7),
            encryption: row.get(8),
            partial_max: row.get(9),
            delete,
        });
    }
    Ok(out)
}

async fn inline_names(client: &impl GenericClient, table_id: i64) -> Result<Vec<String>> {
    Ok(client
        .query(
            "SELECT table_name FROM ducklake.ducklake_inlined_data_tables
              WHERE table_id = $1 ORDER BY table_name",
            &[&table_id],
        )
        .await
        .context("reading DuckLake inlined table names")?
        .into_iter()
        .map(|row| row.get(0))
        .collect())
}

fn incoming_keys(batches: &[RecordBatch], indices: &[usize]) -> Result<HashSet<Vec<u8>>> {
    let mut keys = HashSet::new();
    for batch in batches {
        let arrays = indices
            .iter()
            .map(|index| batch.column(*index).clone())
            .collect::<Vec<_>>();
        for key in row_keys(&arrays)? {
            if !keys.insert(key) {
                fail!(InvalidInput, "upsert input contains duplicate unique keys");
            }
        }
    }
    Ok(keys)
}

fn row_keys(arrays: &[ArrayRef]) -> Result<Vec<Vec<u8>>> {
    let arrays = arrays
        .iter()
        .map(|array| -> Result<ArrayRef> {
            Ok(match array.data_type() {
                DataType::Float32 => Arc::new(Float32Array::from_iter(
                    array
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .context("invalid float32 key array")?
                        .iter()
                        .map(|value| {
                            value.map(|v| {
                                if v.is_nan() {
                                    f32::NAN
                                } else if v == 0.0 {
                                    0.0
                                } else {
                                    v
                                }
                            })
                        }),
                )),
                DataType::Float64 => Arc::new(Float64Array::from_iter(
                    array
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .context("invalid float64 key array")?
                        .iter()
                        .map(|value| {
                            value.map(|v| {
                                if v.is_nan() {
                                    f64::NAN
                                } else if v == 0.0 {
                                    0.0
                                } else {
                                    v
                                }
                            })
                        }),
                )),
                _ => array.clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let fields = arrays
        .iter()
        .map(|array| SortField::new(array.data_type().clone()))
        .collect::<Vec<_>>();
    let converter = RowConverter::new(fields)?;
    Ok(converter
        .convert_columns(&arrays)?
        .iter()
        .map(|row| row.as_ref().to_vec())
        .collect())
}

fn keys_for_batch(batch: &RecordBatch, indices: &[usize]) -> Result<Vec<Vec<u8>>> {
    let arrays = indices
        .iter()
        .map(|index| batch.column(*index).clone())
        .collect::<Vec<_>>();
    row_keys(&arrays)
}

fn find_new_delete_positions(
    keys: &[Vec<u8>],
    incoming: &HashSet<Vec<u8>>,
    existing: &[i64],
    offset: i64,
) -> Vec<i64> {
    let mut deleted = 0;
    let mut matching = Vec::new();
    for (index, key) in keys.iter().enumerate() {
        let position = offset + index as i64;
        while existing.get(deleted).is_some_and(|old| *old < position) {
            deleted += 1;
        }
        let was_deleted = existing.get(deleted) == Some(&position);
        if !was_deleted && incoming.contains(key) {
            matching.push(position);
        }
    }
    matching
}

fn merge_positions(existing: &[i64], added: &[i64]) -> Vec<i64> {
    let mut merged = Vec::with_capacity(existing.len() + added.len());
    let (mut old, mut new) = (0, 0);
    while old < existing.len() || new < added.len() {
        let value = match (existing.get(old), added.get(new)) {
            (Some(a), Some(b)) if a < b => {
                old += 1;
                *a
            }
            (Some(a), Some(b)) if a == b => {
                old += 1;
                new += 1;
                *a
            }
            (_, Some(b)) => {
                new += 1;
                *b
            }
            (Some(a), None) => {
                old += 1;
                *a
            }
            (None, None) => break,
        };
        merged.push(value);
    }
    merged
}

fn source_schema(meta: &TableMeta) -> Result<SchemaRef> {
    let mut columns = meta.columns.iter().collect::<Vec<_>>();
    columns.sort_by_key(|(_, column)| column.id);
    let fields = columns
        .into_iter()
        .map(|(name, column)| {
            Ok(
                Field::new(name, types::arrow_type(&column.ty)?, true).with_metadata(
                    HashMap::from([(PARQUET_FIELD_ID_META_KEY.to_owned(), column.id.to_string())]),
                ),
            )
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Arc::new(Schema::new(fields)))
}

fn normalize_source_batch(
    batch: &RecordBatch,
    target: &SchemaRef,
    source: &Source,
) -> Result<RecordBatch> {
    let mut input_by_id = HashMap::<String, usize>::new();
    for (index, field) in batch.schema().fields().iter().enumerate() {
        let id = field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .with_context(|| {
                format!(
                    "source {} has Parquet field {:?} without a DuckLake field ID",
                    source.id,
                    field.name()
                )
            })?;
        if input_by_id.insert(id.clone(), index).is_some() {
            fail!(
                InvalidData,
                "source {} has duplicate Parquet field ID {id}",
                source.id
            );
        }
        match id.as_str() {
            ROW_ID_FIELD if field.name() != "_ducklake_internal_row_id" => {
                fail!(
                    InvalidData,
                    "source {} has an invalid row identity field",
                    source.id
                );
            }
            SNAPSHOT_ID_FIELD if field.name() != "_ducklake_internal_snapshot_id" => {
                fail!(
                    InvalidData,
                    "source {} has an invalid snapshot identity field",
                    source.id
                );
            }
            _ => {}
        }
    }
    let known = target
        .fields()
        .iter()
        .filter_map(|field| field.metadata().get(PARQUET_FIELD_ID_META_KEY).cloned())
        .collect::<HashSet<_>>();
    for id in input_by_id.keys() {
        if !known.contains(id) && id != ROW_ID_FIELD && id != SNAPSHOT_ID_FIELD {
            fail!(
                UnsupportedFeature,
                "source {} has unsupported Parquet field ID {id}",
                source.id
            );
        }
    }
    let mut arrays = Vec::with_capacity(target.fields().len());
    for field in target.fields() {
        let id = field
            .metadata()
            .get(PARQUET_FIELD_ID_META_KEY)
            .context("target schema is missing DuckLake field ID")?;
        let index = input_by_id
            .get(id)
            .with_context(|| format!("source {} is missing field ID {id}", source.id))?;
        let array = batch.column(*index);
        if array.data_type() != field.data_type() {
            fail!(
                InvalidData,
                "source {} field ID {id} has type {}, expected {}",
                source.id,
                array.data_type(),
                field.data_type()
            );
        }
        arrays.push(array.clone());
    }
    for id in [ROW_ID_FIELD, SNAPSHOT_ID_FIELD] {
        if let Some(index) = input_by_id.get(id)
            && batch.column(*index).data_type() != &DataType::Int64
        {
            fail!(
                InvalidData,
                "source {} internal field ID {id} is not int64",
                source.id
            );
        }
    }
    Ok(RecordBatch::try_new(target.clone(), arrays)?)
}

async fn load_delete_positions(
    table: &Table,
    meta: &TableMeta,
    source: &Source,
) -> Result<Vec<i64>> {
    let Some(delete) = &source.delete else {
        return Ok(Vec::new());
    };
    if delete.format != "parquet" || delete.encryption.is_some() || delete.partial_max.is_some() {
        fail!(
            UnsupportedFeature,
            "delete file {} is mapped, encrypted, partial, or not Parquet",
            delete.id
        );
    }
    let bytes = read_object(table, &location(meta, &delete.path, delete.relative))
        .await
        .with_context(|| format!("reading delete file {}", delete.id))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .with_context(|| format!("opening delete file {}", delete.id))?
        .with_batch_size(8192)
        .build()?;
    let mut positions = Vec::new();
    for batch in reader {
        let batch = batch?;
        let pos_index = validate_delete_schema(&batch)?;
        if let Some(paths) = batch.column_by_name("file_path") {
            let paths = paths
                .as_any()
                .downcast_ref::<StringArray>()
                .context("delete file_path must be utf8")?;
            let expected = location(meta, &source.path, source.relative);
            if paths.iter().any(|path| path != Some(expected.as_str())) {
                fail!(
                    InvalidData,
                    "delete file {} references a different data file",
                    delete.id
                );
            }
        }
        for value in batch
            .column(pos_index)
            .as_any()
            .downcast_ref::<Int64Array>()
            .context("delete positions are not int64")?
            .iter()
        {
            let position = value.context("delete position is null")?;
            if position < 0
                || position >= source.rows
                || positions
                    .last()
                    .is_some_and(|previous| *previous >= position)
            {
                fail!(
                    InvalidData,
                    "delete positions in file {} must be unique, ascending, and within source bounds",
                    delete.id
                );
            }
            positions.push(position);
        }
    }
    if positions.len() as i64 != delete.count {
        fail!(
            InvalidData,
            "delete file {} count mismatch: expected {}, found {}",
            delete.id,
            delete.count,
            positions.len()
        );
    }
    Ok(positions)
}

fn validate_delete_schema(batch: &RecordBatch) -> Result<usize> {
    let schema = batch.schema();
    let pos = schema
        .index_of("pos")
        .context("delete file has no pos column")?;
    if schema.field(pos).data_type() != &DataType::Int64
        || schema
            .fields()
            .iter()
            .any(|field| field.name() != "pos" && field.name() != "file_path")
        || schema
            .fields()
            .iter()
            .filter(|field| field.name() == "pos")
            .count()
            != 1
        || schema
            .fields()
            .iter()
            .filter(|field| field.name() == "file_path")
            .count()
            > 1
        || batch.num_columns() > 2
    {
        fail!(
            InvalidData,
            "positional delete Parquet batches must contain an int64 pos and optional file_path"
        );
    }
    Ok(pos)
}

async fn prepare_deletes(
    table: &Table,
    meta: &TableMeta,
    sources: &[Source],
    incoming: &HashSet<Vec<u8>>,
    unique: &[String],
    uploads: Uploads<'_>,
) -> Result<Vec<NewDelete>> {
    let target = source_schema(meta)?;
    let key_indices = unique
        .iter()
        .map(|name| target.index_of(name).map_err(Error::from))
        .collect::<Result<Vec<_>>>()?;
    let mut new_deletes = Vec::new();
    for source in sources {
        if source.format != "parquet"
            || source.mapping.is_some()
            || source.encryption.is_some()
            || source.partial_max.is_some()
        {
            fail!(
                UnsupportedFeature,
                "source file {} is mapped, encrypted, partial, or not Parquet",
                source.id
            );
        }
        let old_positions = load_delete_positions(table, meta, source).await?;
        let bytes = read_object(table, &location(meta, &source.path, source.relative))
            .await
            .with_context(|| format!("reading source data file {}", source.id))?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
            .with_context(|| format!("opening source data file {}", source.id))?
            .with_batch_size(8192)
            .build()?;
        let mut offset = 0i64;
        let mut added = Vec::new();
        for batch in reader {
            let batch = batch?;
            let normalized = normalize_source_batch(&batch, &target, source)?;
            let keys = keys_for_batch(&normalized, &key_indices)?;
            added.extend(find_new_delete_positions(
                &keys,
                incoming,
                &old_positions,
                offset,
            ));
            offset = offset
                .checked_add(batch.num_rows() as i64)
                .context("source row offset overflow")?;
        }
        if offset != source.rows {
            fail!(
                InvalidData,
                "source {} row count mismatch: catalog says {}, file has {offset}",
                source.id,
                source.rows
            );
        }
        if added.is_empty() {
            continue;
        }
        let merged = merge_positions(&old_positions, &added);
        let bytes = write_delete_file(&location(meta, &source.path, source.relative), &merged)?;
        let name = format!("ducklake-delete-{}.parquet", Uuid::now_v7());
        let size = i64::try_from(bytes.len()).context("delete file size overflow")?;
        let footer = i64::from(u32::from_le_bytes(
            bytes[bytes.len() - 8..bytes.len() - 4].try_into()?,
        ));
        let path = ObjPath::from(format!("{}{name}", uploads.prefix));
        uploads.paths.push(path.clone());
        uploads
            .store
            .put(&path, PutPayload::from(Bytes::from(bytes)))
            .await
            .with_context(|| format!("uploading positional deletes for file {}", source.id))?;
        new_deletes.push(NewDelete {
            source_id: source.id,
            count: i64::try_from(merged.len()).context("delete count overflow")?,
            name,
            size,
            footer,
        });
    }
    Ok(new_deletes)
}

fn write_delete_file(source_path: &str, positions: &[i64]) -> Result<Vec<u8>> {
    if positions.is_empty() || positions.windows(2).any(|pair| pair[0] >= pair[1]) {
        fail!(
            InvalidInput,
            "delete positions must be nonempty and ascending"
        );
    }
    let schema = Arc::new(Schema::new(vec![
        Field::new("file_path", DataType::Utf8, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_owned(),
            "2147483646".to_owned(),
        )])),
        Field::new("pos", DataType::Int64, false).with_metadata(HashMap::from([(
            PARQUET_FIELD_ID_META_KEY.to_owned(),
            "2147483645".to_owned(),
        )])),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(StringArray::from(vec![source_path; positions.len()])),
            Arc::new(Int64Array::from(positions.to_vec())),
        ],
    )?;
    let options = ArrowWriterOptions::new()
        .with_properties(
            WriterProperties::builder()
                .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
                .build(),
        )
        .with_skip_arrow_metadata(true);
    let mut bytes = Vec::new();
    let mut writer = datafusion::parquet::arrow::ArrowWriter::try_new_with_options(
        &mut bytes,
        batch.schema(),
        options,
    )?;
    writer.write(&batch)?;
    writer.close()?;
    Ok(bytes)
}

async fn read_inline_matches(
    client: &impl GenericClient,
    meta: &TableMeta,
    names: &[String],
    unique: &[String],
    incoming: &HashSet<Vec<u8>>,
) -> Result<HashMap<String, Vec<String>>> {
    let mut matches = HashMap::new();
    for name in names {
        let qualified = format!("ducklake.{}", identifier(name)?);
        let types = client
            .query(
                "SELECT attname, atttypid = 'pg_catalog.bytea'::regtype
                   FROM pg_catalog.pg_attribute
                  WHERE attrelid = pg_catalog.to_regclass($1)
                    AND attnum > 0 AND NOT attisdropped AND attname = ANY($2)",
                &[&qualified, &unique],
            )
            .await
            .with_context(|| format!("reading inline column types in {name}"))?
            .into_iter()
            .map(|row| (row.get::<_, String>(0), row.get::<_, bool>(1)))
            .collect::<HashMap<_, _>>();
        let selection = unique
            .iter()
            .map(|column| {
                let quoted = identifier(column)?;
                let is_bytes = types
                    .get(column)
                    .with_context(|| format!("inline table {name} has no key column {column:?}"))?;
                if *is_bytes {
                    if meta.columns[column].ty != "varchar" {
                        fail!(
                            UnsupportedFeature,
                            "inline bytea key {column:?} is not varchar"
                        );
                    }
                    Ok(format!("convert_from({quoted}, 'UTF8')"))
                } else {
                    Ok(format!("{quoted}::text"))
                }
            })
            .collect::<Result<Vec<_>>>()?
            .join(", ");
        let rows = client
            .query(
                &format!(
                    "SELECT ctid::text, {selection} FROM ducklake.{} WHERE end_snapshot IS NULL ORDER BY ctid",
                    identifier(name)?
                ),
                &[],
            )
            .await
            .with_context(|| format!("reading active inline rows in {name}"))?;
        let mut ctids = Vec::with_capacity(rows.len());
        let mut values = vec![Vec::<Option<String>>::with_capacity(rows.len()); unique.len()];
        for row in &rows {
            ctids.push(row.get::<_, String>(0));
            for (index, column_values) in values.iter_mut().enumerate() {
                column_values.push(row.try_get(index + 1)?);
            }
        }
        let arrays = unique
            .iter()
            .zip(values)
            .map(|(column, values)| {
                let ty = types::arrow_type(&meta.columns[column].ty)?;
                Ok(cast(&StringArray::from(values), &ty)?)
            })
            .collect::<Result<Vec<_>>>()?;
        let row_keys = row_keys(&arrays)?;
        let selected = row_keys
            .iter()
            .zip(ctids)
            .filter_map(|(key, ctid)| incoming.contains(key).then_some(ctid))
            .collect::<Vec<_>>();
        matches.insert(name.clone(), selected);
    }
    Ok(matches)
}

async fn commit_insert(
    table: &Table,
    client: &mut Client,
    expected_meta: &TableMeta,
    expected_snapshot: i64,
    file: &FileInfo,
) -> Result<LoadReport> {
    let tx = client.transaction().await?;
    tx.batch_execute("LOCK TABLE ducklake.ducklake_snapshot IN SHARE ROW EXCLUSIVE MODE")
        .await
        .context("locking DuckLake snapshots for insert")?;
    let meta = table_meta(&tx, &table.schema, &table.table).await?;
    if &meta != expected_meta {
        fail!(CatalogChanged, "table metadata changed while writing");
    }
    let (previous, version, next_catalog, next_file) = latest_snapshot(&tx).await?;
    if previous < expected_snapshot {
        fail!(CatalogChanged, "DuckLake snapshot moved backwards");
    }
    let snapshot = previous.checked_add(1).context("snapshot ID overflow")?;
    let stats_row = tx
        .query_opt(
            "SELECT next_row_id FROM ducklake.ducklake_table_stats WHERE table_id = $1",
            &[&meta.table_id],
        )
        .await?;
    let row_id = stats_row
        .as_ref()
        .and_then(|row| row.get::<_, Option<i64>>(0))
        .unwrap_or(0);
    tx.execute(
        "INSERT INTO ducklake.ducklake_snapshot
         (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
         VALUES ($1, now(), $2, $3, $4)",
        &[&snapshot, &version, &next_catalog, &(next_file + 1)],
    )
    .await
    .context("creating insert snapshot")?;
    tx.execute(
        "INSERT INTO ducklake.ducklake_snapshot_changes
         (snapshot_id, changes_made, author, commit_message, commit_extra_info)
         VALUES ($1, $2, $3, $4, NULL)",
        &[
            &snapshot,
            &format!("inserted_into_table:{}", meta.table_id),
            &table.author.as_deref().unwrap_or("dkplus"),
            &format!("insert into {table}"),
        ],
    )
    .await
    .context("recording insert snapshot changes")?;
    append_data_file(&tx, &meta, snapshot, next_file, row_id, file).await?;
    let next_row_id = row_id
        .checked_add(file.rows)
        .context("next DuckLake row ID overflow")?;
    update_table_stats(
        &tx,
        meta.table_id,
        stats_row.is_some(),
        next_row_id,
        &file.stats,
    )
    .await?;
    tx.commit().await.context("committing insert snapshot")?;
    Ok(load_report(snapshot, next_file, &meta, file))
}

#[allow(clippy::too_many_arguments)]
async fn commit_upsert(
    table: &Table,
    client: &mut Client,
    expected_meta: &TableMeta,
    expected_snapshot: i64,
    expected_sources: &[Source],
    expected_inline_names: &[String],
    expected_inline: &HashMap<String, Vec<String>>,
    incoming: &HashSet<Vec<u8>>,
    unique: &[String],
    deletes: &[NewDelete],
    data_file: &FileInfo,
) -> Result<LoadReport> {
    let tx = client.transaction().await?;
    tx.batch_execute("LOCK TABLE ducklake.ducklake_snapshot IN SHARE ROW EXCLUSIVE MODE")
        .await
        .context("locking DuckLake snapshots for upsert")?;
    let meta = table_meta(&tx, &table.schema, &table.table).await?;
    if &meta != expected_meta {
        fail!(CatalogChanged, "table metadata changed while upserting");
    }
    ensure_unpartitioned(&tx, meta.table_id).await?;
    let (previous, version, next_catalog, next_file) = latest_snapshot(&tx).await?;
    if previous != expected_snapshot {
        fail!(
            CatalogChanged,
            "catalog changed while upserting; retry against the latest snapshot"
        );
    }
    if sources(&tx, meta.table_id).await?.as_slice() != expected_sources {
        fail!(
            CatalogChanged,
            "active data or deletion files changed while upserting"
        );
    }
    if inline_names(&tx, meta.table_id).await?.as_slice() != expected_inline_names {
        fail!(
            CatalogChanged,
            "inlined table metadata changed while upserting"
        );
    }
    let current_inline =
        read_inline_matches(&tx, &meta, expected_inline_names, unique, incoming).await?;
    if &current_inline != expected_inline {
        fail!(CatalogChanged, "inlined rows changed while upserting");
    }

    let deleted_rows = deletes.iter().try_fold(0i64, |sum, delete| {
        let old = expected_sources
            .iter()
            .find(|source| source.id == delete.source_id)
            .context("delete references an unknown source")?
            .delete
            .as_ref()
            .map_or(0, |old| old.count);
        let newly_deleted = delete
            .count
            .checked_sub(old)
            .context("replacement delete count is smaller than the existing delete count")?;
        sum.checked_add(newly_deleted)
            .context("deleted row count overflow")
    })?;
    let inline_deleted = expected_inline.values().try_fold(0i64, |sum, rows| {
        sum.checked_add(rows.len() as i64)
            .context("inlined deleted row count overflow")
    })?;
    let retired_rows = deleted_rows
        .checked_add(inline_deleted)
        .context("retired row count overflow")?;

    let stats_row = tx
        .query_opt(
            "SELECT next_row_id FROM ducklake.ducklake_table_stats WHERE table_id = $1",
            &[&meta.table_id],
        )
        .await?;
    let row_id = stats_row
        .as_ref()
        .and_then(|row| row.get::<_, Option<i64>>(0))
        .unwrap_or(0);
    let snapshot = previous.checked_add(1).context("snapshot ID overflow")?;
    let delete_file_count =
        i64::try_from(deletes.len()).context("DuckLake delete file count overflow")?;
    let allocated = 1i64
        .checked_add(delete_file_count)
        .context("DuckLake file ID overflow")?;
    let next_id = next_file
        .checked_add(allocated)
        .context("DuckLake file ID overflow")?;
    tx.execute(
        "INSERT INTO ducklake.ducklake_snapshot
         (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
         VALUES ($1, now(), $2, $3, $4)",
        &[&snapshot, &version, &next_catalog, &next_id],
    )
    .await
    .context("creating upsert snapshot")?;
    tx.execute(
        "INSERT INTO ducklake.ducklake_snapshot_changes
         (snapshot_id, changes_made, author, commit_message, commit_extra_info)
         VALUES ($1, $2, $3, $4, NULL)",
        &[
            &snapshot,
            &if retired_rows > 0 {
                format!(
                    "inserted_into_table:{0},deleted_from_table:{0}",
                    meta.table_id
                )
            } else {
                format!("inserted_into_table:{}", meta.table_id)
            },
            &table.author.as_deref().unwrap_or("dkplus"),
            &format!("upsert into {table}"),
        ],
    )
    .await
    .context("recording upsert snapshot changes")?;

    for (index, delete) in deletes.iter().enumerate() {
        let delete_id = next_file
            .checked_add(1)
            .and_then(|id| id.checked_add(i64::try_from(index).ok()?))
            .context("DuckLake delete file ID overflow")?;
        let old = expected_sources
            .iter()
            .find(|source| source.id == delete.source_id)
            .and_then(|source| source.delete.as_ref());
        if old.is_some() {
            let ended = tx
                .execute(
                    "UPDATE ducklake.ducklake_delete_file SET end_snapshot = $1
                  WHERE table_id = $2 AND data_file_id = $3 AND end_snapshot IS NULL",
                    &[&snapshot, &meta.table_id, &delete.source_id],
                )
                .await
                .context("retiring replaced positional delete file")?;
            if ended != 1 {
                fail!(
                    CatalogChanged,
                    "active positional delete file changed for data file {}",
                    delete.source_id
                );
            }
        }
        tx.execute(
            "INSERT INTO ducklake.ducklake_delete_file
             (delete_file_id, table_id, data_file_id, begin_snapshot, end_snapshot,
              path, path_is_relative, delete_count, format, file_size_bytes, footer_size, encryption_key, partial_max)
             VALUES ($1,$2,$3,$4,NULL,$5,true,$6,'parquet',$7,$8,NULL,NULL)",
            &[
                &delete_id,
                &meta.table_id,
                &delete.source_id,
                &snapshot,
                &delete.name,
                &delete.count,
                &delete.size,
                &delete.footer,
            ],
        )
        .await
        .context("registering positional delete file")?;
    }

    for name in expected_inline_names {
        let rows = expected_inline.get(name).map(Vec::as_slice).unwrap_or(&[]);
        if rows.is_empty() {
            continue;
        }
        let changed = tx
            .execute(
                &format!(
                    "UPDATE ducklake.{} SET end_snapshot = $1
                      WHERE end_snapshot IS NULL AND ctid::text = ANY($2)",
                    identifier(name)?
                ),
                &[&snapshot, &rows],
            )
            .await
            .with_context(|| format!("retiring matching inline rows in {name}"))?;
        if changed != rows.len() as u64 {
            fail!(
                CatalogChanged,
                "inline rows in {name} changed while committing upsert"
            );
        }
    }

    append_data_file(&tx, &meta, snapshot, next_file, row_id, data_file).await?;
    let next_row_id = row_id
        .checked_add(data_file.rows)
        .context("next DuckLake row ID overflow")?;
    update_table_stats(
        &tx,
        meta.table_id,
        stats_row.is_some(),
        next_row_id,
        &data_file.stats,
    )
    .await?;
    tx.commit().await.context("committing upsert snapshot")?;
    let mut report = load_report(snapshot, next_file, &meta, data_file);
    report.retired_rows = retired_rows;
    Ok(report)
}

async fn append_data_file(
    tx: &impl GenericClient,
    meta: &TableMeta,
    snapshot: i64,
    id: i64,
    row_id: i64,
    file: &FileInfo,
) -> Result<()> {
    tx.execute(
        "INSERT INTO ducklake.ducklake_data_file
         (data_file_id, table_id, begin_snapshot, end_snapshot, file_order, path,
          path_is_relative, file_format, record_count, file_size_bytes, footer_size,
          row_id_start, partition_id, encryption_key, mapping_id, partial_max)
         VALUES ($1,$2,$3,NULL,NULL,$4,true,'parquet',$5,$6,$7,$8,NULL,NULL,NULL,NULL)",
        &[
            &id,
            &meta.table_id,
            &snapshot,
            &file.name,
            &file.rows,
            &file.size,
            &file.footer,
            &row_id,
        ],
    )
    .await
    .context("registering DuckLake data file")?;
    for stat in &file.stats {
        tx.execute(
            "INSERT INTO ducklake.ducklake_file_column_stats
             (data_file_id,table_id,column_id,column_size_bytes,value_count,null_count,
              min_value,max_value,contains_nan,extra_stats)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,NULL)",
            &[
                &id,
                &meta.table_id,
                &stat.column_id,
                &stat.size_bytes,
                &stat.value_count,
                &stat.null_count,
                &stat.min,
                &stat.max,
                &stat.contains_nan,
            ],
        )
        .await
        .context("registering DuckLake file statistics")?;
    }
    Ok(())
}

fn location(meta: &TableMeta, path: &str, relative: bool) -> String {
    if relative {
        format!("{}{path}", meta.root)
    } else {
        path.to_owned()
    }
}

async fn read_object(table: &Table, path: &str) -> Result<Bytes> {
    let (store, key) = object_store_for(&table.cfg, path)?;
    Ok(store.get(&ObjPath::from(key)).await?.bytes().await?)
}

async fn cleanup_if_unreferenced(
    table: &Table,
    client: &Client,
    meta: &TableMeta,
    store: &dyn ObjectStore,
    path: &ObjPath,
) {
    cleanup_paths(table, client, meta, store, std::slice::from_ref(path)).await;
}

async fn cleanup_paths(
    table: &Table,
    client: &Client,
    meta: &TableMeta,
    store: &dyn ObjectStore,
    paths: &[ObjPath],
) {
    for path in paths {
        let name = path.filename().unwrap_or_default();
        let referenced = client
            .query_one(
                "SELECT
                   EXISTS (SELECT 1 FROM ducklake.ducklake_data_file
                            WHERE table_id = $1 AND path = $2)
                   OR EXISTS (SELECT 1 FROM ducklake.ducklake_delete_file
                               WHERE table_id = $1 AND path = $2)",
                &[&meta.table_id, &name],
            )
            .await;
        match referenced {
            Ok(row) if !row.get::<_, bool>(0) => {
                if let Err(error) = store.delete(path).await {
                    (table.cfg.log)(&format!(
                        "could not clean up unreferenced write file {path}: {error}"
                    ));
                }
            }
            Ok(_) => {}
            Err(error) => (table.cfg.log)(&format!(
                "could not determine whether failed write file {path} was committed; leaving it in place: {error}"
            )),
        }
    }
}

fn load_report(snapshot: i64, id: i64, meta: &TableMeta, file: &FileInfo) -> LoadReport {
    LoadReport {
        snapshot_id: snapshot,
        data_file_id: id,
        path: format!("{}{}", meta.root, file.name),
        rows: file.rows,
        file_size_bytes: file.size,
        retired_files: Vec::new(),
        retired_rows: 0,
        rewritten_files: Vec::new(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn positional_filter_keeps_every_repeated_match_and_skips_old_deletes() {
        let a = vec![1];
        let b = vec![2];
        let keys = vec![a.clone(), b, a.clone(), a.clone(), a.clone()];
        let incoming = HashSet::from([a]);
        let existing = [1, 3];
        assert_eq!(
            find_new_delete_positions(&keys, &incoming, &existing, 0),
            [0, 2, 4]
        );
    }

    #[test]
    fn positional_delete_parquet_has_ducklake_fields() {
        let bytes = write_delete_file("/table/file.parquet", &[0, 3, 5]).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))
            .unwrap()
            .build()
            .unwrap();
        let batches = reader.collect::<std::result::Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches.len(), 1);
        assert!(validate_delete_schema(&batches[0]).is_ok());
        assert_eq!(
            batches[0]
                .column_by_name("pos")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values(),
            &[0, 3, 5]
        );
    }

    #[test]
    fn malformed_positional_delete_batches_are_rejected() {
        let wrong_name = RecordBatch::try_from_iter([(
            "position",
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        )])
        .unwrap();
        let wrong_type = RecordBatch::try_from_iter([(
            "pos",
            Arc::new(datafusion::arrow::array::Int32Array::from(vec![1])) as ArrayRef,
        )])
        .unwrap();
        let extra = RecordBatch::try_from_iter([
            ("pos", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
            ("other", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        ])
        .unwrap();
        let duplicate = RecordBatch::try_from_iter([
            ("pos", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
            ("pos", Arc::new(Int64Array::from(vec![1])) as ArrayRef),
        ])
        .unwrap();
        assert!(validate_delete_schema(&wrong_name).is_err());
        assert!(validate_delete_schema(&wrong_type).is_err());
        assert!(validate_delete_schema(&extra).is_err());
        assert!(validate_delete_schema(&duplicate).is_err());
    }

    #[test]
    fn floating_point_keys_follow_duckdb_zero_and_nan_equality() {
        let arrays: Vec<ArrayRef> = vec![Arc::new(Float64Array::from(vec![
            0.0,
            -0.0,
            f64::NAN,
            f64::from_bits(0xfff8_0000_0000_0001),
            1.0,
        ]))];
        let keys = row_keys(&arrays).unwrap();
        assert_eq!(keys[0], keys[1]);
        assert_eq!(keys[2], keys[3]);
        assert_ne!(keys[0], keys[4]);
    }
}
