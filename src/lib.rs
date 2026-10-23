#![doc = include_str!("../README.md")]

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use config::identifier;
pub use config::{Config, Log, Storage, Table, TableBuilder, TimeType};
use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::datatypes::{
    DataType, Date32Type, Field, Schema, TimeUnit, TimestampMicrosecondType,
};
use datafusion::parquet::arrow::arrow_writer::ArrowWriterOptions;
use datafusion::parquet::arrow::{ArrowWriter, PARQUET_FIELD_ID_META_KEY};
use datafusion::parquet::basic::{Compression, ZstdLevel};
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::parquet::file::properties::WriterProperties;
use object_store::path::Path as ObjPath;
use object_store::{ObjectStore, ObjectStoreExt, PutPayload};
use rustls::{ClientConfig, RootCertStore};
use std::collections::HashMap;
use std::sync::Arc;
use tokio_postgres::error::SqlState;
use tokio_postgres::{Client, GenericClient, IsolationLevel};
use tokio_postgres_rustls::MakeRustlsConnect;
use uuid::Uuid;

pub mod config;
pub mod maintenance;
pub mod parquet;
pub mod repack;
mod types;

const MAX_COMMIT_ATTEMPTS: u32 = 6;

#[derive(Debug)]
pub struct LoadReport {
    pub snapshot_id: i64,
    pub data_file_id: i64,
    pub path: String,
    pub rows: i64,
    pub file_size_bytes: i64,
    /// Files retired via `end_snapshot`.
    pub retired_files: Vec<i64>,
    pub retired_rows: i64,
    /// Retired files rewritten to retain their rows outside the replacement window.
    pub rewritten_files: Vec<i64>,
}

#[derive(Debug)]
pub struct MergeDiffReport {
    pub target: LoadReport,
    /// The appended history file, if any values changed.
    pub diff: Option<LoadReport>,
}

#[derive(PartialEq)]
struct Column {
    id: i64,
    ty: String,
}

#[derive(PartialEq)]
struct TableMeta {
    table_id: i64,
    /// Absolute directory of the table, ending in `/`.
    root: String,
    columns: HashMap<String, Column>,
}

pub(crate) struct FileColumnStats {
    column_id: i64,
    ty: String,
    size_bytes: i64,
    value_count: i64,
    null_count: i64,
    min: Option<String>,
    max: Option<String>,
    contains_nan: Option<bool>,
}

pub(crate) struct FileInfo {
    pub(crate) name: String,
    pub(crate) rows: i64,
    pub(crate) size: i64,
    pub(crate) footer: i64,
    pub(crate) stats: Vec<FileColumnStats>,
}

pub async fn connect(cfg: &Config) -> Result<Client> {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let tls = MakeRustlsConnect::new(
        ClientConfig::builder_with_provider(
            Arc::new(rustls::crypto::aws_lc_rs::default_provider()),
        )
        .with_safe_default_protocol_versions()?
        .with_root_certificates(roots)
        .with_no_client_auth(),
    );
    let (client, conn) = cfg
        .postgres
        .connect(tls)
        .await
        .context("connecting to postgres")?;
    let log = cfg.log.clone();
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            log(&format!("postgres connection error: {e}"));
        }
    });
    Ok(client)
}

fn join_dir(base: &str, rel: &str) -> String {
    let mut out = base.to_string();
    if !out.ends_with('/') {
        out.push('/');
    }
    out.push_str(rel);
    if !out.ends_with('/') {
        out.push('/');
    }
    out
}

async fn has_inlined_rows(
    c: &impl GenericClient,
    table_id: i64,
    table: &Table,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<bool> {
    let tables = c
        .query(
            "SELECT table_name FROM ducklake.ducklake_inlined_data_tables WHERE table_id = $1",
            &[&table_id],
        )
        .await
        .context("reading inlined data tables")?;
    for r in tables {
        let name: String = r.get(0);
        let has_rows: bool = c
            .query_one(
                &format!(
                    "SELECT EXISTS (SELECT 1 FROM ducklake.\"{}\" WHERE end_snapshot IS NULL
                       AND {}::{} >= $1::text::{} AND {}::{} < $2::text::{})",
                    name.replace('"', "\"\""),
                    identifier(&table.time_column)?,
                    table.time_type.sql(),
                    table.time_type.sql(),
                    identifier(&table.time_column)?,
                    table.time_type.sql(),
                    table.time_type.sql()
                ),
                &[&table.bound(start), &table.bound(end)],
            )
            .await
            .with_context(|| format!("checking inlined rows in {name}"))?
            .get(0);
        if has_rows {
            return Ok(true);
        }
    }
    Ok(false)
}

async fn table_meta(c: &impl GenericClient, schema: &str, table: &str) -> Result<TableMeta> {
    let row = c
        .query_opt(
            "SELECT t.table_id, t.path, t.path_is_relative, s.path, s.path_is_relative,
                    (SELECT value FROM ducklake.ducklake_metadata
                      WHERE key = 'data_path' AND scope IS NULL LIMIT 1)
               FROM ducklake.ducklake_table t
               JOIN ducklake.ducklake_schema s USING (schema_id)
              WHERE s.schema_name = $1 AND t.table_name = $2
                AND t.end_snapshot IS NULL AND s.end_snapshot IS NULL",
            &[&schema, &table],
        )
        .await
        .context("looking up ducklake table")?
        .ok_or_else(|| anyhow!("ducklake table {schema}.{table} not found"))?;
    let table_id: i64 = row.get(0);
    let t_path: String = row.get(1);
    let t_rel: Option<bool> = row.get(2);
    let s_path: String = row.get(3);
    let s_rel: Option<bool> = row.get(4);
    let data_path: Option<String> = row.get(5);
    let root = catalog_table_root(&t_path, t_rel, &s_path, s_rel, data_path.as_deref())?;
    let partitioned: bool = c
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM ducklake.ducklake_partition_info
                             WHERE table_id = $1 AND end_snapshot IS NULL)",
            &[&table_id],
        )
        .await?
        .get(0);
    if partitioned {
        bail!("{schema}.{table} is partitioned; partitioned registration isn't supported");
    }

    let columns = c
        .query(
            "SELECT column_name, column_id, column_type FROM ducklake.ducklake_column
              WHERE table_id = $1 AND end_snapshot IS NULL AND parent_column IS NULL",
            &[&table_id],
        )
        .await
        .context("reading ducklake columns")?
        .into_iter()
        .map(|r| {
            (
                r.get::<_, String>(0),
                Column {
                    id: r.get(1),
                    ty: r.get(2),
                },
            )
        })
        .collect();
    Ok(TableMeta {
        table_id,
        root,
        columns,
    })
}

fn catalog_table_root(
    table_path: &str,
    table_relative: Option<bool>,
    schema_path: &str,
    schema_relative: Option<bool>,
    data_path: Option<&str>,
) -> Result<String> {
    if table_relative != Some(true) {
        return Ok(join_dir(table_path, ""));
    }
    let schema_dir = if schema_relative == Some(true) {
        join_dir(data_path.context("catalog has no data_path")?, schema_path)
    } else {
        join_dir(schema_path, "")
    };
    Ok(join_dir(&schema_dir, table_path))
}

async fn table_tags(c: &impl GenericClient, table_id: i64) -> Result<HashMap<String, String>> {
    let rows = c
        .query(
            "SELECT tag.key, tag.value
               FROM ducklake.ducklake_tag tag
               CROSS JOIN LATERAL (
                   SELECT max(snapshot_id) AS snapshot_id
                     FROM ducklake.ducklake_snapshot
               ) latest
              WHERE tag.object_id = $1
                AND tag.key IN (
                    'dkplus.time_column', 'dkplus.timezone', 'dkplus.row_group_col',
                    'dkplus.sort_cols', 'dkplus.unique_by_cols', 'dkplus.dedup_timestamp'
                )
                AND tag.begin_snapshot <= latest.snapshot_id
                AND (tag.end_snapshot IS NULL OR tag.end_snapshot > latest.snapshot_id)",
            &[&table_id],
        )
        .await
        .context("reading active DuckLake table tags")?;
    let mut tags = HashMap::new();
    for row in rows {
        let key: String = row.get(0);
        let value: String = row.get(1);
        if tags.insert(key.clone(), value).is_some() {
            bail!("multiple active DuckLake tags for key {key:?}");
        }
    }
    Ok(tags)
}

const TAG_TIME_COLUMN: &str = "dkplus.time_column";
const TAG_TIMEZONE: &str = "dkplus.timezone";
const TAG_ROW_GROUP_COL: &str = "dkplus.row_group_col";
const TAG_SORT_COLS: &str = "dkplus.sort_cols";
const TAG_UNIQUE_BY_COLS: &str = "dkplus.unique_by_cols";
const TAG_DEDUP_TIMESTAMP: &str = "dkplus.dedup_timestamp";

fn tag_value<T: serde::de::DeserializeOwned>(
    tags: &HashMap<String, String>,
    key: &str,
) -> Result<Option<T>> {
    tags.get(key)
        .map(|value| {
            serde_json::from_str(value).with_context(|| format!("decoding DuckLake tag {key:?}"))
        })
        .transpose()
}

fn overridden_tag<T: serde::de::DeserializeOwned>(
    explicit: Option<T>,
    tags: &HashMap<String, String>,
    key: &str,
) -> Result<Option<T>> {
    match explicit {
        Some(value) => Ok(Some(value)),
        None => tag_value(tags, key),
    }
}

async fn column_timezone_tags(
    c: &impl GenericClient,
    table_id: i64,
) -> Result<HashMap<i64, String>> {
    let rows = c
        .query(
            "SELECT tag.column_id, tag.value
               FROM ducklake.ducklake_column_tag tag
               CROSS JOIN LATERAL (
                   SELECT max(snapshot_id) AS snapshot_id
                     FROM ducklake.ducklake_snapshot
               ) latest
              WHERE tag.table_id = $1 AND tag.key = 'dkplus.timezone'
                AND tag.begin_snapshot <= latest.snapshot_id
                AND (tag.end_snapshot IS NULL OR tag.end_snapshot > latest.snapshot_id)",
            &[&table_id],
        )
        .await
        .context("reading active DuckLake column timezone tags")?;
    let mut tags = HashMap::new();
    for row in rows {
        let column_id: i64 = row.get(0);
        if tags.insert(column_id, row.get(1)).is_some() {
            bail!("multiple active DuckLake timezone tags for column {column_id}");
        }
    }
    Ok(tags)
}

pub(crate) async fn resolve_table(builder: TableBuilder) -> Result<Table> {
    if builder.schema.is_empty() || builder.table.is_empty() || builder.table.contains('.') {
        bail!("table name must have the form schema.table");
    }
    identifier(&builder.schema)?;
    identifier(&builder.table)?;
    let mut client = connect(&builder.cfg).await?;
    let tx = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;
    let meta = table_meta(&tx, &builder.schema, &builder.table).await?;
    let tags = table_tags(&tx, meta.table_id).await?;
    let column_tags = column_timezone_tags(&tx, meta.table_id).await?;
    tx.commit()
        .await
        .context("finishing table policy read transaction")?;
    resolve_table_settings(builder, &meta, &tags, &column_tags)
}

fn resolve_table_settings(
    builder: TableBuilder,
    meta: &TableMeta,
    tags: &HashMap<String, String>,
    column_tags: &HashMap<i64, String>,
) -> Result<Table> {
    let time_column = overridden_tag(builder.time_column, tags, TAG_TIME_COLUMN)?
        .context("missing required table setting \"time_column\"")?;
    let time_col = meta
        .columns
        .get(&time_column)
        .with_context(|| format!("table has no configured time column {time_column:?}"))?;
    let timezone = match builder.timezone {
        Some(timezone) => timezone,
        None => match column_tags.get(&time_col.id) {
            Some(value) => Some(
                serde_json::from_str::<String>(value)
                    .context("decoding DuckLake column tag \"dkplus.timezone\"")?,
            ),
            None => tag_value::<String>(tags, TAG_TIMEZONE)?,
        }
        .map(|zone| {
            zone.parse::<chrono_tz::Tz>()
                .with_context(|| format!("invalid timezone tag {zone:?}"))
        })
        .transpose()?
        .context("missing required table setting \"timezone\"")?,
    };
    let sort_cols = overridden_tag(builder.sort_cols, tags, TAG_SORT_COLS)?
        .context("missing required table setting \"sort_cols\"")?;
    let row_group_col =
        overridden_tag(builder.row_group_col, tags, TAG_ROW_GROUP_COL)?.unwrap_or(None);
    let unique_by_cols =
        overridden_tag(builder.unique_by_cols, tags, TAG_UNIQUE_BY_COLS)?.unwrap_or(None);
    let dedup_timestamp =
        overridden_tag(builder.dedup_timestamp, tags, TAG_DEDUP_TIMESTAMP)?.unwrap_or(None);
    let time_type = match types::arrow_type(&time_col.ty)? {
        DataType::Date32 => TimeType::Date,
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => TimeType::TimestampTz,
        DataType::Timestamp(TimeUnit::Microsecond, None) => TimeType::Timestamp,
        _ => {
            bail!("configured time column {time_column:?} must be a date or microsecond timestamp")
        }
    };
    let table = Table {
        cfg: builder.cfg,
        schema: builder.schema,
        table: builder.table.clone(),
        time_column,
        time_type,
        timezone,
        row_group_col,
        sort_cols,
        unique_by_cols,
        dedup_timestamp,
        author: builder.author,
        lock_namespace: builder.lock_namespace.unwrap_or(builder.table),
    };
    table.validate()?;
    for name in std::iter::once(table.time_column.as_str())
        .chain(table.row_group_col.as_deref())
        .chain(table.sort_cols.iter().map(String::as_str))
        .chain(
            table
                .unique_by_cols
                .iter()
                .flat_map(|cols| cols.iter().map(String::as_str)),
        )
        .chain(table.dedup_timestamp.as_deref())
    {
        if !meta.columns.contains_key(name) {
            bail!("configured column {name:?} is not in table");
        }
    }
    if let Some(timestamp) = &table.dedup_timestamp
        && !matches!(
            types::arrow_type(&meta.columns[timestamp].ty)?,
            DataType::Timestamp(TimeUnit::Microsecond, _)
        )
    {
        bail!("dedup timestamp column {timestamp:?} must be a microsecond timestamp");
    }
    if let Some(unique) = &table.unique_by_cols {
        for name in unique {
            if !meta.columns.contains_key(name) {
                bail!("configured unique column {name:?} is not in table");
            }
        }
    }
    Ok(table)
}

fn arrow_type_matches(catalog_ty: &str, dt: &DataType) -> bool {
    types::matches(catalog_ty, dt)
}

/// Catalog columns for each batch field, in batch order. Every catalog column must be present.
fn resolve_columns<'a>(meta: &'a TableMeta, schema: &Schema) -> Result<Vec<&'a Column>> {
    let mut out = Vec::with_capacity(schema.fields().len());
    for f in schema.fields() {
        let col = meta
            .columns
            .get(f.name())
            .ok_or_else(|| anyhow!("column {} not in ducklake table", f.name()))?;
        if !arrow_type_matches(&col.ty, f.data_type()) {
            bail!(
                "column {} is {} in ducklake but {} in batch",
                f.name(),
                col.ty,
                f.data_type()
            );
        }
        out.push(col);
    }
    if out.len() != meta.columns.len() {
        bail!(
            "batch has {} columns but ducklake table has {}",
            out.len(),
            meta.columns.len()
        );
    }
    Ok(out)
}

fn write_parquet(batches: &[RecordBatch], cols: &[&Column]) -> Result<(Vec<u8>, ParquetMetaData)> {
    let src = batches[0].schema();
    // DuckLake maps parquet columns to catalog columns by field_id.
    let fields: Vec<Field> = src
        .fields()
        .iter()
        .zip(cols)
        .map(|(f, c)| {
            f.as_ref().clone().with_metadata(HashMap::from([(
                PARQUET_FIELD_ID_META_KEY.to_string(),
                c.id.to_string(),
            )]))
        })
        .collect();
    let schema = Arc::new(Schema::new(fields));
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(3)?))
        .build();
    let opts = ArrowWriterOptions::new()
        .with_properties(props)
        .with_skip_arrow_metadata(true);
    let mut buf = Vec::new();
    let mut w = ArrowWriter::try_new_with_options(&mut buf, schema.clone(), opts)?;
    for b in batches {
        w.write(&RecordBatch::try_new(schema.clone(), b.columns().to_vec())?)?;
    }
    let meta = w.close()?;
    Ok((buf, meta))
}

fn column_bounds(
    batches: &[RecordBatch],
    idx: usize,
) -> Result<(Option<String>, Option<String>, Option<bool>)> {
    types::bounds(batches, idx)
}

fn file_info(
    name: String,
    buf: &[u8],
    pq: &ParquetMetaData,
    batches: &[RecordBatch],
    cols: &[&Column],
) -> Result<FileInfo> {
    let n = buf.len();
    let footer = u32::from_le_bytes(buf[n - 8..n - 4].try_into()?) as i64;
    let rows: i64 = batches.iter().map(|b| b.num_rows() as i64).sum();
    let mut stats = Vec::with_capacity(cols.len());
    for (i, c) in cols.iter().enumerate() {
        let size_bytes = pq
            .row_groups()
            .iter()
            .map(|rg| rg.column(i).compressed_size())
            .sum();
        let null_count = batches
            .iter()
            .map(|b| b.column(i).null_count() as i64)
            .sum();
        let (min, max, contains_nan) = column_bounds(batches, i)?;
        stats.push(FileColumnStats {
            column_id: c.id,
            ty: c.ty.clone(),
            size_bytes,
            value_count: rows,
            null_count,
            min,
            max,
            contains_nan,
        });
    }
    Ok(FileInfo {
        name,
        rows,
        size: n as i64,
        footer,
        stats,
    })
}

/// Splits `az://container/prefix/` into a store for the container and the key prefix.
fn object_store_for(cfg: &Config, root: &str) -> Result<(Arc<dyn ObjectStore>, String)> {
    cfg.storage.resolve(root)
}

fn cmp_bound(ty: &str, a: &str, b: &str) -> Option<std::cmp::Ordering> {
    types::compare(ty, a, b)
}

/// Widen an existing table-level bound with a file bound. Unknown stays unknown.
fn widen(ty: &str, old: Option<String>, new: &Option<String>, want_max: bool) -> Option<String> {
    let (old, new) = (old?, new.as_ref()?);
    match cmp_bound(ty, &old, new)? {
        std::cmp::Ordering::Less if want_max => Some(new.clone()),
        std::cmp::Ordering::Greater if !want_max => Some(new.clone()),
        _ => Some(old),
    }
}

enum Attempt {
    Done(LoadReport),
    NeedsRewrite,
}

async fn try_register(
    client: &mut Client,
    table: &Table,
    meta: &TableMeta,
    utc_col: i64,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    file: &FileInfo,
) -> Result<Attempt> {
    let tx = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .start()
        .await?;
    let table_id = meta.table_id;

    let snap = tx
        .query_one(
            "SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
               FROM ducklake.ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1",
            &[],
        )
        .await
        .context("reading latest snapshot")?;
    let (prev_snap, schema_version, next_catalog_id, next_file_id): (i64, i64, i64, i64) =
        (snap.get(0), snap.get(1), snap.get(2), snap.get(3));

    let existing = tx
        .query(
            &format!(
                "SELECT f.data_file_id, f.record_count,
                    s.min_value::{ty} >= $3::text::{ty} AND s.max_value::{ty} < $4::text::{ty},
                    s.min_value::{ty} < $4::text::{ty} AND s.max_value::{ty} >= $3::text::{ty}
               FROM ducklake.ducklake_data_file f
               LEFT JOIN ducklake.ducklake_file_column_stats s
                 ON s.data_file_id = f.data_file_id AND s.table_id = f.table_id AND s.column_id = $2
              WHERE f.table_id = $1 AND f.end_snapshot IS NULL",
                ty = table.time_type.sql()
            ),
            &[&table_id, &utc_col, &table.bound(start), &table.bound(end)],
        )
        .await
        .context("reading live data files")?;
    let mut retire = Vec::new();
    let mut retired_rows = 0i64;
    for r in &existing {
        let inside: Option<bool> = r.get(2);
        let overlaps: Option<bool> = r.get(3);
        match (inside, overlaps) {
            (_, Some(false)) => {}
            (Some(true), _) => {
                retire.push(r.get::<_, i64>(0));
                retired_rows += r.get::<_, Option<i64>>(1).unwrap_or(0);
            }
            // Straddles the window (e.g. after compaction) or has no stats.
            _ => return Ok(Attempt::NeedsRewrite),
        }
    }

    if has_inlined_rows(&tx, table_id, table, start, end).await? {
        return Ok(Attempt::NeedsRewrite);
    }

    let snapshot_id = prev_snap + 1;
    let data_file_id = next_file_id;
    tx.execute(
        "INSERT INTO ducklake.ducklake_snapshot
           (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
         VALUES ($1, now(), $2, $3, $4)",
        &[
            &snapshot_id,
            &schema_version,
            &next_catalog_id,
            &(next_file_id + 1),
        ],
    )
    .await
    .context("inserting snapshot")?;

    let mut changes = format!("inserted_into_table:{table_id}");
    if !retire.is_empty() {
        changes.push_str(&format!(",deleted_from_table:{table_id}"));
    }
    let message = format!("{table} {}", start.format("%Y-%m-%dT%H:%MZ"));
    tx.execute(
        "INSERT INTO ducklake.ducklake_snapshot_changes
           (snapshot_id, changes_made, author, commit_message, commit_extra_info)
         VALUES ($1, $2, $3, $4, NULL)",
        &[
            &snapshot_id,
            &changes,
            &table.author_for("writer", start, end),
            &message,
        ],
    )
    .await
    .context("inserting snapshot changes")?;

    if !retire.is_empty() {
        tx.execute(
            "UPDATE ducklake.ducklake_data_file SET end_snapshot = $1
              WHERE end_snapshot IS NULL AND data_file_id = ANY($2)",
            &[&snapshot_id, &retire],
        )
        .await
        .context("retiring data files")?;
        tx.execute(
            "UPDATE ducklake.ducklake_delete_file SET end_snapshot = $1
              WHERE end_snapshot IS NULL AND data_file_id = ANY($2)",
            &[&snapshot_id, &retire],
        )
        .await
        .context("retiring delete files")?;
    }

    let stats_row = tx
        .query_opt(
            "SELECT next_row_id FROM ducklake.ducklake_table_stats WHERE table_id = $1",
            &[&table_id],
        )
        .await?;
    let row_id_start: i64 = stats_row
        .as_ref()
        .and_then(|r| r.get::<_, Option<i64>>(0))
        .unwrap_or(0);

    tx.execute(
        "INSERT INTO ducklake.ducklake_data_file
           (data_file_id, table_id, begin_snapshot, end_snapshot, file_order, path,
            path_is_relative, file_format, record_count, file_size_bytes, footer_size,
            row_id_start, partition_id, encryption_key, mapping_id, partial_max)
         VALUES ($1, $2, $3, NULL, NULL, $4, true, 'parquet', $5, $6, $7, $8, NULL, NULL, NULL, NULL)",
        &[
            &data_file_id,
            &table_id,
            &snapshot_id,
            &file.name,
            &file.rows,
            &file.size,
            &file.footer,
            &row_id_start,
        ],
    )
    .await
    .context("inserting data file")?;

    for s in &file.stats {
        tx.execute(
            "INSERT INTO ducklake.ducklake_file_column_stats
               (data_file_id, table_id, column_id, column_size_bytes, value_count, null_count,
                min_value, max_value, contains_nan, extra_stats)
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL)",
            &[
                &data_file_id,
                &table_id,
                &s.column_id,
                &s.size_bytes,
                &s.value_count,
                &s.null_count,
                &s.min,
                &s.max,
                &s.contains_nan,
            ],
        )
        .await
        .context("inserting file column stats")?;
    }

    update_table_stats(
        &tx,
        table_id,
        stats_row.is_some(),
        row_id_start + file.rows,
        &file.stats,
    )
    .await?;

    tx.commit().await.context("committing ducklake snapshot")?;
    Ok(Attempt::Done(LoadReport {
        snapshot_id,
        data_file_id,
        path: format!("{}{}", meta.root, file.name),
        rows: file.rows,
        file_size_bytes: file.size,
        retired_files: retire,
        retired_rows,
        rewritten_files: Vec::new(),
    }))
}

pub(crate) async fn update_table_stats(
    tx: &impl GenericClient,
    table_id: i64,
    has_row: bool,
    next_row_id: i64,
    stats: &[FileColumnStats],
) -> Result<()> {
    let totals = tx
        .query_one(
            "SELECT
               (SELECT COALESCE(SUM(record_count), 0)::bigint FROM ducklake.ducklake_data_file
                 WHERE table_id = $1 AND end_snapshot IS NULL)
             - (SELECT COALESCE(SUM(delete_count), 0)::bigint FROM ducklake.ducklake_delete_file
                 WHERE table_id = $1 AND end_snapshot IS NULL),
               (SELECT COALESCE(SUM(file_size_bytes), 0)::bigint FROM ducklake.ducklake_data_file
                 WHERE table_id = $1 AND end_snapshot IS NULL)",
            &[&table_id],
        )
        .await
        .context("summing table stats")?;
    let inlined_count = inlined_record_count(tx, table_id).await?;
    let (record_count, size): (i64, i64) = (totals.get::<_, i64>(0) + inlined_count, totals.get(1));
    if has_row {
        tx.execute(
            "UPDATE ducklake.ducklake_table_stats
                SET record_count = $2, next_row_id = $3, file_size_bytes = $4
              WHERE table_id = $1",
            &[&table_id, &record_count, &next_row_id, &size],
        )
        .await?;
    } else {
        tx.execute(
            "INSERT INTO ducklake.ducklake_table_stats (table_id, record_count, next_row_id, file_size_bytes)
             VALUES ($1, $2, $3, $4)",
            &[&table_id, &record_count, &next_row_id, &size],
        )
        .await?;
    }

    async fn inlined_record_count(tx: &impl GenericClient, table_id: i64) -> Result<i64> {
        let tables = tx
            .query(
                "SELECT table_name FROM ducklake.ducklake_inlined_data_tables WHERE table_id = $1",
                &[&table_id],
            )
            .await
            .context("reading inlined data tables for statistics")?;
        let mut count = 0i64;
        for row in tables {
            let name: String = row.get(0);
            let n: i64 = tx
                .query_one(
                    &format!(
                        "SELECT count(*) FROM ducklake.{} WHERE end_snapshot IS NULL",
                        identifier(&name)?
                    ),
                    &[],
                )
                .await
                .with_context(|| format!("counting current inlined rows in {name}"))?
                .get(0);
            count += n;
        }
        Ok(count)
    }

    #[derive(Clone)]
    struct TableColumnStats {
        contains_null: Option<bool>,
        contains_nan: Option<bool>,
        min: Option<String>,
        max: Option<String>,
    }

    let existing: HashMap<i64, TableColumnStats> = tx
        .query(
            "SELECT column_id, contains_null, contains_nan, min_value, max_value
               FROM ducklake.ducklake_table_column_stats WHERE table_id = $1",
            &[&table_id],
        )
        .await?
        .into_iter()
        .map(|r| {
            (
                r.get(0),
                TableColumnStats {
                    contains_null: r.get(1),
                    contains_nan: r.get(2),
                    min: r.get(3),
                    max: r.get(4),
                },
            )
        })
        .collect();
    tx.execute(
        "DELETE FROM ducklake.ducklake_table_column_stats WHERE table_id = $1",
        &[&table_id],
    )
    .await?;
    for s in stats {
        let file_null = s.null_count > 0;
        let (contains_null, contains_nan, min, max) = match existing.get(&s.column_id).cloned() {
            None if has_row => (None, None, None, None),
            None => (
                Some(file_null),
                s.contains_nan,
                s.min.clone(),
                s.max.clone(),
            ),
            Some(TableColumnStats {
                contains_null: cn,
                contains_nan: cnan,
                min: lo,
                max: hi,
            }) => (
                cn.map(|v| v || file_null),
                match (cnan, s.contains_nan) {
                    (Some(a), Some(b)) => Some(a || b),
                    _ => None,
                },
                widen(&s.ty, lo, &s.min, false),
                widen(&s.ty, hi, &s.max, true),
            ),
        };
        tx.execute(
            "INSERT INTO ducklake.ducklake_table_column_stats
               (table_id, column_id, contains_null, contains_nan, min_value, max_value, extra_stats)
             VALUES ($1, $2, $3, $4, $5, $6, NULL)",
            &[
                &table_id,
                &s.column_id,
                &contains_null,
                &contains_nan,
                &min,
                &max,
            ],
        )
        .await
        .context("writing table column stats")?;
    }
    Ok(())
}

fn is_commit_conflict(e: &anyhow::Error) -> bool {
    e.chain()
        .filter_map(|c| c.downcast_ref::<tokio_postgres::Error>())
        .filter_map(|pe| pe.code())
        .any(|code| {
            *code == SqlState::UNIQUE_VIOLATION || *code == SqlState::T_R_SERIALIZATION_FAILURE
        })
}

impl Table {
    /// Merge by unique keys and latest dedup timestamp, archiving changed older rows.
    /// `diff_table` names an existing, same-schema table in this table's schema,
    /// or a schema-qualified table. Both writes share one catalog snapshot.
    pub async fn merge_diff(
        &self,
        batches: Vec<RecordBatch>,
        diff_table: &str,
    ) -> Result<MergeDiffReport> {
        repack::merge_diff::run(self, batches, diff_table).await
    }

    fn replacement_bounds(
        &self,
        batches: &[RecordBatch],
    ) -> Result<(DateTime<Utc>, DateTime<Utc>)> {
        let mut minimum = None::<i64>;
        let mut maximum = None::<i64>;
        for batch in batches {
            if batch.num_rows() == 0 {
                continue;
            }
            let column = batch.column(batch.schema().index_of(&self.time_column)?);
            if column.null_count() != 0 {
                bail!("replacement time column must not contain nulls");
            }
            let (lo, hi) = match (self.time_type, column.data_type()) {
                (TimeType::Date, DataType::Date32) => {
                    let values = column.as_primitive::<Date32Type>();
                    (
                        datafusion::arrow::compute::min(values).map(i64::from),
                        datafusion::arrow::compute::max(values).map(i64::from),
                    )
                }
                (TimeType::TimestampTz, DataType::Timestamp(TimeUnit::Microsecond, Some(_)))
                | (TimeType::Timestamp, DataType::Timestamp(TimeUnit::Microsecond, None)) => {
                    let values = column.as_primitive::<TimestampMicrosecondType>();
                    (
                        datafusion::arrow::compute::min(values),
                        datafusion::arrow::compute::max(values),
                    )
                }
                _ => bail!("replacement time column type does not match table"),
            };
            if let (Some(lo), Some(hi)) = (lo, hi) {
                minimum = Some(minimum.map_or(lo, |value| value.min(lo)));
                maximum = Some(maximum.map_or(hi, |value| value.max(hi)));
            }
        }
        let start = minimum.context("no rows to load")?;
        let end = maximum
            .context("no rows to load")?
            .checked_add(1)
            .context("replacement window end is out of range")?;
        let convert = |value| match self.time_type {
            TimeType::Date => chrono::NaiveDate::from_ymd_opt(1970, 1, 1)
                .unwrap()
                .checked_add_signed(chrono::Duration::days(value))
                .and_then(|date| date.and_hms_opt(0, 0, 0))
                .map(|time| time.and_utc()),
            _ => DateTime::from_timestamp_micros(value),
        };
        Ok((
            convert(start).context("replacement window start is out of range")?,
            convert(end).context("replacement window end is out of range")?,
        ))
    }

    /// Add, replace, or remove table tags in one DuckLake snapshot.
    ///
    /// Each map entry updates only its key: `Some(value)` sets the raw tag
    /// value, while `None` removes the tag. Tag values used by dkplus settings
    /// should be JSON-encoded strings. The returned snapshot ID is unchanged
    /// when every requested update is already in effect.
    pub async fn update_tags(&self, updates: HashMap<String, Option<String>>) -> Result<i64> {
        if updates.keys().any(|key| key.is_empty()) {
            bail!("DuckLake tag keys must not be empty");
        }
        let mut client = connect(&self.cfg).await?;
        let tx = client.transaction().await?;
        tx.batch_execute("LOCK TABLE ducklake.ducklake_snapshot IN SHARE ROW EXCLUSIVE MODE")
            .await
            .context("locking catalog snapshots for tag update")?;

        let meta = table_meta(&tx, &self.schema, &self.table).await?;
        let latest = tx
            .query_one(
                "SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
                   FROM ducklake.ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1",
                &[],
            )
            .await
            .context("reading latest snapshot for tag update")?;
        let (previous, schema_version, next_catalog_id, next_file_id): (i64, i64, i64, i64) =
            (latest.get(0), latest.get(1), latest.get(2), latest.get(3));

        if updates.is_empty() {
            tx.commit().await.context("finishing empty tag update")?;
            return Ok(previous);
        }

        let keys: Vec<_> = updates.keys().cloned().collect();
        let rows = tx
            .query(
                "SELECT key, value FROM ducklake.ducklake_tag
                  WHERE object_id = $1 AND key = ANY($2)
                    AND begin_snapshot <= $3
                    AND (end_snapshot IS NULL OR end_snapshot > $3)",
                &[&meta.table_id, &keys, &previous],
            )
            .await
            .context("reading active table tags")?;
        let mut active = HashMap::with_capacity(rows.len());
        for row in rows {
            let key: String = row.get(0);
            let value: String = row.get(1);
            if active.insert(key.clone(), value).is_some() {
                bail!("multiple active DuckLake tags for key {key:?}");
            }
        }

        let changed = updates
            .iter()
            .any(|(key, value)| active.get(key) != value.as_ref());
        if !changed {
            tx.commit()
                .await
                .context("finishing unchanged tag update")?;
            return Ok(previous);
        }

        let snapshot_id = previous + 1;
        tx.execute(
            "INSERT INTO ducklake.ducklake_snapshot
               (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
             VALUES ($1, now(), $2, $3, $4)",
            &[
                &snapshot_id,
                &schema_version,
                &next_catalog_id,
                &next_file_id,
            ],
        )
        .await
        .context("inserting tag update snapshot")?;
        tx.execute(
            "INSERT INTO ducklake.ducklake_snapshot_changes
               (snapshot_id, changes_made, author, commit_message, commit_extra_info)
             VALUES ($1, $2, $3, $4, NULL)",
            &[
                &snapshot_id,
                &format!("altered_table:{}", meta.table_id),
                &self.author.as_deref().unwrap_or("dkplus"),
                &format!("updated tags for {self}"),
            ],
        )
        .await
        .context("recording tag update snapshot changes")?;

        for (key, value) in updates {
            let current = active.get(&key);
            if current == value.as_ref() {
                continue;
            }
            if current.is_some() {
                let ended = tx
                    .execute(
                        "UPDATE ducklake.ducklake_tag SET end_snapshot = $4
                          WHERE object_id = $1 AND key = $2
                            AND begin_snapshot <= $3
                            AND (end_snapshot IS NULL OR end_snapshot > $3)",
                        &[&meta.table_id, &key, &previous, &snapshot_id],
                    )
                    .await
                    .with_context(|| format!("ending active DuckLake tag {key:?}"))?;
                if ended != 1 {
                    bail!("active DuckLake tag {key:?} changed during update");
                }
            }
            if let Some(value) = value {
                tx.execute(
                    "INSERT INTO ducklake.ducklake_tag
                       (object_id, begin_snapshot, end_snapshot, key, value)
                     VALUES ($1, $2, NULL, $3, $4)",
                    &[&meta.table_id, &snapshot_id, &key, &value],
                )
                .await
                .with_context(|| format!("writing DuckLake tag {key:?}"))?;
            }
        }

        tx.commit()
            .await
            .context("committing tag update snapshot")?;
        Ok(snapshot_id)
    }

    /// Replace the range from the minimum through maximum input time, inclusive.
    /// The half-open upper bound is one day (date) or microsecond (timestamp) later.
    pub async fn replace_window(&self, batches: Vec<RecordBatch>) -> Result<LoadReport> {
        let table = self;
        let cfg = &self.cfg;
        self.validate()?;
        let batches: Vec<_> = batches.into_iter().filter(|b| b.num_rows() > 0).collect();
        let (start, end) = self.replacement_bounds(&batches)?;
        for batch in &batches {
            if batch.schema() != batches[0].schema() {
                bail!("load batch schemas differ");
            }
        }
        (cfg.log)(&format!("{self} [{start}, {end}): replacing window"));
        let mut client = connect(cfg).await?;
        let meta = table_meta(&client, &table.schema, &table.table).await?;
        let cols = resolve_columns(&meta, &batches[0].schema())?;
        let utc_col = meta
            .columns
            .get(&table.time_column)
            .context("table has no configured window column")?;
        if types::arrow_type(&utc_col.ty)? != types::arrow_type(table.time_type.sql())? {
            bail!("window column type does not match table");
        }
        let (buf, pq) = write_parquet(&batches, &cols)?;
        let name = format!("ducklake-{}.parquet", Uuid::now_v7());
        let file = file_info(name, &buf, &pq, &batches, &cols)?;
        let (store, prefix) = object_store_for(cfg, &meta.root)?;
        let obj = ObjPath::from(format!("{prefix}{}", file.name));
        store
            .put(&obj, PutPayload::from(Bytes::from(buf)))
            .await
            .with_context(|| format!("uploading {}{}", meta.root, file.name))?;
        let result = register(&mut client, table, &meta, utc_col.id, start, end, &file).await;
        if result.is_err()
            && let Err(e) = store.delete(&obj).await
        {
            (cfg.log)(&format!("could not remove orphaned {obj}: {e}"));
        }
        if let Ok(report) = &result {
            (cfg.log)(&format!(
                "{self}: committed replacement snapshot {}; wrote {} rows",
                report.snapshot_id, report.rows
            ));
        }
        result
    }
}

async fn register(
    client: &mut Client,
    table: &Table,
    meta: &TableMeta,
    utc_col: i64,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    file: &FileInfo,
) -> Result<LoadReport> {
    for attempt in 1..=MAX_COMMIT_ATTEMPTS {
        match try_register(client, table, meta, utc_col, start, end, file).await {
            Ok(Attempt::Done(report)) => return Ok(report),
            Ok(Attempt::NeedsRewrite) => {
                let window = repack::Window { start, end };
                return repack::replace_window(client, meta, utc_col, table, window, file).await;
            }
            Err(e) if is_commit_conflict(&e) && attempt < MAX_COMMIT_ATTEMPTS => {
                (table.cfg.log)(&format!(
                    "ducklake commit conflict (attempt {attempt}), retrying: {e:#}"
                ));
                tokio::time::sleep(std::time::Duration::from_millis(250 * attempt as u64)).await;
            }
            Err(e) => return Err(e),
        }
    }
    bail!("could not commit ducklake snapshot after {MAX_COMMIT_ATTEMPTS} attempts")
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use datafusion::arrow::array::{ArrayRef, Date32Array, TimestampMicrosecondArray};

    fn replacement_batch(table: &Table, values: Vec<i64>) -> RecordBatch {
        let column: ArrayRef = match table.time_type {
            TimeType::Date => Arc::new(Date32Array::from(
                values
                    .into_iter()
                    .map(|value| i32::try_from(value).unwrap())
                    .collect::<Vec<_>>(),
            )),
            TimeType::Timestamp => Arc::new(TimestampMicrosecondArray::from(values)),
            TimeType::TimestampTz => {
                Arc::new(TimestampMicrosecondArray::from(values).with_timezone("UTC"))
            }
        };
        RecordBatch::try_from_iter([(table.time_column.as_str(), column)]).unwrap()
    }

    #[test]
    fn replacement_bounds_span_every_batch_and_include_the_maximum() {
        for kind in [TimeType::Date, TimeType::Timestamp, TimeType::TimestampTz] {
            let table = Table {
                time_type: kind,
                timezone: chrono_tz::America::Los_Angeles,
                ..crate::test_support::table()
            };
            let batches = vec![
                replacement_batch(&table, vec![]),
                replacement_batch(&table, vec![5, -2, 3]),
                replacement_batch(&table, vec![9, 1]),
            ];
            let (start, end) = table.replacement_bounds(&batches).unwrap();
            let unit = if kind == TimeType::Date {
                86_400_000_000
            } else {
                1
            };
            assert_eq!(start.timestamp_micros(), -2 * unit);
            assert_eq!(end.timestamp_micros(), 10 * unit);
            let (start, end) = table
                .replacement_bounds(&[replacement_batch(&table, vec![4])])
                .unwrap();
            assert_eq!(end.timestamp_micros() - start.timestamp_micros(), unit);
        }
    }

    #[tokio::test]
    async fn replacement_rejects_invalid_input_before_connecting() {
        let table = crate::test_support::table();
        for batches in [vec![], vec![replacement_batch(&table, vec![])]] {
            assert!(
                table
                    .replace_window(batches)
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("no rows")
            );
        }
        let column =
            Arc::new(TimestampMicrosecondArray::from(vec![Some(1), None]).with_timezone("UTC"))
                as ArrayRef;
        let batch = RecordBatch::try_from_iter([(table.time_column.as_str(), column)]).unwrap();
        assert!(
            table
                .replace_window(vec![batch])
                .await
                .unwrap_err()
                .to_string()
                .contains("nulls")
        );
        let wrong_type = Table {
            time_type: TimeType::Date,
            ..table.clone()
        };
        assert!(
            table
                .replace_window(vec![replacement_batch(&wrong_type, vec![1])])
                .await
                .unwrap_err()
                .to_string()
                .contains("type")
        );
        assert!(
            table
                .replace_window(vec![replacement_batch(&table, vec![i64::MAX])])
                .await
                .unwrap_err()
                .to_string()
                .contains("out of range")
        );
        let date_table = Table {
            time_type: TimeType::Date,
            ..table.clone()
        };
        assert!(
            date_table
                .replacement_bounds(&[replacement_batch(&date_table, vec![i64::from(i32::MAX)])])
                .is_err()
        );
        let mut other = table.clone();
        other.time_column = "other".into();
        assert!(
            table
                .replace_window(vec![replacement_batch(&other, vec![1])])
                .await
                .is_err()
        );
        let first = replacement_batch(&table, vec![1]);
        let extra = RecordBatch::try_from_iter([
            (table.time_column.as_str(), first.column(0).clone()),
            ("extra", first.column(0).clone()),
        ])
        .unwrap();
        assert!(
            table
                .replace_window(vec![first, extra])
                .await
                .unwrap_err()
                .to_string()
                .contains("schemas differ")
        );
        assert!(
            table
                .update_tags(HashMap::from([("".into(), Some("value".into()))]))
                .await
                .unwrap_err()
                .to_string()
                .contains("tag keys must not be empty")
        );
    }

    #[test]
    fn table_policy_uses_tags_and_explicit_overrides() {
        let meta = TableMeta {
            table_id: 9,
            root: String::new(),
            columns: HashMap::from([
                (
                    "observed".into(),
                    Column {
                        id: 1,
                        ty: "date".into(),
                    },
                ),
                (
                    "station".into(),
                    Column {
                        id: 2,
                        ty: "int32".into(),
                    },
                ),
                (
                    "fetched_at".into(),
                    Column {
                        id: 3,
                        ty: "timestamp".into(),
                    },
                ),
            ]),
        };
        let tags = HashMap::from([
            (TAG_TIME_COLUMN.into(), "not-json".into()),
            (TAG_TIMEZONE.into(), "\"Not/AZone\"".into()),
            (TAG_ROW_GROUP_COL.into(), "null".into()),
            (TAG_SORT_COLS.into(), r#"["station","observed"]"#.into()),
            (
                TAG_UNIQUE_BY_COLS.into(),
                r#"["station","observed"]"#.into(),
            ),
            (TAG_DEDUP_TIMESTAMP.into(), "\"fetched_at\"".into()),
        ]);
        let table = resolve_table_settings(
            Table::builder("weather.observations", &Table::default().cfg)
                .with_time_column("observed")
                .with_timezone(chrono_tz::UTC)
                .with_row_group_col("station")
                .with_lock_namespace("weather.repack"),
            &meta,
            &tags,
            &HashMap::new(),
        )
        .unwrap();

        assert_eq!(table.time_type, TimeType::Date);
        assert_eq!(table.time_column, "observed");
        assert_eq!(table.timezone, chrono_tz::UTC);
        assert_eq!(table.row_group_col.as_deref(), Some("station"));
        assert_eq!(table.sort_cols, ["station", "observed"]);
        assert_eq!(
            table.unique_by_cols.as_deref(),
            Some(["station".to_string(), "observed".to_string()].as_slice())
        );
        assert_eq!(table.dedup_timestamp.as_deref(), Some("fetched_at"));
        assert_eq!(table.lock_namespace, "weather.repack");
        assert_eq!(table.author, None);

        let from_column = resolve_table_settings(
            Table::default()
                .cfg
                .table_builder("weather.observations")
                .with_time_column("observed"),
            &meta,
            &tags,
            &HashMap::from([(1, r#""America/New_York""#.into()), (2, "not-json".into())]),
        )
        .unwrap();
        assert_eq!(from_column.timezone, chrono_tz::America::New_York);

        let optional_overrides = resolve_table_settings(
            Table::builder("weather.observations", &Table::default().cfg)
                .with_time_column("observed")
                .with_timezone(chrono_tz::UTC)
                .with_sort_cols(vec!["station".into(), "observed".into()])
                .without_row_grouping()
                .without_deduplication(),
            &meta,
            &tags,
            &HashMap::new(),
        )
        .unwrap();
        assert_eq!(optional_overrides.row_group_col, None);
        assert_eq!(optional_overrides.unique_by_cols, None);
        assert_eq!(optional_overrides.dedup_timestamp, None);
        assert_eq!(optional_overrides.lock_namespace, "observations");
        assert_eq!(
            optional_overrides.author_for(
                "repacker",
                Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
                Utc.with_ymd_and_hms(2026, 2, 1, 0, 0, 0).unwrap(),
            ),
            "repacker_20260101T000000Z_20260201T000000Z"
        );
    }

    #[test]
    fn missing_required_table_tags_fail() {
        let meta = TableMeta {
            table_id: 1,
            root: String::new(),
            columns: HashMap::new(),
        };
        let error = resolve_table_settings(
            Table::builder("weather.observations", &Table::default().cfg),
            &meta,
            &HashMap::new(),
            &HashMap::new(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("time_column"));
    }

    #[test]
    fn widens_bounds_by_type() {
        let lo = widen("float64", Some("-5".into()), &Some("-10.5".into()), false);
        assert_eq!(lo.as_deref(), Some("-10.5"));
        let hi = widen("float64", Some("100".into()), &Some("20".into()), true);
        assert_eq!(hi.as_deref(), Some("100"));
        let ts = widen(
            "timestamptz",
            Some("2026-10-16 07:00:00+00".into()),
            &Some("2026-10-16 06:55:00+00".into()),
            false,
        );
        assert_eq!(ts.as_deref(), Some("2026-10-16 06:55:00+00"));
        assert_eq!(widen("varchar", None, &Some("A".into()), true), None);
    }

    #[test]
    fn formats_timestamp_bounds() {
        use datafusion::common::ScalarValue;
        let format = |value| {
            types::text(&ScalarValue::TimestampMicrosecond(
                Some(value),
                Some("UTC".into()),
            ))
            .unwrap()
        };
        assert_eq!(format(1_792_134_000_000_000), "2026-10-16 07:00:00+00");
        assert_eq!(
            format(1_792_134_000_123_456),
            "2026-10-16 07:00:00.123456+00"
        );
    }

    #[test]
    fn joins_paths() {
        assert_eq!(
            join_dir("az://ducklake/ciso", "rtprices/"),
            "az://ducklake/ciso/rtprices/"
        );
        assert_eq!(
            join_dir("az://ducklake/ciso/rtprices/", ""),
            "az://ducklake/ciso/rtprices/"
        );
    }
}

#[cfg(test)]
mod test_support;
