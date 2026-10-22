#![doc = include_str!("../README.md")]

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use chrono::{DateTime, Utc};
use config::identifier;
pub use config::{Config, Layout, Storage, TimeType, Window};
use datafusion::arrow::array::{Array, AsArray, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, TimeUnit, TimestampMicrosecondType};
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
    /// Files fully inside the window that were retired via `end_snapshot`.
    pub retired_files: Vec<i64>,
    pub retired_rows: i64,
    /// Rows removed through pg_ducklake `DELETE` (only when a file straddles the window).
    pub sql_deleted: Option<u64>,
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

struct ColStats {
    column_id: i64,
    ty: String,
    size_bytes: i64,
    value_count: i64,
    null_count: i64,
    min: Option<String>,
    max: Option<String>,
    contains_nan: Option<bool>,
}

struct FileInfo {
    name: String,
    rows: i64,
    size: i64,
    footer: i64,
    stats: Vec<ColStats>,
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
    tokio::spawn(async move {
        if let Err(e) = conn.await {
            eprintln!("postgres connection error: {e}");
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
    layout: &Layout,
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
                    identifier(&layout.time_column)?,
                    layout.time_type.sql(),
                    layout.time_type.sql(),
                    identifier(&layout.time_column)?,
                    layout.time_type.sql(),
                    layout.time_type.sql()
                ),
                &[&layout.bound(start), &layout.bound(end)],
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
        stats.push(ColStats {
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
    NeedsSqlDelete,
}

async fn try_register(
    client: &mut Client,
    table: &str,
    layout: &Layout,
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
                ty = layout.time_type.sql()
            ),
            &[
                &table_id,
                &utc_col,
                &layout.bound(start),
                &layout.bound(end),
            ],
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
            _ => return Ok(Attempt::NeedsSqlDelete),
        }
    }

    if has_inlined_rows(&tx, table_id, layout, start, end).await? {
        return Ok(Attempt::NeedsSqlDelete);
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
    let message = format!(
        "{}.{table} {}",
        layout.schema,
        start.format("%Y-%m-%dT%H:%MZ")
    );
    tx.execute(
        "INSERT INTO ducklake.ducklake_snapshot_changes
           (snapshot_id, changes_made, author, commit_message, commit_extra_info)
         VALUES ($1, $2, $3, $4, NULL)",
        &[&snapshot_id, &changes, &layout.author, &message],
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
        sql_deleted: None,
    }))
}

async fn update_table_stats(
    tx: &impl GenericClient,
    table_id: i64,
    has_row: bool,
    next_row_id: i64,
    stats: &[ColStats],
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
    let (record_count, size): (i64, i64) = (totals.get(0), totals.get(1));
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

    let existing: HashMap<i64, (Option<bool>, Option<bool>, Option<String>, Option<String>)> = tx
        .query(
            "SELECT column_id, contains_null, contains_nan, min_value, max_value
               FROM ducklake.ducklake_table_column_stats WHERE table_id = $1",
            &[&table_id],
        )
        .await?
        .into_iter()
        .map(|r| (r.get(0), (r.get(1), r.get(2), r.get(3), r.get(4))))
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
            Some((cn, cnan, lo, hi)) => (
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

/// Fallback for windows whose rows share files with other hours: let pg_ducklake delete them.
async fn sql_delete(
    client: &Client,
    table: &str,
    layout: &Layout,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
) -> Result<u64> {
    let sql = format!(
        "DELETE FROM {}.{} WHERE {} >= $1::text::{} AND {} < $2::text::{}",
        identifier(&layout.schema)?,
        identifier(table)?,
        identifier(&layout.time_column)?,
        layout.time_type.sql(),
        identifier(&layout.time_column)?,
        layout.time_type.sql()
    );
    client
        .execute(&sql, &[&layout.bound(start), &layout.bound(end)])
        .await
        .context("pg_ducklake DELETE for window")
}

/// Replace a typed half-open window with one new Parquet file and catalog snapshot.
pub async fn replace_window(
    cfg: &Config,
    layout: &Layout,
    table: &str,
    window: &Window,
    batches: Vec<RecordBatch>,
) -> Result<LoadReport> {
    layout.validate()?;
    identifier(table)?;
    let (start, end) = window.bounds(layout)?;
    let batches: Vec<_> = batches.into_iter().filter(|b| b.num_rows() > 0).collect();
    if batches.is_empty() {
        bail!("no rows to load for {start}");
    }
    let mut client = connect(cfg).await?;
    let meta = table_meta(&client, &layout.schema, table).await?;
    let cols = resolve_columns(&meta, &batches[0].schema())?;
    let utc_col = meta
        .columns
        .get(&layout.time_column)
        .context("table has no configured window column")?;
    if types::arrow_type(&utc_col.ty)? != types::arrow_type(layout.time_type.sql())? {
        bail!("window column type does not match layout");
    }
    for batch in &batches {
        resolve_columns(&meta, &batch.schema())?;
        if batch.schema() != batches[0].schema() {
            bail!("load batch schemas differ");
        }
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
    let result = register(
        &mut client,
        table,
        layout,
        &meta,
        utc_col.id,
        start,
        end,
        &file,
    )
    .await;
    if result.is_err() {
        if let Err(e) = store.delete(&obj).await {
            eprintln!("could not remove orphaned {obj}: {e}");
        }
    }
    result
}

async fn register(
    client: &mut Client,
    table: &str,
    layout: &Layout,
    meta: &TableMeta,
    utc_col: i64,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    file: &FileInfo,
) -> Result<LoadReport> {
    let mut sql_deleted = None;
    for attempt in 1..=MAX_COMMIT_ATTEMPTS {
        match try_register(client, table, layout, meta, utc_col, start, end, file).await {
            Ok(Attempt::Done(mut report)) => {
                report.sql_deleted = sql_deleted;
                return Ok(report);
            }
            Ok(Attempt::NeedsSqlDelete) => {
                if sql_deleted.is_some() {
                    bail!("rows for {start} still present after pg_ducklake DELETE");
                }
                sql_deleted = Some(sql_delete(client, table, layout, start, end).await?);
            }
            Err(e) if is_commit_conflict(&e) && attempt < MAX_COMMIT_ATTEMPTS => {
                eprintln!("ducklake commit conflict (attempt {attempt}), retrying: {e:#}");
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
    #[tokio::test]
    #[ignore = "requires MAINTENANCE_TEST_DATABASE_URL pointing to isolated test Postgres"]
    async fn typed_sql_delete_uses_half_open_bounds_and_quoted_identifiers() {
        let url = std::env::var("MAINTENANCE_TEST_DATABASE_URL").unwrap();
        let (client, connection) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
            .await
            .unwrap();
        let connection = tokio::spawn(async move { connection.await.unwrap() });
        client
            .batch_execute("SET TIME ZONE 'Pacific/Auckland'")
            .await
            .unwrap();
        for kind in [TimeType::TimestampTz, TimeType::Timestamp, TimeType::Date] {
            let layout = Layout {
                schema: "pg_temp".into(),
                time_column: "event time".into(),
                time_type: kind,
                ..Layout::default()
            };
            client.batch_execute(&format!(
                "CREATE TEMP TABLE records (\"event time\" {});
                 INSERT INTO records VALUES ('2026-01-01 00:00:00+00'),('2026-01-02 00:00:00+00'),('2026-01-03 00:00:00+00')",
                kind.sql(),
            )).await.unwrap();
            let start = DateTime::parse_from_rfc3339("2026-01-02T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc);
            let end = DateTime::parse_from_rfc3339("2026-01-03T00:00:00Z")
                .unwrap()
                .with_timezone(&Utc);
            assert_eq!(
                sql_delete(&client, "records", &layout, start, end)
                    .await
                    .unwrap(),
                1
            );
            assert_eq!(
                client
                    .query_one("SELECT count(*) FROM records", &[])
                    .await
                    .unwrap()
                    .get::<_, i64>(0),
                2
            );
            client.batch_execute("DROP TABLE records").await.unwrap();
        }
        drop(client);
        connection.await.unwrap();
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
