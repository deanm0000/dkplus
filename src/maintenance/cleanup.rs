use crate::error::{Context, fail};
use crate::{Config, Storage, catalog_table_root, config::identifier, connect, join_dir};
use crate::{Error, Result};
use chrono::{DateTime, Utc};
use futures::TryStreamExt;
use object_store::{ObjectStore, ObjectStoreExt};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

mod temp_directories;
pub use temp_directories::DirectoryCleanupReport;

/// Cleanup never expires snapshots or discovers orphaned files.
#[derive(Debug, Clone)]
pub enum CleanupScope {
    Catalog,
    /// Only files beneath this current table's exclusive data directory.
    Table {
        schema: String,
        table: String,
    },
}
#[derive(Debug, Clone)]
pub struct CleanupOptions {
    pub scope: CleanupScope,
    /// Strict cutoff on the time a file was scheduled, not its modification time.
    pub older_than: DateTime<Utc>,
    /// Validate and report candidates without deleting objects or queue entries.
    pub dry_run: bool,
}

#[derive(Debug)]
pub struct CleanupFile {
    /// The queue's ID, which may identify a data file or a delete file.
    pub data_file_id: i64,
    pub path: String,
    pub schedule_start: DateTime<Utc>,
}

#[derive(Debug, Default)]
pub struct CleanupReport {
    pub files: Vec<CleanupFile>,
    /// Successful delete calls; some stores also return success for absent files.
    pub deleted: usize,
    /// Delete calls that explicitly returned NotFound.
    pub already_missing: usize,
}

#[derive(Debug, Default)]
pub struct TempCleanupReport {
    pub candidates: usize,
    pub deleted: usize,
    pub already_missing: usize,
    pub skipped_tables: Vec<String>,
    pub directories: DirectoryCleanupReport,
}

/// Delete files last modified before `older_than` under catalog table `_tmp` directories.
/// Readers and writers are not tracked; the caller must choose an appropriate age.
pub async fn cleanup_stale_temp_files(
    cfg: &Config,
    older_than: DateTime<Utc>,
    dry_run: bool,
) -> Result<TempCleanupReport> {
    let client = connect(cfg).await?;
    let data_path: Option<String> = client
        .query_opt(
            "SELECT value FROM ducklake.ducklake_metadata WHERE key = 'data_path' AND scope IS NULL",
            &[],
        )
        .await?
        .map(|r| r.try_get(0))
        .transpose()?;
    let rows = client
        .query(
            "SELECT s.schema_name, t.table_name, t.path, t.path_is_relative,
                    s.path, s.path_is_relative
             FROM ducklake.ducklake_table t JOIN ducklake.ducklake_schema s USING (schema_id)",
            &[],
        )
        .await
        .context("listing catalog table directories for temporary cleanup")?;
    drop(client);

    let mut report = TempCleanupReport::default();
    let mut roots = HashSet::new();
    let mut stores = HashMap::new();
    let directories = match &cfg.storage {
        Storage::Azure {
            account,
            access_key,
        } => Some(temp_directories::AzureDirectories::new(
            account, access_key,
        )?),
        Storage::Store { .. } => None,
    };
    for row in rows {
        let schema: String = row.try_get(0)?;
        let table: String = row.try_get(1)?;
        let root = catalog_table_root(
            &row.try_get::<_, String>(2)?,
            row.try_get(3)?,
            &row.try_get::<_, String>(4)?,
            row.try_get(5)?,
            data_path.as_deref(),
        )?;
        let root = if matches!(&cfg.storage, Storage::Azure { .. }) {
            if let Some(path) = root.strip_prefix("azure://") {
                format!("az://{path}")
            } else if root.starts_with("az://") {
                root
            } else {
                let name = format!("{schema}.{table}");
                (cfg.log)(&format!(
                    "temporary cleanup skipped {name}: table directory is outside configured Azure storage"
                ));
                report.skipped_tables.push(name);
                continue;
            }
        } else {
            root
        };
        let (store, key) = cached_resolve(&cfg.storage, &root, &mut stores)
            .with_context(|| format!("resolving temporary directory for {schema}.{table}"))?;
        let tmp = object_store::path::Path::parse(format!("{}_tmp", key))?;
        let identity = cached_identity(&cfg.storage, &format!("{root}_tmp/"), &mut stores)?;
        if !roots.insert(identity) {
            continue;
        }
        let mut list = store.list(Some(&tmp));
        let mut candidates = Vec::new();
        while let Some(file) = list
            .try_next()
            .await
            .with_context(|| format!("listing temporary files for {schema}.{table}"))?
        {
            if file.last_modified < older_than {
                candidates.push(file.location);
            }
        }
        report.candidates += candidates.len();
        if !dry_run {
            for path in candidates {
                let current = match store.head(&path).await {
                    Ok(meta) => meta,
                    Err(object_store::Error::NotFound { .. }) => {
                        report.already_missing += 1;
                        continue;
                    }
                    Err(error) => {
                        return Err(error)
                            .with_context(|| format!("checking temporary file {path}"));
                    }
                };
                if current.last_modified >= older_than {
                    continue;
                }
                match store.delete(&path).await {
                    Ok(()) => report.deleted += 1,
                    Err(object_store::Error::NotFound { .. }) => report.already_missing += 1,
                    Err(error) => {
                        return Err(error)
                            .with_context(|| format!("deleting temporary file {path}"));
                    }
                }
            }
        }
        if let Some(directories) = &directories {
            let container = root
                .strip_prefix("az://")
                .and_then(|path| path.split('/').next())
                .context("resolving Azure container for temporary directories")?;
            directories
                .cleanup(
                    container,
                    &tmp,
                    older_than,
                    dry_run,
                    &mut report.directories,
                )
                .await
                .with_context(|| format!("cleaning temporary directories for {schema}.{table}"))?;
        }
    }
    (cfg.log)(&format!(
        "temporary cleanup: {} candidates, {} deleted, {} already missing, {} tables skipped; dry-run={dry_run}",
        report.candidates,
        report.deleted,
        report.already_missing,
        report.skipped_tables.len()
    ));
    (cfg.log)(&format!(
        "temporary directories: {} old candidates, {} deleted, {} nonempty, {} changed, {} already missing; dry-run={dry_run}",
        report.directories.candidates,
        report.directories.deleted,
        report.directories.nonempty,
        report.directories.changed,
        report.directories.already_missing
    ));
    Ok(report)
}

/// Queue retired table files that are no longer visible in any retained snapshot.
/// This does not expire snapshots or delete objects. Queue timestamps are new.
pub async fn schedule_unreferenced_table_files(
    cfg: &Config,
    schema: &str,
    table: &str,
) -> Result<usize> {
    identifier(schema)?;
    identifier(table)?;
    let mut client = connect(cfg).await?;
    let tx = client.transaction().await?;
    crate::lock_catalog_write(&tx).await?;
    let row = tx
        .query_opt(
            "SELECT t.table_id, t.path, t.path_is_relative, s.path, s.path_is_relative,
                    (SELECT value FROM ducklake.ducklake_metadata
                     WHERE key = 'data_path' AND scope IS NULL)
             FROM ducklake.ducklake_table t
             JOIN ducklake.ducklake_schema s USING (schema_id)
             WHERE s.schema_name = $1 AND t.table_name = $2
               AND s.end_snapshot IS NULL AND t.end_snapshot IS NULL",
            &[&schema, &table],
        )
        .await
        .context("resolving table for obsolete-file scheduling")?
        .ok_or_else(|| Error::TableNotFound {
            table: format!("{schema}.{table}"),
        })?;
    let table_id: i64 = row.try_get(0)?;
    let data_path: Option<String> = row.try_get(5)?;
    let root = catalog_table_root(
        &row.try_get::<_, String>(1)?,
        row.try_get(2)?,
        &row.try_get::<_, String>(3)?,
        row.try_get(4)?,
        data_path.as_deref(),
    )?;
    let data = tx
        .query(
            "DELETE FROM ducklake.ducklake_data_file f
             WHERE f.table_id = $1 AND f.begin_snapshot IS NOT NULL
               AND f.end_snapshot > f.begin_snapshot
               AND NOT EXISTS (
                   SELECT 1 FROM ducklake.ducklake_snapshot s
                   WHERE s.snapshot_id >= f.begin_snapshot AND s.snapshot_id < f.end_snapshot)
             RETURNING data_file_id, path, path_is_relative",
            &[&table_id],
        )
        .await
        .context("retiring unreferenced data-file metadata")?;
    let removed: Vec<i64> = data.iter().map(|row| row.get(0)).collect();
    let deletes = tx
        .query(
            "DELETE FROM ducklake.ducklake_delete_file f
             WHERE f.table_id = $1 AND (f.data_file_id = ANY($2) OR (
                 f.begin_snapshot IS NOT NULL AND f.end_snapshot > f.begin_snapshot
                 AND NOT EXISTS (
                     SELECT 1 FROM ducklake.ducklake_snapshot s
                     WHERE s.snapshot_id >= f.begin_snapshot AND s.snapshot_id < f.end_snapshot)))
             RETURNING delete_file_id, path, path_is_relative",
            &[&table_id, &removed],
        )
        .await
        .context("retiring unreferenced delete-file metadata")?;
    if !removed.is_empty() {
        for stats in [
            "ducklake_file_column_stats",
            "ducklake_file_partition_value",
            "ducklake_file_variant_stats",
        ] {
            tx.execute(
                &format!(
                    "DELETE FROM ducklake.{stats} WHERE table_id = $1 AND data_file_id = ANY($2)"
                ),
                &[&table_id, &removed],
            )
            .await
            .with_context(|| format!("removing obsolete metadata from {stats}"))?;
        }
    }
    let mut ids = Vec::with_capacity(data.len() + deletes.len());
    let mut paths = Vec::with_capacity(ids.capacity());
    let mut stores = HashMap::new();
    for row in data.iter().chain(&deletes) {
        ids.push(row.try_get::<_, i64>(0)?);
        let path: String = row.try_get(1)?;
        let path = if row.try_get::<_, Option<bool>>(2)? == Some(true) {
            format!("{root}{path}")
        } else {
            path
        };
        cached_identity(&cfg.storage, &path, &mut stores)?;
        paths.push(path);
    }
    if !ids.is_empty() {
        tx.execute(
            "INSERT INTO ducklake.ducklake_files_scheduled_for_deletion
                 (data_file_id, path, path_is_relative, schedule_start)
             SELECT id, path, false, now()
             FROM unnest($1::bigint[], $2::text[]) AS files(id, path)",
            &[&ids, &paths],
        )
        .await
        .context("scheduling obsolete files for deletion")?;
    }
    tx.commit()
        .await
        .context("committing obsolete-file scheduling")?;
    (cfg.log)(&format!(
        "{schema}.{table}: scheduled {} unreferenced files for cleanup",
        ids.len()
    ));
    Ok(ids.len())
}

// Store keys alone are insufficient for Azure: containers are separate namespaces.
fn identity(storage: &Storage, path: &str) -> Result<String> {
    if path.split('/').any(|part| part == "." || part == "..") {
        fail!(
            InvalidStoragePath,
            "cleanup path contains a relative traversal component: {path}"
        );
    }
    let (_, key) = storage.resolve(path)?;
    let key = object_store::path::Path::parse(key)?.to_string();
    let key = if path.ends_with('/') && !key.is_empty() {
        format!("{key}/")
    } else {
        key
    };
    Ok(match storage {
        Storage::Store { .. } => key,
        Storage::Azure { .. } => {
            let container = path
                .strip_prefix("az://")
                .context("expected az:// path")?
                .split('/')
                .next()
                .context("missing Azure container")?;
            format!("{container}/{key}")
        }
    })
}

fn cached_identity(
    storage: &Storage,
    path: &str,
    stores: &mut HashMap<String, Arc<dyn ObjectStore>>,
) -> Result<String> {
    if !matches!(storage, Storage::Azure { .. }) {
        return identity(storage, path);
    }
    if path.split('/').any(|part| part == "." || part == "..") {
        fail!(
            InvalidStoragePath,
            "cleanup path contains a relative traversal component: {path}"
        );
    }
    let (_, key) = cached_resolve(storage, path, stores)?;
    let container = path
        .strip_prefix("az://")
        .context("expected az:// path")?
        .split('/')
        .next()
        .context("missing Azure container")?;
    let mut key = object_store::path::Path::parse(key)?.to_string();
    if path.ends_with('/') && !key.is_empty() {
        key.push('/');
    }
    Ok(format!("{container}/{key}"))
}

fn cached_resolve(
    storage: &Storage,
    path: &str,
    stores: &mut HashMap<String, Arc<dyn ObjectStore>>,
) -> Result<(Arc<dyn ObjectStore>, String)> {
    if matches!(storage, Storage::Azure { .. }) {
        let rest = path.strip_prefix("az://").context("expected az:// path")?;
        let (container, key) = rest.split_once('/').unwrap_or((rest, ""));
        if container.is_empty() {
            fail!(InvalidStoragePath, "Azure storage path has no container");
        }
        if let Some(store) = stores.get(container) {
            return Ok((store.clone(), key.to_owned()));
        }
        let (store, key) = storage.resolve(path)?;
        stores.insert(container.to_owned(), store.clone());
        Ok((store, key))
    } else {
        storage.resolve(path)
    }
}

fn in_directory(path: &str, directory: &str) -> bool {
    path.starts_with(directory)
}

/// Delete files explicitly queued by DuckLake snapshot expiry, then acknowledge
/// their queue entries. Retired files still referenced by metadata are rejected.
///
/// Choose a cutoff older than any active reader. Run exclusively with respect to
/// other object-store cleanup tools; Postgres locks coordinate catalog changes,
/// not direct object-store writers. Failures leave queue entries retryable.
/// A failed call may already have deleted objects; retrying acknowledges absent
/// files. This function neither creates a snapshot nor changes table statistics.
pub async fn cleanup_expired_files(
    cfg: &Config,
    options: &CleanupOptions,
) -> Result<CleanupReport> {
    let batch_size = if !options.dry_run && matches!(&options.scope, CleanupScope::Catalog) {
        Some(100_i64)
    } else {
        None
    };
    let mut total = CleanupReport::default();
    loop {
        let report = cleanup_expired_files_batch(cfg, options, batch_size).await?;
        let count = report.files.len();
        total.files.extend(report.files);
        total.deleted += report.deleted;
        total.already_missing += report.already_missing;
        if batch_size.is_none_or(|size| count < size as usize) {
            return Ok(total);
        }
    }
}

async fn cleanup_expired_files_batch(
    cfg: &Config,
    options: &CleanupOptions,
    limit: Option<i64>,
) -> Result<CleanupReport> {
    if let CleanupScope::Table { schema, table } = &options.scope {
        identifier(schema)?;
        identifier(table)?;
    }
    (cfg.log)("checking expired files for cleanup");
    let mut client = connect(cfg).await?;
    let tx = client.transaction().await?;
    // Retain metadata locks through physical deletion to prevent concurrent
    // publication from introducing a reference after our safety check.
    tx.batch_execute(
        "LOCK TABLE ducklake.ducklake_files_scheduled_for_deletion IN SHARE ROW EXCLUSIVE MODE;
         LOCK TABLE ducklake.ducklake_data_file, ducklake.ducklake_delete_file,
                    ducklake.ducklake_table, ducklake.ducklake_schema,
                    ducklake.ducklake_metadata IN SHARE MODE;",
    )
    .await
    .context("locking catalog for expired-file cleanup")?;
    let data_path: Option<String> = tx
        .query_opt(
            "SELECT value FROM ducklake.ducklake_metadata
             WHERE key = 'data_path' AND scope IS NULL",
            &[],
        )
        .await?
        .map(|r| r.try_get(0))
        .transpose()?;
    let mut roots = HashMap::new();
    let mut target = None;
    let mut stores = HashMap::new();
    for row in tx
        .query(
            "SELECT t.table_id, t.path, t.path_is_relative, s.path, s.path_is_relative,
                    s.schema_name, t.table_name, t.end_snapshot, s.end_snapshot
             FROM ducklake.ducklake_table t JOIN ducklake.ducklake_schema s USING (schema_id)",
            &[],
        )
        .await?
    {
        let id: i64 = row.try_get(0)?;
        let root = catalog_table_root(
            &row.try_get::<_, String>(1)?,
            row.try_get(2)?,
            &row.try_get::<_, String>(3)?,
            row.try_get(4)?,
            data_path.as_deref(),
        )?;
        if let CleanupScope::Table { schema, table } = &options.scope
            && row.try_get::<_, String>(5)? == *schema
            && row.try_get::<_, String>(6)? == *table
            && row.try_get::<_, Option<i64>>(7)?.is_none()
            && row.try_get::<_, Option<i64>>(8)?.is_none()
        {
            target = Some(id);
        }
        roots.insert(id, root);
    }
    let directory = match &options.scope {
        CleanupScope::Catalog => None,
        CleanupScope::Table { schema, table } => {
            let id = target.ok_or_else(|| Error::TableNotFound {
                table: format!("{schema}.{table}"),
            })?;
            let directory = cached_identity(&cfg.storage, &roots[&id], &mut stores)?;
            // An empty key is the entire store, not an exclusive table directory.
            if directory.is_empty() {
                fail!(
                    InvalidStoragePath,
                    "table cleanup requires an exclusive, non-root data directory"
                );
            }
            for (other_id, root) in &roots {
                if *other_id != id {
                    let other = cached_identity(&cfg.storage, root, &mut stores)?;
                    if in_directory(&other, &directory) || in_directory(&directory, &other) {
                        fail!(
                            InvalidStoragePath,
                            "table cleanup is ambiguous: overlapping table data directories"
                        );
                    }
                }
            }
            Some(directory)
        }
    };
    let mut report = CleanupReport::default();
    let mut queue_paths = Vec::new();
    for row in tx
        .query(
            "SELECT data_file_id, path, path_is_relative, schedule_start
             FROM ducklake.ducklake_files_scheduled_for_deletion
             WHERE schedule_start < $1 ORDER BY schedule_start, data_file_id
             LIMIT $2",
            &[&options.older_than, &limit],
        )
        .await
        .context("reading expired-file deletion queue")?
    {
        let raw_path: String = row.try_get(1)?;
        let relative: Option<bool> = row.try_get(2)?;
        let path = if relative == Some(true) {
            format!(
                "{}{raw_path}",
                join_dir(
                    data_path.as_deref().context("catalog has no data_path")?,
                    ""
                )
            )
        } else {
            raw_path.clone()
        };
        let key = cached_identity(&cfg.storage, &path, &mut stores)?;
        if directory.as_ref().is_some_and(|d| !in_directory(&key, d)) {
            continue;
        }
        report.files.push(CleanupFile {
            data_file_id: row.try_get(0)?,
            path,
            schedule_start: row.try_get(3)?,
        });
        queue_paths.push((raw_path, relative));
    }
    if !report.files.is_empty() {
        let mut referenced = HashSet::new();
        for row in tx
            .query(
                "SELECT table_id, path, path_is_relative FROM ducklake.ducklake_data_file
                 UNION ALL
                 SELECT table_id, path, path_is_relative FROM ducklake.ducklake_delete_file",
                &[],
            )
            .await?
        {
            let table_id: i64 = row.try_get(0)?;
            let path: String = row.try_get(1)?;
            let path = if row.try_get::<_, Option<bool>>(2)? == Some(true) {
                format!(
                    "{}{path}",
                    roots
                        .get(&table_id)
                        .context("referenced file has no table")?
                )
            } else {
                path
            };
            referenced.insert(cached_identity(&cfg.storage, &path, &mut stores)?);
        }
        for file in &report.files {
            if referenced.contains(&cached_identity(&cfg.storage, &file.path, &mut stores)?) {
                fail!(
                    InvalidData,
                    "queued file is still referenced by catalog metadata: {}",
                    file.path
                );
            }
        }
    }
    if !options.dry_run {
        for (file, (raw_path, relative)) in report.files.iter().zip(&queue_paths) {
            let (store, key) = cached_resolve(&cfg.storage, &file.path, &mut stores)?;
            match store.delete(&object_store::path::Path::parse(key)?).await {
                Ok(()) => report.deleted += 1,
                Err(object_store::Error::NotFound { .. }) => report.already_missing += 1,
                Err(e) => {
                    return Err(e).with_context(|| format!("deleting expired file {}", file.path));
                }
            }
            tx.execute(
                "DELETE FROM ducklake.ducklake_files_scheduled_for_deletion
                 WHERE data_file_id = $1 AND schedule_start = $2 AND path = $3
                   AND path_is_relative IS NOT DISTINCT FROM $4",
                &[&file.data_file_id, &file.schedule_start, raw_path, relative],
            )
            .await
            .with_context(|| format!("acknowledging expired file {}", file.path))?;
        }
    }
    tx.commit()
        .await
        .context("committing expired-file cleanup")?;
    (cfg.log)(&format!(
        "expired-file cleanup: {} candidates, {} deleted, {} already missing; dry-run={}",
        report.files.len(),
        report.deleted,
        report.already_missing,
        options.dry_run
    ));
    Ok(report)
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::memory::InMemory;
    use std::sync::Arc;

    #[test]
    fn identities_and_directory_boundaries() {
        let storage = Storage::Store {
            root: "memory:///".into(),
            store: Arc::new(InMemory::new()),
        };
        let directory = identity(&storage, "memory:///weather/a/").unwrap();
        assert!(in_directory("weather/a/old.parquet", &directory));
        assert!(!in_directory("weather/ab/old.parquet", &directory));
        assert_eq!(
            identity(&storage, "memory:////weather/a/old.parquet").unwrap(),
            "weather/a/old.parquet"
        );
        assert!(identity(&storage, "memory:///weather/a/../b").is_err());
        assert!(identity(&storage, "elsewhere:///a").is_err());
    }

    #[test]
    fn azure_container_namespaces_are_distinct() {
        let storage = Storage::Azure {
            account: "testaccount".into(),
            access_key: "dGVzdA==".into(),
        };
        assert_ne!(
            identity(&storage, "az://one/weather/a.parquet").unwrap(),
            identity(&storage, "az://two/weather/a.parquet").unwrap()
        );
        assert!(identity(&storage, "az://one/weather/../a.parquet").is_err());
        let mut containers = HashMap::new();
        for path in [
            "az://one/weather/a.parquet",
            "az://one/weather/b.parquet",
            "az://two/weather/a.parquet",
            "az://two/weather/",
        ] {
            assert_eq!(
                cached_identity(&storage, path, &mut containers).unwrap(),
                identity(&storage, path).unwrap()
            );
        }
        assert_eq!(containers.len(), 2);
        let (first, _) = cached_resolve(&storage, "az://one/a", &mut containers).unwrap();
        let (second, _) = cached_resolve(&storage, "az://one/b", &mut containers).unwrap();
        let (other, _) = cached_resolve(&storage, "az://two/a", &mut containers).unwrap();
        assert!(Arc::ptr_eq(&first, &second));
        assert!(!Arc::ptr_eq(&first, &other));
        assert!(cached_identity(&storage, "az://one/weather/../a", &mut containers).is_err());
        assert!(cached_identity(&storage, "az:///bad", &mut containers).is_err());
    }
}
