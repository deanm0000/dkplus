use crate::{Config, Storage, catalog_table_root, config::identifier, connect, join_dir};
use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use object_store::ObjectStoreExt;
use std::collections::{HashMap, HashSet};

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

// Store keys alone are insufficient for Azure: containers are separate namespaces.
fn identity(storage: &Storage, path: &str) -> Result<String> {
    if path.split('/').any(|part| part == "." || part == "..") {
        bail!("cleanup path contains a relative traversal component: {path}");
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
    if let CleanupScope::Table { schema, table } = &options.scope {
        identifier(schema)?;
        identifier(table)?;
    }
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
        CleanupScope::Table { .. } => {
            let id = target.context("cleanup table not found")?;
            let directory = identity(&cfg.storage, &roots[&id])?;
            // An empty key is the entire store, not an exclusive table directory.
            if directory.is_empty() {
                bail!("table cleanup requires an exclusive, non-root data directory");
            }
            for (other_id, root) in &roots {
                if *other_id != id {
                    let other = identity(&cfg.storage, root)?;
                    if in_directory(&other, &directory) || in_directory(&directory, &other) {
                        bail!("table cleanup is ambiguous: overlapping table data directories");
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
             WHERE schedule_start < $1 ORDER BY schedule_start, data_file_id",
            &[&options.older_than],
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
        let key = identity(&cfg.storage, &path)?;
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
            referenced.insert(identity(&cfg.storage, &path)?);
        }
        for file in &report.files {
            if referenced.contains(&identity(&cfg.storage, &file.path)?) {
                bail!(
                    "queued file is still referenced by catalog metadata: {}",
                    file.path
                );
            }
        }
    }
    if !options.dry_run {
        for (file, (raw_path, relative)) in report.files.iter().zip(&queue_paths) {
            let (store, key) = cfg.storage.resolve(&file.path)?;
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
    }
}
