use super::{Source, TableMeta, location};
use anyhow::{Context, Result, bail};
use tokio_postgres::Transaction;

pub(super) async fn lock(tx: &Transaction<'_>) -> Result<()> {
    // Match cleanup's queue-first order. Acquire these before the first MVCC
    // read so publication/expiry checks see a consistent, writable catalog.
    tx.batch_execute(
        "LOCK TABLE ducklake.ducklake_files_scheduled_for_deletion IN SHARE ROW EXCLUSIVE MODE;
         LOCK TABLE ducklake.ducklake_data_file, ducklake.ducklake_delete_file,
                    ducklake.ducklake_snapshot IN SHARE ROW EXCLUSIVE MODE;
         LOCK TABLE ducklake.ducklake_table, ducklake.ducklake_schema,
                    ducklake.ducklake_metadata IN SHARE MODE;",
    )
    .await
    .context("locking catalog for repack snapshot expiry")
}

/// Expire only the commit's predecessor. Collect only this repack's obsolete
/// files; unrelated obsolete catalog objects can be reclaimed by native expiry.
pub(super) async fn predecessor(
    tx: &Transaction<'_>,
    meta: &TableMeta,
    selected: &[Source],
    snapshot: i64,
) -> Result<()> {
    let n = tx
        .execute(
            "DELETE FROM ducklake.ducklake_snapshot
             WHERE snapshot_id = $1 AND snapshot_id < (
                 SELECT max(snapshot_id) FROM ducklake.ducklake_snapshot)",
            &[&snapshot],
        )
        .await
        .context("expiring repack predecessor snapshot")?;
    if n != 1 {
        bail!("repack predecessor snapshot changed before expiry");
    }
    // Keep snapshot_changes: transactions that started before expiry still need
    // those records for conflict detection, just as with native DuckLake expiry.
    let source_ids: Vec<_> = selected.iter().map(|s| s.id).collect();
    let data = tx
        .query(
            "DELETE FROM ducklake.ducklake_data_file f
             WHERE f.table_id = $1 AND f.data_file_id = ANY($2)
               AND f.end_snapshot IS NOT NULL
               AND NOT EXISTS (
                   SELECT 1 FROM ducklake.ducklake_snapshot s
                   WHERE s.snapshot_id >= f.begin_snapshot
                     AND s.snapshot_id < f.end_snapshot)
             RETURNING data_file_id, path, path_is_relative",
            &[&meta.table_id, &source_ids],
        )
        .await
        .context("retiring unreferenced repack data-file metadata")?;
    let removed: Vec<i64> = data.iter().map(|r| r.get(0)).collect();
    // Delete files attached to removed data files are no longer useful, even
    // if their own visibility interval contains a retained snapshot.
    let deletes = tx
        .query(
            "DELETE FROM ducklake.ducklake_delete_file f
             WHERE f.table_id = $1 AND f.data_file_id = ANY($2)
               AND (f.data_file_id = ANY($3) OR (
                   f.end_snapshot IS NOT NULL AND NOT EXISTS (
                       SELECT 1 FROM ducklake.ducklake_snapshot s
                       WHERE s.snapshot_id >= f.begin_snapshot
                         AND s.snapshot_id < f.end_snapshot)))
             RETURNING delete_file_id, path, path_is_relative",
            &[&meta.table_id, &source_ids, &removed],
        )
        .await
        .context("retiring unreferenced repack delete-file metadata")?;
    if !removed.is_empty() {
        for table in [
            "ducklake_file_column_stats",
            "ducklake_file_partition_value",
            "ducklake_file_variant_stats",
        ] {
            tx.execute(
                &format!(
                    "DELETE FROM ducklake.{table} WHERE table_id = $1 AND data_file_id = ANY($2)"
                ),
                &[&meta.table_id, &removed],
            )
            .await
            .with_context(|| format!("removing expired repack metadata from {table}"))?;
        }
    }
    for row in data.iter().chain(&deletes) {
        let id: i64 = row.try_get(0)?;
        let path: String = row.try_get(1)?;
        let path = location(
            meta,
            &path,
            row.try_get::<_, Option<bool>>(2)? == Some(true),
        );
        tx.execute(
            "INSERT INTO ducklake.ducklake_files_scheduled_for_deletion
             (data_file_id, path, path_is_relative, schedule_start)
             VALUES ($1, $2, false, now())",
            &[&id, &path],
        )
        .await
        .with_context(|| format!("scheduling expired repack file {path} for cleanup"))?;
    }
    Ok(())
}
