//! Candidate discovery and expired-file cleanup; callers own scheduling and snapshot expiry.
use crate::{Config, Layout, TimeType, config::identifier, connect};
use anyhow::{Context, Result, bail};
use chrono::NaiveDateTime;
use tokio_postgres::GenericClient;

mod cleanup;
pub use cleanup::{
    CleanupFile, CleanupOptions, CleanupReport, CleanupScope, cleanup_expired_files,
};

#[derive(Debug)]
pub struct Candidate {
    pub begin: NaiveDateTime,
    pub end: NaiveDateTime,
    pub files: Vec<String>,
}

const CANDIDATES: &str = "
    WITH s2 AS (
        SELECT date_trunc('month', min_value) AS mo, path, min_value, max_value
          FROM pg_temp.maintenance_s1
         WHERE extract(hour FROM max_value) = 23 AND extract(hour FROM min_value) = 0
    )
    SELECT min(min_value) AS begin_date, max(max_value) AS end_date, array_agg(path) AS files
      FROM s2 GROUP BY mo
    HAVING min(min_value) = mo
       AND count(1) = extract(day FROM mo + interval '1 month' - interval '5 minutes')
     ORDER BY mo";

/// Runs inside the caller's transaction. Materialization stays separate from the
/// candidate query so casts cannot be pushed down onto unrelated column stats.
pub async fn table_candidates(
    client: &impl GenericClient,
    layout: &Layout,
    table: &str,
    granularity: chrono::Duration,
) -> Result<Vec<Candidate>> {
    layout.validate()?;
    identifier(table)?;
    let seconds = granularity.num_seconds();
    if seconds <= 0 || granularity != chrono::Duration::seconds(seconds) {
        bail!("maintenance granularity must be a positive whole number of seconds");
    }
    if layout.time_type == TimeType::Date && seconds % 86_400 != 0 {
        bail!("date maintenance granularity must be a whole number of days");
    }
    let ty = layout.time_type.sql();
    let local = |bound: &str| match layout.time_type {
        TimeType::TimestampTz => format!("s.{bound}::{ty} AT TIME ZONE $5"),
        TimeType::Timestamp | TimeType::Date => format!("s.{bound}::{ty}::timestamp"),
    };
    // Date files have inclusive date maxima rather than a final sub-day interval.
    let max = match layout.time_type {
        TimeType::Date => format!("({} + interval '23 hours')", local("max_value")),
        _ => local("max_value"),
    };
    let elapsed = match layout.time_type {
        TimeType::Date => "s.max_value::date::timestamp - s.min_value::date::timestamp".to_string(),
        _ => format!("s.max_value::{ty} - s.min_value::{ty}"),
    };
    let sql = format!(
        "
        CREATE TEMP TABLE maintenance_s1 ON COMMIT DROP AS
        SELECT f.path,
               extract(epoch FROM {elapsed}
                       + $3::bigint * interval '1 second') / 3600 AS hours,
               {} AS min_value, {max} AS max_value, f.record_count
          FROM ducklake.ducklake_data_file f
          INNER JOIN ducklake.ducklake_file_column_stats s USING (data_file_id, table_id)
          INNER JOIN ducklake.ducklake_table t USING (table_id)
          INNER JOIN ducklake.ducklake_schema sc USING (schema_id)
          INNER JOIN ducklake.ducklake_column c USING (table_id, column_id)
         WHERE f.end_snapshot IS NULL AND sc.schema_name = $1
           AND t.table_name = $2 AND c.column_name = $4",
        local("min_value")
    );
    if layout.time_type == TimeType::TimestampTz {
        client
            .execute(
                &sql,
                &[
                    &layout.schema,
                    &table,
                    &seconds,
                    &layout.time_column,
                    &layout.timezone.name(),
                ],
            )
            .await?;
    } else {
        client
            .execute(
                &sql,
                &[&layout.schema, &table, &seconds, &layout.time_column],
            )
            .await?;
    }
    client
        .query(CANDIDATES, &[])
        .await
        .context("finding maintenance ranges")?
        .into_iter()
        .map(|row| {
            Ok(Candidate {
                begin: row.try_get(0)?,
                end: row.try_get(1)?,
                files: row.try_get(2)?,
            })
        })
        .collect()
}

/// Owns one transaction, with the temporary table dropped on commit or rollback.
pub async fn discover(
    cfg: &Config,
    layout: &Layout,
    table: &str,
    granularity: chrono::Duration,
) -> Result<Vec<Candidate>> {
    let mut client = connect(cfg).await?;
    let tx = client.transaction().await?;
    let candidates = table_candidates(&tx, layout, table, granularity).await?;
    tx.commit()
        .await
        .context("dropping maintenance temp table")?;
    Ok(candidates)
}
