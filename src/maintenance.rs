//! Tag-driven repack planning and expired-file cleanup; callers own scheduling.
use crate::error::{Context, fail};
use crate::repack::{Bound, ExpireSnapshot, Strictness};
use crate::{Error, Result};
use crate::{Table, TimeType, connect, table_meta, types};
use chrono::{DateTime, Utc};
use datafusion::arrow::datatypes::DataType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio_postgres::{GenericClient, IsolationLevel};

mod cleanup;
mod plan;
mod rules;
pub use cleanup::{
    CleanupFile, CleanupOptions, CleanupReport, CleanupScope, cleanup_expired_files,
};
pub(crate) use rules::Clock;
pub use rules::{CalendarUnit, Granularity, Interval, RepackLevel};

/// One group of current files that a level allows to be repacked into
/// `[begin, end)`. Serializable so it can be sent to a worker.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PotentialRepack {
    /// Qualified `schema.table` name.
    pub table: String,
    pub level: RepackLevel,
    pub granularity: Granularity,
    pub begin: Bound,
    /// Exclusive.
    pub end: Bound,
    pub files: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RepackOutcome {
    Repacked,
    /// The plan no longer holds against current statistics.
    Skipped(String),
}

fn utc_sql(ty: TimeType, value: &str) -> String {
    match ty {
        TimeType::TimestampTz => format!("{value}::timestamptz"),
        TimeType::Timestamp => format!("{value}::timestamp AT TIME ZONE 'UTC'"),
        TimeType::Date => format!("{value}::date::timestamp AT TIME ZONE 'UTC'"),
    }
}

fn column_time_type(ty: &str) -> Result<TimeType> {
    Ok(match types::arrow_type(ty)? {
        DataType::Date32 => TimeType::Date,
        DataType::Timestamp(_, Some(_)) => TimeType::TimestampTz,
        DataType::Timestamp(_, None) => TimeType::Timestamp,
        other => fail!(
            InvalidInput,
            "dedup timestamp column type {other} is not a date or timestamp"
        ),
    })
}

/// Reads current files' time and dedup-timestamp statistics; no data files are read.
async fn load_files(client: &impl GenericClient, table: &Table) -> Result<Vec<plan::FileStat>> {
    let meta = table_meta(client, &table.schema, &table.table).await?;
    let time = meta
        .columns
        .get(&table.time_column)
        .with_context(|| format!("{table} has no column {}", table.time_column))?;
    let dedup = match &table.dedup_timestamp {
        Some(name) => {
            let column = meta
                .columns
                .get(name)
                .with_context(|| format!("{table} has no dedup timestamp column {name}"))?;
            Some((column.id, column_time_type(&column.ty)?))
        }
        None => None,
    };
    // Casts stay in the select list so they only see the joined column's stats.
    let dedup_ty = dedup.map_or(TimeType::TimestampTz, |(_, ty)| ty);
    let sql = format!(
        "SELECT f.path, {}, {}, {}, {}
           FROM ducklake.ducklake_data_file f
           LEFT JOIN ducklake.ducklake_file_column_stats t
             ON t.table_id = f.table_id AND t.data_file_id = f.data_file_id AND t.column_id = $2
           LEFT JOIN ducklake.ducklake_file_column_stats d
             ON d.table_id = f.table_id AND d.data_file_id = f.data_file_id AND d.column_id = $3
          WHERE f.table_id = $1 AND f.end_snapshot IS NULL
          ORDER BY f.data_file_id",
        utc_sql(table.time_type, "t.min_value"),
        utc_sql(table.time_type, "t.max_value"),
        utc_sql(dedup_ty, "d.min_value"),
        utc_sql(dedup_ty, "d.max_value"),
    );
    let dedup_id = dedup.map(|(id, _)| id);
    let rows = client
        .query(&sql, &[&meta.table_id, &time.id, &dedup_id])
        .await
        .context("loading maintenance file statistics")?;
    rows.into_iter()
        .map(|row| {
            let path: String = row.try_get(0)?;
            let min: Option<DateTime<Utc>> = row.try_get(1)?;
            let max: Option<DateTime<Utc>> = row.try_get(2)?;
            let (Some(min), Some(max)) = (min, max) else {
                fail!(
                    InvalidData,
                    "{table}: file {path} has missing or null {} statistics",
                    table.time_column
                );
            };
            Ok(plan::FileStat {
                path,
                min,
                max,
                dedup_min: row.try_get(3)?,
                dedup_max: row.try_get(4)?,
            })
        })
        .collect()
}

fn context(table: &Table, granularity: Granularity, files: &[plan::FileStat]) -> plan::Context {
    let (clock, epsilon) = match table.time_type {
        TimeType::TimestampTz => (Clock(Some(table.timezone)), Interval::micros(1)),
        TimeType::Timestamp => (Clock(None), Interval::micros(1)),
        TimeType::Date => (Clock(None), Interval::days(1)),
    };
    plan::Context {
        clock,
        granularity,
        epsilon,
        now: Utc::now(),
        has_dedup: table.dedup_timestamp.is_some(),
        table_max: files
            .iter()
            .map(|f| f.max)
            .max()
            .unwrap_or(DateTime::<Utc>::MIN_UTC),
    }
}

/// Finds non-overlapping repacks for every configured level using only catalog
/// statistics, inside the caller's transaction. Lower levels claim files
/// first; within a level, earlier groups win.
pub async fn find_potential_repacks(
    client: &impl GenericClient,
    table: &Table,
) -> Result<Vec<PotentialRepack>> {
    table.validate()?;
    if table.repack_levels.is_empty() {
        return Ok(Vec::new());
    }
    let granularity = table
        .granularity
        .ok_or_else(|| Error::MissingRequiredSetting {
            table: table.to_string(),
            setting: "granularity",
        })?;
    let files = load_files(client, table).await?;
    let context = context(table, granularity, &files);
    let mut entries = Vec::new();
    for level in &table.repack_levels {
        if level.uses_dedup_timestamp() && !context.has_dedup {
            (table.cfg.log)(&format!(
                "{table}: skipping repack level {} because age/relage rules need a dedup timestamp",
                level.level
            ));
            continue;
        }
        for group in plan::plan_level(&context, level, &files) {
            entries.push((
                level.level,
                group.min,
                PotentialRepack {
                    table: table.to_string(),
                    level: level.clone(),
                    granularity,
                    begin: Bound::for_table(table, group.begin),
                    end: Bound::for_table(table, group.end),
                    files: group.files,
                },
            ));
        }
    }
    Ok(plan::dedupe(entries, |entry| entry.files.as_slice()))
}

impl Table {
    /// [`find_potential_repacks`] in its own read-only snapshot.
    pub async fn find_potential_repacks(&self) -> Result<Vec<PotentialRepack>> {
        let mut client = connect(&self.cfg).await?;
        let tx = client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await?;
        let repacks = find_potential_repacks(&tx, self).await?;
        tx.commit().await?;
        (self.cfg.log)(&format!(
            "{self}: found {} potential repacks",
            repacks.len()
        ));
        Ok(repacks)
    }
}

/// Re-checks `repack` against current statistics and runs it. Files outside
/// the plan that hold rows in the window, or plan files with rows outside it,
/// are handled by `strictness`.
///
/// Expires every catalog snapshot referencing a replaced file, even when
/// deduplicating. This removes time travel to those versions for all tables.
/// Replaced files are queued for [`cleanup_expired_files`], not physically deleted.
pub async fn do_repack(
    table: &Table,
    repack: &PotentialRepack,
    strictness: Strictness,
) -> Result<RepackOutcome> {
    if repack.table != table.to_string() {
        fail!(
            InvalidInput,
            "repack is for {} but table is {table}",
            repack.table
        );
    }
    let files = {
        let mut client = connect(&table.cfg).await?;
        let tx = client
            .build_transaction()
            .isolation_level(IsolationLevel::RepeatableRead)
            .read_only(true)
            .start()
            .await?;
        let files = load_files(&tx, table).await?;
        tx.commit().await?;
        files
    };
    let context = context(table, repack.granularity, &files);
    let by_path: HashMap<&str, &plan::FileStat> =
        files.iter().map(|f| (f.path.as_str(), f)).collect();
    let mut members = Vec::with_capacity(repack.files.len());
    for path in &repack.files {
        match by_path.get(path.as_str()) {
            Some(file) => members.push(*file),
            None => {
                return Ok(skip(table, format!("file {path} is no longer current")));
            }
        }
    }
    if repack.level.uses_dedup_timestamp() && !context.has_dedup {
        return Ok(skip(
            table,
            "age/relage rules need a dedup timestamp".into(),
        ));
    }
    let (begin, end) = match plan::evaluate(&context, &repack.level, &members) {
        Ok(window) => window,
        Err(reason) => return Ok(skip(table, reason)),
    };
    if (begin, end) != (repack.begin.utc(), repack.end.utc()) {
        return Ok(skip(table, format!("window is now [{begin}, {end})")));
    }
    table
        .repack_between(repack.begin, repack.end)
        .with_files(repack.files.clone())
        .with_strictness(strictness)
        .with_expire_snapshot(ExpireSnapshot::Referencing)
        .run()
        .await?;
    Ok(RepackOutcome::Repacked)
}

fn skip(table: &Table, reason: String) -> RepackOutcome {
    (table.cfg.log)(&format!("{table}: skipping repack: {reason}"));
    RepackOutcome::Skipped(reason)
}
