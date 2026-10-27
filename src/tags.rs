//! Validated, atomic updates of dkplus policy tags across existing catalog tables.

use crate::*;
use serde_json::Value;
use std::collections::{BTreeMap, HashSet};

/// Table names mapped to setting names and JSON values. Names may omit `dkplus.`.
/// JSON null disables nullable settings; it does not delete a tag.
pub type Updates = BTreeMap<String, BTreeMap<String, Value>>;

fn normalize(settings: &BTreeMap<String, Value>) -> Result<HashMap<String, String>> {
    let mut tags = HashMap::new();
    for (name, value) in settings {
        let key = format!("dkplus.{}", name.strip_prefix("dkplus.").unwrap_or(name));
        match key.as_str() {
            TAG_TIME_COLUMN | TAG_TIMEZONE => {
                if !value.is_string() {
                    fail!(InvalidInput, "{name} must be a string");
                }
            }
            TAG_ROW_GROUP_COL | TAG_DEDUP_TIMESTAMP | TAG_GRANULARITY => {
                if !value.is_string() && !value.is_null() {
                    fail!(InvalidInput, "{name} must be a string or null");
                }
            }
            TAG_SORT_COLS | TAG_UNIQUE_BY_COLS => {
                if !(key == TAG_UNIQUE_BY_COLS && value.is_null()) {
                    let names: Vec<String> = serde_json::from_value(value.clone())
                        .with_context(|| format!("{name} must be an array of column names"))?;
                    if names.is_empty() || names.iter().collect::<HashSet<_>>().len() != names.len()
                    {
                        fail!(
                            InvalidInput,
                            "{name} must contain nonempty, distinct column names"
                        );
                    }
                }
            }
            _ if key.starts_with(TAG_LEVEL_PREFIX) => {
                if !value.is_string() && !value.is_null() {
                    fail!(InvalidInput, "{name} must be a string or null");
                }
                let single = HashMap::from([(key.clone(), value.to_string())]);
                for (_, rule, _) in level_tags(&single)?.into_values().flatten() {
                    if rule != "calendar_begin" && !LEVEL_INTERVAL_RULES.contains(&rule) {
                        fail!(InvalidInput, "unknown repack level rule {name:?}");
                    }
                }
            }
            _ => fail!(InvalidInput, "unknown dkplus setting {name:?}"),
        }
        if tags.insert(key.clone(), value.to_string()).is_some() {
            fail!(InvalidInput, "duplicate setting {key:?}");
        }
    }
    Ok(tags)
}

async fn validate(
    tx: &impl GenericClient,
    schema: &str,
    table: &str,
    meta: &TableMeta,
    tags: &HashMap<String, String>,
    columns: &ColumnTags,
) -> Result<()> {
    let time: Option<String> = tag_value(tags, TAG_TIME_COLUMN)?;
    let time_col = time
        .as_ref()
        .map(|name| {
            meta.columns
                .get(name)
                .with_context(|| format!("table has no time column {name:?}"))
        })
        .transpose()?;
    let mut policy = Table {
        schema: schema.to_owned(),
        table: table.to_owned(),
        ..Table::default()
    };
    if let Some(col) = time_col {
        policy.time_type = match types::arrow_type(&col.ty)? {
            DataType::Date32 => TimeType::Date,
            DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => TimeType::TimestampTz,
            DataType::Timestamp(TimeUnit::Microsecond, None) => TimeType::Timestamp,
            _ => fail!(
                InvalidInput,
                "time_column must be a date or microsecond timestamp"
            ),
        };
    }
    if let Some(name) = time {
        policy.time_column = name;
    }
    policy.row_group_col = tag_value::<Option<String>>(tags, TAG_ROW_GROUP_COL)?.flatten();
    // Missing required settings are allowed here, but never persisted as defaults.
    policy.sort_cols = tag_value(tags, TAG_SORT_COLS)?.unwrap_or_else(|| {
        vec![
            policy
                .row_group_col
                .clone()
                .unwrap_or_else(|| policy.time_column.clone()),
        ]
    });
    policy.unique_by_cols = tag_value::<Option<Vec<String>>>(tags, TAG_UNIQUE_BY_COLS)?.flatten();
    policy.dedup_timestamp = tag_value::<Option<String>>(tags, TAG_DEDUP_TIMESTAMP)?.flatten();
    let mut names = Vec::new();
    for key in [TAG_TIME_COLUMN, TAG_ROW_GROUP_COL, TAG_DEDUP_TIMESTAMP] {
        if let Some(name) = tag_value::<Option<String>>(tags, key)?.flatten() {
            names.push(name);
        }
    }
    for key in [TAG_SORT_COLS, TAG_UNIQUE_BY_COLS] {
        if let Some(list) = tag_value::<Option<Vec<String>>>(tags, key)?.flatten() {
            if list.iter().collect::<HashSet<_>>().len() != list.len() {
                fail!(InvalidInput, "{key} must contain distinct column names");
            }
            names.extend(list);
        }
    }
    for name in names {
        if !meta.columns.contains_key(&name) {
            fail!(InvalidInput, "configured column {name:?} is not in table");
        }
    }
    if let Some(name) = &policy.dedup_timestamp
        && !matches!(
            types::arrow_type(&meta.columns[name].ty)?,
            DataType::Timestamp(TimeUnit::Microsecond, _)
        )
    {
        fail!(
            InvalidInput,
            "dedup timestamp column {name:?} must be a microsecond timestamp"
        );
    }
    let effective = |key| {
        time_col
            .and_then(|col| time_column_tag(tags, columns, col.id, key))
            .or_else(|| tags.get(key).map(|value| (value, false)))
    };
    if let Some((value, column)) = effective(TAG_TIMEZONE) {
        let zone: String = decode_tag(value, TAG_TIMEZONE, column)?;
        policy.timezone = zone
            .parse()
            .with_context(|| format!("invalid timezone {zone:?}"))?;
    }
    let granularity = effective(TAG_GRANULARITY)
        .map(|(value, column)| decode_tag::<Option<String>>(value, TAG_GRANULARITY, column))
        .transpose()?
        .flatten();
    let mut texts = Vec::new();
    if let Some(text) = &granularity
        && text != UNPREDICTABLE
    {
        texts.push(text.clone());
    }
    for (key, rule, value) in level_tags(tags)?.into_values().flatten() {
        if LEVEL_INTERVAL_RULES.contains(&rule)
            && let Some(text) = decode_tag::<Option<String>>(value, key, false)?
        {
            texts.push(text);
        }
    }
    texts.sort();
    texts.dedup();
    let intervals = parse_intervals(tx, texts).await?;
    policy.granularity = granularity
        .map(|text| {
            if text == UNPREDICTABLE {
                Ok(maintenance::Granularity::Unpredictable)
            } else {
                interval(&intervals, &text).map(maintenance::Granularity::Fixed)
            }
        })
        .transpose()?;
    policy.repack_levels = resolve_levels(tags, &intervals)?;
    policy.validate()
}

struct Change {
    table_id: i64,
    column_id: Option<i64>,
    key: String,
    value: String,
    replace: bool,
}

/// Validate and persist all tables in one transaction and one snapshot.
///
/// Tables must be qualified as `schema.table`. Column references and the
/// resulting partial policy are validated before any writes. Timezone and
/// granularity are column tags on the new or existing `time_column`.
/// Returns the current snapshot without incrementing it for a no-op.
/// This operation needs no object-store credentials.
pub async fn update(postgres: &tokio_postgres::Config, updates: Updates) -> Result<i64> {
    if updates.is_empty() || updates.values().any(BTreeMap::is_empty) {
        fail!(
            InvalidInput,
            "provide at least one table and at least one setting per table"
        );
    }
    let mut normalized = Vec::new();
    for (name, settings) in updates {
        let (schema, table) = name
            .split_once('.')
            .context("table name must be schema.table")?;
        if table.contains('.') {
            fail!(InvalidInput, "table name must be schema.table");
        }
        identifier(schema)?;
        identifier(table)?;
        normalized.push((
            name.clone(),
            schema.to_owned(),
            table.to_owned(),
            normalize(&settings)
                .with_context(|| format!("validating settings for {name}"))
                .map_err(|error| error.for_table(&name))?,
        ));
    }
    let mut client = connect_catalog(postgres, Arc::new(|message| eprintln!("{message}"))).await?;
    let tx = client.transaction().await?;
    tx.batch_execute("LOCK TABLE ducklake.ducklake_snapshot IN SHARE ROW EXCLUSIVE MODE")
        .await
        .context("locking snapshots for policy update")?;
    let latest = tx
        .query_one(
            "SELECT snapshot_id, schema_version, next_catalog_id, next_file_id
           FROM ducklake.ducklake_snapshot ORDER BY snapshot_id DESC LIMIT 1",
            &[],
        )
        .await
        .context("reading latest catalog snapshot")?;
    let previous: i64 = latest.get(0);
    let mut changes = Vec::new();
    for (name, schema, table, requested) in normalized {
        let row = tx
            .query_opt(
                "SELECT t.table_id FROM ducklake.ducklake_table t
               JOIN ducklake.ducklake_schema s USING (schema_id)
              WHERE s.schema_name = $1 AND t.table_name = $2
                AND s.end_snapshot IS NULL AND t.end_snapshot IS NULL",
                &[&schema, &table],
            )
            .await
            .context("looking up DuckLake table")?
            .ok_or_else(|| Error::TableNotFound {
                table: name.clone(),
            })?;
        let table_id: i64 = row.get(0);
        let mut meta = TableMeta {
            table_id,
            root: String::new(),
            columns: HashMap::new(),
        };
        for row in tx
            .query(
                "SELECT column_name, column_id, column_type FROM ducklake.ducklake_column
              WHERE table_id = $1 AND end_snapshot IS NULL AND parent_column IS NULL",
                &[&table_id],
            )
            .await
            .context("reading DuckLake columns")?
        {
            meta.columns.insert(
                row.get(0),
                Column {
                    id: row.get(1),
                    ty: row.get(2),
                },
            );
        }
        let mut active = table_tags(&tx, table_id).await?;
        let mut columns = column_tags(&tx, table_id).await?;
        let mut effective = active.clone();
        effective.extend(requested.clone());
        let time: Option<String> = tag_value(&effective, TAG_TIME_COLUMN)?;
        for (key, value) in requested {
            let column_id = if key == TAG_TIMEZONE || key == TAG_GRANULARITY {
                let time = time.as_ref().ok_or_else(|| Error::MissingRequiredSetting {
                    table: name.clone(),
                    setting: "time_column",
                })?;
                Some(
                    meta.columns
                        .get(time)
                        .ok_or_else(|| Error::InvalidConfiguration {
                            table: name.clone(),
                            reason: format!("column {time:?} does not exist"),
                        })?
                        .id,
                )
            } else {
                None
            };
            let target = match column_id {
                Some(id) => columns.entry(id).or_default(),
                None => &mut active,
            };
            if target.get(&key) != Some(&value) {
                let replace = target.insert(key.clone(), value.clone()).is_some();
                changes.push(Change {
                    table_id,
                    column_id,
                    key,
                    value,
                    replace,
                });
            }
        }
        validate(&tx, &schema, &table, &meta, &active, &columns)
            .await
            .with_context(|| format!("validating policy for {name}"))
            .map_err(|error| error.for_table(&name))?;
    }
    if changes.is_empty() {
        tx.commit()
            .await
            .context("finishing unchanged policy update")?;
        return Ok(previous);
    }
    let snapshot = previous.checked_add(1).context("snapshot ID overflow")?;
    tx.execute(
        "INSERT INTO ducklake.ducklake_snapshot
           (snapshot_id, snapshot_time, schema_version, next_catalog_id, next_file_id)
         VALUES ($1, now(), $2, $3, $4)",
        &[
            &snapshot,
            &latest.get::<_, i64>(1),
            &latest.get::<_, i64>(2),
            &latest.get::<_, i64>(3),
        ],
    )
    .await
    .context("creating policy snapshot")?;
    let changed_tables: std::collections::BTreeSet<_> =
        changes.iter().map(|c| c.table_id).collect();
    let summary = changed_tables
        .iter()
        .map(|id| format!("altered_table:{id}"))
        .collect::<Vec<_>>()
        .join(",");
    tx.execute(
        "INSERT INTO ducklake.ducklake_snapshot_changes
           (snapshot_id, changes_made, author, commit_message, commit_extra_info)
         VALUES ($1, $2, 'dkplus', 'updated dkplus policy tags', NULL)",
        &[&snapshot, &summary],
    )
    .await
    .context("recording policy snapshot")?;
    for change in changes {
        let Change {
            table_id,
            column_id,
            key,
            value,
            replace,
        } = change;
        if replace {
            let ended = match column_id {
                Some(id) => {
                    tx.execute(
                        "UPDATE ducklake.ducklake_column_tag SET end_snapshot = $5
                      WHERE table_id = $1 AND column_id = $2 AND key = $3
                        AND begin_snapshot <= $4 AND (end_snapshot IS NULL OR end_snapshot > $4)",
                        &[&table_id, &id, &key, &previous, &snapshot],
                    )
                    .await
                }
                None => {
                    tx.execute(
                        "UPDATE ducklake.ducklake_tag SET end_snapshot = $4
                      WHERE object_id = $1 AND key = $2
                        AND begin_snapshot <= $3 AND (end_snapshot IS NULL OR end_snapshot > $3)",
                        &[&table_id, &key, &previous, &snapshot],
                    )
                    .await
                }
            }
            .with_context(|| format!("ending active tag {key:?}"))?;
            if ended != 1 {
                fail!(CatalogChanged, "active tag {key:?} changed during update");
            }
        }
        match column_id {
            Some(id) => tx.execute(
                "INSERT INTO ducklake.ducklake_column_tag
                   (table_id, column_id, begin_snapshot, end_snapshot, key, value)
                 VALUES ($1, $2, $3, NULL, $4, $5)",
                &[&table_id, &id, &snapshot, &key, &value]
            ).await,
            None => tx.execute(
                "INSERT INTO ducklake.ducklake_tag (object_id, begin_snapshot, end_snapshot, key, value)
                 VALUES ($1, $2, NULL, $3, $4)",
                &[&table_id, &snapshot, &key, &value]
            ).await,
        }.with_context(|| format!("writing tag {key:?}"))?;
    }
    tx.commit().await.context("committing policy snapshot")?;
    Ok(snapshot)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn setting_shapes_and_names_are_checked() {
        for value in [
            serde_json::json!({"unknown": "x"}),
            serde_json::json!({"timezone": null}),
            serde_json::json!({"sort_cols": "a,b"}),
            serde_json::json!({"sort_cols": []}),
            serde_json::json!({"sort_cols": ["a", "a"]}),
            serde_json::json!({"time_column": "a", "dkplus.time_column": "b"}),
            serde_json::json!({"repack_level0_age_min": "1 day"}),
            serde_json::json!({"repack_level1_typo": null}),
        ] {
            assert!(
                normalize(&serde_json::from_value(value.clone()).unwrap()).is_err(),
                "{value}"
            );
        }
        normalize(
            &serde_json::from_value(serde_json::json!({
                "time_column": "observed", "timezone": "UTC", "row_group_col": null,
                "sort_cols": ["observed"], "repack_level1_age_min": "1 day",
                "repack_level1_relage_min": "1 day", "repack_level1_relage_max": "2 days"
            }))
            .unwrap(),
        )
        .unwrap();
    }
}
