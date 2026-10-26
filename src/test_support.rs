use crate::*;
use datafusion::arrow::datatypes::SchemaRef;

pub fn table() -> Table {
    Table {
        schema: "ciso".into(),
        table: "rtprices".into(),
        time_column: "utcbegin".into(),
        row_group_col: Some("node".into()),
        sort_cols: vec!["node".into(), "utcbegin".into()],
        unique_by_cols: None,
        dedup_timestamp: Some("downloaded_at".into()),
        timezone: chrono_tz::America::Los_Angeles,
        author: Some("fixture".into()),
        lock_namespace: "rustciso.repack".into(),
        ..Table::default()
    }
}

/// Deserializes a job against the fixture table. Legacy `begin_date` and
/// `end_date` strings become fixture-timezone midnight bounds.
pub fn job_from_value(mut value: serde_json::Value) -> serde_json::Result<repack::Job> {
    use serde::de::Error;
    let qualified = value["table"]
        .as_str()
        .is_some_and(|name| name.contains('.'));
    if let Some(object) = value.as_object_mut() {
        for (legacy, key) in [("begin_date", "begin"), ("end_date", "end")] {
            let Some(date) = object.remove(legacy) else {
                continue;
            };
            let date: chrono::NaiveDate = serde_json::from_value(date)?;
            let bound = table().midnight(date).map_err(serde_json::Error::custom)?;
            object.insert(key.into(), serde_json::to_value(repack::Bound::Tz(bound))?);
        }
    }
    let mut job: repack::Job = serde_json::from_value(value)?;
    let mut table = table();
    if qualified {
        table.schema = job.table.schema.clone();
    }
    table.table = job.table.table.clone();
    job.table = table;
    Ok(job)
}

pub fn captured_log() -> (Log, Arc<std::sync::Mutex<Vec<String>>>) {
    let messages = Arc::new(std::sync::Mutex::new(Vec::new()));
    let captured = messages.clone();
    (
        Arc::new(move |message| captured.lock().unwrap().push(message.into())),
        messages,
    )
}

pub fn target_schema(prefix: &str) -> SchemaRef {
    let time = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let mut fields = vec![
        Field::new("utcbegin", time.clone(), true),
        Field::new("node", DataType::Utf8, true),
    ];
    for suffix in ["lmp", "mcc", "mce", "mcl", "mghg"] {
        fields.push(Field::new(
            format!("{prefix}{suffix}"),
            DataType::Float64,
            true,
        ));
    }
    fields.push(Field::new("downloaded_at", time, true));
    Arc::new(Schema::new(fields))
}
