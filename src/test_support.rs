use crate::*;
use datafusion::arrow::datatypes::SchemaRef;

pub fn layout() -> Layout {
    Layout {
        schema: "ciso".into(),
        time_column: "utcbegin".into(),
        row_group_col: Some("node".into()),
        sort_cols: vec!["node".into(), "utcbegin".into()],
        dedup_timestamp: "downloaded_at".into(),
        timezone: chrono_tz::America::Los_Angeles,
        lock_timezone: chrono_tz::America::Los_Angeles,
        author: "fixture".into(),
        lock_namespace: "rustciso.repack".into(),
        ..Layout::default()
    }
}

pub fn job_from_value(value: serde_json::Value) -> serde_json::Result<repack::Job> {
    let mut job: repack::Job = serde_json::from_value(value)?;
    job.layout = layout();
    Ok(job)
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
