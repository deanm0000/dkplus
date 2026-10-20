use chrono::{NaiveDate, TimeZone, Utc};
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{Field, Schema};
use dkplus::{Config, Layout, Storage, TimeType, Window};
use object_store::{ObjectStore, memory::InMemory};
use std::sync::Arc;

const CATALOG: &str = "
CREATE SCHEMA ducklake;
CREATE TABLE ducklake.ducklake_schema (
    schema_id bigint, schema_name text, path text, path_is_relative bool, end_snapshot bigint);
CREATE TABLE ducklake.ducklake_table (
    table_id bigint, schema_id bigint, table_name text, path text, path_is_relative bool, end_snapshot bigint);
CREATE TABLE ducklake.ducklake_metadata (key text, value text, scope text);
CREATE TABLE ducklake.ducklake_partition_info (table_id bigint, end_snapshot bigint);
CREATE TABLE ducklake.ducklake_column (
    table_id bigint, column_id bigint, column_name text, column_type text, parent_column bigint, end_snapshot bigint);
CREATE TABLE ducklake.ducklake_inlined_data_tables (table_id bigint, table_name text);
CREATE TABLE ducklake.ducklake_snapshot (
    snapshot_id bigint PRIMARY KEY, snapshot_time timestamptz, schema_version bigint,
    next_catalog_id bigint, next_file_id bigint);
CREATE TABLE ducklake.ducklake_snapshot_changes (
    snapshot_id bigint, changes_made text, author text, commit_message text, commit_extra_info text);
CREATE TABLE ducklake.ducklake_data_file (
    data_file_id bigint PRIMARY KEY, table_id bigint, begin_snapshot bigint, end_snapshot bigint,
    file_order bigint, path text, path_is_relative bool, file_format text, record_count bigint,
    file_size_bytes bigint, footer_size bigint, row_id_start bigint, partition_id bigint,
    encryption_key text, mapping_id bigint, partial_max bigint);
CREATE TABLE ducklake.ducklake_delete_file (
    delete_file_id bigint, table_id bigint, data_file_id bigint, begin_snapshot bigint, end_snapshot bigint,
    path text, path_is_relative bool, delete_count bigint, format text, encryption_key text, partial_max bigint);
CREATE TABLE ducklake.ducklake_file_column_stats (
    data_file_id bigint, table_id bigint, column_id bigint, column_size_bytes bigint,
    value_count bigint, null_count bigint, min_value text, max_value text, contains_nan bool, extra_stats text);
CREATE TABLE ducklake.ducklake_table_stats (
    table_id bigint PRIMARY KEY, record_count bigint, next_row_id bigint, file_size_bytes bigint);
CREATE TABLE ducklake.ducklake_table_column_stats (
    table_id bigint, column_id bigint, contains_null bool, contains_nan bool,
    min_value text, max_value text, extra_stats text);
INSERT INTO ducklake.ducklake_snapshot VALUES (0, now(), 0, 1, 1);
INSERT INTO ducklake.ducklake_schema VALUES (1, 'weather', 'weather/', true, NULL);
";

#[tokio::test]
#[ignore = "requires DKPLUS_TEST_DATABASE_URL pointing to an isolated Postgres database"]
async fn non_ciso_load_repack_and_typed_catalog_statistics() {
    exercise_catalog(Storage::Store {
        root: "memory:///".into(),
        store: Arc::new(InMemory::new()),
    })
    .await;
}

#[tokio::test]
#[ignore = "requires DKPLUS_TEST_DATABASE_URL pointing to an isolated Postgres database"]
async fn filesystem_catalog_paths_load_and_repack() {
    let dir = std::env::temp_dir().join(format!("dkplus-catalog-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir(&dir).unwrap();
    exercise_catalog(Storage::local(&dir).unwrap()).await;
    std::fs::remove_dir_all(&dir).unwrap();
}

async fn exercise_catalog(storage: Storage) {
    let url = std::env::var("DKPLUS_TEST_DATABASE_URL").unwrap();
    let postgres: tokio_postgres::Config = url.parse().unwrap();
    let (client, connection) = postgres.connect(tokio_postgres::NoTls).await.unwrap();
    let connection = tokio::spawn(async move { connection.await.unwrap() });
    // Run only against a deliberately empty test database.
    client.batch_execute(CATALOG).await.unwrap();
    let root = match &storage {
        Storage::Store { root, .. } if root.starts_with("file://") => format!(
            "{}/",
            url::Url::parse(root)
                .unwrap()
                .to_file_path()
                .unwrap()
                .display()
        ),
        Storage::Store { root, .. } => root.clone(),
        Storage::Azure { .. } => unreachable!(),
    };
    let (store, _) = storage.resolve(&root).unwrap();
    let cfg = Config {
        postgres,
        storage,
        repack_run_bytes: 1,
        repack_node_bytes: 16 * 1024 * 1024,
    };
    client
        .execute(
            "INSERT INTO ducklake.ducklake_metadata VALUES ('data_path', $1, NULL)",
            &[&root],
        )
        .await
        .unwrap();
    for (index, kind, ty) in [
        (1_i64, TimeType::TimestampTz, "timestamptz"),
        (2, TimeType::Timestamp, "timestamp"),
        (3, TimeType::Date, "date"),
    ] {
        let table = format!("observations_{index}");
        client
            .execute(
                "INSERT INTO ducklake.ducklake_table VALUES ($1, 1, $2, $3, true, NULL)",
                &[&index, &table, &format!("{table}/")],
            )
            .await
            .unwrap();
        let layout = Layout {
            schema: "weather".into(),
            time_column: "observed".into(),
            time_type: kind,
            timezone: chrono_tz::Asia::Tokyo,
            row_group_col: Some("station".into()),
            sort_cols: vec!["station".into(), "observed".into()],
            dedup_timestamp: "fetched".into(),
            author: "weather-loader".into(),
            ..Layout::default()
        };
        let columns = [
            ("observed", ty),
            ("station", "uint64"),
            ("fetched", "timestamp"),
            ("flag", "boolean"),
            ("temperature", "float32"),
            ("tiny", "int8"),
            ("small", "int16"),
            ("integer", "int32"),
            ("big", "int64"),
            ("utiny", "uint8"),
            ("usmall", "uint16"),
            ("uinteger", "uint32"),
            ("day", "date"),
            ("label", "varchar"),
            ("measure", "float64"),
        ];
        for (position, (name, ty)) in columns.iter().enumerate() {
            client
                .execute(
                    "INSERT INTO ducklake.ducklake_column VALUES ($1,$2,$3,$4,NULL,NULL)",
                    &[&index, &(position as i64 + 1), name, ty],
                )
                .await
                .unwrap();
        }
        let begin = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
        let end = NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();
        let instant = Utc.with_ymd_and_hms(2026, 1, 2, 0, 0, 0).unwrap();
        let times: ArrayRef = match kind {
            TimeType::TimestampTz => Arc::new(
                TimestampMicrosecondArray::from(vec![instant.timestamp_micros(); 3])
                    .with_timezone("UTC"),
            ),
            TimeType::Timestamp => Arc::new(TimestampMicrosecondArray::from(vec![
                instant
                    .timestamp_micros(
                    );
                3
            ])),
            TimeType::Date => Arc::new(Date32Array::from(vec![20455; 3])),
        };
        let arrays: Vec<ArrayRef> = vec![
            times,
            Arc::new(UInt64Array::from(vec![u64::MAX; 3])),
            Arc::new(TimestampMicrosecondArray::from(vec![1, 3, 2])),
            Arc::new(BooleanArray::from(vec![false, true, false])),
            Arc::new(Float32Array::from(vec![Some(f32::NAN), Some(3.5), None])),
            Arc::new(Int8Array::from(vec![i8::MIN, 0, i8::MAX])),
            Arc::new(Int16Array::from(vec![i16::MIN, 0, i16::MAX])),
            Arc::new(Int32Array::from(vec![i32::MIN, 0, i32::MAX])),
            Arc::new(Int64Array::from(vec![i64::MIN, 0, i64::MAX])),
            Arc::new(UInt8Array::from(vec![0, 1, u8::MAX])),
            Arc::new(UInt16Array::from(vec![0, 1, u16::MAX])),
            Arc::new(UInt32Array::from(vec![0, 1, u32::MAX])),
            Arc::new(Date32Array::from(vec![-100, 0, 100])),
            Arc::new(StringArray::from(vec!["z", "a", "b"])),
            Arc::new(Float64Array::from(vec![
                f64::NEG_INFINITY,
                1.5,
                f64::INFINITY,
            ])),
        ];
        let fields = arrays
            .iter()
            .zip(columns)
            .map(|(array, (name, _))| Field::new(name, array.data_type().clone(), true))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
        let window = match kind {
            TimeType::TimestampTz => Window::TimestampTz {
                start: begin.and_hms_opt(0, 0, 0).unwrap().and_utc(),
                end: end.and_hms_opt(0, 0, 0).unwrap().and_utc(),
            },
            TimeType::Timestamp => Window::Timestamp {
                start: begin.and_hms_opt(0, 0, 0).unwrap(),
                end: end.and_hms_opt(0, 0, 0).unwrap(),
            },
            TimeType::Date => Window::Date { start: begin, end },
        };
        let loaded = dkplus::replace_window(&cfg, &layout, &table, &window, vec![batch])
            .await
            .unwrap();
        assert_eq!(loaded.rows, 3);
        let stats = client.query_one("SELECT min_value,max_value FROM ducklake.ducklake_file_column_stats WHERE data_file_id=$1 AND column_id=2",
            &[&loaded.data_file_id]).await.unwrap();
        assert_eq!(stats.get::<_, String>(0), u64::MAX.to_string());
        assert_eq!(stats.get::<_, String>(1), u64::MAX.to_string());
        let float = client.query_one("SELECT contains_nan,null_count FROM ducklake.ducklake_file_column_stats WHERE data_file_id=$1 AND column_id=5",
            &[&loaded.data_file_id]).await.unwrap();
        assert!(float.get::<_, bool>(0));
        assert_eq!(float.get::<_, i64>(1), 1);
        let expected = [
            (4_i64, "false".to_string(), "true".to_string()),
            (5, "3.5".into(), "3.5".into()),
            (6, i8::MIN.to_string(), i8::MAX.to_string()),
            (7, i16::MIN.to_string(), i16::MAX.to_string()),
            (8, i32::MIN.to_string(), i32::MAX.to_string()),
            (9, i64::MIN.to_string(), i64::MAX.to_string()),
            (10, "0".into(), u8::MAX.to_string()),
            (11, "0".into(), u16::MAX.to_string()),
            (12, "0".into(), u32::MAX.to_string()),
            (14, "a".into(), "z".into()),
            (15, "-inf".into(), "inf".into()),
        ];
        for (column, min, max) in expected {
            let row = client.query_one(
                "SELECT min_value,max_value FROM ducklake.ducklake_file_column_stats WHERE data_file_id=$1 AND column_id=$2",
                &[&loaded.data_file_id, &column],
            ).await.unwrap();
            assert_eq!(row.get::<_, String>(0), min);
            assert_eq!(row.get::<_, String>(1), max);
        }
        let job = dkplus::repack::Job {
            layout: layout.clone(),
            table: format!("weather.{table}"),
            begin_date: begin,
            end_date: end,
            unique: Some(vec!["station".into(), "observed".into()]),
            files: None,
        };
        let mut log = Vec::new();
        dkplus::repack::run(&cfg, &job, &mut log).await.unwrap();
        assert!(
            log.iter()
                .any(|line| line.contains("aligned fastpack completed successfully"))
        );
        assert!(log.iter().any(|line| line.contains("committed compaction")));
        let totals = client.query_one("SELECT record_count,next_row_id FROM ducklake.ducklake_table_stats WHERE table_id=$1",
            &[&index]).await.unwrap();
        assert_eq!(totals.get::<_, i64>(0), 1);
        assert_eq!(totals.get::<_, i64>(1), 3);
        // A second pass exercises reading preserved IDs and typed group statistics.
        dkplus::repack::run(&cfg, &job, &mut log).await.unwrap();
        let mut external = job.clone();
        external.layout.row_group_col = None;
        let mut external_log = Vec::new();
        dkplus::repack::run(&cfg, &external, &mut external_log)
            .await
            .unwrap();
        assert!(
            external_log
                .iter()
                .any(|line| line.contains("merging in-range region"))
        );
        let authors = client.query_one("SELECT count(*) FROM ducklake.ducklake_snapshot_changes WHERE author <> 'weather-loader'", &[]).await.unwrap();
        assert_eq!(authors.get::<_, i64>(0), 0);
        let discovery_id = 100 + index;
        let discovery_table = format!("daily_{index}");
        client
            .execute(
                "INSERT INTO ducklake.ducklake_table VALUES ($1,1,$2,$3,true,NULL)",
                &[
                    &discovery_id,
                    &discovery_table,
                    &format!("{discovery_table}/"),
                ],
            )
            .await
            .unwrap();
        client
            .execute(
                "INSERT INTO ducklake.ducklake_column VALUES ($1,1,'observed',$2,NULL,NULL)",
                &[&discovery_id, &ty],
            )
            .await
            .unwrap();
        for day in 1..=31 {
            let date = NaiveDate::from_ymd_opt(2026, 1, day).unwrap();
            let naive = date.and_hms_opt(0, 0, 0).unwrap();
            let (min, max) = match kind {
                TimeType::TimestampTz => {
                    let min = layout
                        .timezone
                        .from_local_datetime(&naive)
                        .single()
                        .unwrap();
                    (
                        min.to_rfc3339(),
                        (min + chrono::Duration::hours(23)).to_rfc3339(),
                    )
                }
                TimeType::Timestamp => (
                    naive.to_string(),
                    (naive + chrono::Duration::hours(23)).to_string(),
                ),
                TimeType::Date => (date.to_string(), date.to_string()),
            };
            let file_id = -(index * 100 + i64::from(day));
            let path = format!("daily-{day}.parquet");
            client.execute("INSERT INTO ducklake.ducklake_data_file(data_file_id,table_id,path,record_count) VALUES ($1,$2,$3,1)",
                &[&file_id, &discovery_id, &path]).await.unwrap();
            client.execute("INSERT INTO ducklake.ducklake_file_column_stats(data_file_id,table_id,column_id,min_value,max_value) VALUES ($1,$2,1,$3,$4)",
                &[&file_id, &discovery_id, &min, &max]).await.unwrap();
        }
        let granularity = if kind == TimeType::Date {
            chrono::Duration::days(1)
        } else {
            chrono::Duration::hours(1)
        };
        let candidates =
            dkplus::maintenance::discover(&cfg, &layout, &discovery_table, granularity)
                .await
                .unwrap();
        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].files.len(), 31);
        assert_eq!(candidates[0].begin.date(), begin);
        assert_eq!(candidates[0].end.date(), end.pred_opt().unwrap());
        let inline_table = format!("inline_{index}");
        client
            .batch_execute(&format!(
                "CREATE TABLE ducklake.{inline_table}(observed text, end_snapshot bigint);
            INSERT INTO ducklake.{inline_table} VALUES ('2026-01-02 00:00:00+00',NULL)"
            ))
            .await
            .unwrap();
        client
            .execute(
                "INSERT INTO ducklake.ducklake_inlined_data_tables VALUES ($1,$2)",
                &[&index, &inline_table],
            )
            .await
            .unwrap();
        assert!(
            dkplus::repack::run(&cfg, &job, &mut log)
                .await
                .unwrap_err()
                .to_string()
                .contains("inlined data")
        );
    }
    assert!(
        !store
            .list(None)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
            .is_empty()
    );
    client
        .batch_execute("DROP SCHEMA ducklake CASCADE")
        .await
        .unwrap();
    drop(client);
    connection.await.unwrap();
}

use futures::TryStreamExt;
