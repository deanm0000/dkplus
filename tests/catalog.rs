use chrono::{NaiveDate, TimeZone, Utc};
use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{Field, Schema};
use dkplus::{Config, Layout, Storage, TimeType, Window};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory};
use std::sync::Arc;

const CATALOG: &str = "
CREATE SCHEMA ducklake;
CREATE TABLE ducklake.ducklake_schema (
    schema_id bigint, schema_name text, path text, path_is_relative bool, end_snapshot bigint);
CREATE TABLE ducklake.ducklake_table (
    table_id bigint, schema_id bigint, table_name text, path text, path_is_relative bool, end_snapshot bigint);
CREATE TABLE ducklake.ducklake_metadata (key text, value text, scope text);
CREATE TABLE ducklake.ducklake_tag (
    object_id bigint, begin_snapshot bigint, end_snapshot bigint, key text, value text);
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
CREATE TABLE ducklake.ducklake_file_partition_value (
    data_file_id bigint, table_id bigint, partition_key_index bigint, partition_value text);
CREATE TABLE ducklake.ducklake_file_variant_stats (
    data_file_id bigint, table_id bigint, column_id bigint);
CREATE TABLE ducklake.ducklake_files_scheduled_for_deletion (
    data_file_id bigint, path text, path_is_relative bool, schedule_start timestamptz);
CREATE TABLE ducklake.ducklake_table_stats (
    table_id bigint PRIMARY KEY, record_count bigint, next_row_id bigint, file_size_bytes bigint);
CREATE TABLE ducklake.ducklake_table_column_stats (
    table_id bigint, column_id bigint, contains_null bool, contains_nan bool,
    min_value text, max_value text, extra_stats text);
INSERT INTO ducklake.ducklake_snapshot VALUES (0, now(), 0, 1, 1);
INSERT INTO ducklake.ducklake_schema VALUES (1, 'weather', 'weather/', true, NULL);
";

async fn expiry_fixture(
    client: &tokio_postgres::Client,
    postgres: &tokio_postgres::Config,
) -> (Config, dkplus::repack::Job, dkplus::LoadReport) {
    client.batch_execute(CATALOG).await.unwrap();
    client
        .batch_execute(
            "INSERT INTO ducklake.ducklake_metadata VALUES ('data_path', 'memory:///', NULL);
             INSERT INTO ducklake.ducklake_table VALUES (1,1,'observations','observations/',true,NULL);
             INSERT INTO ducklake.ducklake_column VALUES
                 (1,1,'observed','date',NULL,NULL), (1,2,'station','int32',NULL,NULL),
                 (1,3,'fetched','timestamp',NULL,NULL);",
        )
        .await
        .unwrap();
    for (key, value, end_snapshot) in [
        ("dkplus.time_column", r#""stale""#, Some(0_i64)),
        ("dkplus.time_column", r#""observed""#, None),
        ("dkplus.timezone", r#""UTC""#, None),
        ("dkplus.row_group_col", r#""station""#, None),
        ("dkplus.sort_cols", r#"["station","observed"]"#, None),
        ("dkplus.dedup_timestamp", r#""fetched""#, None),
    ] {
        client
            .execute(
                "INSERT INTO ducklake.ducklake_tag VALUES (1,0,$1,$2,$3)",
                &[&end_snapshot, &key, &value],
            )
            .await
            .unwrap();
    }
    let cfg = Config {
        postgres: postgres.clone(),
        storage: Storage::Store {
            root: "memory:///".into(),
            store: Arc::new(InMemory::new()),
        },
        repack_run_bytes: 1,
        repack_node_bytes: 16 * 1024 * 1024,
    };
    let layout = Layout::builder("weather", "observations")
        .with_author("weather-loader")
        .resolve(&cfg)
        .await
        .unwrap();
    let start: NaiveDate = "2026-01-01".parse().unwrap();
    let end: NaiveDate = "2026-02-01".parse().unwrap();
    let day = start
        .signed_duration_since(NaiveDate::from_ymd_opt(1970, 1, 1).unwrap())
        .num_days() as i32;
    let batch = RecordBatch::try_from_iter([
        (
            "observed",
            Arc::new(Date32Array::from(vec![day; 3])) as ArrayRef,
        ),
        (
            "station",
            Arc::new(Int32Array::from(vec![1, 1, 1])) as ArrayRef,
        ),
        (
            "fetched",
            Arc::new(TimestampMicrosecondArray::from(vec![1, 2, 3])) as ArrayRef,
        ),
    ])
    .unwrap();
    let loaded = dkplus::replace_window(&cfg, &layout, &Window::Date { start, end }, vec![batch])
        .await
        .unwrap();
    let job = dkplus::repack::Job {
        layout,
        table: "weather.observations".into(),
        begin_date: start,
        end_date: end,
        unique: None,
        files: None,
        expire_snapshot: dkplus::repack::ExpireSnapshot::Never,
    };
    (cfg, job, loaded)
}

async fn count(client: &tokio_postgres::Client, sql: &str) -> i64 {
    client.query_one(sql, &[]).await.unwrap().get(0)
}

#[tokio::test]
#[ignore = "requires DKPLUS_TEST_DATABASE_URL pointing to an isolated Postgres database"]
async fn repack_expiry_policies_retention_and_atomicity() {
    use dkplus::maintenance::{CleanupOptions, CleanupScope, cleanup_expired_files};
    use dkplus::repack::{ExpireSnapshot, run};
    use object_store::path::Path;

    let postgres: tokio_postgres::Config = std::env::var("DKPLUS_TEST_DATABASE_URL")
        .unwrap()
        .parse()
        .unwrap();
    let (client, connection) = postgres.connect(tokio_postgres::NoTls).await.unwrap();
    let connection = tokio::spawn(async move { connection.await.unwrap() });
    for policy in [
        ExpireSnapshot::Never,
        ExpireSnapshot::IfNoDedup,
        ExpireSnapshot::Always,
    ] {
        for dedup in [false, true] {
            for external in [false, true] {
                let (cfg, mut job, loaded) = expiry_fixture(&client, &postgres).await;
                job.expire_snapshot = policy;
                if dedup {
                    job.unique = Some(vec!["station".into(), "observed".into()]);
                }
                if external {
                    job.layout.row_group_col = None;
                }
                let expires = policy == ExpireSnapshot::Always
                    || (policy == ExpireSnapshot::IfNoDedup && !dedup);
                let mut log = Vec::new();
                run(&cfg, &job, &mut log).await.unwrap();
                assert_eq!(
                    count(
                        &client,
                        "SELECT max(snapshot_id) FROM ducklake.ducklake_snapshot"
                    )
                    .await,
                    2
                );
                assert_eq!(
                    count(
                        &client,
                        "SELECT count(*) FROM ducklake.ducklake_snapshot WHERE snapshot_id=1"
                    )
                    .await,
                    i64::from(!expires)
                );
                assert_eq!(
                    count(
                        &client,
                        "SELECT count(*) FROM ducklake.ducklake_snapshot WHERE snapshot_id=0"
                    )
                    .await,
                    1
                );
                assert_eq!(count(&client, "SELECT count(*) FROM ducklake.ducklake_snapshot_changes WHERE snapshot_id=1").await, 1);
                assert_eq!(
                    count(
                        &client,
                        "SELECT count(*) FROM ducklake.ducklake_files_scheduled_for_deletion"
                    )
                    .await,
                    i64::from(expires)
                );
                assert_eq!(
                    count(
                        &client,
                        "SELECT record_count FROM ducklake.ducklake_table_stats WHERE table_id=1"
                    )
                    .await,
                    if dedup { 1 } else { 3 }
                );
                assert_eq!(
                    log.iter()
                        .any(|s| s.contains("expired predecessor snapshot 1")),
                    expires
                );
                let (store, key) = cfg.storage.resolve(&loaded.path).unwrap();
                assert!(store.head(&Path::from(key.clone())).await.is_ok());
                if expires {
                    let file = client.query_one(
                        "SELECT data_file_id,path,path_is_relative FROM ducklake.ducklake_files_scheduled_for_deletion", &[]
                    ).await.unwrap();
                    assert_eq!(file.get::<_, i64>(0), loaded.data_file_id);
                    assert_eq!(file.get::<_, String>(1), loaded.path);
                    assert!(!file.get::<_, bool>(2));
                    assert_eq!(count(&client, "SELECT count(*) FROM ducklake.ducklake_file_column_stats WHERE data_file_id=1").await, 0);
                    assert_eq!(
                        count(
                            &client,
                            "SELECT count(*) FROM ducklake.ducklake_data_file WHERE data_file_id=1"
                        )
                        .await,
                        0
                    );
                    let options = CleanupOptions {
                        scope: CleanupScope::Catalog,
                        older_than: Utc::now() + chrono::Duration::seconds(1),
                        dry_run: false,
                    };
                    let report = cleanup_expired_files(&cfg, &options).await.unwrap();
                    assert_eq!(report.deleted, 1);
                    assert!(store.head(&Path::from(key)).await.is_err());
                }
                client
                    .batch_execute("DROP SCHEMA ducklake CASCADE")
                    .await
                    .unwrap();
            }
        }
    }
    // IfNoDedup tests whether dedup was requested, not the number removed.
    let (cfg, mut job, _) = expiry_fixture(&client, &postgres).await;
    job.expire_snapshot = ExpireSnapshot::IfNoDedup;
    job.unique = Some(vec!["fetched".into()]);
    run(&cfg, &job, &mut Vec::new()).await.unwrap();
    assert_eq!(
        count(
            &client,
            "SELECT record_count FROM ducklake.ducklake_table_stats WHERE table_id=1"
        )
        .await,
        3
    );
    assert_eq!(
        count(
            &client,
            "SELECT count(*) FROM ducklake.ducklake_snapshot WHERE snapshot_id=1"
        )
        .await,
        1
    );
    client
        .batch_execute("DROP SCHEMA ducklake CASCADE")
        .await
        .unwrap();

    // A transient publication conflict retries the whole transaction, including
    // expiry, without duplicating queue entries or losing snapshot-change history.
    let (cfg, mut job, _) = expiry_fixture(&client, &postgres).await;
    job.expire_snapshot = ExpireSnapshot::Always;
    client
        .batch_execute(
            "CREATE SEQUENCE ducklake.expiry_attempts;
         CREATE FUNCTION ducklake.fail_first_expiry() RETURNS trigger LANGUAGE plpgsql AS
         $$ BEGIN
             IF nextval('ducklake.expiry_attempts') = 1 THEN
                 RAISE EXCEPTION 'injected publication conflict' USING ERRCODE = '23505';
             END IF;
             RETURN OLD;
         END $$;
         CREATE TRIGGER fail_first_expiry BEFORE DELETE ON ducklake.ducklake_snapshot
         FOR EACH ROW EXECUTE FUNCTION ducklake.fail_first_expiry();",
        )
        .await
        .unwrap();
    let mut log = Vec::new();
    run(&cfg, &job, &mut log).await.unwrap();
    assert!(
        log.iter()
            .any(|line| line.contains("repack commit conflict (attempt 1)"))
    );
    assert_eq!(
        count(
            &client,
            "SELECT max(snapshot_id) FROM ducklake.ducklake_snapshot"
        )
        .await,
        2
    );
    assert_eq!(
        count(
            &client,
            "SELECT count(*) FROM ducklake.ducklake_snapshot WHERE snapshot_id=1"
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &client,
            "SELECT count(*) FROM ducklake.ducklake_files_scheduled_for_deletion"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &client,
            "SELECT count(*) FROM ducklake.ducklake_snapshot_changes"
        )
        .await,
        2
    );
    client
        .batch_execute("DROP SCHEMA ducklake CASCADE")
        .await
        .unwrap();

    // Source began in 1, while a different table committed snapshot 2.
    // Expire 2, not 1: the older retained snapshot still needs the source.
    let (cfg, mut job, loaded) = expiry_fixture(&client, &postgres).await;
    client.batch_execute(
        "INSERT INTO ducklake.ducklake_table VALUES (2,1,'other','other/',true,NULL);
         INSERT INTO ducklake.ducklake_snapshot
         SELECT 2,now(),schema_version,next_catalog_id,next_file_id FROM ducklake.ducklake_snapshot WHERE snapshot_id=1;
         INSERT INTO ducklake.ducklake_snapshot_changes VALUES (2,'inserted_into_table:2','other',NULL,NULL);
         INSERT INTO ducklake.ducklake_data_file
         (data_file_id,table_id,begin_snapshot,path,path_is_relative)
         VALUES (100,2,2,'other.parquet',true);"
    ).await.unwrap();
    job.expire_snapshot = ExpireSnapshot::Always;
    let mut log = Vec::new();
    run(&cfg, &job, &mut log).await.unwrap();
    assert_eq!(
        count(
            &client,
            "SELECT count(*) FROM ducklake.ducklake_snapshot WHERE snapshot_id=2"
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &client,
            "SELECT count(*) FROM ducklake.ducklake_snapshot WHERE snapshot_id=1"
        )
        .await,
        1
    );
    assert_eq!(count(&client, "SELECT count(*) FROM ducklake.ducklake_data_file WHERE data_file_id=1 AND end_snapshot=3").await, 1);
    assert_eq!(count(&client, "SELECT count(*) FROM ducklake.ducklake_data_file WHERE data_file_id=100 AND end_snapshot IS NULL").await, 1);
    assert_eq!(
        count(
            &client,
            "SELECT count(*) FROM ducklake.ducklake_files_scheduled_for_deletion"
        )
        .await,
        0
    );
    let (store, key) = cfg.storage.resolve(&loaded.path).unwrap();
    assert!(store.head(&Path::from(key)).await.is_ok());
    assert!(
        log.iter()
            .any(|s| s.contains("expired predecessor snapshot 2"))
    );

    // The next repack's input only belongs to snapshot 3: expiry can reclaim it,
    // without removing the older source still needed by snapshot 1.
    run(&cfg, &job, &mut log).await.unwrap();
    assert_eq!(
        count(
            &client,
            "SELECT count(*) FROM ducklake.ducklake_snapshot WHERE snapshot_id IN (1,4)"
        )
        .await,
        2
    );
    assert_eq!(
        count(
            &client,
            "SELECT count(*) FROM ducklake.ducklake_files_scheduled_for_deletion"
        )
        .await,
        1
    );
    client
        .batch_execute("DROP SCHEMA ducklake CASCADE")
        .await
        .unwrap();

    // Expiry also retires positional-delete metadata, including old delete
    // versions whose parent data file is no longer referenced.
    let (cfg, mut job, loaded) = expiry_fixture(&client, &postgres).await;
    job.expire_snapshot = ExpireSnapshot::Always;
    let (store, _) = cfg.storage.resolve(&loaded.path).unwrap();
    for name in ["current-delete.parquet", "historical-delete.parquet"] {
        let path = Path::from(format!("weather/observations/{name}"));
        let batch =
            RecordBatch::try_from_iter([("pos", Arc::new(Int64Array::from(vec![0])) as ArrayRef)])
                .unwrap();
        let mut writer = dkplus::parquet::ParquetWriter::new(
            store.clone(),
            path,
            batch.schema(),
            dkplus::parquet::Grouping::None,
        )
        .unwrap();
        writer.write(&batch).await.unwrap();
        writer.finish().await.unwrap();
    }
    client.batch_execute(
        "INSERT INTO ducklake.ducklake_delete_file
         (delete_file_id,table_id,data_file_id,begin_snapshot,end_snapshot,path,path_is_relative,delete_count,format)
         VALUES (90,1,1,1,NULL,'current-delete.parquet',true,1,'parquet'),
                (91,1,1,0,1,'historical-delete.parquet',true,1,'parquet');
         UPDATE ducklake.ducklake_table_stats SET record_count=2;
         UPDATE ducklake.ducklake_snapshot SET next_file_id=92 WHERE snapshot_id=1;
         INSERT INTO ducklake.ducklake_file_partition_value VALUES (1,1,0,'test');
         INSERT INTO ducklake.ducklake_file_variant_stats VALUES (1,1,1);"
    ).await.unwrap();
    run(&cfg, &job, &mut Vec::new()).await.unwrap();
    assert_eq!(
        count(
            &client,
            "SELECT record_count FROM ducklake.ducklake_table_stats WHERE table_id=1"
        )
        .await,
        2
    );
    assert_eq!(
        count(
            &client,
            "SELECT count(*) FROM ducklake.ducklake_delete_file"
        )
        .await,
        0
    );
    assert_eq!(
        count(
            &client,
            "SELECT count(*) FROM ducklake.ducklake_files_scheduled_for_deletion"
        )
        .await,
        3
    );
    for table in [
        "ducklake_file_partition_value",
        "ducklake_file_variant_stats",
        "ducklake_file_column_stats",
    ] {
        assert_eq!(
            count(
                &client,
                &format!("SELECT count(*) FROM ducklake.{table} WHERE data_file_id=1")
            )
            .await,
            0
        );
    }
    let report = cleanup_expired_files(
        &cfg,
        &CleanupOptions {
            scope: CleanupScope::Catalog,
            older_than: Utc::now() + chrono::Duration::seconds(1),
            dry_run: false,
        },
    )
    .await
    .unwrap();
    assert_eq!(report.deleted, 3);
    client
        .batch_execute("DROP SCHEMA ducklake CASCADE")
        .await
        .unwrap();

    // A queueing failure must roll back the new snapshot AND expiry.
    let (cfg, mut job, _) = expiry_fixture(&client, &postgres).await;
    job.expire_snapshot = ExpireSnapshot::Always;
    client
        .batch_execute(
            "CREATE FUNCTION ducklake.fail_queue() RETURNS trigger LANGUAGE plpgsql AS
         $$ BEGIN RAISE EXCEPTION 'injected queue failure'; END $$;
         CREATE TRIGGER fail_queue BEFORE INSERT ON ducklake.ducklake_files_scheduled_for_deletion
         FOR EACH ROW EXECUTE FUNCTION ducklake.fail_queue();",
        )
        .await
        .unwrap();
    let error = run(&cfg, &job, &mut Vec::new()).await.unwrap_err();
    assert!(error.to_string().contains("scheduling expired repack file"));
    assert_eq!(
        count(
            &client,
            "SELECT max(snapshot_id) FROM ducklake.ducklake_snapshot"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &client,
            "SELECT count(*) FROM ducklake.ducklake_file_column_stats WHERE data_file_id=1"
        )
        .await,
        3
    );
    assert_eq!(count(&client, "SELECT count(*) FROM ducklake.ducklake_data_file WHERE data_file_id=1 AND end_snapshot IS NULL").await, 1);
    assert_eq!(
        count(
            &client,
            "SELECT count(*) FROM ducklake.ducklake_files_scheduled_for_deletion"
        )
        .await,
        0
    );
    client
        .batch_execute(
            "DROP TRIGGER fail_queue ON ducklake.ducklake_files_scheduled_for_deletion;
         DROP FUNCTION ducklake.fail_queue();",
        )
        .await
        .unwrap();
    // No-op guards and empty ranges must not expire the predecessor.
    job.files = Some(vec![]);
    run(&cfg, &job, &mut Vec::new()).await.unwrap();
    job.files = None;
    job.begin_date = "2030-01-01".parse().unwrap();
    job.end_date = "2030-02-01".parse().unwrap();
    run(&cfg, &job, &mut Vec::new()).await.unwrap();
    assert_eq!(
        count(
            &client,
            "SELECT max(snapshot_id) FROM ducklake.ducklake_snapshot"
        )
        .await,
        1
    );
    assert_eq!(
        count(
            &client,
            "SELECT count(*) FROM ducklake.ducklake_snapshot WHERE snapshot_id=1"
        )
        .await,
        1
    );
    client
        .batch_execute("DROP SCHEMA ducklake CASCADE")
        .await
        .unwrap();
    drop(client);
    connection.await.unwrap();
}

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
        let layout = Layout::builder("weather", &table)
            .with_time_column("observed")
            .with_timezone(chrono_tz::Asia::Tokyo)
            .with_row_group_col("station")
            .with_sort_cols(vec!["station".into(), "observed".into()])
            .with_dedup_timestamp("fetched")
            .with_author("weather-loader")
            .resolve(&cfg)
            .await
            .unwrap();
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
        let loaded = dkplus::replace_window(&cfg, &layout, &window, vec![batch])
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
            expire_snapshot: dkplus::repack::ExpireSnapshot::Never,
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
        let discovery_layout = Layout::builder("weather", &discovery_table)
            .with_time_column("observed")
            .with_timezone(chrono_tz::Asia::Tokyo)
            .with_sort_cols(vec!["observed".into()])
            .without_row_grouping()
            .resolve(&cfg)
            .await
            .unwrap();
        let candidates = dkplus::maintenance::discover(&cfg, &discovery_layout, granularity)
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
