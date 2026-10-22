use chrono::{Duration, Utc};
use dkplus::{
    Config, Storage,
    config::{DEFAULT_REPACK_NODE_BYTES, DEFAULT_REPACK_RUN_BYTES},
    maintenance::{CleanupOptions, CleanupScope, cleanup_expired_files},
};
use object_store::{ObjectStoreExt, memory::InMemory, path::Path};
use std::sync::Arc;

const CATALOG: &str = "
CREATE SCHEMA ducklake;
CREATE TABLE ducklake.ducklake_metadata (key text, value text, scope text);
CREATE TABLE ducklake.ducklake_schema (
    schema_id bigint, schema_name text, path text, path_is_relative bool, end_snapshot bigint);
CREATE TABLE ducklake.ducklake_table (
    table_id bigint, schema_id bigint, table_name text, path text, path_is_relative bool, end_snapshot bigint);
CREATE TABLE ducklake.ducklake_data_file (
    table_id bigint, path text, path_is_relative bool, end_snapshot bigint);
CREATE TABLE ducklake.ducklake_delete_file (
    table_id bigint, path text, path_is_relative bool, end_snapshot bigint);
CREATE TABLE ducklake.ducklake_files_scheduled_for_deletion (
    data_file_id bigint, path text, path_is_relative bool, schedule_start timestamptz);
CREATE TABLE ducklake.ducklake_snapshot (snapshot_id bigint);
INSERT INTO ducklake.ducklake_snapshot VALUES (42);
INSERT INTO ducklake.ducklake_schema VALUES (1, 'weather', 'weather/', true, NULL);
INSERT INTO ducklake.ducklake_table VALUES
    (1, 1, 'a', 'a/', true, NULL), (2, 1, 'ab', 'ab/', true, NULL);
";

#[tokio::test]
#[ignore = "requires DKPLUS_CLEANUP_TEST_DATABASE_URL pointing to an isolated empty Postgres database"]
async fn expired_file_cleanup_memory_and_local() {
    let postgres: tokio_postgres::Config = std::env::var("DKPLUS_CLEANUP_TEST_DATABASE_URL")
        .unwrap()
        .parse()
        .unwrap();
    let (client, connection) = postgres.connect(tokio_postgres::NoTls).await.unwrap();
    let connection = tokio::spawn(async move { connection.await.unwrap() });
    let dir = std::env::temp_dir().join(format!("dkplus-cleanup-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir(&dir).unwrap();
    for storage in [
        Storage::Store {
            root: "memory:///".into(),
            store: Arc::new(InMemory::new()),
        },
        Storage::local(&dir).unwrap(),
    ] {
        client.batch_execute(CATALOG).await.unwrap();
        let Storage::Store { root, .. } = &storage else {
            unreachable!()
        };
        let root = root.clone();
        client
            .execute(
                "INSERT INTO ducklake.ducklake_metadata VALUES ('data_path', $1, NULL)",
                &[&root],
            )
            .await
            .unwrap();
        let cfg = Config {
            postgres: postgres.clone(),
            storage,
            repack_run_bytes: DEFAULT_REPACK_RUN_BYTES,
            repack_node_bytes: DEFAULT_REPACK_NODE_BYTES,
        };
        let (store, _) = cfg.storage.resolve(&root).unwrap();
        let cutoff = Utc::now() - Duration::days(7);
        let old = cutoff - Duration::days(1);
        let mut options = CleanupOptions {
            scope: CleanupScope::Table {
                schema: "weather".into(),
                table: "a".into(),
            },
            older_than: cutoff,
            dry_run: true,
        };
        // Mix catalog-relative and absolute scheduled paths. The sibling's name
        // deliberately shares a prefix with the selected table.
        for (id, key, relative, scheduled) in [
            (1_i64, "weather/a/old.parquet", true, old),
            (2, "weather/ab/old.parquet", false, old),
            (3, "weather/a/new.parquet", true, cutoff),
        ] {
            store.put(&Path::from(key), "parquet".into()).await.unwrap();
            let path = if relative {
                key.to_string()
            } else if root.starts_with("file://") {
                url::Url::parse(&root)
                    .unwrap()
                    .to_file_path()
                    .unwrap()
                    .join(key)
                    .to_str()
                    .unwrap()
                    .to_string()
            } else {
                format!("{root}{key}")
            };
            client
                .execute(
                    "INSERT INTO ducklake.ducklake_files_scheduled_for_deletion VALUES ($1,$2,$3,$4)",
                    &[&id, &path, &relative, &scheduled],
                )
                .await
                .unwrap();
        }
        let report = cleanup_expired_files(&cfg, &options).await.unwrap();
        assert_eq!(report.files.len(), 1);
        assert_eq!(report.files[0].data_file_id, 1);
        assert_eq!(report.files[0].path, format!("{root}weather/a/old.parquet"));
        assert_eq!(report.deleted, 0);
        assert!(
            store
                .head(&Path::from("weather/a/old.parquet"))
                .await
                .is_ok()
        );

        // Both current and retired metadata references must block deletion.
        for table in ["ducklake_data_file", "ducklake_delete_file"] {
            for end_snapshot in ["NULL", "42"] {
                client
                .batch_execute(&format!(
                    "INSERT INTO ducklake.{table} VALUES (1, 'old.parquet', true, {end_snapshot})"
                ))
                .await
                .unwrap();
                options.dry_run = false;
                let error = cleanup_expired_files(&cfg, &options).await.unwrap_err();
                assert!(error.to_string().contains("still referenced"));
                assert!(
                    store
                        .head(&Path::from("weather/a/old.parquet"))
                        .await
                        .is_ok()
                );
                client
                    .batch_execute(&format!("DELETE FROM ducklake.{table}"))
                    .await
                    .unwrap();
            }
        }
        client
            .execute(
                "INSERT INTO ducklake.ducklake_files_scheduled_for_deletion
                 VALUES (99, 'weather/a/../ab/old.parquet', true, $1)",
                &[&old],
            )
            .await
            .unwrap();
        assert!(cleanup_expired_files(&cfg, &options).await.is_err());
        assert!(
            store
                .head(&Path::from("weather/a/old.parquet"))
                .await
                .is_ok()
        );
        client
            .batch_execute(
                "DELETE FROM ducklake.ducklake_files_scheduled_for_deletion WHERE data_file_id = 99",
            )
            .await
            .unwrap();
        // Shared directories make a table-only request unsafe.
        client
            .batch_execute("UPDATE ducklake.ducklake_table SET path = 'a/' WHERE table_id = 2")
            .await
            .unwrap();
        assert!(
            cleanup_expired_files(&cfg, &options)
                .await
                .unwrap_err()
                .to_string()
                .contains("ambiguous")
        );
        client
            .batch_execute("UPDATE ducklake.ducklake_table SET path = 'ab/' WHERE table_id = 2")
            .await
            .unwrap();

        // A failed queue acknowledgement leaves a retryable entry even though
        // its object has already been deleted.
        client
            .batch_execute(
                "CREATE FUNCTION ducklake.fail_ack() RETURNS trigger LANGUAGE plpgsql AS
                 $$ BEGIN RAISE EXCEPTION 'injected acknowledgement failure'; END $$;
                 CREATE TRIGGER fail_ack BEFORE DELETE ON ducklake.ducklake_files_scheduled_for_deletion
                 FOR EACH ROW EXECUTE FUNCTION ducklake.fail_ack();",
            )
            .await
            .unwrap();
        assert!(
            cleanup_expired_files(&cfg, &options)
                .await
                .unwrap_err()
                .to_string()
                .contains("acknowledging")
        );
        client
            .batch_execute(
                "DROP TRIGGER fail_ack ON ducklake.ducklake_files_scheduled_for_deletion;
                 DROP FUNCTION ducklake.fail_ack();",
            )
            .await
            .unwrap();
        let report = cleanup_expired_files(&cfg, &options).await.unwrap();
        assert_eq!(report.files.len(), 1);
        assert_eq!(report.deleted + report.already_missing, 1);
        assert!(
            store
                .head(&Path::from("weather/ab/old.parquet"))
                .await
                .is_ok()
        );
        assert!(
            store
                .head(&Path::from("weather/a/new.parquet"))
                .await
                .is_ok()
        );
        assert!(
            cleanup_expired_files(&cfg, &options)
                .await
                .unwrap()
                .files
                .is_empty()
        );
        client
            .batch_execute(
                "INSERT INTO ducklake.ducklake_table
                 VALUES (3, 1, 'dropped', 'dropped/', true, 42)",
            )
            .await
            .unwrap();
        store
            .put(&Path::from("weather/dropped/old.parquet"), "parquet".into())
            .await
            .unwrap();
        client
            .execute(
                "INSERT INTO ducklake.ducklake_files_scheduled_for_deletion
                 VALUES (4, 'weather/dropped/old.parquet', true, $1)",
                &[&old],
            )
            .await
            .unwrap();
        options.scope = CleanupScope::Catalog;
        let report = cleanup_expired_files(&cfg, &options).await.unwrap();
        assert_eq!(report.files.len(), 2);
        assert_eq!(report.deleted, 2);
        let remaining: i64 = client
            .query_one(
                "SELECT count(*) FROM ducklake.ducklake_files_scheduled_for_deletion",
                &[],
            )
            .await
            .unwrap()
            .get(0);
        assert_eq!(remaining, 1); // Exactly at the cutoff is retained.
        let snapshot: i64 = client
            .query_one("SELECT snapshot_id FROM ducklake.ducklake_snapshot", &[])
            .await
            .unwrap()
            .get(0);
        assert_eq!(snapshot, 42);
        if root.starts_with("file://") {
            // A directory cannot be deleted as a file. Exercise a genuine
            // storage failure and ensure its queue entry is not acknowledged.
            let error_path = dir.join("weather/a/error.parquet");
            std::fs::create_dir(&error_path).unwrap();
            client
                .execute(
                    "INSERT INTO ducklake.ducklake_files_scheduled_for_deletion
                     VALUES (100, 'weather/a/error.parquet', true, $1)",
                    &[&old],
                )
                .await
                .unwrap();
            assert!(
                cleanup_expired_files(&cfg, &options)
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains("deleting expired file")
            );
            let queued: i64 = client
                .query_one(
                    "SELECT count(*) FROM ducklake.ducklake_files_scheduled_for_deletion
                     WHERE data_file_id = 100",
                    &[],
                )
                .await
                .unwrap()
                .get(0);
            assert_eq!(queued, 1);
            std::fs::remove_dir(error_path).unwrap();
        }
        options.scope = CleanupScope::Table {
            schema: "weather".into(),
            table: "missing".into(),
        };
        assert!(cleanup_expired_files(&cfg, &options).await.is_err());
        options.scope = CleanupScope::Table {
            schema: "weather; DROP SCHEMA ducklake".into(),
            table: "a".into(),
        };
        assert!(cleanup_expired_files(&cfg, &options).await.is_err());
        client
            .batch_execute("DROP SCHEMA ducklake CASCADE")
            .await
            .unwrap();
    }
    drop(client);
    connection.await.unwrap();
    std::fs::remove_dir_all(dir).unwrap();
}
