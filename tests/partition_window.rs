use std::{process::Command, sync::Arc};

use datafusion::arrow::array::{
    ArrayRef, Date32Array, Int32Array, RecordBatch, StringArray, TimestampMicrosecondArray,
};
use dkplus::{Config, Storage};

fn ducklake(python: &str, postgres: &str, root: &str, script: &str) {
    let output = Command::new(python)
        .args(["-c", script])
        .env("PARTITION_TEST_POSTGRES", postgres)
        .env("PARTITION_TEST_ROOT", root)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

fn batch(rows: &[(Option<&str>, i32, i32)]) -> RecordBatch {
    RecordBatch::try_from_iter([
        (
            "market_type",
            Arc::new(StringArray::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "observed",
            Arc::new(Date32Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
        (
            "value",
            Arc::new(Int32Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )) as ArrayRef,
        ),
    ])
    .unwrap()
}

// This test creates the catalog with DuckLake itself, not an approximated schema.
#[tokio::test]
#[ignore = "requires isolated DKPLUS_TEST_DATABASE_URL and DKPLUS_DUCKLAKE_PYTHON with duckdb/DuckLake/postgres extensions"]
async fn partition_window_real_ducklake() {
    let postgres = std::env::var("DKPLUS_TEST_DATABASE_URL").unwrap();
    let python = std::env::var("DKPLUS_DUCKLAKE_PYTHON").unwrap_or_else(|_| "python3".into());
    let root = std::env::current_dir()
        .unwrap()
        .join("target")
        .join(format!("partition-window-{}", uuid::Uuid::now_v7()));
    std::fs::create_dir_all(&root).unwrap();
    let root_str = root.to_str().unwrap();
    ducklake(
        &python,
        &postgres,
        root_str,
        r#"
import duckdb, os
c = duckdb.connect()
c.execute("LOAD ducklake; LOAD postgres")
c.execute("ATTACH 'ducklake:postgres:" + os.environ["PARTITION_TEST_POSTGRES"] +
          "' AS lake (METADATA_SCHEMA 'ducklake', DATA_PATH '" + os.environ["PARTITION_TEST_ROOT"] + "')")
c.execute("CREATE TABLE lake.main.partitioned (market_type VARCHAR, observed DATE, value INTEGER)")
c.execute("ALTER TABLE lake.main.partitioned SET PARTITIONED BY (market_type)")
c.execute("CALL lake.set_option('data_inlining_row_limit', 0)")
c.execute("""INSERT INTO lake.main.partitioned VALUES
    ('dam','2026-01-01',1), ('dam','2026-01-02',2), ('dam','2026-01-03',3),
    ('rtm','2026-01-02',10)""")
"#,
    );
    let cfg = Config {
        postgres: postgres.parse().unwrap(),
        storage: Storage::local(&root).unwrap(),
        ..dkplus::Table::default().cfg
    };
    let (client, connection) = cfg.postgres.connect(tokio_postgres::NoTls).await.unwrap();
    let connection = tokio::spawn(async move { connection.await.unwrap() });
    client.batch_execute(
        r#"INSERT INTO ducklake.ducklake_tag (object_id, begin_snapshot, end_snapshot, key, value)
           SELECT table_id, (SELECT max(snapshot_id) FROM ducklake.ducklake_snapshot), NULL, key, value
             FROM ducklake.ducklake_table
             CROSS JOIN (VALUES ('dkplus.time_column','"observed"'),
                                ('dkplus.timezone','"UTC"'),
                                ('dkplus.sort_cols','["market_type","observed"]')) tags(key,value)
            WHERE table_name = 'partitioned'"#
    ).await.unwrap();
    let table = cfg
        .table_builder("main.partitioned")
        .resolve()
        .await
        .unwrap();
    let snapshot = || async {
        client
            .query_one(
                "SELECT max(snapshot_id) FROM ducklake.ducklake_snapshot",
                &[],
            )
            .await
            .unwrap()
            .get::<_, i64>(0)
    };
    let before = snapshot().await;
    for rows in [
        vec![(Some("rtm"), 20455, 20)],
        vec![(None, 20455, 20)],
        vec![(Some("dam"), 20455, 20), (Some("rtm"), 20455, 20)],
    ] {
        assert!(
            table
                .replace_partition_window(vec![batch(&rows)], "market_type", "dam")
                .await
                .is_err()
        );
    }
    assert!(
        table
            .replace_window(vec![batch(&[(Some("dam"), 20455, 20)])])
            .await
            .is_err()
    );
    assert_eq!(snapshot().await, before);
    // 2026-01-02 is day 20455. A single physical DAM file straddles the window.
    let replacement = table
        .replace_partition_window(
            vec![batch(&[(Some("dam"), 20455, 20)])],
            "market_type",
            "dam",
        )
        .await
        .unwrap();
    assert_eq!(replacement.retired_files.len(), 1);
    assert_eq!(replacement.retired_rows, 3);
    let repeated = table
        .replace_partition_window(
            vec![batch(&[(Some("dam"), 20455, 21)])],
            "market_type",
            "dam",
        )
        .await
        .unwrap();
    assert_eq!(repeated.retired_files, vec![replacement.data_file_id]);
    // Publish a previously absent partition, including punctuation in the value.
    table
        .replace_partition_window(
            vec![batch(&[(Some("hasp'quoted"), 20455, 30)])],
            "market_type",
            "hasp'quoted",
        )
        .await
        .unwrap();
    assert_eq!(
        client
            .query_one(
                "SELECT count(*) FROM ducklake.ducklake_data_file f
                 JOIN ducklake.ducklake_partition_info p USING (table_id, partition_id)
                 JOIN ducklake.ducklake_file_partition_value v USING (table_id, data_file_id)
                 WHERE f.end_snapshot IS NULL AND p.end_snapshot IS NULL
                   AND v.partition_key_index = 0 AND v.partition_value IS NOT NULL",
                &[],
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        5
    );
    ducklake(
        &python,
        &postgres,
        root_str,
        r#"
import duckdb, os
c = duckdb.connect()
c.execute("LOAD ducklake; LOAD postgres")
c.execute("ATTACH 'ducklake:postgres:" + os.environ["PARTITION_TEST_POSTGRES"] +
          "' AS lake (METADATA_SCHEMA 'ducklake')")
rows = c.execute("SELECT market_type, observed::VARCHAR, value FROM lake.main.partitioned ORDER BY 1,2").fetchall()
assert rows == [('dam','2026-01-01',1), ('dam','2026-01-02',21), ('dam','2026-01-03',3),
                ("hasp'quoted",'2026-01-02',30), ('rtm','2026-01-02',10)], rows
assert c.execute("SELECT count(*) FROM lake.main.partitioned AT (VERSION => 3)").fetchone()[0] == 4
"#,
    );
    // Timestamp windows are independent of granularity and preserve adjacent
    // microseconds even when market intervals have different resolutions.
    ducklake(
        &python,
        &postgres,
        root_str,
        r#"
import duckdb, os
c = duckdb.connect()
c.execute("LOAD ducklake; LOAD postgres")
c.execute("ATTACH 'ducklake:postgres:" + os.environ["PARTITION_TEST_POSTGRES"] +
          "' AS lake (METADATA_SCHEMA 'ducklake')")
c.execute("CREATE TABLE lake.main.market_intervals (market_type VARCHAR, observed TIMESTAMPTZ, value INTEGER)")
c.execute("ALTER TABLE lake.main.market_intervals SET PARTITIONED BY (market_type)")
c.execute("""INSERT INTO lake.main.market_intervals VALUES
    ('dam','2026-01-02 00:00:00+00',1),
    ('dam','2026-01-02 00:00:00.000001+00',2),
    ('dam','2026-01-02 00:00:00.000002+00',3),
    ('rtm','2026-01-02 00:00:00.000001+00',10)""")
"#,
    );
    client.batch_execute(
        r#"INSERT INTO ducklake.ducklake_tag (object_id, begin_snapshot, end_snapshot, key, value)
           SELECT table_id, (SELECT max(snapshot_id) FROM ducklake.ducklake_snapshot), NULL, key, value
             FROM ducklake.ducklake_table
             CROSS JOIN (VALUES ('dkplus.time_column','"observed"'),
                                ('dkplus.timezone','"America/Los_Angeles"'),
                                ('dkplus.row_group_col','"market_type"'),
                                ('dkplus.sort_cols','["market_type","observed"]'),
                                ('dkplus.granularity','"unpredictable"')) tags(key,value)
            WHERE table_name = 'market_intervals'"#
    ).await.unwrap();
    let timestamp_table = cfg
        .table_builder("main.market_intervals")
        .resolve()
        .await
        .unwrap();
    let input = RecordBatch::try_from_iter([
        (
            "market_type",
            Arc::new(StringArray::from(vec!["dam"])) as ArrayRef,
        ),
        (
            "observed",
            Arc::new(
                TimestampMicrosecondArray::from(vec![1_767_312_000_000_001]).with_timezone("UTC"),
            ) as ArrayRef,
        ),
        ("value", Arc::new(Int32Array::from(vec![99])) as ArrayRef),
    ])
    .unwrap();
    timestamp_table
        .replace_partition_window(vec![input], "market_type", "dam")
        .await
        .unwrap();
    ducklake(
        &python,
        &postgres,
        root_str,
        r#"
import duckdb, os
c = duckdb.connect()
c.execute("LOAD ducklake; LOAD postgres")
c.execute("ATTACH 'ducklake:postgres:" + os.environ["PARTITION_TEST_POSTGRES"] +
          "' AS lake (METADATA_SCHEMA 'ducklake')")
rows = c.execute("SELECT market_type, epoch_us(observed), value FROM lake.main.market_intervals ORDER BY 1,2").fetchall()
assert rows == [('dam',1767312000000000,1), ('dam',1767312000000001,99),
                ('dam',1767312000000002,3), ('rtm',1767312000000001,10)], rows
"#,
    );
    // Unsupported transforms and compound keys must fail before publication.
    let before = snapshot().await;
    client.batch_execute(
        "UPDATE ducklake.ducklake_partition_column SET transform = 'year'
          WHERE table_id = (SELECT table_id FROM ducklake.ducklake_table WHERE table_name = 'partitioned')"
    ).await.unwrap();
    let error = table
        .replace_partition_window(
            vec![batch(&[(Some("dam"), 20455, 22)])],
            "market_type",
            "dam",
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("identity VARCHAR"), "{error}");
    assert_eq!(snapshot().await, before);
    client.batch_execute(
        "UPDATE ducklake.ducklake_partition_column SET transform = 'identity'
          WHERE table_id = (SELECT table_id FROM ducklake.ducklake_table WHERE table_name = 'partitioned');
         INSERT INTO ducklake.ducklake_partition_column
         SELECT partition_id, table_id, 1, column_id, transform FROM ducklake.ducklake_partition_column
          WHERE table_id = (SELECT table_id FROM ducklake.ducklake_table WHERE table_name = 'partitioned')"
    ).await.unwrap();
    assert!(
        table
            .replace_partition_window(
                vec![batch(&[(Some("dam"), 20455, 22)])],
                "market_type",
                "dam",
            )
            .await
            .is_err()
    );
    assert_eq!(snapshot().await, before);
    client
        .batch_execute(
            "DELETE FROM ducklake.ducklake_partition_column WHERE partition_key_index = 1",
        )
        .await
        .unwrap();
    // A legacy/mixed file cannot be safely selected with partition metadata.
    client
        .execute(
            "UPDATE ducklake.ducklake_data_file SET partition_id = NULL
             WHERE data_file_id = $1",
            &[&repeated.data_file_id],
        )
        .await
        .unwrap();
    let before = snapshot().await;
    let error = table
        .replace_partition_window(
            vec![batch(&[(Some("dam"), 20455, 22)])],
            "market_type",
            "dam",
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("legacy"), "{error}");
    assert_eq!(snapshot().await, before);
    client
        .execute(
            "UPDATE ducklake.ducklake_data_file SET partition_id =
             (SELECT partition_id FROM ducklake.ducklake_partition_info p
               WHERE p.table_id = ducklake.ducklake_data_file.table_id AND p.end_snapshot IS NULL)
             WHERE data_file_id = $1",
            &[&repeated.data_file_id],
        )
        .await
        .unwrap();
    ducklake(
        &python,
        &postgres,
        root_str,
        r#"
import duckdb, os
c = duckdb.connect()
c.execute("LOAD ducklake; LOAD postgres")
c.execute("ATTACH 'ducklake:postgres:" + os.environ["PARTITION_TEST_POSTGRES"] +
          "' AS lake (METADATA_SCHEMA 'ducklake')")
c.execute("CALL lake.set_option('data_inlining_row_limit', 10)")
c.execute("INSERT INTO lake.main.partitioned VALUES ('rtm','2026-01-02',11)")
"#,
    );
    let before = snapshot().await;
    let error = table
        .replace_partition_window(
            vec![batch(&[(Some("dam"), 20455, 22)])],
            "market_type",
            "dam",
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("inlined"), "{error}");
    assert_eq!(snapshot().await, before);
    client
        .batch_execute("DROP SCHEMA ducklake CASCADE")
        .await
        .unwrap();
    drop(client);
    connection.await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
