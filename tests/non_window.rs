use datafusion::arrow::array::{ArrayRef, Float64Array, RecordBatch, StringArray};
use dkplus::{Config, Storage};
use std::{process::Command, sync::Arc};

fn ducklake(python: &str, postgres: &str, root: &str, script: &str) -> String {
    let output = Command::new(python)
        .args(["-c", script])
        .env("DKPLUS_TEST_POSTGRES", postgres)
        .env("DKPLUS_TEST_ROOT", root)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}\n{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).unwrap()
}

fn batch(rows: &[(&str, &str)]) -> RecordBatch {
    RecordBatch::try_from_iter([
        (
            "col1",
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef,
        ),
        (
            "col2",
            Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.1))) as ArrayRef,
        ),
    ])
    .unwrap()
}

#[tokio::test]
#[ignore = "requires isolated DKPLUS_TEST_DATABASE_URL and DKPLUS_DUCKLAKE_PYTHON with DuckLake"]
async fn writes_are_readable_by_ducklake() {
    let postgres = std::env::var("DKPLUS_TEST_DATABASE_URL").unwrap();
    let python = std::env::var("DKPLUS_DUCKLAKE_PYTHON").unwrap_or_else(|_| "python3".into());
    let root = std::env::current_dir()
        .unwrap()
        .join("target")
        .join(format!("non-window-{}", uuid::Uuid::now_v7()));
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
c.execute("ATTACH 'ducklake:postgres:" + os.environ["DKPLUS_TEST_POSTGRES"] +
          "' AS lake (METADATA_SCHEMA 'ducklake', DATA_PATH '" + os.environ["DKPLUS_TEST_ROOT"] + "')")
c.execute("CREATE TABLE lake.main.pairs (col1 VARCHAR, col2 VARCHAR)")
c.execute("CALL lake.set_option('data_inlining_row_limit', 0)")
c.execute("INSERT INTO lake.main.pairs VALUES ('a','old'), ('b','keep'), ('x','deleted')")
c.execute("DELETE FROM lake.main.pairs WHERE col1='x'")
c.execute("CALL lake.set_option('data_inlining_row_limit', 10)")
c.execute("INSERT INTO lake.main.pairs VALUES ('a','inline-old'), ('i','inline-keep'), (NULL,'inline-null')")
c.execute("CREATE TABLE lake.main.floatpairs (k DOUBLE, value VARCHAR)")
c.execute("CALL lake.set_option('data_inlining_row_limit', 0)")
c.execute("INSERT INTO lake.main.floatpairs VALUES (0.0,'zero-old'), ('NaN'::DOUBLE,'nan-old')")
"#,
    );
    let cfg = Config {
        postgres: postgres.parse().unwrap(),
        storage: Storage::local(&root).unwrap(),
        ..dkplus::Table::default().cfg
    };
    let table = cfg
        .table_builder("main.pairs")
        .with_unique_by_cols(vec!["col1".into()])
        .resolve()
        .await
        .unwrap();
    table
        .insert(vec![batch(&[("c", "inserted")])])
        .await
        .unwrap();
    let pairs = RecordBatch::try_from_iter([
        (
            "col1",
            Arc::new(StringArray::from(vec![Some("a"), Some("d"), None])) as ArrayRef,
        ),
        (
            "col2",
            Arc::new(StringArray::from(vec!["new", "new-d", "new-null"])) as ArrayRef,
        ),
    ])
    .unwrap();
    table.upsert(vec![pairs]).await.unwrap();
    let floats = cfg
        .table_builder("main.floatpairs")
        .with_unique_by_cols(vec!["k".into()])
        .resolve()
        .await
        .unwrap();
    let float_batch = RecordBatch::try_from_iter([
        (
            "k",
            Arc::new(Float64Array::from(vec![
                -0.0,
                f64::from_bits(0xfff8_0000_0000_0001),
            ])) as ArrayRef,
        ),
        (
            "value",
            Arc::new(StringArray::from(vec!["zero-new", "nan-new"])) as ArrayRef,
        ),
    ])
    .unwrap();
    floats.upsert(vec![float_batch]).await.unwrap();
    ducklake(
        &python,
        &postgres,
        root_str,
        r#"
import duckdb, os
c = duckdb.connect()
c.execute("LOAD ducklake; LOAD postgres")
c.execute("ATTACH 'ducklake:postgres:" + os.environ["DKPLUS_TEST_POSTGRES"] +
          "' AS lake (METADATA_SCHEMA 'ducklake')")
rows = c.execute("SELECT col1, col2 FROM lake.main.pairs ORDER BY col1").fetchall()
assert rows == [('a','new'), ('b','keep'), ('c','inserted'), ('d','new-d'), ('i','inline-keep'), (None,'new-null')], rows
rows = c.execute("SELECT value FROM lake.main.floatpairs ORDER BY value").fetchall()
assert rows == [('nan-new',), ('zero-new',)], rows
"#,
    );
    let (client, connection) = cfg.postgres.connect(tokio_postgres::NoTls).await.unwrap();
    let connection = tokio::spawn(async move { connection.await.unwrap() });
    client
        .batch_execute("DROP SCHEMA ducklake CASCADE")
        .await
        .unwrap();
    drop(client);
    connection.await.unwrap();
    std::fs::remove_dir_all(root).unwrap();
}
