# dkplus

Reusable Rust storage for a Postgres-hosted DuckLake 1.0 catalog and streaming
Parquet output.

## Dependency

```toml
[dependencies]
dkplus = { path = "../dkplus" } # adjust relative to the consuming Cargo.toml
datafusion = "55"             # compatible Arrow 59 / Parquet 59 types
object_store = "0.13"
tokio = { version = "1", features = ["macros", "rt-multi-thread"] }
anyhow = "1"
```

Keep Arrow/Parquet/object_store versions compatible across the consumer and this
crate. `rustciso` uses `../../modules/dkplus` from its manifest. These are independent
crates, not members of a shared workspace.

## Standalone row-group Parquet writing

No catalog connection, DuckLake field IDs, or Azure credentials are required:

```rust,no_run
use std::sync::Arc;
use datafusion::arrow::{
    array::{Int32Array, RecordBatch},
    datatypes::{DataType, Field, Schema},
};
use dkplus::parquet::{Grouping, ParquetWriter};
use object_store::{memory::InMemory, path::Path};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let store = Arc::new(InMemory::new());
    let schema = Arc::new(Schema::new(vec![Field::new("station", DataType::Int32, false)]));
    let mut writer = ParquetWriter::new(
        store, Path::from("observations.parquet"), schema.clone(),
        Grouping::SingleKey("station".into()),
    )?;
    let batch = RecordBatch::try_new(
        schema, vec![Arc::new(Int32Array::from(vec![1, 1, 2]))],
    )?;
    writer.write(&batch).await?;
    let file = writer.finish().await?;
    assert_eq!(file.rows, 3);
    Ok(())
}
```

Grouping modes:

- `None`: size-based groups, without a grouping column.
- `WholeKeys(column)`: never split a key; flush at a key boundary after a size
  target is reached. An individual key can exceed the targets.
- `SingleKey(column)`: flush when a key changes; split large keys at the targets.

Input group keys must be non-null and sorted ascending, including across batches;
disordered keys and schema changes return errors. Supported scalar key types are
listed below. Output uses ZSTD level 3, untruncated chunk statistics, and targets
1,048,576 rows or approximately 128 MiB of encoded, uncompressed data. Byte targets
are checked after writes and are not hard memory caps. The caller owns deletion of
partial standalone output after errors; `finish` returns path, row count, size,
and footer size.

## Catalog configuration and operations

Callers own credential/environment parsing and table policy:

```rust,no_run
use std::sync::Arc;
use dkplus::{Config, Layout, Storage, TimeType, Window};
use dkplus::config::{DEFAULT_REPACK_NODE_BYTES, DEFAULT_REPACK_RUN_BYTES};
use object_store::memory::InMemory;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = Config {
        postgres: std::env::var("DATABASE_URL")?.parse()?,
        storage: Storage::Store {
            root: "memory:///".into(), store: Arc::new(InMemory::new()),
        },
        repack_run_bytes: DEFAULT_REPACK_RUN_BYTES,
        repack_node_bytes: DEFAULT_REPACK_NODE_BYTES,
    };
    let layout = Layout {
        schema: "weather".into(), time_column: "observed".into(),
        time_type: TimeType::Date, row_group_col: Some("station".into()),
        sort_cols: vec!["station".into(), "observed".into()],
        dedup_timestamp: "fetched".into(), author: "weather-scraper".into(),
        ..Layout::default()
    };
    let begin = "2026-01-01".parse()?;
    let end = "2026-02-01".parse()?;
    let window = Window::Date { start: begin, end };
    // With nonempty Arrow batches matching the existing catalog table:
    // dkplus::replace_window(&cfg, &layout, "observations", &window, batches).await?;
    let job = dkplus::repack::Job {
        layout, table: "weather.observations".into(), begin_date: begin, end_date: end,
        unique: Some(vec!["station".into(), "observed".into()]), files: None,
    };
    dkplus::repack::run(&cfg, &job, &mut Vec::new()).await?;
    Ok(())
}
```

Tables must already exist; dkplus neither creates tables nor migrates schemas.
`replace_window` keeps the basic initial-load Parquet layout, replacing rows using
file retirement or pg_ducklake SQL DELETE for straddling/inlined data.
`Window::TimestampTz`, `Window::Timestamp`, and `Window::Date` carry UTC instants,
naive microsecond timestamps, and dates respectively. The window kind must match
the configured column. Bounds are half-open.

Repack accepts calendar dates and resolves aware-timestamp midnights in
`Layout::timezone`; ambiguous/nonexistent midnights fail. Naive/date windows use
wall-clock midnights/dates without a UTC offset. Bare table names use
`Layout::schema`; table components in repack jobs allow letters, digits, underscores.
The grouping column must lead the ascending sort layout.

Deduplication is optional. It retains the latest configured microsecond timestamp,
breaking ties by highest DuckLake row ID, with null timestamps last. It affects
only in-range rows. Repack preserves positional deletes, original row/snapshot
identities, before/after regions, and retained snapshots. Month-or-longer ranges
and outside regions use single-key groups; shorter ranges use whole-key packing.
Aligned row groups are checked against actual values, falling back to bounded
external sorting when metadata, ordering, or memory budgets are unsuitable.

`files`, when supplied, is an any-current-catalog-path freshness guard, not a source
filter. Empty/already-retired lists skip successfully. Source and schema changes
at publication fail explicitly. Snapshot allocation conflicts retry. Repack retains
candidate outputs on uncertain commit errors; do not delete them without checking
every retained snapshot. Temporary prefixes may remain after process termination.

All workers for a table must share `lock_namespace` and `lock_timezone`, even if
their request timezones differ. Locks cover the actual half-open range projected
into that fixed lock timezone (date/naive domains use calendar dates). Existing
applications can retain their old namespace/timezone for deployed-worker
compatibility. Locks survive transactions and are released with the connection.

## Backends and scalar types

- `Storage::Azure { account, access_key }`: catalog paths use `az://container/key`.
- `Storage::local(directory)`: directory must exist; catalog paths may be absolute
  filesystem paths or `file://` URLs beneath it.
- `Storage::Store { root, store }`: caller-owned object store, including `InMemory`;
  the explicit catalog root must end in `/`. Paths outside the root fail.
- `storage.resolve(path)` exposes the store/key pair for standalone Parquet use.

The catalog remains Postgres, with TLS support. No SQLite/DuckDB catalog engine,
S3/GCS configuration, or fallback credentials are included.

Supported DuckLake types: `boolean`, `int8/int16/int32/int64`,
`uint8/uint16/uint32/uint64`, `float32/float64`, `varchar`, `date` (Arrow Date32),
`timestamp`/`timestamp_us` (naive microseconds), and `timestamptz` (microseconds).
Statistics use typed comparisons, including full-range uint64, nulls, NaNs and
infinities. Other precisions/types, partitioning, mapped/encrypted/partial files,
nested data and Puffin deletes fail explicitly. In-range inlined data must be
flushed before repacking.

## Maintenance

`maintenance::discover(&cfg, &layout, table, granularity)` returns candidates
without enqueueing anything. `table_candidates` can use a caller-owned transaction.
Each table discovery materializes stats in a separate statement before timestamp
casts, preserving temporary-table cleanup and avoiding casts of unrelated stats.
The monthly heuristic selects midnight-to-hour-23 file bounds, first-of-month
minimum, and a file count equal to the month's day count. Date maxima represent
whole inclusive days. This is not proof of contiguous data or one file per day.
Granularity must be positive whole seconds, or whole days for date columns.
Callers choose reports, turn inclusive maxima into exclusive end dates, schedule,
and deliver jobs. Serialized `Job` omits `layout`: set application policy explicitly
after deserializing rather than trusting a queue message to configure storage.

## Verification

```sh
cargo test
cargo fmt --check
cargo check --all-targets
```

For all catalog/locking regressions, use an **isolated, empty** Postgres database:

```sh
DKPLUS_TEST_DATABASE_URL='postgresql://user@localhost/testdb' \
MAINTENANCE_TEST_DATABASE_URL='postgresql://user@localhost/testdb' \
  cargo test -- --include-ignored --test-threads=1
```

These tests cover memory/filesystem load and repack publication, all supported
scalar mappings/statistics, date/naive/aware windows, typed grouping/fastpack,
external sorting, deduplication, row IDs, exact group boundaries and thresholds.
They create catalog fixtures, not real Azure queues or production data.
