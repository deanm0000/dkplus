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

Callers own credential/environment parsing. Table policy can live in DuckLake
tags and be overridden per operation:

```rust,no_run
use std::sync::Arc;
use dkplus::{Config, Table, Storage};
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
        log: Arc::new(|message| eprintln!("{message}")),
    };
    let table = Table::builder("weather", "observations")
        .with_time_column("observed")
        .with_timezone(chrono_tz::America::New_York)
        .with_row_group_col("station")
        .with_sort_cols(vec!["station".into(), "observed".into()])
        .with_unique_by_cols(vec!["station".into(), "observed".into()])
        .with_dedup_timestamp("fetched")
        .resolve(&cfg)
        .await?;
    let begin = "2026-01-01".parse()?;
    let end = "2026-02-01".parse()?;
    // With nonempty Arrow batches matching the existing catalog table:
    // table.replace_window(batches).await?;
    table.repack_now(begin, end).await?;
    Ok(())
}
```

Tables must already exist; dkplus neither creates tables nor migrates schemas.
`replace_window` keeps the basic initial-load Parquet layout. It retires files
fully covered by the replacement range, rewrites straddling Parquet files to keep
their out-of-range rows, and versions matching inlined rows out of the new snapshot.
These operations are implemented directly against DuckLake's catalog and files.
The replacement window is inferred from the minimum and maximum `time_column`
values across all input batches. It includes the maximum: the half-open upper
bound is one microsecond later for timestamps or one day later for dates.
Gaps between input times are replaced too; this does not round to calendar days.
Empty inputs, null times, incompatible types, and unrepresentable bounds fail
before connecting or uploading data.

Repack accepts calendar dates and resolves aware-timestamp midnights in
`Table::timezone`; ambiguous/nonexistent midnights fail. Naive/date windows use
wall-clock midnights/dates without a UTC offset. Repack uses the resolved
`Table::schema` and `Table::table`; components allow letters, digits, underscores.
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

Repack uses UTC as the fixed lock timezone. All workers must use the same
`lock_namespace`; it defaults to the table name and can be overridden when
replacing a deployed worker. Locks cover the half-open range projected into UTC
(date/naive domains use calendar dates). Locks survive transactions and are
released with the connection.

## Using dkplus as a library

The typical lifecycle is: build a `Config` (catalog, storage, and logging), resolve a
`Table` (which retains a cloned config and per-table policy), write data with
`table.replace_window(batches)`, then periodically compact with
`table.repack_now(begin, end)` or `table.repack(begin, end).run()`.

### 1. Configure

```rust,no_run
use dkplus::{Config, Table, Storage};
use dkplus::config::{DEFAULT_REPACK_NODE_BYTES, DEFAULT_REPACK_RUN_BYTES};
use std::sync::Arc;

async fn build() -> anyhow::Result<Table> {
    let cfg = Config {
        postgres: "host=localhost user=ducklake dbname=catalog".parse()?,
        storage: Storage::local(std::path::Path::new("/data/lake"))?,
        repack_run_bytes: DEFAULT_REPACK_RUN_BYTES,   // external-sort run size
        repack_node_bytes: DEFAULT_REPACK_NODE_BYTES, // merge-node memory budget
        log: Arc::new(|message| eprintln!("{message}")),
    };
    let table = Table::builder("weather", "observations")
        .with_time_column("observed")
        .with_timezone(chrono_tz::America::New_York)
        .with_row_group_col("station")
        .with_sort_cols(vec!["station".into(), "observed".into()])
        .with_dedup_timestamp("fetched_at")
        .resolve(&cfg)
        .await?;
    Ok(table)
}
```

`Config` options:

| Field | Meaning |
|---|---|
| `postgres` | `tokio_postgres::Config` for the DuckLake catalog (TLS via rustls/webpki roots). |
| `storage` | `Storage::Azure { account, access_key }`, `Storage::local(dir)`, or `Storage::Store { root, store }`. |
| `repack_run_bytes` | Size of in-memory sorted runs when repack falls back to external sorting (default 128 MiB). |
| `repack_node_bytes` | Memory budget per merge node (default 256 MiB). |
| `log` | Shared `Arc<dyn Fn(&str) + Send + Sync>` callback for progress and background errors. Cloned configs/tables share the callback; it may be invoked concurrently. |

`Table::builder(schema, table)` resolves a table's policy from explicit builder
values and active DuckLake table tags. Explicit values override tags. Required
settings left unresolved by both sources fail with an error.
Resolve a builder when starting an operation to pick up the latest committed
tags. `with_*` values are per-table overrides and do not write tags back to
DuckLake; use DuckLake's tag-management support to persist changes.

| Setting | Meaning |
|---|---|
| `time_column` | Required window column; its `date`, `timestamp`, or `timestamptz` type is inferred from the catalog. |
| `timezone` | Required zone for resolving repack date boundaries on `timestamptz` tables. It is not inferable from `timestamptz`, which stores instants, not a named zone. |
| `row_group_col` | Optional grouping column; must lead `sort_cols`. Missing/`null` means no row grouping. |
| `sort_cols` | Required ascending sort order of repacked files. |
| `unique_by_cols` | Optional columns defining duplicates; missing/`null` disables deduplication. |
| `dedup_timestamp` | Optional microsecond timestamp used to choose the retained duplicate. Required when `unique_by_cols` is set. |
| `lock_namespace` | Optional explicit lock namespace; defaults to the table name. |
| `author` | Optional snapshot author override; defaults to an operation/window-derived value. |

The recognized settings are table-level entries in DuckLake's `ducklake_tag` table,
namespaced as `dkplus.time_column`, `dkplus.timezone`, `dkplus.row_group_col`,
`dkplus.sort_cols`, `dkplus.unique_by_cols`, and `dkplus.dedup_timestamp`.
String settings are JSON strings and list settings are JSON arrays; nullable
optional settings can be JSON `null`. `ducklake_tag` values are snapshot-versioned;
the builder reads values active in the catalog's latest snapshot. Manage tags
through DuckLake's tag facilities rather than editing catalog tables directly.

### 2. Write to DuckLake

`Table::replace_window` atomically replaces the inferred half-open window with one
new Parquet file and one catalog snapshot. Empty batches are ignored; at least one
row is required. Nonempty batches must share one schema and use column names/types
of the existing table, with non-null values in the configured time column.

```rust,no_run
use std::sync::Arc;
use chrono::{TimeZone, Utc};
use datafusion::arrow::{
    array::{Int32Array, RecordBatch, TimestampMicrosecondArray},
    datatypes::{DataType, Field, Schema, TimeUnit},
};
use dkplus::Table;

async fn load(table: &Table) -> anyhow::Result<()> {
    let ts = DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()));
    let schema = Arc::new(Schema::new(vec![
        Field::new("station", DataType::Int32, false),
        Field::new("observed", ts, false),
    ]));
    let t0 = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
    let batch = RecordBatch::try_new(schema, vec![
        Arc::new(Int32Array::from(vec![1, 2])),
        Arc::new(
            TimestampMicrosecondArray::from(vec![t0.timestamp_micros(); 2]).with_timezone("UTC"),
        ),
    ])?;

    let report = table.replace_window(vec![batch]).await?;
    println!(
        "snapshot {} wrote {} rows to {}; retired {} files ({} rows), rewrote {} files",
        report.snapshot_id, report.rows, report.path,
        report.retired_files.len(), report.retired_rows, report.rewritten_files.len(),
    );
    Ok(())
}
```

`LoadReport` fields: `snapshot_id`, `data_file_id`, `path`, `rows`,
`file_size_bytes`, `retired_files` / `retired_rows` (source files and their
record counts retired via `end_snapshot`), and `rewritten_files` (retired source
files processed by the rewrite path to preserve rows outside the replacement
window). Commit conflicts are retried automatically. Use Arrow `Date32` for dates
or microsecond timestamps (timezone-aware for `timestamptz`, naive for `timestamp`),
matching the resolved table's time column.

### 3. Maintenance: discover and repack

`maintenance::discover` finds complete months whose files look like one file per
day; `Job::run` compacts a range into sorted, optionally deduplicated files
while preserving snapshots.

```rust,no_run
use dkplus::{Table, maintenance, repack};

async fn compact(table: &Table) -> anyhow::Result<()> {
    // One-hour granularity: files are expected to span whole hours.
    let candidates =
        maintenance::discover(&table.cfg, table, chrono::Duration::hours(1)).await?;

    for c in candidates {
        // `end` is an inclusive maximum; repack's end date is exclusive.
        table.repack(c.begin.date(), c.end.date().succ_opt().unwrap())
            .with_files(c.files)
            .with_expire_snapshot(repack::ExpireSnapshot::IfNoDedup)
            .run()
            .await?;
    }
    Ok(())
}
```

`table.repack_now(begin_date, end_date).await?` validates and runs immediately.
It uses `unique_by_cols` from resolved table metadata (or no deduplication),
`files: None`, and `ExpireSnapshot::IfNoDedup`.

`table.repack(begin_date, end_date)` returns a `repack::Job` with the same defaults.
Customize it with `.with_unique(vec![...])`, `.with_files(vec![...])`, and
`.with_expire_snapshot(policy)`, then `.run().await?`. Validation is automatic;
calling `validate()` separately is optional.

`repack::Job` options:

| Field | Meaning |
|---|---|
| `table` | Resolved `Table`, including config and policy. Serialized only as a qualified table name. |
| `begin_date`, `end_date` | Calendar dates, `end_date` exclusive. |
| `unique` | Optional explicit key-column override; if omitted, uses `unique_by_cols` resolved from the `Table`. Configure `Table::builder(...).without_deduplication()` to explicitly disable table-tag deduplication. |
| `files` | Optional freshness guard of catalog paths; skip if none are current. |
| `expire_snapshot` | `Never`, `IfNoDedup` (builder/convenience default), or `Always`; see snapshot expiry below. Omitted serialized policies retain the legacy `Never` default. |

For custom transactions use `maintenance::table_candidates(&client, ...)`.
Candidates are only a heuristic; they enqueue nothing.

### How repack works

**Selecting sources.** Repack reads the catalog (`ducklake.ducklake_*` tables in
Postgres) and selects every *current* data file (`end_snapshot IS NULL`) of the
table whose `Table::time_column` statistics overlap `[begin_date, end_date)`, plus
files whose min/max/null statistics are missing. File *names* are never parsed and
no naming convention is assumed; overlap is decided purely from the catalog's
per-file column min/max. Date boundaries are midnights: `Table::timezone` for
`timestamptz`, and plain wall-clock midnights for `timestamp`/`date` columns.

**Rewriting.** Rows are split into three regions: *before* the range, *in-range*,
and *after* (only the in-range region is deduplicated). Each region is written to
its own new Parquet file (`ducklake-<uuidv7>.parquet`, ZSTD) in the table's data
directory, sorted by `Table::sort_cols` (ascending). Straddling files therefore
keep their out-of-range rows. The time column must not contain nulls. Positional
delete files are applied (deleted rows disappear from the output), and each row
keeps its original `_ducklake_internal_row_id` and `_ducklake_internal_snapshot_id`
so time travel and row identity are preserved.

**Publishing (one catalog transaction, one new snapshot).** At `REPEATABLE READ`
repack re-checks that the sources, table metadata and inlined data are unchanged,
then:

- inserts one new snapshot (id = previous + 1) with change
  `compacted_table:<id>` (plus `deleted_from_table:<id>` if deduplication removed
  rows),   the configured author override (or a generated operation/window author) and
  a commit message `repack <table> [begin, end) <tz>`;
- sets `end_snapshot` on the source data files and their delete files (they are
  *retired*, not deleted, so older snapshots still read them);
- inserts the new data files and their per-column stats, and updates
  `ducklake_table_stats` (file size, record count minus deduplicated rows).

Commit conflicts retry up to 6 times; a changed source set or schema fails with a
"retry the job" error.

**Optional snapshot expiry.** Set `Job::expire_snapshot`:

| Policy | Behavior |
|---|---|
| `ExpireSnapshot::Never` | Default: retain the previous snapshot. |
| `ExpireSnapshot::IfNoDedup` | Expire only if deduplication is disabled in both the job override and resolved table policy. |
| `ExpireSnapshot::Always` | Expire even when deduplication changes logical data. |

Serialized jobs use `"expire_snapshot": "never"`, `"if_no_dedup"` or `"always"`;
omitting the field preserves the default. Existing Rust `Job` literals must add
the field explicitly.

Expiry happens **atomically in the repack publication transaction**: publishing
snapshot 13 expires its immediate predecessor 12. If unrelated commits happen
while sorting, the predecessor is the latest snapshot at publication—not the
snapshot seen when the job started. Conflict retries recompute it. A skipped
job or failed transaction does not expire anything. `IfNoDedup` means deduplication
was not requested, not merely that it removed zero rows.

**Snapshot expiry is catalog-wide**: opting in discards time travel to that
version for every table. It does not expire older versions. Files replaced by
this repack (including their positional-delete files) are queued for deletion
only when no retained snapshot needs them. Consequently, expiring 12 does **not**
necessarily reclaim its input files: snapshots 1–11 might still require them.
Eligible files have their file metadata/statistics removed and absolute paths
added to DuckLake's deletion queue, with a new scheduling timestamp. Snapshot
change records are retained for conflict detection. This targeted operation
does not collect unrelated obsolete catalog objects or inlined data; use native
DuckLake expiry for general catalog garbage collection.

No objects are physically deleted during expiry. Run
`maintenance::cleanup_expired_files` later, with a cutoff old enough for active
readers to finish. Enabling expiry takes additional catalog locks during
publication, so it can temporarily block other writers/cleanup.

**Cleanup.** Repack removes its own temporary sort runs (`<table dir>/_tmp/repack-*/`)
and any unpublished outputs on failure. It does **not** physically delete retired
source Parquet files. By default it does not expire snapshots; opt in above or expire them through DuckLake,
then use `maintenance::cleanup_expired_files` (below) for queued files.
After an uncertain commit failure candidate outputs
are deliberately kept.

**Not supported / preconditions.** Inlined rows in the range (flush them first),
mapped, encrypted, partial or non-Parquet files, Puffin deletes, nested types, and
column types outside the supported list below all fail with an error rather than
being rewritten. An empty overlap or an already-completed `files` guard returns
`Ok` without creating a snapshot.

### Row groups per "node"

A *node* is one distinct value of `Table::row_group_col` (for example a station
or device ID). You pick it once, in the `Table`, and it must also be the first of
`sort_cols`:

```rust,no_run
async fn configure(cfg: &dkplus::Config) -> anyhow::Result<()> {
    let _layout = dkplus::Table::builder("weather", "observations")
        .with_timezone(chrono_tz::UTC)
        .with_row_group_col("station")
        .with_sort_cols(vec!["station".into(), "observed".into()])
        .resolve(cfg)
        .await?;
    Ok(())
}
```

Because files are sorted by the node first, each node's rows are contiguous and a
row group's min/max statistics on that column make readers able to skip every
other node. Packing depends on the region and range length:

- **Outside regions, and ranges of one calendar month or longer:** *single-node*
  row groups. A row group never mixes nodes; a node larger than the size target
  (1,048,576 rows / ~128 MiB encoded) is split into several row groups, but a
  small node still gets its own group.
- **In-range region for shorter ranges:** nodes are never split across row groups
  but several small nodes may share one group (flushed at a node boundary once the
  target is reached; a single large node can exceed the target).
- **No `row_group_col`:** size-based row groups only, with no node guarantee, and
  the aligned fast path below is skipped.

Node keys must be non-null and of a supported scalar type: `boolean`, signed and
unsigned 8–64-bit integers, `float32/float64`, `varchar`, `date`, or microsecond
`timestamp`/`timestamptz`. Nulls in the node column, unsupported types, or
unsorted input cause an error or the slow path.

**Fast path vs external sort.** If the sources were already written node-sorted
(`row_group_col` leads `sort_cols`, and if deduplicating, `unique` includes the node
column), repack tries an *aligned* merge that streams whole row groups, verifying
actual values against statistics. When metadata, ordering or the
`repack_node_bytes` budget doesn't fit, it falls back to a general external sort that
spills sorted runs of about `repack_run_bytes` to the `_tmp` prefix, merges them and
writes the final files. Both paths give the same result; the fast path just uses
less I/O. Progress is logged to stderr and the `log` vector.

### Supported column types

`boolean`, `int8/16/32/64`, `uint8/16/32/64`, `float32/64`, `varchar`, `date`
(Date32), `timestamp`/`timestamp_us` and `timestamptz` (microseconds). The time
column must be `timestamptz`, `timestamp` or `date`; dkplus infers that type from
the catalog. The dedup timestamp must be a microsecond timestamp.

### Catalog assumptions

- DuckLake 1.0 metadata lives in the Postgres schema `ducklake`; the table must
  already exist and its schema is read from the catalog.
- Window column statistics are present in `ducklake_file_column_stats`; for
  `date` columns max values are treated as inclusive whole days.
- Concurrent repacks of overlapping ranges serialise on Postgres advisory locks
  keyed by `lock_namespace`, table and UTC calendar day.

### Cleaning up expired files

`maintenance::cleanup_expired_files` physically deletes files from DuckLake's
`ducklake.ducklake_files_scheduled_for_deletion` queue. **Retired is not expired:**
files retired by repack can still belong to retained snapshots. Expire those
snapshots through DuckLake or opt in to repack's predecessor expiry first;
this function does not expire snapshots, scan
for orphaned files, or remove abandoned repack output.

```rust,no_run
use chrono::{Duration, Utc};
use dkplus::{Config, maintenance::{
    CleanupOptions, CleanupScope, cleanup_expired_files,
}};

async fn cleanup(cfg: &Config) -> anyhow::Result<()> {
    let mut options = CleanupOptions {
        scope: CleanupScope::Table {
            schema: "weather".into(),
            table: "observations".into(),
        },
        older_than: Utc::now() - Duration::days(7),
        dry_run: true,
    };
    let preview = cleanup_expired_files(cfg, &options).await?;
    for file in &preview.files {
        println!("would delete {} (scheduled {})", file.path, file.schedule_start);
    }

    options.dry_run = false;
    let report = cleanup_expired_files(cfg, &options).await?;
    println!("deleted {}, already missing {}", report.deleted, report.already_missing);

    // Same cutoff, across the entire catalog instead of one table.
    options.scope = CleanupScope::Catalog;
    let _catalog_report = cleanup_expired_files(cfg, &options).await?;
    Ok(())
}
```

- `older_than` is a **strict cutoff on `schedule_start`**, not file modification
  time or snapshot time. Choose a delay longer than any active read transaction;
  seven days above is an example, not an automatically enforced safety period.
- `dry_run` resolves and validates candidates without deleting objects or queue
  entries. The preview is not reserved: the subsequent call reselects candidates.
- `CleanupReport::files` lists selected IDs, absolute paths, and schedule times.
  `deleted` counts successful object-store delete calls; some stores treat an
  already absent object as success. `already_missing` counts explicit `NotFound`
  responses. Both counters are zero for dry runs.
- The official queue has no table ID. Table scope uses the current table's data
  directory and rejects overlapping directories belonging to other catalog
  tables (including dropped tables). A root covering the whole configured store
  is also rejected. Catalog scope includes queued files for dropped tables.
  Table scope cannot attribute historical files outside the current directory;
  use catalog scope for those.
- Relative queue paths are relative to the **catalog's `data_path`**, unlike
  data/delete-file metadata paths, which are relative to the table directory.
  All paths must be accessible through `Config::storage`; invalid/out-of-root
  paths fail explicitly.
- Before any deletion, all candidates are checked against **all** data-file and
  delete-file metadata, not just the current snapshot. Still-referenced files
  cause an error even if someone incorrectly queued them.
- Cleanup holds Postgres locks on the queue and relevant metadata throughout
  deletion. It can block writers; schedule it accordingly. Run exclusively with
  respect to other cleanup tools or direct object-store writers.
- Objects are deleted before queue entries are removed. Storage/database errors
  are returned with context. A failed call can have deleted some objects while
  leaving their queue entries intact; retrying safely acknowledges missing files.
  Cleanup does not create/increment a snapshot or change table statistics.

### 4. Standalone Parquet and object stores

`parquet::ParquetWriter` (see above) needs no catalog. To write to the same
location a catalog path points at, resolve it:

```rust,no_run
use dkplus::Storage;

fn resolve(storage: &Storage) -> anyhow::Result<()> {
    let (store, key) = storage.resolve("az://container/lake/table/file.parquet")?;
    let _ = (store, key); // feed into ParquetWriter::new(store, Path::from(key), ...)
    Ok(())
}
```

Errors are `anyhow::Result`; use `dkplus::connect(&cfg)` directly for a
TLS-enabled `tokio_postgres::Client` when you need ad hoc catalog queries.

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

`maintenance::discover(&table.cfg, &table, granularity)` returns candidates
without enqueueing anything. `table_candidates` can use a caller-owned transaction.
Each table discovery materializes stats in a separate statement before timestamp
casts, preserving temporary-table cleanup and avoiding casts of unrelated stats.
The monthly heuristic selects midnight-to-hour-23 file bounds, first-of-month
minimum, and a file count equal to the month's day count. Date maxima represent
whole inclusive days. This is not proof of contiguous data or one file per day.
Granularity must be positive whole seconds, or whole days for date columns.
Callers choose reports, turn inclusive maxima into exclusive end dates, schedule,
and deliver jobs. Serialized `Job` includes only the qualified table name, dates,
and operation options, never config or policy. After deserializing, assign a
freshly resolved `Table` to `job.table` before calling `job.run().await?`;
unresolved queued jobs fail validation rather than trusting queue messages to
configure storage. Credentials and callbacks are not serialized.

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

Cleanup regressions use a separate **isolated, empty** Postgres database:

```sh
DKPLUS_CLEANUP_TEST_DATABASE_URL='postgresql://user@localhost/cleanup_testdb' \
  cargo test --test cleanup -- --include-ignored
```
