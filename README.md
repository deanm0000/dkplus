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

### Set policy tags from the command line

This package builds both the library and the `dkplus` binary (`cargo build --bin
dkplus`, or `cargo install --path .`). The binary only needs a Postgres connection;
no object-store credentials are required.

```sh
dkplus "host=localhost dbname=catalog user=ducklake" "ciso.rtprices" \
  "time_column" "utcbegin" "row_group_col" "node"

dkplus "host=localhost dbname=catalog user=ducklake" "ciso.rtprices" \
  "timezone" "America/Los_Angeles" "granularity" "5 minutes"

dkplus "host=localhost dbname=catalog user=ducklake" --file somefile.json
```

The JSON file maps qualified table names to settings:

```json
{
  "ciso.rtprices": {
    "time_column": "utcbegin",
    "row_group_col": "node",
    "sort_cols": ["node", "utcbegin"],
    "timezone": "America/Los_Angeles",
    "granularity": "5 minutes"
  },
  "ciso.daprices": {
    "time_column": "utcbegin",
    "timezone": "America/Los_Angeles",
    "granularity": "1 hour"
  }
}
```

Keys accept either the short name or the `dkplus.` prefix. Supported settings
are `time_column`, `row_group_col`, `sort_cols`, `unique_by_cols`,
`dedup_timestamp`, `timezone`, `granularity`, and the `repack_level<N>_<rule>`
settings documented below. Unknown keys are errors. File values use native JSON
strings, arrays, or null, not strings containing JSON. On the command line, pass
lists as JSON arrays, for example `'["node","utcbegin"]'`; scalar values are
ordinary strings. `null` disables nullable settings (`row_group_col`,
`unique_by_cols`, `dedup_timestamp`, `granularity`, and repack rules); it does not
delete their tags. Other tags remain unchanged.

`timezone` and `granularity` are written as **column tags** on the effective
`time_column`: the value supplied in the same request, or the existing table tag.
An error is returned if that column cannot be resolved. Existing column tags on
other columns are left untouched. Changing `time_column` does not move those tags;
supply timezone/granularity again if they should apply to the new column.

All tables and referenced columns must exist. Time columns must be dates or
microsecond timestamps, and dedup timestamp columns must be microsecond
timestamps. Timezones must be recognized IANA names; granularity must be a
positive Postgres interval (whole days/months for date columns), or
`unpredictable`. Repack intervals must be nonnegative except for `relage_min`
and `relage_max`, which are signed to support future-dated records. Calendar
units must match the documented choices. The resulting policy is checked for
consistency, including sort/group ordering and deduplication dependencies.

Partial configuration is allowed: missing required settings need not all be
supplied in one command, but must be present before resolving a table for data
operations. Every effective change across the entire invocation is committed
atomically in **one snapshot increment**. A validation failure changes nothing;
an unchanged request does not allocate a snapshot. The command prints the
resulting snapshot ID and exits nonzero on errors. `dkplus --help` shows usage.
Connection strings passed as arguments may be visible to other local processes.

The same validated batch operation is available to library callers as
`dkplus::tags::update(&postgres_config, updates)`, where `updates` is a
`dkplus::tags::Updates` map of table names to setting names and JSON values.
Unlike the lower-level `Table::update_tags` API, this operation validates dkplus
settings and routes timezone/granularity to column tags.

### Resolve policy in library code

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
    let table = Table::builder("weather.observations", &cfg)
        .with_time_column("observed") // Optional if DuckLake's dkplus.time_column tag is set.
        .with_timezone(chrono_tz::America::New_York) // Optional if the column's dkplus.timezone tag is set.
        .with_row_group_col("station") // Optional if DuckLake's dkplus.row_group_col tag is set.
        .with_sort_cols(vec!["station".into(), "observed".into()]) // Optional if DuckLake's dkplus.sort_cols tag is set.
        .with_unique_by_cols(vec!["station".into(), "observed".into()]) // Optional if DuckLake's dkplus.unique_by_cols tag is set.
        .with_dedup_timestamp("fetched") // Optional if DuckLake's dkplus.dedup_timestamp tag is set.
        .resolve()
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
    let table = cfg.table_builder("weather.observations")
        .with_time_column("observed") // Optional if DuckLake's dkplus.time_column tag is set.
        .with_timezone(chrono_tz::America::New_York) // Optional if the column's dkplus.timezone tag is set.
        .with_row_group_col("station") // Optional if DuckLake's dkplus.row_group_col tag is set.
        .with_sort_cols(vec!["station".into(), "observed".into()]) // Optional if DuckLake's dkplus.sort_cols tag is set.
        .with_dedup_timestamp("fetched_at") // Optional if DuckLake's dkplus.dedup_timestamp tag is set.
        .resolve()
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

`Table::builder("schema.table", &cfg)` and `cfg.table_builder("schema.table")`
return the same builder, which retains a cloned config. Call `.resolve().await?`
to resolve policy from explicit values and active DuckLake table/column tags.
Use `cfg.table("schema.table").await?` to resolve using only DuckLake metadata,
without any overrides. Explicit values override tags. Required
settings left unresolved by both sources fail with an error.
Resolve a builder when starting an operation to pick up the latest committed
tags. `with_*` values are per-table overrides and do not write tags back to
DuckLake; use `Table::update_tags` to persist table-level changes.

| Setting | Meaning |
|---|---|
| `time_column` | Required window column; its `date`, `timestamp`, or `timestamptz` type is inferred from the catalog. |
| `timezone` | Target IANA zone, supplied explicitly or by the time column's `dkplus.timezone` tag (with the table tag as a legacy fallback). Required even for naive/date tables, where boundaries remain wall-clock values. It is not inferable from `timestamptz`, which stores instants, not a named zone. |
| `row_group_col` | Optional grouping column; must lead `sort_cols`. Missing/`null` means no row grouping. |
| `sort_cols` | Required ascending sort order of repacked files. |
| `unique_by_cols` | Optional columns defining duplicates; missing/`null` disables deduplication. |
| `dedup_timestamp` | Optional microsecond timestamp used to choose the retained duplicate. Required when `unique_by_cols` is set. |
| `granularity` | Spacing of time-column values (`with_granularity`), or `Granularity::Unpredictable`. Required only to find maintenance repacks. |
| `repack_levels` | Maintenance repack levels (`with_repack_levels`); see [Maintenance repack levels](#maintenance-repack-levels). |
| `lock_namespace` | Optional explicit lock namespace; defaults to the table name. |
| `author` | Optional snapshot author override; defaults to an operation/window-derived value. |

The recognized settings are table-level entries in DuckLake's `ducklake_tag` table,
except that `dkplus.timezone` should be set on the time column in
`ducklake_column_tag` (`table_id`, `column_id`, `begin_snapshot`, `end_snapshot`,
`key`, `value`). The column tag takes precedence over the legacy table-level
timezone tag; an explicit `with_timezone` overrides both.
The `key` identifies the setting, and `value` is a string containing JSON:

| Tag key | JSON value example | How dkplus uses it |
|---|---|---|
| `dkplus.time_column` | `"observed"` | Required column defining repack windows. Its catalog type determines whether boundaries are dates or timestamps. |
| `dkplus.timezone` | `"America/New_York"` | Time-column tag defining the target IANA time zone for repack boundaries; a table-level tag remains supported as a fallback. |
| `dkplus.row_group_col` | `"station"` or `null` | Optional row-group key. When set, it must be the first column in `dkplus.sort_cols`. |
| `dkplus.sort_cols` | `["station", "observed"]` | Required ascending sort order for repacked files. |
| `dkplus.unique_by_cols` | `["station", "observed"]` or `null` | Optional duplicate key. When set, dkplus keeps the row selected using `dkplus.dedup_timestamp`. |
| `dkplus.dedup_timestamp` | `"fetched_at"` or `null` | Optional microsecond timestamp column used to select a retained row for each duplicate key. Required when `unique_by_cols` is set. |
| `dkplus.granularity` | `"5 minutes"`, `"unpredictable"` or `null` | Time-column tag (table-level fallback) giving the spacing of time values as a Postgres interval. Used by `find_potential_repacks`. |
| `dkplus.repack_level<N>_<rule>` | `"1 day"`, `"month"` or `null` | Maintenance repack level rules; see below. |

For example, the tag values for a table configured to group and sort by station
could be:

```text
dkplus.time_column      = "observed"
dkplus.row_group_col    = "station"
dkplus.sort_cols        = ["station", "observed"]
dkplus.unique_by_cols   = ["station", "observed"]
dkplus.dedup_timestamp  = "fetched_at"
```

On the `observed` column, set `dkplus.timezone = "America/New_York"` (and
optionally `dkplus.granularity = "5 minutes"`) in `ducklake_column_tag`, with the
value encoded as a JSON string just like table tags. Only tags active at the latest snapshot are used. Duplicate active timezone
tags for a column, malformed JSON, and invalid IANA zones fail explicitly.

List values are JSON arrays; nullable optional values can be JSON `null` (for
example, `dkplus.row_group_col = null` disables row grouping). Missing optional
tags also mean no grouping or deduplication. The builder reads only values
active at the latest catalog snapshot. Explicit `Table::builder(...).with_*`
settings override the corresponding tags and are not written back to DuckLake.

### Inspecting tags with DuckDB Python

The DuckLake extension exposes table tags through DuckDB's `duckdb_tables()`
metadata function. For an existing DuckLake catalog, attach it and query the
tag map for the table:

```python
import duckdb

con = duckdb.connect()
con.execute("INSTALL ducklake")
con.execute("LOAD ducklake")
con.execute("ATTACH 'ducklake:metadata.ducklake' AS lake")

tags = con.execute("""
    SELECT tags
    FROM duckdb_tables()
    WHERE database_name = 'lake'
      AND schema_name = 'weather'
      AND table_name = 'observations'
""").fetchone()[0]
print(tags)
```

Replace the attach string with the connection string for your catalog. The
DuckLake extension has no supported SQL command for writing arbitrary tags.
Use dkplus's `Table::update_tags` method rather than editing `ducklake_tag`
rows directly; it versions the changes and groups them into one snapshot.

The Python SDK and DuckDB extension do not currently provide arbitrary tag
writes. Use this crate's API:

```rust,no_run
use std::collections::HashMap;

async fn update_policy(table: &dkplus::Table) -> anyhow::Result<i64> {
    let snapshot_id = table.update_tags(HashMap::from([
        ("dkplus.time_column".into(), Some(r#""observed""#.into())),
        ("dkplus.timezone".into(), Some(r#""America/New_York""#.into())),
        ("dkplus.row_group_col".into(), Some(r#""station""#.into())),
        ("dkplus.sort_cols".into(), Some(r#"["station","observed"]"#.into())),
        ("dkplus.unique_by_cols".into(), Some(r#"["station","observed"]"#.into())),
        ("dkplus.dedup_timestamp".into(), Some(r#""fetched_at""#.into())),
    ])).await?;
    Ok(snapshot_id)
}
```

Each map entry changes only its named key: `Some(value)` adds or replaces its
value, and `None` removes it. Values are stored as provided, so dkplus settings
must be JSON-encoded strings. All effective changes in one call share a
snapshot; unchanged requests do not create a new snapshot.

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

#### Merge updates and retain changed versions

`Table::merge_diff(batches, "entries_diff")` infers the same window, but merges
rows by the configured `unique_by_cols` instead of deleting rows absent from the
input. It requires `unique_by_cols` and `dedup_timestamp`. The diff table must
already exist with the same column names and types; its DuckLake column IDs may
differ. An unqualified name uses the target's schema; `"schema.entries_diff"`
selects another schema in the same catalog.

For each matching key, the row with the latest `dedup_timestamp` wins. Every
other column is compared with null-safe equality. If any data value differs,
the older row, including its original timestamp, is appended to the diff table
via Parquet. Thus older incoming data never rolls back the target, but is still
archived if its values differ. Unchanged values advance to the newer timestamp
without adding a diff row. New keys are inserted, and existing keys missing
from the input are preserved, including gaps inside the inferred window.

Duplicate keys on either side are reduced to that side's latest row before
comparison. Null keys match null keys; null dedup timestamps are rejected.
Equal timestamps prefer the existing row, archiving the incoming row only if
its values differ. History is append-only and is not deduplicated, including
across repeated calls.

```rust,no_run
use datafusion::arrow::array::RecordBatch;
use dkplus::Table;

async fn merge(table: &Table, batches: Vec<RecordBatch>) -> anyhow::Result<()> {
    let report = table.merge_diff(batches, "entries_diff").await?;
    println!("snapshot {}: {} target rows, {} history rows",
        report.target.snapshot_id,
        report.target.rows,
        report.diff.as_ref().map_or(0, |diff| diff.rows));
    Ok(())
}
```

The returned `MergeDiffReport` contains a target `LoadReport` and an optional
diff `LoadReport` (absent when no values changed). Both files are published
atomically in one snapshot; existing history is never retired. Overlapping
target files are rewritten to preserve rows outside the window, positional
deletes are respected, and inlined rows in the window participate in the merge.
The comparison materializes the input and existing window in memory; boundary
rewrites use the repack memory budgets.

### 3. Maintenance: find and run repacks

`Table::find_potential_repacks` reads only catalog statistics
(`ducklake_file_column_stats`), never data files, and returns serializable
`maintenance::PotentialRepack` groups. Nothing is enqueued or rewritten. Each
group can be sent to a worker, which runs `maintenance::do_repack` against the
real data.

```rust,no_run
use dkplus::{Table, maintenance, repack::Strictness};

async fn find(table: &Table) -> anyhow::Result<Vec<String>> {
    // Serialized repacks can be sent to remote workers.
    table
        .find_potential_repacks()
        .await?
        .iter()
        .map(|repack| Ok(serde_json::to_string(repack)?))
        .collect()
}

async fn work(table: &Table, message: &str) -> anyhow::Result<()> {
    // `table` is resolved on the worker; the message holds only the plan.
    let repack: maintenance::PotentialRepack = serde_json::from_str(message)?;
    match maintenance::do_repack(table, &repack, Strictness::Raise).await? {
        maintenance::RepackOutcome::Repacked => {}
        maintenance::RepackOutcome::Skipped(reason) => eprintln!("skipped: {reason}"),
    }
    Ok(())
}
```

`maintenance::find_potential_repacks(&client, &table)` does the same inside a
caller-owned transaction.

#### Maintenance repack levels

Levels are numbered table tags `dkplus.repack_level<N>_<rule>`, with `N` a
positive integer without leading zeros. There can be any number of levels. Each
value is a JSON string (or `null` to ignore the rule). Intervals use Postgres
interval syntax and are parsed by Postgres when the table resolves. All bounds
are inclusive.

| Rule | Meaning |
|---|---|
| `targetwindow_min`, `targetwindow_max` | Size of the repacked window: `begin + min <= end <= begin + max`. |
| `sourcewindow_min`, `sourcewindow_max` | The same bounds, applied to each source file's own span. |
| `age_min`, `age_max` | `now - newest dedup_timestamp >= age_min` and `now - oldest dedup_timestamp <= age_max`, per file. A level with age rules is skipped when the table has no `dedup_timestamp`. Naive dedup timestamps are treated as UTC. |
| `relage_min`, `relage_max` | Signed bounds on `dedup_timestamp - time_column`, evaluated per file using the conservative min/max checks below. Negative intervals are allowed. A level with relage rules is skipped when the table has no `dedup_timestamp`. |
| `calendar_begin` | `minute`, `hour`, `day`, `week`, `month`, `quarter` or `year`. The window must start and end on these boundaries in the table timezone. |

`relage` compares the two timestamps directly; unlike `age`, it is not relative
to the current time. A positive result means the `dedup_timestamp` is later than
the reported `time_column` (for example, a report ingested today about an event
from a week ago has a relative age of about `7 days`). A negative result means
the `time_column` is in the future of the `dedup_timestamp` (for example, a
forecast for four days from now has a relative age of `-4 days`).

For each file, let `t_min` and `t_max` be the minimum and maximum
`time_column` statistics, and `d_min` and `d_max` the minimum and maximum
`dedup_timestamp` statistics. The rules require the entire possible range of
`dedup_timestamp - time_column` to fit within the configured bounds:

```text
relage_min: d_min - t_max >= relage_min
relage_max: d_max - t_min <= relage_max
```

These are the conservative extremes: the smallest possible difference uses the
earliest dedup timestamp and latest event time; the largest uses the latest
dedup timestamp and earliest event time. For example, `relage_min = "6 days"`
and `relage_max = "8 days"` select only files whose full relative-age range is
between six and eight days. To select reports about future events four to two
days ahead, use `relage_min = "-4 days"` and `relage_max = "-2 days"`. Bounds
are inclusive. For timestamps, negative intervals work in the same way as
positive ones; `age_min` and `age_max` remain nonnegative and retain their
separate meaning relative to now.

Window and source-window interval arithmetic follows Postgres
`timestamptz + interval` in the table timezone. Months and days move the local
wall clock, so a 23- or 25-hour DST day is still `1 day`. Hours, minutes and
seconds are elapsed time. `age` and `relage` compare timestamps as UTC instants
(naive timestamps are treated as UTC); for those comparisons a day is 24 hours,
and a month is applied using the UTC calendar.

A file covers `[min, max + granularity)`. For example, with a 5-minute
granularity, an hour is complete when its last file's maximum is `xx:55`. For
`date` columns, granularity must be whole days.

For each level, the finder:

1. Keeps files that pass the source-window, age, and relage rules.
2. Sorts them by time and greedily takes the longest contiguous run, with no
   gaps, that satisfies the target-window and calendar rules.
3. Accepts the run only if no other current file overlaps the window and the
   run has at least two files.

With `"unpredictable"` granularity there is no gap check:
- With `calendar_begin`, a window snaps to calendar boundaries. It is complete
  once the table holds a value at or after its end.
- Without `calendar_begin`, a window ends just after its newest value and needs
  any later value in the table.

Results are ordered by level, then by earliest time value. An entry is dropped
when any of its files is already used by an earlier kept entry. Finding repacks
fails if the time column has no granularity or any file lacks time statistics.

```text
# Full days only, after the day ends.
dkplus.repack_level1_targetwindow_min = "1 day"
dkplus.repack_level1_targetwindow_max = "1 day"
dkplus.repack_level1_calendar_begin   = "day"
# 1-7 days of week-old data.
dkplus.repack_level2_age_min          = "7 days"
dkplus.repack_level2_targetwindow_min = "1 day"
dkplus.repack_level2_targetwindow_max = "7 days"
dkplus.repack_level2_calendar_begin   = "day"
# Calendar months from files that already span at least a day.
dkplus.repack_level3_sourcewindow_min = "1 day"
dkplus.repack_level3_targetwindow_min = "1 month"
dkplus.repack_level3_targetwindow_max = "1 month"
dkplus.repack_level3_calendar_begin   = "month"
```

`do_repack` first re-checks the plan against current statistics, using the
plan's level and granularity. It returns `RepackOutcome::Skipped` when a plan
file is no longer current, the rules no longer hold, or the window changed.
Otherwise it repacks `[begin, end)` with the plan's files as a freshness guard.
Every current file overlapping the window is a source, as with any repack.
On successful publication, `do_repack` uses `ExpireSnapshot::Referencing`:
it expires **every catalog snapshot that references a replaced file**, even
when deduplication is enabled. This removes time travel to those versions for
**all tables**, not just the repacked table. Snapshots from before the replaced
files existed are retained. The replaced data and positional-delete files are
queued for `cleanup_expired_files`; repacking does not physically delete them.
Skipped or failed repacks do not expire snapshots or queue files.
`Strictness` controls what happens when that would not produce exactly one file,
because a source holds rows outside the window:

| Strictness | Behavior |
|---|---|
| `Raise` | Fails before reading data if statistics cannot prove every source row is in the window, or if the output would be more than one file. |
| `Mash` | Writes every row of the selected sources into one file, ignoring the window for splitting. |

#### Ad hoc repacks

`table.repack_now(begin_date, end_date).await?` validates and runs immediately.
It uses `unique_by_cols` from resolved table metadata (or no deduplication),
`files: None`, and `ExpireSnapshot::IfNoDedup`.

`table.repack(begin_date, end_date)?` returns a `repack::Job` between
table-timezone midnights with the same defaults. `table.repack_between(begin,
end)` accepts arbitrary `repack::Bound`s. Customize a job with
`.with_unique(vec![...])`, `.with_files(vec![...])`,
`.with_expire_snapshot(policy)` and `.with_strictness(strictness)`, then
`.run().await?`. Validation is automatic; calling `validate()` separately is
optional.

`repack::Job` options:

| Field | Meaning |
|---|---|
| `table` | Resolved `Table`, including config and policy. Serialized only as a qualified table name. |
| `begin`, `end` | `Bound::Tz(instant)` for `timestamptz` columns, `Bound::Naive(wall clock)` for `timestamp`/`date` (midnight for `date`); `end` is exclusive. |
| `unique` | Optional explicit key-column override; if omitted, uses `unique_by_cols` resolved from the `Table`. Configure `Table::builder(...).without_deduplication()` to explicitly disable table-tag deduplication. |
| `files` | Optional freshness guard of catalog paths; skip if none are current. |
| `expire_snapshot` | `Never`, `IfNoDedup` (builder/convenience default), `Always`, or `Referencing` (`do_repack`); see snapshot expiry below. Omitted serialized policies retain the legacy `Never` default. |
| `strictness` | Optional `raise` or `mash`; omitted keeps out-of-window rows in separate before/after files. |

### How repack works

**Selecting sources.** Repack reads the catalog (`ducklake.ducklake_*` tables in
Postgres) and selects every *current* data file (`end_snapshot IS NULL`) of the
table whose `Table::time_column` statistics overlap `[begin, end)`, plus
files whose min/max/null statistics are missing. File *names* are never parsed and
no naming convention is assumed; overlap is decided purely from the catalog's
per-file column min/max. `Table::repack` date boundaries are midnights:
`Table::timezone` for `timestamptz`, and plain wall-clock midnights for
`timestamp`/`date` columns.

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

**Snapshot expiry.** Set `Job::expire_snapshot` (maintenance `do_repack` sets
`Referencing` automatically):

| Policy | Behavior |
|---|---|
| `ExpireSnapshot::Never` | Retain all snapshots. Legacy serialized-job default. |
| `ExpireSnapshot::IfNoDedup` | Expire the immediate predecessor only if deduplication is disabled in both the job override and resolved table policy. Builder/convenience default. |
| `ExpireSnapshot::Always` | Expire the immediate predecessor even when deduplication changes logical data. |
| `ExpireSnapshot::Referencing` | Expire every snapshot referencing a replaced file, including with deduplication. Guarantees replaced files are queued for cleanup. |

Serialized jobs use `"expire_snapshot": "never"`, `"if_no_dedup"`, `"always"` or
`"referencing"`; omitting the field preserves the legacy `Never` default. Existing Rust `Job` literals must add
the field explicitly.

Expiry happens **atomically in the repack publication transaction**. With
`Always` or enabled `IfNoDedup`, publishing snapshot 13 expires its immediate
predecessor 12. If unrelated commits happen
while sorting, the predecessor is the latest snapshot at publication—not the
snapshot seen when the job started. Conflict retries recompute it. A skipped
job or failed transaction does not expire anything. `IfNoDedup` means deduplication
was not requested, not merely that it removed zero rows.

**Snapshot expiry is catalog-wide**: expiring a version discards time travel to
it for every table. Predecessor-only policies do not expire older versions.
Consequently, expiring 12 does **not** necessarily reclaim its input files:
snapshots 1–11 might still require them. `Referencing` instead expires every
retained snapshot in any replaced file's visibility interval, so none of those
files remain pinned. For example, a source introduced in snapshot 8 and replaced
by snapshot 13 requires expiring retained snapshots 8–12, while preserving 0–7
and the new snapshot 13. Files replaced by this repack (including their
positional-delete files) are queued for deletion only when no retained snapshot
needs them.
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
source Parquet files. Maintenance `do_repack` expires referencing snapshots;
ad hoc builders default to predecessor expiry only when deduplication is disabled.
Use `Never` to retain snapshots, or expire them through DuckLake, then use
`maintenance::cleanup_expired_files` (below) for queued files.
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
    let _layout = dkplus::Table::builder("weather.observations", cfg)
        .with_timezone(chrono_tz::UTC) // Optional if the column's dkplus.timezone tag is set.
        .with_row_group_col("station") // Optional if DuckLake's dkplus.row_group_col tag is set.
        .with_sort_cols(vec!["station".into(), "observed".into()]) // Optional if DuckLake's dkplus.sort_cols tag is set.
        .resolve()
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
files retired by repack can still belong to retained snapshots. Maintenance
`do_repack` expires all snapshots referencing its replaced files automatically.
For ad hoc jobs, use `ExpireSnapshot::Referencing` for the same guarantee, or
expire the necessary snapshots through DuckLake first;
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
  Files queued during repack become eligible only when `older_than` is strictly
  later than their queue timestamp. A cutoff from before that repack will not
  delete them, regardless of how old the original files were.
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

`find_potential_repacks` returns plans without enqueueing anything. Callers
choose reports, schedule, and deliver plans or jobs. Serialized
`PotentialRepack` and `Job` values include only the qualified table name, bounds,
rules and operation options, never config or policy. After deserializing a job,
assign a freshly resolved `Table` to `job.table` before calling
`job.run().await?`. For plans, pass a freshly resolved `Table` to `do_repack`.
Unresolved queued jobs fail validation rather than trusting queue messages to
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
