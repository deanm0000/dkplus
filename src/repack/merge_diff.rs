use super::*;
use datafusion::datasource::MemTable;
use datafusion::prelude::SessionContext;

pub(super) struct DiffCommit<'a> {
    pub table: &'a Table,
    pub meta: &'a TableMeta,
    pub file: Option<&'a FileInfo>,
    pub inlined: &'a [RecordBatch],
}

pub(crate) async fn run(
    table: &Table,
    batches: Vec<RecordBatch>,
    diff_name: &str,
) -> Result<crate::MergeDiffReport> {
    table.validate()?;
    let unique = table
        .unique_by_cols
        .as_ref()
        .ok_or_else(|| Error::MissingRequiredSetting {
            table: table.to_string(),
            setting: "unique_by_cols",
        })?;
    let timestamp =
        table
            .dedup_timestamp
            .as_deref()
            .ok_or_else(|| Error::MissingRequiredSetting {
                table: table.to_string(),
                setting: "dedup_timestamp",
            })?;
    if unique.iter().any(|name| name == timestamp) {
        fail!(InvalidInput, "dedup_timestamp must not be a unique key");
    }
    let batches: Vec<_> = batches.into_iter().filter(|b| b.num_rows() > 0).collect();
    let (start, end) = table.replacement_bounds(&batches)?;
    for batch in &batches {
        if batch.schema() != batches[0].schema() {
            fail!(InvalidInput, "load batch schemas differ");
        }
    }
    let (diff_schema, diff_name) = diff_name
        .split_once('.')
        .unwrap_or((&table.schema, diff_name));
    let diff_table = Table {
        schema: diff_schema.into(),
        table: diff_name.into(),
        ..table.clone()
    };
    diff_table.validate()?;
    if diff_table.schema == table.schema && diff_table.table == table.table {
        fail!(InvalidInput, "diff table must differ from the target table");
    }
    let window = Window { start, end };
    let last = end
        .checked_sub_signed(chrono::Duration::microseconds(1))
        .context("merge window underflow")?;
    let date = |time: DateTime<Utc>| match table.time_type {
        TimeType::TimestampTz => time.with_timezone(&table.timezone).date_naive(),
        _ => time.date_naive(),
    };
    let job = table
        .repack(
            date(start),
            date(last)
                .succ_opt()
                .context("merge window end out of range")?,
        )?
        .with_expire_snapshot(ExpireSnapshot::Never);
    job.validate()?;
    if table.cfg.repack_run_bytes == 0 || table.cfg.repack_node_bytes == 0 {
        fail!(InvalidInput, "repack memory budgets must be positive");
    }
    let mut client = connect(&table.cfg).await?;
    lock_range(&client, &job, Window::for_job(&job)?).await?;
    let tx = client
        .build_transaction()
        .isolation_level(IsolationLevel::RepeatableRead)
        .read_only(true)
        .start()
        .await?;
    let meta = table_meta(&tx, &table.schema, &table.table).await?;
    let diff_meta = table_meta(&tx, &diff_table.schema, &diff_table.table).await?;
    resolve_columns(&meta, &batches[0].schema())?;
    resolve_columns(&diff_meta, &batches[0].schema())?;
    let data_schema = schema(&meta)?;
    job.validate_schema(&data_schema)?;
    let utc_col = meta
        .columns
        .get(&table.time_column)
        .context("missing time column")?;
    if types::arrow_type(&utc_col.ty)? != types::arrow_type(table.time_type.sql())? {
        fail!(InvalidInput, "window column type does not match table");
    }
    let inlined = read_inlined(&tx, &meta, table, window).await?;
    let selected = sources(&tx, meta.table_id, utc_col.id, table, window).await?;
    tx.commit().await?;
    (table.cfg.log)(&format!(
        "{table} [{start}, {end}): merging with diff table {diff_table}"
    ));

    let (store, prefix) = object_store_for(&table.cfg, &meta.root)?;
    let mut work = Work::with_limits(
        store,
        prefix,
        table.cfg.repack_run_bytes,
        table.cfg.repack_node_bytes,
    );
    let (store, prefix) = object_store_for(&table.cfg, &diff_meta.root)?;
    let mut diff_work = Work::with_limits(
        store,
        prefix,
        table.cfg.repack_run_bytes,
        table.cfg.repack_node_bytes,
    );
    let result = async {
        let mut existing = read_window(table, &meta, &selected, window).await?;
        existing.extend(inlined.iter().cloned());
        let (merged, history) = compare(&data_schema, existing, batches, unique, timestamp).await?;
        let files = if selected.is_empty() {
            Vec::new()
        } else {
            prepare(PrepareContext {
                cfg: &table.cfg,
                meta: &meta,
                sources: &selected,
                job: &job,
                window,
                discard_inside: true,
                work: &mut work,
                log: &table.cfg.log,
            })
            .await?
        };
        let incoming = upload(&meta, &merged, &mut work).await?;
        let archived = if history.iter().any(|batch| batch.num_rows() > 0) {
            Some(upload(&diff_meta, &history, &mut diff_work).await?)
        } else {
            None
        };
        let mut attempt = 1;
        let (snapshot_id, incoming_id, _, _) = loop {
            match commit(
                &mut client,
                CommitContext {
                    partition: None,
                    meta: &meta,
                    utc_col: utc_col.id,
                    selected: &selected,
                    job: &job,
                    window,
                    files: &files,
                    replacement: Some(&incoming),
                    diff: Some(DiffCommit {
                        table: &diff_table,
                        meta: &diff_meta,
                        file: archived.as_ref(),
                        inlined: &inlined,
                    }),
                },
            )
            .await
            {
                Ok(report) => break report,
                Err(error) if is_commit_conflict(&error) && attempt < MAX_COMMIT_ATTEMPTS => {
                    (table.cfg.log)(&format!(
                        "merge_diff commit conflict (attempt {attempt}), retrying: {error:#}"
                    ));
                    tokio::time::sleep(Duration::from_millis(250 * attempt as u64)).await;
                    attempt += 1;
                }
                Err(error) if is_commit_conflict(&error) => {
                    return Err(Error::CommitRetriesExhausted {
                        attempts: attempt,
                        source: Box::new(error),
                    });
                }
                Err(error) => return Err(error),
            }
        };
        let incoming_id = incoming_id.context("merged file was not published")?;
        work.outputs.clear();
        diff_work.outputs.clear();
        let report = |meta: &TableMeta, file: &FileInfo, id| crate::LoadReport {
            snapshot_id,
            data_file_id: id,
            path: format!("{}{}", meta.root, file.name),
            rows: file.rows,
            file_size_bytes: file.size,
            retired_files: Vec::new(),
            retired_rows: 0,
            rewritten_files: Vec::new(),
        };
        let mut target = report(&meta, &incoming, incoming_id);
        target.retired_files = selected.iter().map(|source| source.id).collect();
        target.retired_rows = selected.iter().map(|source| source.rows).sum();
        target.rewritten_files = target.retired_files.clone();
        let diff = archived.as_ref().map(|file| report(&diff_meta, file, incoming_id + 1));
        (table.cfg.log)(&format!(
            "{table}: committed merge_diff snapshot {snapshot_id}; wrote {} target rows and {} diff rows",
            target.rows, diff.as_ref().map_or(0, |report| report.rows)
        ));
        Ok(crate::MergeDiffReport { target, diff })
    }
    .await;
    work.cleanup(&table.cfg.log).await;
    diff_work.cleanup(&table.cfg.log).await;
    result
}

async fn upload(meta: &TableMeta, batches: &[RecordBatch], work: &mut Work) -> Result<FileInfo> {
    let cols = resolve_columns(meta, &batches[0].schema())?;
    let (buffer, parquet) = write_parquet(batches, &cols)?;
    let name = format!("ducklake-{}.parquet", Uuid::now_v7());
    let file = file_info(name, &buffer, &parquet, batches, &cols)?;
    let path = ObjPath::from(format!("{}{}", work.prefix, file.name));
    work.outputs.push(path.clone());
    work.store
        .put(&path, PutPayload::from(Bytes::from(buffer)))
        .await
        .with_context(|| format!("uploading {}{}", meta.root, file.name))?;
    Ok(file)
}

async fn read_window(
    table: &Table,
    meta: &TableMeta,
    selected: &[Source],
    window: Window,
) -> Result<Vec<RecordBatch>> {
    let data_schema = schema(meta)?;
    let target = with_identity(&data_schema);
    let indices: Vec<_> = (0..data_schema.fields().len()).collect();
    let mut batches = Vec::new();
    for source in selected {
        let mut deleted = Deletes::open(&table.cfg, meta, source).await?;
        let full = location(meta, &source.path, source.relative);
        let (store, path) = object_store_for(&table.cfg, &full)?;
        let mut stream = sort::reader(store, ObjPath::from(path)).await?;
        let mut offset = 0;
        while let Some(batch) = stream.try_next().await? {
            let normalized = normalize(&batch, &target, source, offset)?;
            let keep = deleted.mask(offset, batch.num_rows()).await?;
            offset += batch.num_rows() as i64;
            let batch = filter_record_batch(&normalized, &keep)?;
            let [_, inside, _] = split_in_window(&batch, table, window)?;
            if inside.num_rows() > 0 {
                batches.push(inside.project(&indices)?);
            }
        }
        if offset != source.rows || deleted.peek().await?.is_some() {
            fail!(
                InvalidInput,
                "record count or delete positions mismatch for source {}",
                source.id
            );
        }
    }
    Ok(batches)
}

pub(super) async fn read_inlined(
    client: &impl GenericClient,
    meta: &TableMeta,
    table: &Table,
    window: Window,
) -> Result<Vec<RecordBatch>> {
    use datafusion::arrow::array::StringArray;

    let schema = schema(meta)?;
    let columns = schema
        .fields()
        .iter()
        .map(|field| identifier(field.name()).map(|name| format!("{name}::text")))
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let time = identifier(&table.time_column)?;
    let ty = table.time_type.sql();
    let tables = client.query(
        "SELECT table_name FROM ducklake.ducklake_inlined_data_tables WHERE table_id = $1 ORDER BY table_name",
        &[&meta.table_id],
    ).await?;
    let mut batches = Vec::new();
    for row in tables {
        let name: String = row.get(0);
        let rows = client
            .query(
                &format!(
                    "SELECT {columns} FROM ducklake.{} WHERE end_snapshot IS NULL
                 AND {time}::{ty} >= $1::text::{ty} AND {time}::{ty} < $2::text::{ty}
                 ORDER BY {columns}",
                    identifier(&name)?,
                ),
                &[&table.bound(window.start), &table.bound(window.end)],
            )
            .await
            .with_context(|| format!("reading inlined rows in {name} for merge_diff"))?;
        if rows.is_empty() {
            continue;
        }
        let arrays = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| {
                let values = rows
                    .iter()
                    .map(|row| row.try_get::<_, Option<String>>(index))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                Ok(cast(&StringArray::from(values), field.data_type())?)
            })
            .collect::<Result<Vec<_>>>()?;
        batches.push(RecordBatch::try_new(schema.clone(), arrays)?);
    }
    Ok(batches)
}

async fn compare(
    schema: &SchemaRef,
    existing: Vec<RecordBatch>,
    incoming: Vec<RecordBatch>,
    unique: &[String],
    timestamp: &str,
) -> Result<(Vec<RecordBatch>, Vec<RecordBatch>)> {
    let ctx = SessionContext::new();
    for (name, batches) in [("existing", existing), ("incoming", incoming)] {
        let batches = batches
            .into_iter()
            .map(|batch| {
                let columns = schema
                    .fields()
                    .iter()
                    .map(|field| {
                        let array = batch
                            .column_by_name(field.name())
                            .with_context(|| format!("missing column {}", field.name()))?;
                        Ok(cast(array, field.data_type())?)
                    })
                    .collect::<Result<Vec<_>>>()?;
                let batch = RecordBatch::try_new(schema.clone(), columns)?;
                if batch.column(schema.index_of(timestamp)?).null_count() > 0 {
                    fail!(
                        InvalidInput,
                        "merge_diff dedup timestamp must not contain nulls"
                    );
                }
                Ok(batch)
            })
            .collect::<Result<Vec<_>>>()?;
        ctx.register_table(
            name,
            Arc::new(MemTable::try_new(schema.clone(), vec![batches])?),
        )?;
    }
    let keys = unique
        .iter()
        .map(|key| identifier(key))
        .collect::<Result<Vec<_>>>()?
        .join(", ");
    let ts = identifier(timestamp)?;
    let mut rank = "__dkplus_rank".to_owned();
    while schema.index_of(&rank).is_ok() {
        rank.push('_');
    }
    let rank = identifier(&rank)?;
    // Rank each side independently so duplicate keys cannot multiply the join.
    for (name, input) in [("old_rows", "existing"), ("new_rows", "incoming")] {
        ctx.register_table(name, ctx.sql(&format!(
            "SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY {keys} ORDER BY {ts} DESC) AS {rank} FROM {input}) WHERE {rank} = 1"
        )).await?.into_view())?;
    }
    let join = unique
        .iter()
        .map(|key| identifier(key).map(|key| format!("(o.{key} IS NOT DISTINCT FROM n.{key})")))
        .collect::<Result<Vec<_>>>()?
        .join(" AND ");
    let newer = format!("(o.{rank} IS NULL OR n.{ts} > o.{ts})");
    let select = |winner: bool| -> Result<String> {
        schema
            .fields()
            .iter()
            .map(|field| {
                let column = identifier(field.name())?;
                let condition = if winner {
                    format!("n.{rank} IS NOT NULL AND {newer}")
                } else {
                    format!("NOT {newer}")
                };
                Ok(format!(
                    "CASE WHEN {condition} THEN n.{column} ELSE o.{column} END AS {column}"
                ))
            })
            .collect::<Result<Vec<_>>>()
            .map(|columns| columns.join(", "))
    };
    let changed = schema
        .fields()
        .iter()
        .filter(|field| field.name() != timestamp && !unique.contains(field.name()))
        .map(|field| {
            identifier(field.name())
                .map(|column| format!("(o.{column} IS DISTINCT FROM n.{column})"))
        })
        .collect::<Result<Vec<_>>>()?;
    let changed = if changed.is_empty() {
        "false".into()
    } else {
        changed.join(" OR ")
    };
    let merged = ctx
        .sql(&format!(
            "SELECT {} FROM old_rows o FULL OUTER JOIN new_rows n ON {join}",
            select(true)?
        ))
        .await?
        .collect()
        .await?;
    let history = ctx
        .sql(&format!(
            "SELECT {} FROM old_rows o JOIN new_rows n ON {join} WHERE {changed}",
            select(false)?
        ))
        .await?
        .collect()
        .await?;
    Ok((merged, history))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::{Int32Array, StringArray, TimestampMicrosecondArray};
    use datafusion::common::ScalarValue;

    fn batch(rows: &[(&str, Option<i32>, Option<i64>)]) -> RecordBatch {
        RecordBatch::try_from_iter([
            (
                "node",
                Arc::new(StringArray::from_iter_values(rows.iter().map(|r| r.0))) as ArrayRef,
            ),
            (
                "value",
                Arc::new(Int32Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
            (
                "fetched",
                Arc::new(TimestampMicrosecondArray::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    fn rows(batches: &[RecordBatch]) -> Vec<(String, Option<i32>, i64)> {
        let mut rows = Vec::new();
        for batch in batches {
            for row in 0..batch.num_rows() {
                let ScalarValue::Utf8(Some(node)) =
                    ScalarValue::try_from_array(batch.column(0), row).unwrap()
                else {
                    panic!("expected node");
                };
                let ScalarValue::Int32(value) =
                    ScalarValue::try_from_array(batch.column(1), row).unwrap()
                else {
                    panic!("expected value");
                };
                let ScalarValue::TimestampMicrosecond(Some(timestamp), _) =
                    ScalarValue::try_from_array(batch.column(2), row).unwrap()
                else {
                    panic!("expected timestamp");
                };
                rows.push((node, value, timestamp));
            }
        }
        rows.sort();
        rows
    }

    async fn merge(old: RecordBatch, new: RecordBatch) -> (Vec<RecordBatch>, Vec<RecordBatch>) {
        compare(
            &old.schema(),
            vec![old.clone()],
            vec![new],
            &["node".into()],
            "fetched",
        )
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn newer_data_archives_only_changed_values_with_original_timestamp() {
        let old = batch(&[
            ("apple", Some(27), Some(23)),
            ("banana", Some(10), Some(23)),
        ]);
        let new = batch(&[
            ("apple", Some(28), Some(24)),
            ("banana", Some(10), Some(24)),
        ]);
        let (target, diff) = merge(old, new).await;
        assert_eq!(
            rows(&target),
            vec![
                ("apple".into(), Some(28), 24),
                ("banana".into(), Some(10), 24)
            ]
        );
        assert_eq!(rows(&diff), vec![("apple".into(), Some(27), 23)]);
    }

    #[tokio::test]
    async fn older_data_archives_changed_incoming_row_without_rolling_back_target() {
        let old = batch(&[
            ("apple", Some(27), Some(23)),
            ("banana", Some(10), Some(23)),
        ]);
        let new = batch(&[
            ("apple", Some(28), Some(22)),
            ("banana", Some(10), Some(22)),
        ]);
        let (target, diff) = merge(old.clone(), new).await;
        assert_eq!(rows(&target), rows(&[old]));
        assert_eq!(rows(&diff), vec![("apple".into(), Some(28), 22)]);
    }

    #[tokio::test]
    async fn mixed_timestamps_null_values_and_unmatched_keys() {
        let old = batch(&[
            ("changed", None, Some(1)),
            ("older", Some(2), Some(3)),
            ("same", None, Some(1)),
            ("missing", Some(4), Some(1)),
            ("tie", Some(5), Some(1)),
        ]);
        let new = batch(&[
            ("changed", Some(1), Some(2)),
            ("older", None, Some(2)),
            ("same", None, Some(2)),
            ("added", Some(3), Some(2)),
            ("tie", Some(6), Some(1)),
        ]);
        let (target, diff) = merge(old, new).await;
        assert_eq!(
            rows(&target),
            vec![
                ("added".into(), Some(3), 2),
                ("changed".into(), Some(1), 2),
                ("missing".into(), Some(4), 1),
                ("older".into(), Some(2), 3),
                ("same".into(), None, 2),
                ("tie".into(), Some(5), 1),
            ]
        );
        assert_eq!(
            rows(&diff),
            vec![
                ("changed".into(), None, 1),
                ("older".into(), None, 2),
                ("tie".into(), Some(6), 1),
            ]
        );
    }

    #[tokio::test]
    async fn duplicate_input_keys_use_latest_timestamp_across_batches() {
        let old = batch(&[("apple", Some(27), Some(23)), ("apple", Some(26), Some(22))]);
        let schema = old.schema();
        let new = vec![
            batch(&[("apple", Some(28), Some(25))]),
            batch(&[("apple", Some(29), Some(24))]),
        ];
        let (target, diff) = compare(&schema, vec![old], new, &["node".into()], "fetched")
            .await
            .unwrap();
        assert_eq!(rows(&target), vec![("apple".into(), Some(28), 25)]);
        assert_eq!(rows(&diff), vec![("apple".into(), Some(27), 23)]);
    }

    #[tokio::test]
    async fn empty_source_and_null_timestamps() {
        let new = batch(&[("apple", Some(28), Some(25))]);
        let (target, diff) = compare(
            &new.schema(),
            vec![],
            vec![new.clone()],
            &["node".into()],
            "fetched",
        )
        .await
        .unwrap();
        assert_eq!(rows(&target), rows(&[new]));
        assert!(rows(&diff).is_empty());
        let invalid = batch(&[("apple", Some(28), None)]);
        let error = compare(
            &invalid.schema(),
            vec![],
            vec![invalid.clone()],
            &["node".into()],
            "fetched",
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("must not contain nulls"));
    }

    #[tokio::test]
    async fn composite_nullable_keys_compare_all_value_columns_and_allow_internal_names() {
        use datafusion::arrow::array::Float64Array;

        let make = |timestamp, rank, amount| {
            RecordBatch::try_from_iter([
                (
                    "node",
                    Arc::new(StringArray::from(vec![None::<&str>])) as ArrayRef,
                ),
                ("time", Arc::new(Int32Array::from(vec![1])) as ArrayRef),
                (
                    "value",
                    Arc::new(Float64Array::from(vec![f64::NAN])) as ArrayRef,
                ),
                (
                    "__dkplus_rank",
                    Arc::new(Int32Array::from(vec![rank])) as ArrayRef,
                ),
                (
                    "amount",
                    Arc::new(Int32Array::from(vec![amount])) as ArrayRef,
                ),
                (
                    "fetched",
                    Arc::new(TimestampMicrosecondArray::from(vec![timestamp])) as ArrayRef,
                ),
            ])
            .unwrap()
        };
        let old = make(1, 7, 10);
        for new in [make(2, 8, 10), make(2, 7, 11)] {
            let (target, diff) = compare(
                &old.schema(),
                vec![old.clone()],
                vec![new],
                &["node".into(), "time".into()],
                "fetched",
            )
            .await
            .unwrap();
            assert_eq!(target.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
            assert_eq!(diff.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
            assert_eq!(
                diff[0]
                    .column_by_name("__dkplus_rank")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .value(0),
                7
            );
        }
        let (_, diff) = compare(
            &old.schema(),
            vec![old.clone()],
            vec![make(2, 7, 10)],
            &["node".into(), "time".into()],
            "fetched",
        )
        .await
        .unwrap();
        assert_eq!(diff.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    }

    #[tokio::test]
    async fn no_value_columns_produce_no_history() {
        let old = batch(&[("apple", Some(27), Some(23))])
            .project(&[0, 2])
            .unwrap();
        let new = batch(&[("apple", Some(28), Some(24))])
            .project(&[0, 2])
            .unwrap();
        let (target, diff) = compare(
            &old.schema(),
            vec![old.clone()],
            vec![new],
            &["node".into()],
            "fetched",
        )
        .await
        .unwrap();
        assert_eq!(target.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
        assert_eq!(diff.iter().map(RecordBatch::num_rows).sum::<usize>(), 0);
    }

    #[tokio::test]
    async fn invalid_input_fails_before_connecting() {
        let mut table = crate::test_support::table();
        let input = batch(&[("apple", Some(27), Some(23))]);
        assert!(matches!(
            table
                .merge_diff(vec![input.clone()], "history")
                .await
                .unwrap_err(),
            Error::MissingRequiredSetting {
                setting: "unique_by_cols",
                ..
            }
        ));
        table.unique_by_cols = Some(vec!["node".into()]);
        table.dedup_timestamp = Some("node".into());
        assert!(
            table
                .merge_diff(vec![input], "history")
                .await
                .unwrap_err()
                .to_string()
                .contains("must not be a unique key")
        );
        table.dedup_timestamp = Some("fetched".into());
        assert!(
            table
                .merge_diff(vec![], "history")
                .await
                .unwrap_err()
                .to_string()
                .contains("no rows to load")
        );
    }
}
