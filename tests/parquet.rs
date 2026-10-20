use datafusion::arrow::array::*;
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use dkplus::{
    Storage,
    parquet::{Grouping, ParquetWriter, TARGET_ROWS},
};
use object_store::{ObjectStore, ObjectStoreExt, memory::InMemory, path::Path};
use std::sync::Arc;

fn scalar_arrays() -> Vec<ArrayRef> {
    vec![
        Arc::new(BooleanArray::from(vec![false, true, true])),
        Arc::new(Int8Array::from(vec![i8::MIN, 0, i8::MAX])),
        Arc::new(Int16Array::from(vec![i16::MIN, 0, i16::MAX])),
        Arc::new(Int32Array::from(vec![i32::MIN, 0, i32::MAX])),
        Arc::new(Int64Array::from(vec![i64::MIN, 0, i64::MAX])),
        Arc::new(UInt8Array::from(vec![0, 1, u8::MAX])),
        Arc::new(UInt16Array::from(vec![0, 1, u16::MAX])),
        Arc::new(UInt32Array::from(vec![0, 1, u32::MAX])),
        Arc::new(UInt64Array::from(vec![0, 1, u64::MAX])),
        Arc::new(Float32Array::from(vec![
            f32::NEG_INFINITY,
            0.0,
            f32::INFINITY,
        ])),
        Arc::new(Float64Array::from(vec![
            f64::NEG_INFINITY,
            0.0,
            f64::INFINITY,
        ])),
        Arc::new(StringArray::from(vec!["A", "B", "Z"])),
        Arc::new(Date32Array::from(vec![-100, 0, 100])),
        Arc::new(TimestampMicrosecondArray::from(vec![-100, 0, 100])),
        Arc::new(TimestampMicrosecondArray::from(vec![-100, 0, 100]).with_timezone("UTC")),
    ]
}

#[tokio::test]
async fn every_scalar_type_roundtrips_with_typed_grouping_across_batches() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    for (index, array) in scalar_arrays().into_iter().enumerate() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "sensor",
            array.data_type().clone(),
            false,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![array.clone()]).unwrap();
        let path = Path::from(format!("{index}.parquet"));
        let mut writer = ParquetWriter::new(
            store.clone(),
            path.clone(),
            schema,
            Grouping::SingleKey("sensor".into()),
        )
        .unwrap();
        writer.write(&batch.slice(0, 1)).await.unwrap();
        writer.write(&batch.slice(1, 2)).await.unwrap();
        let report = writer.finish().await.unwrap();
        assert_eq!(report.rows, 3);
        assert!(report.size_bytes > report.footer_bytes);
        let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let expected_groups = if array.data_type() == &DataType::Boolean {
            2
        } else {
            3
        };
        assert_eq!(
            reader.metadata().num_row_groups(),
            expected_groups,
            "{:?}",
            array.data_type()
        );
        for group in reader.metadata().row_groups() {
            let stats = group.column(0).statistics().unwrap();
            match array.data_type() {
                DataType::Float32 => assert_eq!(
                    f32::from_le_bytes(stats.min_bytes_opt().unwrap().try_into().unwrap()),
                    f32::from_le_bytes(stats.max_bytes_opt().unwrap().try_into().unwrap()),
                ),
                DataType::Float64 => assert_eq!(
                    f64::from_le_bytes(stats.min_bytes_opt().unwrap().try_into().unwrap()),
                    f64::from_le_bytes(stats.max_bytes_opt().unwrap().try_into().unwrap()),
                ),
                _ => assert_eq!(stats.min_bytes_opt(), stats.max_bytes_opt()),
            }
            assert!(stats.min_is_exact() && stats.max_is_exact());
        }
        let read = reader
            .build()
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        let output = datafusion::arrow::compute::concat_batches(&batch.schema(), &read).unwrap();
        assert_eq!(output.column(0).as_ref(), array.as_ref());
    }
}

#[tokio::test]
async fn rejects_unsorted_null_and_changed_schema_inputs() {
    let store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let schema = Arc::new(Schema::new(vec![Field::new(
        "sensor",
        DataType::Int32,
        true,
    )]));
    let mut writer = ParquetWriter::new(
        store.clone(),
        Path::from("bad.parquet"),
        schema.clone(),
        Grouping::WholeKeys("sensor".into()),
    )
    .unwrap();
    let batch = |values| {
        RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(values))]).unwrap()
    };
    writer.write(&batch(vec![Some(2)])).await.unwrap();
    assert!(writer.write(&batch(vec![Some(1)])).await.is_err());
    assert!(writer.write(&batch(vec![None])).await.is_err());
    let wrong = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new(
            "other",
            DataType::Int32,
            true,
        )])),
        vec![Arc::new(Int32Array::from(vec![3]))],
    )
    .unwrap();
    assert!(writer.write(&wrong).await.is_err());
}

#[tokio::test]
async fn filesystem_output_and_ungrouped_row_limit() {
    let dir = std::env::temp_dir().join(format!("dkplus local {}", uuid::Uuid::now_v7()));
    std::fs::create_dir(&dir).unwrap();
    let storage = Storage::local(&dir).unwrap();
    let table_url = url::Url::from_directory_path(dir.join("table")).unwrap();
    assert_eq!(storage.resolve(table_url.as_str()).unwrap().1, "table/");
    let file_url = url::Url::from_file_path(dir.join("part one.parquet")).unwrap();
    assert_eq!(
        storage.resolve(file_url.as_str()).unwrap().1,
        "part one.parquet"
    );
    assert_eq!(
        storage
            .resolve(dir.join("part one.parquet").to_str().unwrap())
            .unwrap()
            .1,
        "part one.parquet"
    );
    let Storage::Store { store, .. } = storage else {
        unreachable!()
    };
    let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
    let path = Path::from("output.parquet");
    let mut writer =
        ParquetWriter::new(store.clone(), path.clone(), schema.clone(), Grouping::None).unwrap();
    let array = Arc::new(Int32Array::from_value(7, TARGET_ROWS + 1));
    writer
        .write(&RecordBatch::try_new(schema, vec![array]).unwrap())
        .await
        .unwrap();
    writer.finish().await.unwrap();
    let bytes = store.get(&path).await.unwrap().bytes().await.unwrap();
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
    assert_eq!(reader.metadata().num_row_groups(), 2);
    assert_eq!(
        reader.metadata().row_group(0).num_rows(),
        TARGET_ROWS as i64
    );
    assert_eq!(reader.metadata().row_group(1).num_rows(), 1);
    store.delete(&path).await.unwrap();
    std::fs::remove_dir(dir).unwrap();
}
