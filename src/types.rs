use anyhow::{Context, Result, bail};
use chrono::{DateTime, NaiveDate};
use datafusion::arrow::array::{Array, ArrayRef, AsArray, RecordBatch};
use datafusion::arrow::compute;
use datafusion::arrow::datatypes::*;
use datafusion::arrow::row::{RowConverter, SortField};
use datafusion::common::ScalarValue;
use std::cmp::Ordering;

pub(crate) fn arrow_type(ty: &str) -> Result<DataType> {
    Ok(match ty {
        "boolean" => DataType::Boolean,
        "int8" => DataType::Int8,
        "int16" => DataType::Int16,
        "int32" => DataType::Int32,
        "int64" => DataType::Int64,
        "uint8" => DataType::UInt8,
        "uint16" => DataType::UInt16,
        "uint32" => DataType::UInt32,
        "uint64" => DataType::UInt64,
        "float32" => DataType::Float32,
        "float64" => DataType::Float64,
        "varchar" => DataType::Utf8,
        "date" => DataType::Date32,
        "timestamp" | "timestamp_us" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "timestamptz" => DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
        _ => bail!("unsupported DuckLake column type {ty}"),
    })
}

pub(crate) fn matches(ty: &str, dt: &DataType) -> bool {
    if ty == "timestamptz" {
        return matches!(dt, DataType::Timestamp(TimeUnit::Microsecond, Some(_)));
    }
    arrow_type(ty).is_ok_and(|expected| expected == *dt)
}

pub(crate) fn supported(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::Date32
            | DataType::Timestamp(TimeUnit::Microsecond, _)
    )
}

pub(crate) fn catalog_type(dt: &DataType) -> Result<&'static str> {
    Ok(match dt {
        DataType::Boolean => "boolean",
        DataType::Int8 => "int8",
        DataType::Int16 => "int16",
        DataType::Int32 => "int32",
        DataType::Int64 => "int64",
        DataType::UInt8 => "uint8",
        DataType::UInt16 => "uint16",
        DataType::UInt32 => "uint32",
        DataType::UInt64 => "uint64",
        DataType::Float32 => "float32",
        DataType::Float64 => "float64",
        DataType::Utf8 => "varchar",
        DataType::Date32 => "date",
        DataType::Timestamp(TimeUnit::Microsecond, None) => "timestamp",
        DataType::Timestamp(TimeUnit::Microsecond, Some(_)) => "timestamptz",
        _ => bail!("unsupported column type {dt}"),
    })
}

fn is_nan(value: &ScalarValue) -> bool {
    matches!(value, ScalarValue::Float32(Some(v)) if v.is_nan())
        || matches!(value, ScalarValue::Float64(Some(v)) if v.is_nan())
}

pub(crate) fn text(value: &ScalarValue) -> Result<String> {
    Ok(match value {
        ScalarValue::TimestampMicrosecond(Some(v), tz) => {
            let time = DateTime::from_timestamp_micros(*v).context("timestamp out of range")?;
            if tz.is_some() {
                time.format("%Y-%m-%d %H:%M:%S%.f+00").to_string()
            } else {
                time.naive_utc().to_string()
            }
        }
        ScalarValue::Date32(Some(v)) => NaiveDate::from_ymd_opt(1970, 1, 1)
            .unwrap()
            .checked_add_signed(chrono::Duration::days(i64::from(*v)))
            .context("date out of range")?
            .to_string(),
        _ => value.to_string(),
    })
}

pub(crate) fn bounds(
    batches: &[RecordBatch],
    index: usize,
) -> Result<(Option<String>, Option<String>, Option<bool>)> {
    let dt = batches[0].schema().field(index).data_type().clone();
    if !supported(&dt) {
        bail!("unsupported column type {dt}");
    }
    let float = matches!(dt, DataType::Float32 | DataType::Float64);
    let (mut min, mut max): (Option<ScalarValue>, Option<ScalarValue>) = (None, None);
    let mut nan = false;
    for batch in batches {
        let (lo, hi, has_nan) = array_bounds(batch.column(index))?;
        nan |= has_nan;
        for value in [lo, hi].into_iter().flatten() {
            if min
                .as_ref()
                .is_none_or(|v| value.partial_cmp(v) == Some(Ordering::Less))
            {
                min = Some(value.clone());
            }
            if max
                .as_ref()
                .is_none_or(|v| value.partial_cmp(v) == Some(Ordering::Greater))
            {
                max = Some(value);
            }
        }
    }
    Ok((
        min.as_ref().map(text).transpose()?,
        max.as_ref().map(text).transpose()?,
        float.then_some(nan),
    ))
}

fn array_bounds(array: &ArrayRef) -> Result<(Option<ScalarValue>, Option<ScalarValue>, bool)> {
    macro_rules! primitive {
        ($ty:ty, $variant:ident) => {{
            let array = array.as_primitive::<$ty>();
            (
                compute::min(array).map(|v| ScalarValue::$variant(Some(v))),
                compute::max(array).map(|v| ScalarValue::$variant(Some(v))),
                false,
            )
        }};
    }
    macro_rules! float {
        ($ty:ty, $native:ty, $variant:ident) => {{
            let (mut lo, mut hi, mut nan) = (<$native>::INFINITY, <$native>::NEG_INFINITY, false);
            for value in array.as_primitive::<$ty>().iter().flatten() {
                if value.is_nan() {
                    nan = true;
                } else {
                    lo = lo.min(value);
                    hi = hi.max(value);
                }
            }
            if lo > hi {
                (None, None, nan)
            } else {
                (
                    Some(ScalarValue::$variant(Some(lo))),
                    Some(ScalarValue::$variant(Some(hi))),
                    nan,
                )
            }
        }};
    }
    Ok(match array.data_type() {
        DataType::Int8 => primitive!(Int8Type, Int8),
        DataType::Int16 => primitive!(Int16Type, Int16),
        DataType::Int32 => primitive!(Int32Type, Int32),
        DataType::Int64 => primitive!(Int64Type, Int64),
        DataType::UInt8 => primitive!(UInt8Type, UInt8),
        DataType::UInt16 => primitive!(UInt16Type, UInt16),
        DataType::UInt32 => primitive!(UInt32Type, UInt32),
        DataType::UInt64 => primitive!(UInt64Type, UInt64),
        DataType::Date32 => primitive!(Date32Type, Date32),
        DataType::Float32 => float!(Float32Type, f32, Float32),
        DataType::Float64 => float!(Float64Type, f64, Float64),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            let array = array.as_primitive::<TimestampMicrosecondType>();
            (
                compute::min(array).map(|v| ScalarValue::TimestampMicrosecond(Some(v), tz.clone())),
                compute::max(array).map(|v| ScalarValue::TimestampMicrosecond(Some(v), tz.clone())),
                false,
            )
        }
        DataType::Utf8 => {
            let array = array.as_string::<i32>();
            (
                compute::min_string(array).map(|v| ScalarValue::Utf8(Some(v.into()))),
                compute::max_string(array).map(|v| ScalarValue::Utf8(Some(v.into()))),
                false,
            )
        }
        DataType::Boolean => {
            let array = array.as_boolean();
            (
                compute::min_boolean(array).map(|v| ScalarValue::Boolean(Some(v))),
                compute::max_boolean(array).map(|v| ScalarValue::Boolean(Some(v))),
                false,
            )
        }
        other => bail!("unsupported column type {other}"),
    })
}

pub(crate) fn compare(ty: &str, a: &str, b: &str) -> Option<Ordering> {
    macro_rules! cmp {
        ($t:ty) => {
            a.parse::<$t>().ok()?.partial_cmp(&b.parse::<$t>().ok()?)
        };
    }
    match ty {
        "boolean" => cmp!(bool),
        "int8" | "int16" | "int32" | "int64" => cmp!(i64),
        "uint8" | "uint16" | "uint32" | "uint64" => cmp!(u64),
        "float32" => cmp!(f32),
        "float64" => cmp!(f64),
        "timestamptz" => {
            let parse = |s: &str| DateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f%#z").ok();
            Some(parse(a)?.cmp(&parse(b)?))
        }
        "timestamp" | "timestamp_us" => {
            let parse =
                |s: &str| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f").ok();
            Some(parse(a)?.cmp(&parse(b)?))
        }
        "date" => {
            let parse = |s: &str| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok();
            Some(parse(a)?.cmp(&parse(b)?))
        }
        "varchar" => Some(a.cmp(b)),
        _ => None,
    }
}

/// Order-preserving encoded scalar keys, shared by the writer and aligned repack.
pub(crate) fn keys(array: &ArrayRef) -> Result<Vec<String>> {
    if !supported(array.data_type()) || array.null_count() != 0 {
        bail!("row-group keys must be supported non-null scalars");
    }
    if let Some(strings) = array
        .as_any()
        .downcast_ref::<datafusion::arrow::array::StringArray>()
    {
        return Ok(strings.iter().flatten().map(str::to_owned).collect());
    }
    let converter = RowConverter::new(vec![SortField::new(array.data_type().clone())])?;
    let rows = converter.convert_columns(std::slice::from_ref(array))?;
    Ok(rows
        .iter()
        .map(|row| {
            use std::fmt::Write;
            let mut key = String::with_capacity(row.as_ref().len() * 2);
            for byte in row.as_ref() {
                write!(&mut key, "{byte:02x}").expect("writing to String");
            }
            key
        })
        .collect())
}

pub(crate) fn statistics_key(dt: &DataType, bytes: &[u8]) -> Result<String> {
    macro_rules! read {
        ($t:ty) => {
            <$t>::from_le_bytes(bytes.try_into().context("invalid statistic byte width")?)
        };
    }
    let value = match dt {
        DataType::Utf8 => ScalarValue::Utf8(Some(std::str::from_utf8(bytes)?.into())),
        DataType::Boolean => {
            if bytes.len() != 1 || bytes[0] > 1 {
                bail!("invalid boolean statistic");
            }
            ScalarValue::Boolean(Some(bytes[0] != 0))
        }
        DataType::Int8 => ScalarValue::Int8(Some(read!(i32).try_into()?)),
        DataType::Int16 => ScalarValue::Int16(Some(read!(i32).try_into()?)),
        DataType::Int32 => ScalarValue::Int32(Some(read!(i32))),
        DataType::Int64 => ScalarValue::Int64(Some(read!(i64))),
        DataType::UInt8 => ScalarValue::UInt8(Some(read!(u32).try_into()?)),
        DataType::UInt16 => ScalarValue::UInt16(Some(read!(u32).try_into()?)),
        DataType::UInt32 => ScalarValue::UInt32(Some(read!(u32))),
        DataType::UInt64 => ScalarValue::UInt64(Some(read!(u64))),
        DataType::Float32 => ScalarValue::Float32(Some(read!(f32))),
        DataType::Float64 => ScalarValue::Float64(Some(read!(f64))),
        DataType::Date32 => ScalarValue::Date32(Some(read!(i32))),
        DataType::Timestamp(TimeUnit::Microsecond, tz) => {
            ScalarValue::TimestampMicrosecond(Some(read!(i64)), tz.clone())
        }
        _ => bail!("unsupported row-group statistics type {dt}"),
    };
    if is_nan(&value) {
        bail!("NaN row-group statistics cannot prove exact bounds");
    }
    Ok(keys(&value.to_array_of_size(1)?)?.remove(0))
}
