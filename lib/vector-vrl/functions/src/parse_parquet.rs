use std::panic::{self, AssertUnwindSafe};

use chrono::{DateTime, NaiveDate, Utc};
use parquet::{
    file::reader::{FileReader, SerializedFileReader},
    record::{Field as PqField, Row},
};
use vrl::prelude::*;

fn parse_parquet(value: Value) -> Resolved {
    let bytes = value.try_bytes()?;
    // parquet 39's row reader panics instead of returning errors on corrupt pages.
    panic::catch_unwind(AssertUnwindSafe(|| read_rows(bytes)))
        .unwrap_or_else(|_| Err("unable to parse parquet: malformed page data".into()))
}

fn read_rows(bytes: Bytes) -> Resolved {
    let reader = SerializedFileReader::new(bytes)
        .map_err(|err| format!("unable to parse parquet: {err}"))?;
    let rows = reader
        .get_row_iter(None)
        .map_err(|err| format!("unable to parse parquet: {err}"))?;
    Ok(Value::Array(rows.map(|row| row_to_value(&row)).collect()))
}

fn row_to_value(row: &Row) -> Value {
    Value::Object(
        row.get_column_iter()
            .map(|(name, field)| (name.as_str().into(), field_to_value(field)))
            .collect(),
    )
}

fn field_to_value(field: &PqField) -> Value {
    match field {
        PqField::Null => Value::Null,
        PqField::Bool(v) => Value::Boolean(*v),
        PqField::Byte(v) => Value::Integer(i64::from(*v)),
        PqField::Short(v) => Value::Integer(i64::from(*v)),
        PqField::Int(v) => Value::Integer(i64::from(*v)),
        PqField::Long(v) => Value::Integer(*v),
        PqField::UByte(v) => Value::Integer(i64::from(*v)),
        PqField::UShort(v) => Value::Integer(i64::from(*v)),
        PqField::UInt(v) => Value::Integer(i64::from(*v)),
        // Values above i64::MAX stay lossless as strings.
        PqField::ULong(v) => i64::try_from(*v)
            .map(Value::Integer)
            .unwrap_or_else(|_| Value::from(v.to_string())),
        PqField::Float(v) => float_to_value(f64::from(*v)),
        PqField::Double(v) => float_to_value(*v),
        // Strings keep full precision; floats would not.
        PqField::Decimal(_) => Value::from(field.to_string()),
        PqField::Str(v) => Value::from(v.as_str()),
        PqField::Bytes(v) => Value::Bytes(Bytes::copy_from_slice(v.data())),
        PqField::Date(days) => NaiveDate::from_ymd_opt(1970, 1, 1)
            .and_then(|epoch| epoch.checked_add_signed(chrono::Duration::days(i64::from(*days))))
            .and_then(|date| date.and_hms_opt(0, 0, 0))
            .map(|dt| Value::Timestamp(dt.and_utc()))
            .unwrap_or(Value::Integer(i64::from(*days))),
        PqField::TimestampMillis(v) => DateTime::<Utc>::from_timestamp_millis(*v)
            .map(Value::Timestamp)
            .unwrap_or(Value::Integer(*v)),
        PqField::TimestampMicros(v) => DateTime::<Utc>::from_timestamp_micros(*v)
            .map(Value::Timestamp)
            .unwrap_or(Value::Integer(*v)),
        PqField::Group(row) => row_to_value(row),
        PqField::ListInternal(list) => {
            Value::Array(list.elements().iter().map(field_to_value).collect())
        }
        PqField::MapInternal(map) => Value::Object(
            map.entries()
                .iter()
                .map(|(key, value)| (map_key(key).into(), field_to_value(value)))
                .collect(),
        ),
    }
}

fn float_to_value(v: f64) -> Value {
    NotNan::new(v).map(Value::Float).unwrap_or(Value::Null)
}

fn map_key(key: &PqField) -> String {
    match key {
        PqField::Str(s) => s.clone(),
        other => other.to_string(),
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ParseParquet;

impl Function for ParseParquet {
    fn identifier(&self) -> &'static str {
        "parse_parquet"
    }

    fn usage(&self) -> &'static str {
        indoc! {"
            Parses the provided `value` as an Apache Parquet file and returns its rows as an
            array of objects. Snappy, gzip and zstd column compression are supported.
            Timestamps and dates become timestamps, decimals become strings, and NaN
            floats become null.
        "}
    }

    fn category(&self) -> &'static str {
        Category::Parse.as_ref()
    }

    fn return_kind(&self) -> u16 {
        kind::ARRAY
    }

    fn parameters(&self) -> &'static [Parameter] {
        const PARAMETERS: &[Parameter] = &[Parameter::required(
            "value",
            kind::BYTES,
            "The Parquet file contents to parse.",
        )];
        PARAMETERS
    }

    fn examples(&self) -> &'static [Example] {
        &[example! {
            title: "Invalid Parquet",
            source: r#"parse_parquet!("not parquet")"#,
            result: Err(
                r#"function call error for "parse_parquet" at (0:29): unable to parse parquet: Parquet error: Invalid Parquet file. Corrupt footer"#,
            ),
        }]
    }

    fn compile(
        &self,
        _state: &TypeState,
        _ctx: &mut FunctionCompileContext,
        arguments: ArgumentList,
    ) -> Compiled {
        let value = arguments.required("value");
        Ok(ParseParquetFn { value }.as_expr())
    }
}

#[derive(Debug, Clone)]
struct ParseParquetFn {
    value: Box<dyn Expression>,
}

impl FunctionExpression for ParseParquetFn {
    fn resolve(&self, ctx: &mut Context) -> Resolved {
        let value = self.value.resolve(ctx)?;
        parse_parquet(value)
    }

    fn type_def(&self, _: &TypeState) -> TypeDef {
        TypeDef::array(Collection::from_unknown(Kind::object(Collection::any()))).fallible()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vrl::value;

    const VPC_FLOW_GZIP: &[u8] = include_bytes!("../tests/data/vpc_flow_gzip.parquet");
    const TYPES_ZSTD: &[u8] = include_bytes!("../tests/data/types_zstd.parquet");

    fn parse(bytes: &[u8]) -> Resolved {
        parse_parquet(Value::Bytes(Bytes::copy_from_slice(bytes)))
    }

    fn ts(s: &str) -> Value {
        Value::Timestamp(DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc))
    }

    #[test]
    fn flat_rows_across_row_groups() {
        let rows = parse(VPC_FLOW_GZIP).unwrap();
        let rows = rows.as_array().unwrap();
        assert_eq!(rows.len(), 3);
        assert_eq!(
            rows[0],
            value!({
                "version": 2,
                "account_id": "123456789012",
                "interface_id": "eni-0a1b2c3d",
                "srcaddr": "10.0.0.1",
                "dstaddr": "10.0.1.1",
                "srcport": 443,
                "dstport": 49152,
                "protocol": 6,
                "packets": 10,
                "bytes": 8400,
                "start": 1727136000,
                "end": 1727136060,
                "action": "ACCEPT",
                "log_status": "OK",
            })
        );
        assert_eq!(rows[1].get("srcport"), Some(&Value::Null));
        assert_eq!(rows[2].get("action"), Some(&value!("REJECT")));
    }

    #[test]
    fn type_mapping() {
        let rows = parse(TYPES_ZSTD).unwrap();
        let rows = rows.as_array().unwrap();
        assert_eq!(rows.len(), 1);
        let row = &rows[0];
        let expected = [
            ("b", value!(true)),
            ("i8", value!(-8)),
            ("u64", value!("18446744073709551615")),
            ("f32", Value::Null),
            ("f64", value!(1.5)),
            ("dec", value!("-12345.67")),
            ("s", value!("hello")),
            ("bin", Value::Bytes(Bytes::from_static(b"\x00\xff"))),
            ("d", ts("2024-09-24T00:00:00Z")),
            ("ts_ms", ts("2024-09-24T01:02:03.456Z")),
            ("ts_us", ts("2024-09-24T01:02:03.456789Z")),
            ("st", value!({"x": 1, "y": "a"})),
            ("lst", value!([1, null, 3])),
            ("m", value!({"k1": 1, "k2": 2})),
            ("los", value!([{"n": "a"}, {"n": "b"}])),
        ];
        assert_eq!(row.as_object().unwrap().len(), expected.len());
        for (name, want) in expected {
            assert_eq!(row.get(name), Some(&want), "column {name}");
        }
    }

    #[test]
    fn truncated_file_errors() {
        let err = parse(&VPC_FLOW_GZIP[..VPC_FLOW_GZIP.len() - 1]).unwrap_err();
        assert!(
            err.to_string().starts_with("unable to parse parquet"),
            "{err}"
        );
    }

    #[test]
    fn corrupt_page_header_errors_instead_of_panicking() {
        // Flipping the first page-header byte panics parquet 39's row reader.
        let mut bytes = VPC_FLOW_GZIP.to_vec();
        bytes[4] ^= 0xff;
        let err = parse(&bytes).unwrap_err();
        assert_eq!(
            err.to_string(),
            "unable to parse parquet: malformed page data"
        );
    }

    #[test]
    fn non_bytes_input_errors() {
        assert!(parse_parquet(value!(1)).is_err());
    }
}
