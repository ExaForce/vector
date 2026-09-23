//! Encodes CloudTrail-shaped events against the real 78-column production schema.
//!
//! The deployed encoder is column-major: `encode()` walks every event once per
//! column, so cost scales with columns x events. This measures that, and gives a
//! baseline to compare any row-major rewrite against.

use bytes::BytesMut;
use codecs::encoding::ParquetSerializerConfig;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use parquet::basic::Compression;
use tokio_util::codec::Encoder;
use vector_core::event::{Event, LogEvent, ObjectMap, Value};

/// The production CloudTrail schema: 78 leaf columns, no nested groups.
const SCHEMA: &str = include_str!("fixtures/cloudtrail.schema");

/// Every leaf column name in SCHEMA, in declaration order.
///
/// Names carry literal dots (`userIdentity.accessKeyId`) because cloudtrail.vrl
/// flattens `userIdentity` and merges the flat keys into the record, so the
/// encoder sees top-level keys containing dots -- not a nested object.
fn schema_fields() -> Vec<(String, &'static str)> {
    let mut fields = Vec::new();
    for line in SCHEMA.lines() {
        let line = line.trim().trim_end_matches(';');
        if line.starts_with("message") || line.starts_with('}') || line.is_empty() {
            continue;
        }
        let mut parts = line.split_whitespace();
        let _optionality = parts.next();
        let phys = match parts.next() {
            Some(p) => p,
            None => continue,
        };
        let name = match parts.next() {
            Some(n) => n.to_string(),
            None => continue,
        };
        // Timestamp-annotated int64 columns still take an integer value.
        let kind = match phys {
            "binary" => "binary",
            "int64" => "int64",
            "boolean" => "boolean",
            other => panic!("unhandled physical type in schema: {other}"),
        };
        fields.push((name, kind));
    }
    fields
}

/// Distinct values a column takes across the corpus.
///
/// Cardinality matters as much as length here: parquet dictionary-encodes each
/// column, and a corpus of unique-per-row strings inflates dictionary growth
/// with row count, which would masquerade as encoder scaling. These approximate
/// real CloudTrail -- a handful of regions, a few hundred sources, IDs unique.
fn cardinality(name: &str) -> usize {
    match name {
        "awsRegion" => 30,
        "eventSource" => 200,
        "eventName" => 2_000,
        "eventType" | "eventCategory" | "eventVersion" | "userIdentity.type" => 8,
        "recipientAccountId" | "userIdentity.accountId" => 40,
        "userAgent" => 150,
        "sourceIPAddress" => 5_000,
        // Identifiers are genuinely unique per record.
        "eventID" | "requestID" | "sharedEventID" | "exfS3ObjectName" => usize::MAX,
        _ => 500,
    }
}

/// Builds one event with every column populated. String lengths are in the range
/// real CloudTrail records occupy -- ARNs and user agents dominate the payload.
fn make_event(fields: &[(String, &'static str)], i: usize) -> Event {
    let mut map = ObjectMap::new();
    for (name, kind) in fields {
        let value = match *kind {
            "int64" => Value::from(1_700_000_000_000i64 + i as i64),
            "boolean" => Value::from(i % 2 == 0),
            _ => {
                let card = cardinality(name);
                let n = if card == usize::MAX { i } else { i % card };
                match name.as_str() {
                    x if x.ends_with("arn") || x.ends_with("Arn") => Value::from(format!(
                        "arn:aws:sts::123456789012:assumed-role/ExampleRoleName/session-{n}"
                    )),
                    "userAgent" => Value::from(format!(
                        "aws-sdk-go/1.44.{n} (go1.19.3; linux; amd64) exec-env/AWS_Lambda_go1.x"
                    )),
                    "sourceIPAddress" => {
                        Value::from(format!("10.{}.{}.{}", n / 65536 % 256, n / 256 % 256, n % 256))
                    }
                    _ => Value::from(format!("{name}-value-{n}")),
                }
            }
        };
        map.insert(name.as_str().into(), value);
    }
    Event::Log(LogEvent::from(map))
}

fn bench_encode(c: &mut Criterion) {
    let fields = schema_fields();
    assert_eq!(fields.len(), 78, "schema fixture drifted from 78 columns");

    let mut group = c.benchmark_group("parquet_encode_cloudtrail");
    group.sample_size(10);

    for batch in [1_000usize, 10_000, 50_000] {
        let events: Vec<Event> = (0..batch).map(|i| make_event(&fields, i)).collect();
        group.throughput(Throughput::Elements(batch as u64));
        group.bench_with_input(BenchmarkId::from_parameter(batch), &events, |b, events| {
            b.iter_batched(
                || {
                    (
                        ParquetSerializerConfig::new(SCHEMA.to_string())
                            .build(Compression::UNCOMPRESSED)
                            .expect("schema builds"),
                        events.clone(),
                        BytesMut::with_capacity(16 * 1024 * 1024),
                    )
                },
                |(mut ser, events, mut buf)| {
                    ser.encode(events, &mut buf).expect("encode succeeds");
                    buf.len()
                },
                BatchSize::LargeInput,
            );
        });
    }
    group.finish();
}

criterion_group!(benches, bench_encode);
criterion_main!(benches);
