//! Thread-scaling measurement for the parquet codec.
//!
//! The production failure was not raw encode cost -- it was 32 workers contending
//! on the refcount of ONE shared allocation. Events parsed out of a single S3
//! object hold `Bytes` that all slice the same buffer, so every clone/drop during
//! encoding hits the same cache line.
//!
//! Criterion's per-iteration `format!` corpus cannot show that: each value gets
//! its own allocation and therefore its own refcount. Here every string value is
//! a slice of one shared blob, which is what production looks like.
//!
//! Run with the codec's ROW_GROUP_ROWS set to usize::MAX and again at 1024 to
//! compare. Prints aggregate events/s per thread count.

use bytes::{Bytes, BytesMut};
use codecs::encoding::ParquetSerializerConfig;
use parquet::basic::Compression;
use std::time::Instant;
use tokio_util::codec::Encoder;
use vector_core::event::{Event, LogEvent, ObjectMap, Value};

const SCHEMA: &str = include_str!("fixtures/cloudtrail.schema");
const BATCH: usize = 10_000;

fn schema_fields() -> Vec<(String, &'static str)> {
    let mut fields = Vec::new();
    for line in SCHEMA.lines() {
        let line = line.trim().trim_end_matches(';');
        if line.starts_with("message") || line.starts_with('}') || line.is_empty() {
            continue;
        }
        let mut parts = line.split_whitespace();
        let _optionality = parts.next();
        let phys = parts.next().unwrap_or("");
        let name = match parts.next() {
            Some(n) => n.to_string(),
            None => continue,
        };
        let kind = match phys {
            "binary" => "binary",
            "int64" => "int64",
            "boolean" => "boolean",
            _ => continue,
        };
        fields.push((name, kind));
    }
    fields
}

/// One allocation holding every distinct string the corpus uses, mirroring a
/// parsed S3 object. Returns the blob plus the (offset, len) of each entry.
fn shared_blob(fields: &[(String, &'static str)], rows: usize) -> (Bytes, Vec<Vec<(usize, usize)>>) {
    let mut buf = BytesMut::with_capacity(8 * 1024 * 1024);
    let mut spans: Vec<Vec<(usize, usize)>> = Vec::with_capacity(fields.len());

    for (name, kind) in fields {
        let mut per_field = Vec::new();
        if *kind != "binary" {
            spans.push(per_field);
            continue;
        }
        // Distinct values per column, as in the criterion corpus.
        let card = match name.as_str() {
            "awsRegion" => 30,
            "eventSource" => 200,
            "eventName" => 2_000,
            "eventID" | "requestID" | "sharedEventID" | "exfS3ObjectName" => rows,
            _ => 500,
        }
        .min(rows);
        for n in 0..card {
            let s = if name.ends_with("arn") || name.ends_with("Arn") {
                format!("arn:aws:sts::123456789012:assumed-role/ExampleRoleName/session-{n}")
            } else {
                format!("{name}-value-{n}")
            };
            let off = buf.len();
            buf.extend_from_slice(s.as_bytes());
            per_field.push((off, s.len()));
        }
        spans.push(per_field);
    }
    (buf.freeze(), spans)
}

fn make_events(
    fields: &[(String, &'static str)],
    blob: &Bytes,
    spans: &[Vec<(usize, usize)>],
    rows: usize,
) -> Vec<Event> {
    (0..rows)
        .map(|i| {
            let mut map = ObjectMap::new();
            for (fi, (name, kind)) in fields.iter().enumerate() {
                let value = match *kind {
                    "int64" => Value::from(1_700_000_000_000i64 + i as i64),
                    "boolean" => Value::from(i % 2 == 0),
                    _ => {
                        let per_field = &spans[fi];
                        let (off, len) = per_field[i % per_field.len()];
                        // Slice of the shared blob: same refcount for every event.
                        Value::Bytes(blob.slice(off..off + len))
                    }
                };
                map.insert(name.as_str().into(), value);
            }
            Event::Log(LogEvent::from(map))
        })
        .collect()
}

fn main() {
    let fields = schema_fields();
    let (blob, spans) = shared_blob(&fields, BATCH);
    println!(
        "schema columns: {}  batch: {}  shared blob: {:.1} MiB",
        fields.len(),
        BATCH,
        blob.len() as f64 / 1048576.0
    );
    println!("{:>7}  {:>14}  {:>12}", "threads", "events/s", "vs 1 thread");

    let mut single = 0.0f64;
    for threads in [1usize, 2, 4, 8, 12] {
        // Each thread gets its own event vec, but all values share ONE blob.
        let per_thread: Vec<Vec<Event>> = (0..threads)
            .map(|_| make_events(&fields, &blob, &spans, BATCH))
            .collect();

        let start = Instant::now();
        std::thread::scope(|scope| {
            for events in &per_thread {
                scope.spawn(|| {
                    let mut ser = ParquetSerializerConfig::new(SCHEMA.to_string())
                        .build(Compression::UNCOMPRESSED)
                        .expect("schema builds");
                    let mut buf = BytesMut::with_capacity(16 * 1024 * 1024);
                    ser.encode(events.clone(), &mut buf).expect("encode");
                });
            }
        });
        let elapsed = start.elapsed().as_secs_f64();
        let total = (threads * BATCH) as f64 / elapsed;
        if threads == 1 {
            single = total;
        }
        println!("{threads:>7}  {total:>14.0}  {:>11.2}x", total / single);
    }
}
