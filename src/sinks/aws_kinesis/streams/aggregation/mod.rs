//! Record aggregation for the `aws_kinesis_streams` sink.
//!
//! Kinesis bills each record rounded up to 1 KB, and the default sink encodes
//! and compresses one event per record, so the compressor restarts every time
//! and cannot exploit redundancy between events. Aggregation packs many events
//! into one newline-delimited record compressed as a single frame, which
//! amortizes the rounding away and materially improves the ratio.
//!
//! Off by default; when disabled the sink takes its original code path.

mod build;
mod config;
mod request_builder;
mod sink;

pub use self::{build::build_aggregated_sink, config::KinesisAggregationConfig};
