use std::{num::NonZeroUsize, time::Duration};

use vector_lib::configurable::configurable_component;
use vector_lib::stream::BatcherSettings;

/// Largest aggregate we will ever build, measured BEFORE compression.
///
/// A Kinesis record is capped at 1 MB on the wire. We bound the uncompressed
/// input instead, because the compressed size is not known until after the
/// batch is closed and `RequestBuilder` can only emit one request per batch.
/// Staying well under 1 MB means even wholly incompressible input still fits.
pub const MAX_AGGREGATE_BYTES: usize = 900_000;

/// Aggregation settings for the `aws_kinesis_streams` sink.
///
/// When enabled, many events are concatenated into a single Kinesis record
/// (newline-delimited) and compressed as one unit. Kinesis bills each record
/// rounded up to 1 KB, so aggregation both amortizes that rounding away and
/// lets the compressor exploit redundancy across events instead of restarting
/// per record.
#[configurable_component]
#[derive(Clone, Copy, Debug)]
#[serde(deny_unknown_fields)]
pub struct KinesisAggregationConfig {
    /// Whether to aggregate multiple events into each Kinesis record.
    #[serde(default)]
    pub enabled: bool,

    /// Maximum number of events to place in a single Kinesis record.
    #[serde(default = "default_max_events")]
    pub max_events: usize,

    /// Maximum uncompressed size, in bytes, of a single Kinesis record.
    ///
    /// This bounds the input to the compressor, not the resulting record, so it
    /// must leave headroom under the 1 MB Kinesis record limit for input that
    /// does not compress.
    #[serde(default = "default_max_bytes")]
    pub max_bytes: usize,

    /// Maximum age, in seconds, of an aggregate before it is flushed.
    ///
    /// Only reached by sources that cannot fill a record within the window, so
    /// it trades latency for aggregation on low-volume streams.
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: f64,
}

const fn default_max_events() -> usize {
    500
}

const fn default_max_bytes() -> usize {
    262_144
}

const fn default_timeout_secs() -> f64 {
    5.0
}

impl Default for KinesisAggregationConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_events: default_max_events(),
            max_bytes: default_max_bytes(),
            timeout_secs: default_timeout_secs(),
        }
    }
}

impl KinesisAggregationConfig {
    /// Builds the inner batcher settings, which group events into one record.
    ///
    /// Deliberately does not go through `BatchConfig`: that type is clamped to
    /// the `PutRecords` limits (500 records / 5 MB per API call), which are the
    /// units of the *outer* batch. These are events per record.
    pub fn into_batcher_settings(self) -> crate::Result<BatcherSettings> {
        if self.max_events == 0 {
            return Err("aggregation.max_events must be greater than 0".into());
        }
        if self.max_bytes == 0 {
            return Err("aggregation.max_bytes must be greater than 0".into());
        }
        if self.max_bytes > MAX_AGGREGATE_BYTES {
            return Err(format!(
                "aggregation.max_bytes must be at most {MAX_AGGREGATE_BYTES} to stay \
                 under the 1 MB Kinesis record limit once framing is added, got {}",
                self.max_bytes
            )
            .into());
        }
        if !(self.timeout_secs.is_finite() && self.timeout_secs > 0.0) {
            return Err("aggregation.timeout_secs must be a positive number".into());
        }

        Ok(BatcherSettings::new(
            Duration::from_secs_f64(self.timeout_secs),
            NonZeroUsize::new(self.max_bytes).expect("checked above"),
            NonZeroUsize::new(self.max_events).expect("checked above"),
        ))
    }
}
