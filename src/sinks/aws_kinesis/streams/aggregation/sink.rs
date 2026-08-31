use std::{fmt::Debug, marker::PhantomData};

use vector_lib::stream::batcher::limiter::ItemBatchSize;

use super::request_builder::AggregateRequestBuilder;
use crate::{
    internal_events::SinkRequestBuildError,
    sinks::{
        aws_kinesis::{record::Record, sink::BatchKinesisRequest},
        prelude::*,
        util::StreamSink,
    },
};

/// Sizes an event by its estimated JSON encoding, which is what actually lands
/// in the record. The non-aggregating path instead abuses `ByteSizeOf` on the
/// already-encoded record; here the batch is closed before encoding, so the
/// estimate is the only figure available.
#[derive(Clone, Copy, Debug)]
struct AggregateSizer;

impl ItemBatchSize<Event> for AggregateSizer {
    fn size(&self, item: &Event) -> usize {
        item.estimated_json_encoded_size_of().get()
    }
}

/// A Kinesis Streams sink that packs many events into each record.
///
/// Two batching layers, with different units:
///   1. `aggregate_settings` groups events into one record (bounded by the 1 MB
///      Kinesis record limit).
///   2. `batch_settings` groups records into one `PutRecords` call (bounded by
///      500 records / 5 MB).
///
/// Encoding happens between them, so each record is a single compression frame
/// over all of its events.
#[derive(Clone)]
pub struct AggregatedKinesisSink<S, R> {
    pub batch_settings: BatcherSettings,
    pub aggregate_settings: BatcherSettings,
    pub service: S,
    pub request_builder: AggregateRequestBuilder<R>,
    pub _phantom: PhantomData<R>,
}

impl<S, R> AggregatedKinesisSink<S, R>
where
    S: Service<BatchKinesisRequest<R>> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: Debug + Into<crate::Error> + Send,
    R: Record + Send + Sync + Unpin + Clone + 'static,
{
    async fn run_inner(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        let batch_settings = self.batch_settings;

        input
            // Inner batch: events -> one record's worth.
            .batched(self.aggregate_settings.as_item_size_config(AggregateSizer))
            // Encode and compress the whole aggregate as one unit.
            .request_builder(
                default_request_builder_concurrency_limit(),
                self.request_builder,
            )
            .filter_map(|request| async move {
                match request {
                    Err(error) => {
                        emit!(SinkRequestBuildError { error });
                        None
                    }
                    Ok(req) => Some(req),
                }
            })
            // Outer batch: records -> one PutRecords call.
            .batched(batch_settings.as_byte_size_config())
            .map(|events| {
                let metadata = RequestMetadata::from_batch(
                    events.iter().map(|req| req.get_metadata().clone()),
                );
                BatchKinesisRequest { events, metadata }
            })
            .into_driver(self.service)
            .run()
            .await
    }
}

#[async_trait]
impl<S, R> StreamSink<Event> for AggregatedKinesisSink<S, R>
where
    S: Service<BatchKinesisRequest<R>> + Send + 'static,
    S::Future: Send + 'static,
    S::Response: DriverResponse + Send + 'static,
    S::Error: Debug + Into<crate::Error> + Send,
    R: Record + Send + Sync + Unpin + Clone + 'static,
{
    async fn run(self: Box<Self>, input: BoxStream<'_, Event>) -> Result<(), ()> {
        self.run_inner(input).await
    }
}
