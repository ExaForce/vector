use std::marker::PhantomData;

use vector_lib::codecs::encoding::{Framer, NewlineDelimitedEncoder, Serializer};

use super::{request_builder::AggregateRequestBuilder, sink::AggregatedKinesisSink};
use crate::sinks::{
    aws_kinesis::{
        config::KinesisSinkBaseConfig,
        record::{Record, SendRecord},
        service::{KinesisResponse, KinesisService},
        sink::BatchKinesisRequest,
    },
    prelude::*,
};

/// Builds an aggregating `aws_kinesis_streams` sink.
///
/// Mirrors `aws_kinesis::config::build_sink`, differing only in the encoder and
/// request builder: a newline-delimited `Encoder<Framer>` over `Vec<Event>`
/// instead of an unframed `Encoder<()>` over a single `Event`.
pub fn build_aggregated_sink<C, R, RR, E, RT>(
    config: &KinesisSinkBaseConfig,
    batch_settings: BatcherSettings,
    aggregate_settings: BatcherSettings,
    client: C,
    retry_logic: RT,
) -> crate::Result<VectorSink>
where
    C: SendRecord + Clone + Send + Sync + 'static,
    <C as SendRecord>::T: Send,
    <C as SendRecord>::E: Send + Sync + snafu::Error,
    Vec<<C as SendRecord>::T>: FromIterator<R>,
    R: Send + 'static,
    RR: Record + Record<T = R> + Clone + Send + Sync + Unpin + 'static,
    E: Send + 'static,
    RT: RetryLogic<Request = BatchKinesisRequest<RR>, Response = KinesisResponse> + Default,
{
    let request_limits = config.request.into_settings();

    let region = config.region.region();
    let service = ServiceBuilder::new()
        .settings::<RT, BatchKinesisRequest<RR>>(request_limits, retry_logic)
        .service(KinesisService::<C, R, E> {
            client,
            stream_name: config.stream_name.clone(),
            region,
            _phantom_t: PhantomData,
            _phantom_e: PhantomData,
        });

    let transformer = config.encoding.transformer();
    let serializer = config.encoding.build()?;

    // Newline framing is what makes an aggregate splittable by the consumer,
    // which is only sound if no event can contain a literal newline. JSON
    // escapes them as the two characters `\n`, so a raw 0x0A never appears
    // inside a serialized event. A `text` or `raw_message` payload carries the
    // bytes through untouched, and one embedded newline would silently split
    // one event into two on the far side -- so refuse to build rather than
    // corrupt the stream.
    match serializer {
        Serializer::Json(_) | Serializer::NativeJson(_) => {}
        _ => {
            return Err("aggregation requires `encoding.codec` to be `json` or \
                        `native_json`: events are newline-delimited within a \
                        record, and any other codec may emit a literal newline \
                        that would split one event into two on the consumer"
                .into());
        }
    }

    let encoder = Encoder::<Framer>::new(
        NewlineDelimitedEncoder::default().into(),
        serializer,
    );

    let request_builder = AggregateRequestBuilder::<RR> {
        compression: config.compression,
        encoder: (transformer, encoder),
        _phantom: PhantomData,
    };

    let sink = AggregatedKinesisSink {
        batch_settings,
        aggregate_settings,
        service,
        request_builder,
        _phantom: PhantomData,
    };
    Ok(VectorSink::from_event_streamsink(sink))
}

