use std::marker::PhantomData;

use vector_lib::codecs::encoding::{Framer, NewlineDelimitedEncoder, SerializerConfig};

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

    // Newline framing is what makes an aggregate splittable by the consumer,
    // which is only sound if no event can contain a literal newline. Compact
    // JSON escapes them as the two characters `\n`, so a raw 0x0A never
    // reaches the payload. `pretty` is the trap: it emits real newlines, which
    // would split one event into several unparseable fragments.
    match config.encoding.config() {
        SerializerConfig::NativeJson => {}
        SerializerConfig::Json(json) if !json.options.pretty => {}
        SerializerConfig::Json(_) => {
            return Err("aggregation is incompatible with `encoding.json.pretty`: \
                        pretty-printed JSON contains literal newlines, which \
                        would split one event into several on the consumer"
                .into());
        }
        _ => {
            return Err("aggregation requires `encoding.codec` to be `json` or \
                        `native_json`: events are newline-delimited within a \
                        record, and any other codec may emit a literal newline \
                        that would split one event into two on the consumer"
                .into());
        }
    }

    let transformer = config.encoding.transformer();
    let serializer = config.encoding.build()?;

    let encoder = Encoder::<Framer>::new(NewlineDelimitedEncoder::default().into(), serializer);

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
