use std::{io, marker::PhantomData};

use bytes::Bytes;
use uuid::Uuid;
use vector_lib::{codecs::encoding::Framer, request_metadata::RequestMetadata};

use crate::{
    codecs::{Encoder, Transformer},
    event::{Event, Finalizable},
    sinks::{
        aws_kinesis::{
            record::Record,
            request_builder::{KinesisMetadata, KinesisRequest},
            sink::KinesisKey,
        },
        util::{
            Compression, RequestBuilder, metadata::RequestMetadataBuilder,
            request_builder::EncodeResult,
        },
    },
};

/// Builds one Kinesis record from *many* events.
///
/// The non-aggregating builder is `RequestBuilder<KinesisProcessedEvent>` with
/// `Events = Event`, so `encode_events` constructs a fresh compressor per event
/// and every record ends up its own compression frame. Taking `Vec<Event>` here
/// is what collapses that to one frame per record.
#[derive(Clone)]
pub struct AggregateRequestBuilder<R> {
    pub compression: Compression,
    pub encoder: (Transformer, Encoder<Framer>),
    pub _phantom: PhantomData<R>,
}

impl<R> RequestBuilder<Vec<Event>> for AggregateRequestBuilder<R>
where
    R: Record,
{
    type Metadata = KinesisMetadata;
    type Events = Vec<Event>;
    type Encoder = (Transformer, Encoder<Framer>);
    type Payload = Bytes;
    type Request = KinesisRequest<R>;
    type Error = io::Error;

    fn compression(&self) -> Compression {
        self.compression
    }

    fn encoder(&self) -> &Self::Encoder {
        &self.encoder
    }

    fn split_input(
        &self,
        mut events: Vec<Event>,
    ) -> (Self::Metadata, RequestMetadataBuilder, Self::Events) {
        let builder = RequestMetadataBuilder::from_events(&events);

        // One key per record rather than per event. A random key keeps the MD5
        // hash distribution — and therefore shard distribution — uniform. The
        // sink's `partition_key_field` cannot be honoured here because the
        // events in one record may disagree on it.
        let kinesis_metadata = KinesisMetadata {
            finalizers: events.take_finalizers(),
            partition_key: Uuid::new_v4().to_string(),
        };

        (kinesis_metadata, builder, events)
    }

    fn build_request(
        &self,
        kinesis_metadata: Self::Metadata,
        metadata: RequestMetadata,
        payload: EncodeResult<Self::Payload>,
    ) -> Self::Request {
        let payload_bytes = payload.into_payload();
        let record = R::new(&payload_bytes, &kinesis_metadata.partition_key);

        KinesisRequest::new(
            KinesisKey {
                partition_key: kinesis_metadata.partition_key,
            },
            record,
            kinesis_metadata.finalizers,
            metadata,
        )
    }
}
