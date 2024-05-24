use crate::config::{SourceAcknowledgementsConfig, SourceContext};
use async_stream::stream;
// use crate::internal_events::EventsReceived;
use crate::shutdown::ShutdownSignal;
use crate::sinks::prelude::configurable_component;
use crate::sources::azure_blob::BlobStream;
use crate::SourceSender;
use azure_storage_blobs;
use azure_storage_queues;
use base64::prelude::BASE64_STANDARD;
use base64::Engine;
use futures::{stream::StreamExt, FutureExt};
use serde::Deserialize;
use serde_with::serde_as;
use snafu::Snafu;
use std::{
    io::{BufRead, BufReader, Cursor},
    panic,
    sync::Arc,
};
use tokio::{pin, select};
use vector_lib::config::LogNamespace;
// use vector_lib::internal_event::{BytesReceived, Protocol, Registered};

/// Azure Queue configuration options.
#[serde_as]
#[configurable_component]
#[derive(Clone, Debug, Derivative)]
#[derivative(Default)]
#[serde(deny_unknown_fields)]
pub(super) struct Config {
    /// The name of the storage queue to poll for events.
    pub(super) queue_name: String,
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Snafu)]
pub enum ProcessingError {
    NoValidMessage,
    WrongEventType,
}

pub struct State {
    container_client: Arc<azure_storage_blobs::prelude::ContainerClient>,
    queue_client: Arc<azure_storage_queues::QueueClient>,
}

pub(super) struct Ingestor {
    state: Arc<State>,
}

impl Ingestor {
    pub(super) async fn new(
        container_client: Arc<azure_storage_blobs::prelude::ContainerClient>,
        queue_client: Arc<azure_storage_queues::QueueClient>,
    ) -> Result<Ingestor, ()> {
        let state = Arc::new(State {
            container_client,
            queue_client,
        });

        Ok(Ingestor { state })
    }

    pub(super) async fn run(
        self,
        cx: SourceContext,
        acknowledgements: SourceAcknowledgementsConfig,
        log_namespace: LogNamespace,
    ) -> Result<BlobStream, ProcessingError> {
        let acknowledgements = cx.do_acknowledgements(acknowledgements);
        let process = IngestorProcess::new(
            Arc::clone(&self.state),
            cx.out.clone(),
            cx.shutdown.clone(),
            log_namespace,
            acknowledgements,
        );
        process.run().await
    }
}

pub struct IngestorProcess {
    state: Arc<State>,
    // out: SourceSender,
    shutdown: ShutdownSignal,
    // acknowledgements: bool,
    // log_namespace: LogNamespace,
    // bytes_received: Registered<BytesReceived>,
    // events_received: Registered<EventsReceived>,
}

impl IngestorProcess {
    pub fn new(
        state: Arc<State>,
        _: SourceSender,
        shutdown: ShutdownSignal,
        _: LogNamespace,
        _: bool,
    ) -> Self {
        Self {
            state,
            // out,
            shutdown,
            // acknowledgements,
            // log_namespace,
            // bytes_received: register!(BytesReceived::from(Protocol::HTTP)),
            // events_received: register!(EventsReceived),
        }
    }

    async fn run(mut self) -> Result<BlobStream, ProcessingError> {
        let shutdown = self.shutdown.clone().fuse();
        pin!(shutdown);

        loop {
            select! {
                _ = &mut shutdown => { 
                    // TODO
                                       },
                _ = self.run_once() => {},
            }
        }
    }

    async fn run_once(&mut self) -> Result<BlobStream, ProcessingError> {
        // TODO this is a PoC. Need better error handling
        let messages_result = self.state.queue_client.get_messages().await;
        let messages = messages_result.expect("Failed reading messages");

        for message in messages.messages {
            match self.handle_storage_event(message).await {
                Ok(stream) => {
                    // TODO do telemetry here and below instead of logs.
                    info!("Handled event!");
                    return Ok(stream); // Return the stream here
                }
                Err(err) => {
                    info!("Failed handling event: {:#?}", err);
                }
            }
        }
        Err(ProcessingError::NoValidMessage) // Or some other appropriate error
    }

    async fn handle_storage_event(
        &mut self,
        message: azure_storage_queues::operations::Message,
    ) -> Result<BlobStream, ProcessingError> {
        let decoded_bytes = BASE64_STANDARD
            .decode(&message.message_text)
            .expect("Failed decoding message");
        let decoded_string = String::from_utf8(decoded_bytes).expect("Failed decoding UTF");
        let body: AzureStorageEvent = serde_json::from_str(&decoded_string).expect("Wrong JSON");

        // TODO get the event type const from library?
        if body.event_type != "Microsoft.Storage.BlobCreated" {
            info!(
                "Ignoring event because of wrong event type: {}",
                body.event_type
            );
            return Err(ProcessingError::WrongEventType);
        }

        // TODO some smarter parsing should be done here
        let parts = body.subject.split('/').collect::<Vec<_>>();
        let container = parts[4];
        // TODO here we'd like to check if container matches the container from config.
        let blob = parts[6];
        info!(
            "New blob created in container '{}': '{}'",
            &container, &blob
        );

        let blob_client = self.state.container_client.blob_client(blob);

        let mut result: Vec<u8> = vec![];
        let mut stream = blob_client.get().into_stream();
        while let Some(value) = stream.next().await {
            let mut body = value.unwrap().data;
            while let Some(value) = body.next().await {
                let value = value.expect("Failed to read body chunk");
                result.extend(&value);
            }
        }

        let reader = Cursor::new(result);
        let buffered = BufReader::new(reader);
        let line_stream = stream! {
            for line in buffered.lines() {
                let line = line.map(|line| line.as_bytes().to_vec());
                yield line;
            }
        };
        let boxed_stream: BlobStream = Box::pin(line_stream);

        self.state
            .queue_client
            .pop_receipt_client(message)
            .delete()
            .await
            .expect("Failed removing messages from queue");

        Ok(boxed_stream)
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AzureStorageEvent {
    pub subject: String,
    pub event_type: String,
}

#[test]
fn test_azure_storage_event() {
    let event_value: AzureStorageEvent = serde_json::from_str(
        r#"{
          "topic": "/subscriptions/fa5f2180-1451-4461-9b1f-aae7d4b33cf8/resourceGroups/events_poc/providers/Microsoft.Storage/storageAccounts/eventspocaccount",
          "subject": "/blobServices/default/containers/content/blobs/foo",
          "eventType": "Microsoft.Storage.BlobCreated",
          "id": "be3f21f7-201e-000b-7605-a29195062628",
          "data": {
            "api": "PutBlob",
            "clientRequestId": "1fa42c94-6dd3-4172-95c4-fd9cf56b5009",
            "requestId": "be3f21f7-201e-000b-7605-a29195000000",
            "eTag": "0x8DC701C5D3FFDF6",
            "contentType": "application/octet-stream",
            "contentLength": 0,
            "blobType": "BlockBlob",
            "url": "https://eventspocaccount.blob.core.windows.net/content/foo",
            "sequencer": "0000000000000000000000000005C5360000000000276a63",
            "storageDiagnostics": {
              "batchId": "fec5b12c-2006-0034-0005-a25936000000"
            }
          },
          "dataVersion": "",
          "metadataVersion": "1",
          "eventTime": "2024-05-09T11:37:10.5637878Z"
        }"#,
    ).unwrap();

    assert_eq!(
        event_value.subject,
        "/blobServices/default/containers/content/blobs/foo".to_string()
    );
    assert_eq!(
        event_value.event_type,
        "Microsoft.Storage.BlobCreated".to_string()
    );
}
