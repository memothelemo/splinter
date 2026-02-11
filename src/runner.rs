use tokio::sync::{RwLock, oneshot};
use tracing::{debug, trace, warn};
use twilight_gateway::error::{ReceiveMessageError, ReceiveMessageErrorType};
use twilight_gateway::{
    CloseFrame, ConfigBuilder, Event, EventType, EventTypeFlags, Shard, ShardId, ShardState,
    StreamExt,
};

use std::sync::Arc;

use crate::AnyThreadSafeQueue;
use crate::error::InitShardError;
use crate::event::EventStreamItem;
use crate::handle::ShardHandle;
use crate::manager::ShardManagerConfig;

pub struct ShardRunner {
    config: Arc<RwLock<ShardManagerConfig>>,

    /// The receiver of the associated `close_rx` field.
    close_rx: flume::Receiver<(CloseFrame<'static>, Option<oneshot::Sender<()>>)>,

    /// This field bridges between the runner and handle when it comes
    /// to sending close frames to the shard.
    ///
    /// NOTE: This channel is bounded so it must be handled properly!
    close_tx: flume::Sender<(CloseFrame<'static>, Option<oneshot::Sender<()>>)>,

    /// Locally stored event stream sender for easy access.
    ///
    /// It will be changed based on the current config's event type
    /// flags field if ShardRunnerMessage::Reconfigure is sent.
    event_stream_tx: flume::Sender<EventStreamItem>,

    /// Locally stored event type flags for easy access.
    ///
    /// It will be changed based on the current config's event type
    /// flags field if ShardRunnerMessage::Reconfigure is sent.
    event_type_flags: EventTypeFlags,

    /// A shard handle linked to this shard runner.
    handle: ShardHandle,

    /// This field bridges the communication between the shard controller
    /// or through its associated shard handle, and the runner.
    rx: flume::Receiver<ShardRunnerMessage>,

    /// The sender of the associated `runner_rx` field.
    tx: flume::Sender<ShardRunnerMessage>,

    /// The actual Shard object that this runner is holding.
    shard: Shard<AnyThreadSafeQueue>,

    // TODO: Describe these fields here.
    reconnect_allowance: Option<usize>,

    pub identified_tx: Option<oneshot::Sender<Result<(), InitShardError>>>,
}

#[derive(Debug)]
pub enum ShardRunnerMessage {
    Command {
        json: String,
    },
    Reconfigure {
        tx: Option<oneshot::Sender<Result<(), InitShardError>>>,
    },
}

#[derive(Debug)]
pub enum ShardRunnerAction {
    Close {
        frame: CloseFrame<'static>,
        tx: Option<oneshot::Sender<()>>,
    },

    FatalError {
        error: Option<InitShardError>,
    },

    ReconfigureShard {
        tx: Option<oneshot::Sender<Result<(), InitShardError>>>,
    },

    Reconnect {
        error: ReceiveMessageError,
    },
}

impl ShardRunner {
    #[must_use]
    pub async fn new(config: Arc<RwLock<ShardManagerConfig>>, id: ShardId) -> Self {
        let (close_tx, close_rx) = flume::bounded(1);
        let (tx, rx) = flume::unbounded();
        let (event_stream_tx, event_type_flags, reconnect_allowance, shard) = {
            let config = config.read().await;
            let shard = Self::make_shard(id, &config);
            (
                config.event_stream_tx.clone(),
                config.event_type_flags,
                config.max_attempts,
                shard,
            )
        };

        let handle = ShardHandle::new(&shard, close_tx.clone(), tx.clone());

        ShardRunner {
            close_rx,
            close_tx,

            config,
            event_stream_tx,
            event_type_flags,
            handle,
            rx,
            tx,
            shard,

            reconnect_allowance,
            identified_tx: None,
        }
    }
}

impl ShardRunner {
    #[tracing::instrument(skip_all, fields(shard.id = %self.shard.id()))]
    pub async fn run(&mut self) {
        loop {
            // events > shard runner action
            let (event, action) = self.recv().await;
            if let Some(event) = event {
                let kind = event.kind();
                debug!(event.kind = ?kind, "received event");

                if let EventType::Ready | EventType::Resumed = kind
                    && let Some(tx) = self.identified_tx.take()
                {
                    _ = tx.send(Ok(()));
                }

                _ = self.event_stream_tx.send((self.handle(), event));
            }

            if let Some(action) = action {
                debug!(?action, "received action");
                if !self.handle_action(action).await {
                    break;
                }
            }
        }
    }

    pub fn run_in_background(mut self) {
        let id = self.shard().id();
        tokio::spawn(async move {
            debug!("spawned shard runner {id}");
            self.run().await;
            debug!("shard runner {id} terminated");
        });
    }

    #[must_use]
    pub async fn recv(&mut self) -> (Option<Event>, Option<ShardRunnerAction>) {
        let mut action = None;
        let mut incoming_event = None;

        tokio::select! {
            entry = self.close_rx.recv_async() => {
                let (frame, tx) = entry.expect("shard runner owns close channel");
                action = Some(ShardRunnerAction::Close { frame, tx });
            },
            entry = self.rx.recv_async() => {
                let message = entry.expect("shard runner owns its handle which it holds handle's tx");
                self.handle_runner_message(&mut action, message);
            }
            entry = self.shard.next_event(self.event_type_flags) => {
                let Some(entry) = entry else {
                    unreachable!("gateway fatal errors should be handled internally");
                };
                match entry {
                    Ok(event) => {
                        self.handle_gateway_event(&mut action, &event);
                        incoming_event = Some(event);
                    },
                    Err(error) => self.handle_receive_error(&mut action, error),
                }
            },
        };

        self.update_stats();
        (incoming_event, action)
    }
}

impl ShardRunner {
    pub fn try_send_fatal_error(&mut self, error: InitShardError) {
        if let Some(identified_tx) = self.identified_tx.take() {
            _ = identified_tx.send(Err(error));
        }
    }

    pub async fn reconfigure(&mut self) {
        let config = self.config.read().await;
        let event_type_flags = config.event_type_flags;
        let max_attempts = config.max_attempts;
        let shard = Self::make_shard(self.shard.id(), &config);

        drop(config);

        // If the shard is active, close the connection first...
        if !matches!(self.shard.state(), ShardState::Disconnected { .. }) {
            self.try_close(CloseFrame::NORMAL, None).await;
        }
        self.shard = shard;

        self.event_type_flags = event_type_flags;
        self.reconnect_allowance = max_attempts;
    }

    pub async fn queue_close(&self, frame: CloseFrame<'static>, tx: Option<oneshot::Sender<()>>) {
        self.close_tx
            .send((frame, tx))
            .expect("shard runner owns close channel");
    }

    pub async fn try_close(&mut self, frame: CloseFrame<'static>, tx: Option<oneshot::Sender<()>>) {
        use futures::StreamExt;

        // Alert that the shard is closing to the event consumers
        _ = self
            .event_stream_tx
            .send((self.handle(), Event::GatewayClose(Some(frame.clone()))));

        // Don't need for absolutely shutdown the WebSocket connection if a shard
        // is fatally closed its connection to the Discord gateway
        if matches!(self.shard.state(), ShardState::FatallyClosed) {
            return;
        }
        self.shard.close(frame);

        // Wait until the shard's WebSocket connection is FINALLY CLOSED
        _ = self.shard.next().await;

        if let Some(tx) = tx {
            _ = tx.send(());
        }
    }
}

impl ShardRunner {
    /// Gets the current event stream sender of the associated shard manager.
    ///
    /// [`EventStream`]: crate::event::EventStream
    #[must_use]
    pub fn event_stream(&self) -> flume::Sender<EventStreamItem> {
        self.event_stream_tx.clone()
    }

    #[must_use]
    pub fn sender(&self) -> flume::Sender<ShardRunnerMessage> {
        self.tx.clone()
    }

    /// Returns a cloned [shard handle] that it is linked to this runner.
    ///
    /// [shard handle]: ShardHandle
    #[must_use]
    pub fn handle(&self) -> ShardHandle {
        self.handle.clone()
    }

    /// Gets the current [`Shard`] of the runner.
    #[must_use]
    pub fn shard(&self) -> &Shard<AnyThreadSafeQueue> {
        &self.shard
    }

    /// Gets the mutable current [`Shard`] of the runner.
    #[must_use]
    pub fn shard_mut(&mut self) -> &mut Shard<AnyThreadSafeQueue> {
        &mut self.shard
    }
}

impl ShardRunner {
    /// It returns `true` meaning runner loop should continue.
    async fn handle_action(&mut self, action: ShardRunnerAction) -> bool {
        match action {
            ShardRunnerAction::Close { frame, tx } => {
                self.try_close(frame, tx).await;
                return false;
            }
            ShardRunnerAction::ReconfigureShard { tx } => {
                self.reconfigure().await;
                self.identified_tx = tx;
            }
            ShardRunnerAction::Reconnect { .. } => {
                // nop but it exists to allow customizability
            }
            ShardRunnerAction::FatalError { error } => {
                if let Some(error) = error {
                    self.try_send_fatal_error(error);
                }
                return false;
            }
        };
        true
    }

    fn handle_runner_message(
        &mut self,
        action: &mut Option<ShardRunnerAction>,
        message: ShardRunnerMessage,
    ) {
        match message {
            ShardRunnerMessage::Command { json } => {
                debug!("sent command to shard");
                self.shard.send(json);
            }
            ShardRunnerMessage::Reconfigure { tx } => {
                *action = Some(ShardRunnerAction::ReconfigureShard { tx });
            }
        }
    }

    fn handle_gateway_event(
        &mut self,
        action: &mut Option<ShardRunnerAction>,
        event: &twilight_gateway::Event,
    ) {
        if let Event::GatewayClose(frame) = event {
            let error = InitShardError::from_fatal_close(self.shard.id(), &frame);
            if let Some(error) = error {
                *action = Some(ShardRunnerAction::FatalError { error: Some(error) });
            }
        }
    }

    fn handle_receive_error(
        &mut self,
        action: &mut Option<ShardRunnerAction>,
        error: ReceiveMessageError,
    ) {
        if !matches!(error.kind(), ReceiveMessageErrorType::Reconnect) {
            warn!(%error, "Error while receiving event");
            return;
        }

        let treat_as_fatal = self
            .reconnect_allowance
            .as_mut()
            .map(|remaining| *remaining == 0)
            .unwrap_or(false);

        if treat_as_fatal {
            *action = Some(ShardRunnerAction::FatalError {
                error: Some(InitShardError::Reconnect {
                    error,
                    id: self.shard.id(),
                }),
            });
        } else if let Some(remaining) = self.reconnect_allowance.as_mut() {
            *remaining -= 1;

            trace!(?error, ?remaining, "reconnection failed");
            *action = Some(ShardRunnerAction::Reconnect { error });
        } else {
            trace!(?error, "reconnection failed");
        }
    }

    fn make_shard(id: ShardId, config: &ShardManagerConfig) -> Shard<AnyThreadSafeQueue> {
        let mut builder = ConfigBuilder::new(config.token.to_string(), config.intents)
            .queue(config.queue.clone());

        if let Some(url) = config.resume_url.as_ref() {
            builder = builder.resume_url(url.to_string());
        }

        Shard::with_config(id, builder.build())
    }

    fn update_stats(&self) {
        self.handle.change_state(self.shard.state());
    }
}
