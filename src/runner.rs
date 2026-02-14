use either::Either::{self, Left, Right};
use futures::StreamExt;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use tokio::sync::oneshot;
use twilight_gateway::error::{ReceiveMessageError, ReceiveMessageErrorType};
use twilight_gateway::{CloseFrame, EventType, Shard, ShardId, ShardState};

use crate::ShardHandle;
use crate::config::CommonShardConfig;
use crate::error::{Blocked, InitShardError, InitShardErrorType};
use crate::queue::ThreadSafeQueue;
use crate::util::{extract_event_type, has_fatal_error_code};

pub struct ShardRunner {
    config: Arc<CommonShardConfig>,

    /// The receiver of the associated `close_tx` field in shard handle.
    close_rx: flume::Receiver<(CloseFrame<'static>, Option<oneshot::Sender<()>>)>,

    /// A shard handle linked to this shard runner.
    handle: ShardHandle,

    /// Channel to send the result if it is identified.
    pub identified_tx: Option<oneshot::Sender<Result<(), Either<InitShardError, Blocked>>>>,

    /// The receiver of the associated `reconfigure_tx` field in shard handle.
    reconfigure_rx: flume::Receiver<oneshot::Sender<Result<(), Either<InitShardError, Blocked>>>>,

    /// The number of reconnect attempts that are allowed.
    ///
    /// Once this allowance is depleted, any succeeding reconnection
    /// error is treated as fatal. If `None`, the allowance is infinite.
    reconnect_allowance: Option<usize>,

    /// The actual Shard object that this runner is holding.
    shard: Shard<ThreadSafeQueue>,
}

impl ShardRunner {
    #[must_use]
    pub async fn new(id: ShardId, config: Arc<CommonShardConfig>) -> Self {
        let (close_tx, close_rx) = flume::bounded(1);
        let (reconfigure_tx, reconfigure_rx) = flume::bounded(1);

        let shard = Self::make_shard(id, &config).await;
        let handle = ShardHandle::new(&shard, close_tx, reconfigure_tx);
        let reconnect_allowance = *config.max_attempts.read().await;

        Self {
            close_rx,
            config,
            handle,
            identified_tx: None,
            reconnect_allowance,
            reconfigure_rx,
            shard,
        }
    }

    /// Returns a cloned [shard handle] that it is linked to this runner.
    ///
    /// [shard handle]: ShardHandle
    #[must_use]
    pub fn handle(&self) -> ShardHandle {
        self.handle.clone()
    }

    /// Returns the total allowance of reconnection attempts allowed.
    #[must_use]
    pub fn reconnect_allowance(&self) -> Option<usize> {
        self.reconnect_allowance
    }

    /// Gets the current [`Shard`] of the runner.
    #[must_use]
    pub fn shard(&self) -> &Shard<ThreadSafeQueue> {
        &self.shard
    }

    pub fn try_set_identify_tx(
        &mut self,
        tx: oneshot::Sender<Result<(), Either<InitShardError, Blocked>>>,
    ) {
        if self.identified_tx.is_some() {
            _ = tx.send(Err(Right(Blocked)));
        }
    }
}

#[derive(Debug)]
pub enum ShardRunnerEvent {
    Close {
        frame: CloseFrame<'static>,
        terminate: bool,
        tx: Option<oneshot::Sender<()>>,
    },
    ReceiveEventError {
        error: ReceiveMessageError,
    },
    FatalError {
        error: InitShardError,
    },
    Identified,
    Reconnect(ReconnectCause),
    Reconfigure {
        tx: oneshot::Sender<Result<(), Either<InitShardError, Blocked>>>,
    },
}

#[derive(Debug)]
pub enum ReconnectCause {
    Gateway(Option<CloseFrame<'static>>),
    Error(ReceiveMessageError),
}

impl ShardRunner {
    pub async fn recv(&mut self) -> (Option<ShardRunnerEvent>, Option<twilight_gateway::Event>) {
        let mut runner_event = None;
        let mut gateway_event = None;

        tokio::select! {
            entry = self.close_rx.recv_async() => {
                let (frame, tx) = entry.expect("shard runner owns close channel");
                let terminate = frame.code == CloseFrame::NORMAL.code;
                runner_event = Some(ShardRunnerEvent::Close { frame, terminate, tx });
            },
            entry = self.reconfigure_rx.recv_async() => {
                let tx = entry.expect("shard runner owns reconfigure channel");
                runner_event = Some(ShardRunnerEvent::Reconfigure { tx });
            },
            entry = self.shard.next() => match entry {
                Some(Ok(message)) => self.handle_ws_message(&mut runner_event, &mut gateway_event, message).await,
                Some(Err(error)) if matches!(error.kind(), ReceiveMessageErrorType::Reconnect) => {
                    self.handle_shard_error(&mut runner_event, Right(error));
                },
                Some(Err(error)) => {
                    runner_event = Some(ShardRunnerEvent::ReceiveEventError { error });
                },
                None => {},
            },
        };

        self.update_handle_fields();
        (runner_event, gateway_event)
    }

    pub async fn close(&mut self, frame: CloseFrame<'static>) {
        // Do not attempt to shutdown if the shard is fatally closed.
        if matches!(
            self.shard.state(),
            twilight_gateway::ShardState::FatallyClosed
        ) {
            return;
        }

        // We can finally send the close message to the WebSocket
        self.shard.close(frame);

        // Wait until the shard's WebSocket connection is FINALLY CLOSED
        _ = self.shard.next().await;
    }

    pub async fn reconfigure(&mut self) {
        // If the shard is active, close the connection first...
        if !matches!(self.shard.state(), ShardState::Disconnected { .. }) {
            self.close(CloseFrame::NORMAL).await;
        }

        self.shard = Self::make_shard(self.shard.id(), &self.config).await;
        self.reconnect_allowance = *self.config.max_attempts.read().await;
    }
}

impl ShardRunner {
    async fn handle_ws_message(
        &mut self,
        runner_event: &mut Option<ShardRunnerEvent>,
        gateway_event: &mut Option<twilight_gateway::Event>,
        message: twilight_gateway::Message,
    ) {
        use twilight_gateway::Message;
        match message {
            Message::Text(json) => {
                // We only need the 'Ready' and 'Resumed' events because deserializing
                // the entire event and event matching is expensive!
                if let Some(EventType::Ready | EventType::Resumed) = extract_event_type(&json) {
                    *runner_event = Some(ShardRunnerEvent::Identified);

                    // Reset the reconnection allowance
                    tracing::debug!("handshake completed");
                    self.reconnect_allowance = *self.config.max_attempts.read().await;
                }

                match twilight_gateway::parse(json, self.config.event_type_flags) {
                    Ok(Some(parsed)) => *gateway_event = Some(parsed.into()),
                    Ok(None) => {}
                    Err(error) => {
                        *runner_event = Some(ShardRunnerEvent::ReceiveEventError { error })
                    }
                }
            }
            Message::Close(Some(frame)) if has_fatal_error_code(&frame) => {
                *runner_event = Some(ShardRunnerEvent::FatalError {
                    error: InitShardError {
                        id: self.shard.id(),
                        kind: InitShardErrorType::Gateway(frame),
                    },
                });
            }
            Message::Close(frame) => {
                let frame = frame.unwrap_or_else(|| CloseFrame::new(4000, ""));
                self.handle_shard_error(runner_event, Left(frame));
            }
        }
    }
}

impl ShardRunner {
    async fn make_shard(id: ShardId, config: &CommonShardConfig) -> Shard<ThreadSafeQueue> {
        let mut builder = twilight_gateway::ConfigBuilder::new(
            config.token.read().await.to_string(),
            config.intents,
        )
        .queue(config.queue.clone());

        if let Some(url) = config.resume_url.read().await.as_ref() {
            builder = builder.resume_url(url.to_string());
        }

        Shard::with_config(id, builder.build())
    }
    fn handle_shard_error(
        &mut self,
        runner_event: &mut Option<ShardRunnerEvent>,
        error: Either<CloseFrame<'static>, ReceiveMessageError>,
    ) {
        let treat_as_fatal = self
            .reconnect_allowance
            .map(|remaining| remaining == 0)
            .unwrap_or(false);

        if treat_as_fatal {
            *runner_event = Some(ShardRunnerEvent::FatalError {
                error: InitShardError {
                    id: self.shard.id(),
                    kind: match error {
                        Left(frame) => InitShardErrorType::Gateway(frame),
                        Right(error) => InitShardErrorType::Connect(Box::new(error)),
                    },
                },
            });
            return;
        } else if let Some(remaining) = self.reconnect_allowance.as_mut() {
            *remaining -= 1;
        }

        let cause = match error {
            Left(frame) => ReconnectCause::Gateway(Some(frame)),
            Right(error) => ReconnectCause::Error(error),
        };

        tracing::debug!(%cause, remaining = ?self.reconnect_allowance, "reconnection failed");
        *runner_event = Some(ShardRunnerEvent::Reconnect(cause));
    }

    fn update_handle_fields(&mut self) {
        let handle = self.handle.inner();
        let latency = self.shard.latency().average();

        tracing::trace!(
            handle.latency = ?latency,
            handle.state = ?self.shard.state(),
            "updated handle fields"
        );
        handle.latency.store(latency);
        handle.state.send_replace(self.shard.state());
    }
}

impl ShardRunner {
    pub(crate) fn spawn(mut self) {
        let id = self.shard().id();
        tokio::spawn(async move {
            tracing::debug!("spawned shard runner {id}");
            self.run().await;
            tracing::debug!("shard runner {id} terminated");
        });
    }

    /// `true` - continue looping
    /// `false` - stop looping
    async fn default_handle_event(&mut self, event: ShardRunnerEvent) -> bool {
        match event {
            ShardRunnerEvent::Close {
                frame,
                terminate,
                tx,
            } => {
                self.close(frame).await;
                if let Some(tx) = tx {
                    _ = tx.send(());
                }

                if terminate {
                    return false;
                }
            }
            ShardRunnerEvent::ReceiveEventError { error } => {
                tracing::warn!(?error, "error receiving error");
            }
            ShardRunnerEvent::FatalError { error } => {
                if let Some(tx) = self.identified_tx.take() {
                    _ = tx.send(Err(Left(error)));
                }
            }
            ShardRunnerEvent::Identified => {
                if let Some(tx) = self.identified_tx.take() {
                    _ = tx.send(Ok(()));
                }
            }
            ShardRunnerEvent::Reconfigure { tx } => {
                // Block any incoming tx if identified_tx is already occupied
                if self.identified_tx.is_some() {
                    _ = tx.send(Err(Right(Blocked)));
                } else {
                    self.identified_tx = Some(tx);
                }
            }
            ShardRunnerEvent::Reconnect(..) => {}
        }
        true
    }

    #[tracing::instrument(skip_all, fields(shard.id = %self.shard.id()))]
    pub(crate) async fn run(&mut self) {
        loop {
            // events > shard runner action
            let (runner, gateway) = self.recv().await;
            if let Some(event) = gateway {
                let kind = event.kind();
                tracing::trace!(event.kind = ?kind, "received gateway event");
            }

            if let Some(event) = runner {
                tracing::trace!(?event, "received runner event");
                if !self.default_handle_event(event).await {
                    break;
                }
            }
        }
    }
}

impl fmt::Display for ReconnectCause {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            // twilight's ReceiveMessageError is a bit ambiguous so we need an actual cause
            Self::Error(error) => {
                if let Some(source) = error.source() {
                    fmt::Display::fmt(source, f)
                } else {
                    fmt::Display::fmt(&error, f)
                }
            }
            Self::Gateway(frame) => {
                write!(f, "gateway closed with ")?;
                if let Some(frame) = frame {
                    write!(f, "code {}", frame.code)
                } else {
                    f.write_str("unknown code")
                }
            }
        }
    }
}
