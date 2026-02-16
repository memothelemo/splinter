use crossbeam::atomic::AtomicCell;
use tokio::sync::{RwLock, oneshot, watch};
use twilight_gateway::{CloseFrame, Command, Event as GatewayEvent, ShardId, ShardState};

use std::sync::Arc;
use std::time::Duration;

use crate::config::ShardConfig;
use crate::error::{
    InitShardError, InitShardErrorType, ReconfigureShardError, ReconfigureShardErrorType,
    ShardTerminated,
};
use crate::shard_runner::{ShardRunner, ShardSignal};
use crate::stream::ShardEventStreamItem;
use crate::util::OptionalExt;

/// The state machine that manages a single shard's lifecycle in a background task.
///
/// A `ShardDaemon` is created by a [`ShardManager`] for each shard. It wraps a
/// [`ShardRunner`] and runs its event loop, processing signals and gateway
/// events. It communicates with the outside world via its associated
/// [`ShardHandle`] and an event stream.
///
/// This struct is not intended to be used directly; it is managed by the
/// `spawn` function and its handle.
pub struct ShardDaemon {
    /// Tells whether this daemon is currently busy initializing the entire shard.
    ///
    /// If the daemon is busy, any [shard daemon messages] will be queued
    /// until the daemon is not busy and processed.
    busy: bool,

    /// Channel to receive the messages from shard handle.
    daemon_rx: flume::Receiver<ShardDaemonMessage>,

    /// Channel to send events to the event stream
    event_stream_tx: flume::Sender<ShardEventStreamItem>,

    /// Handle to this shard for external control
    handle: ShardHandle,

    /// Oneshot sender to notify when the shard has identified
    identified_tx: Option<oneshot::Sender<Result<(), Arc<InitShardError>>>>,

    /// The underlying shard runner that communicates with Discord's gateway
    runner: ShardRunner,

    /// Channel to receive the shutdown requests from shard handle.
    ///
    /// The inner value contains optional channel to notify when
    /// the termination is complete.
    shutdown_rx: flume::Receiver<Option<oneshot::Sender<()>>>,
}

/// A request to perform operations to the shard daemon.
#[derive(Debug)]
enum ShardDaemonMessage {
    /// A request to send a command to the shard.
    Command { json: String },

    /// A request to close the shard connection.
    Close {
        /// The close frame to send to the gateway.
        frame: CloseFrame<'static>,

        /// Optional channel to notify when the close is complete
        response: Option<oneshot::Sender<()>>,
    },

    /// A request to rebuild the shard by reconfiguring its token
    /// and other mutable properties of the config.
    Reconfigure {
        /// Optional channel to notify when the reconfiguration is
        /// complete and the shard has successfully reidentified.
        response: Option<oneshot::Sender<Result<(), Arc<InitShardError>>>>,
    },
}

impl ShardDaemon {
    /// It spawns a separate shard daemon as a background task and it
    /// gives back a [shard handle].
    ///
    /// [shard handle]: ShardHandle
    #[must_use]
    pub async fn spawn(
        id: ShardId,
        config: Arc<ShardConfig>,
        event_stream_tx: flume::Sender<ShardEventStreamItem>,
    ) -> ShardHandle {
        let (daemon_tx, daemon_rx) = flume::unbounded();
        let (shutdown_tx, shutdown_rx) = flume::unbounded();

        let runner = ShardRunner::new(id, config).await;
        let (state_tx, _) = watch::channel(runner.state());

        let handle: ShardHandle = ShardHandle {
            inner: Arc::new(ShardHandleInner {
                id,
                daemon_tx,
                error: RwLock::new(None),
                latency: AtomicCell::new(None),
                shutdown_tx,
                state: state_tx,
            }),
        };

        let mut daemon: ShardDaemon = ShardDaemon {
            busy: false,
            daemon_rx,
            event_stream_tx,
            handle: handle.clone(),
            identified_tx: None,
            runner,
            shutdown_rx,
        };

        tokio::spawn(async move {
            tracing::debug!(shard.id = %id, "spawned shard daemon");
            daemon.run().await;
            tracing::debug!(shard.id = %id, "shard daemon terminated");
        });

        handle
    }

    /// Main event loop for the daemon.
    ///
    /// Runs until a termination signal is received.
    async fn run(&mut self) {
        loop {
            let is_runner_active = !self.runner.is_fatally_closed();
            let runner_recv_future = is_runner_active.then(|| self.runner.recv()).optional();
            let message_recv_future = (!self.busy).then(|| self.daemon_rx.recv_async()).optional();

            tokio::select! {
                (signal, event) = runner_recv_future => {
                    self.process_runner_message(signal, event).await;
                    self.handle.update(&self.runner);
                },

                Ok(tx) = self.shutdown_rx.recv_async() => {
                    self.shutdown(tx).await;
                    break;
                },

                // shard daemon owns its handle, which it owns handle.daemon_tx
                Ok(message) = message_recv_future => {
                    self.process_handle_message(message).await;
                },
            }
        }
    }

    /// Performs a shut down sequence for the daemon.
    async fn shutdown(&mut self, tx: Option<oneshot::Sender<()>>) {
        self.runner.close(CloseFrame::NORMAL).await;
        if let Some(tx) = tx {
            _ = tx.send(());
        }

        // Throw an error to the identified tx if it exists.
        if let Some(tx) = self.identified_tx.take() {
            let error = Arc::new(InitShardError {
                id: self.runner.shard().id(),
                kind: InitShardErrorType::Connect(Box::new(ShardTerminated(
                    self.runner.shard().id(),
                ))),
            });
            self.handle.set_error(error.clone()).await;
            _ = tx.send(Err(error));
        }
    }

    /// Processes entry received by the [`ShardDaemonMessage`] receiver channel.
    async fn process_handle_message(&mut self, message: ShardDaemonMessage) {
        match message {
            ShardDaemonMessage::Close { frame, response } => {
                self.busy = true;
                self.runner.close(frame).await;

                if let Some(tx) = response {
                    _ = tx.send(());
                }
            }
            ShardDaemonMessage::Command { json } => {
                self.runner.shard().send(json);
            }
            ShardDaemonMessage::Reconfigure { response } => {
                self.busy = true;
                self.identified_tx = response;
            }
        }
    }

    /// Processes entry returned from [`ShardRunner::recv`].
    async fn process_runner_message(
        &mut self,
        signal: Option<ShardSignal>,
        event: Option<GatewayEvent>,
    ) {
        if let Some(signal) = signal.as_ref() {
            tracing::trace!(?signal, "received runner signal");
        }

        match signal {
            Some(ShardSignal::Hello) => {
                self.handle.clear_error().await;
            }
            Some(ShardSignal::Connected) => {
                self.busy = false;
                if let Some(tx) = self.identified_tx.take() {
                    _ = tx.send(Ok(()));
                }
            }
            Some(ShardSignal::Fatal { error }) => {
                let error = Arc::new(error);
                self.handle.set_error(error.clone()).await;
                self.busy = false;

                if let Some(tx) = self.identified_tx.take() {
                    _ = tx.send(Err(error));
                }
            }
            Some(ShardSignal::Reconnect { cause }) => {
                tracing::warn!(%cause, "got disconnected to the gateway; reconnecting...");
            }
            Some(ShardSignal::ReceiveError { error }) => {
                tracing::warn!(?error, "error receiving event");
            }
            None => {}
        }

        if let Some(event) = event {
            tracing::trace!(event.kind = ?event.kind(), "received gateway event");
            _ = self.event_stream_tx.send((self.handle.clone(), event));
        }
    }
}

/// A lightweight, cloneable handle for interacting with a running [`ShardDaemon`].
///
/// This handle is the public-facing API for a single shard. It allows sending
/// commands, shutting down the shard, and inspecting its real-time state (like
/// latency and connection status). It is designed to be cheaply cloned and
/// shared across different parts of an application.
#[derive(Clone)]
pub struct ShardHandle {
    inner: Arc<ShardHandleInner>,
}

struct ShardHandleInner {
    /// The shard ID
    id: ShardId,

    /// Channel to send the messages to its daemon.
    daemon_tx: flume::Sender<ShardDaemonMessage>,

    /// Current initialization error, if any
    error: RwLock<Option<Arc<InitShardError>>>,

    /// Current average latency to the gateway
    latency: AtomicCell<Option<Duration>>,

    /// Current shard state broadcaster
    state: watch::Sender<ShardState>,

    /// Channel to send shut down request to its daemon.
    ///
    /// The inner value contains optional channel to notify when
    /// the termination is complete.
    shutdown_tx: flume::Sender<Option<oneshot::Sender<()>>>,
}

impl ShardHandle {
    /// Returns the shard ID.
    #[must_use]
    pub fn id(&self) -> ShardId {
        self.inner.id
    }

    /// Returns whether the shard is active and running in background.
    #[must_use]
    pub fn is_active(&self) -> bool {
        !self.inner.daemon_tx.is_disconnected()
    }

    /// Returns the current average latency to the gateway, if available.
    ///
    /// The latency represents the round-trip time for heartbeat acknowledgments.
    #[must_use]
    pub fn latency(&self) -> Option<Duration> {
        self.inner.latency.load()
    }

    /// Returns the current shard state.
    #[must_use]
    pub fn state(&self) -> ShardState {
        *self.inner.state.borrow()
    }

    /// Subscribes to shard state changes.
    ///
    /// Returns a receiver that will be notified whenever the shard state changes.
    #[must_use]
    pub fn subscribe_state(&self) -> watch::Receiver<ShardState> {
        self.inner.state.subscribe()
    }
}

impl ShardHandle {
    /// Queues a [command] to be sent to the shard.
    ///
    /// [command]: Command
    pub fn command(&self, command: &impl Command) -> Result<(), ShardTerminated> {
        let json = serde_json::to_string(command).expect("serialization cannot fail");
        self.send_to_daemon(ShardDaemonMessage::Command { json })
    }

    /// Queues a close request without waiting for completion.
    ///
    /// This is useful when you want to close a shard but don't need to wait
    /// for the close to complete.
    pub fn close(&self, frame: CloseFrame<'static>) -> Result<(), ShardTerminated> {
        self.send_to_daemon(ShardDaemonMessage::Close {
            frame,
            response: None,
        })
    }

    /// Closes the shard connection and waits for it to complete.
    pub async fn close_and_wait(&self, frame: CloseFrame<'static>) -> Result<(), ShardTerminated> {
        let (tx, rx) = oneshot::channel();
        self.send_to_daemon(ShardDaemonMessage::Close {
            frame,
            response: Some(tx),
        })?;
        rx.await.map_err(|_| ShardTerminated(self.id()))
    }

    /// Waits for the shard to identify or fail.
    ///
    /// This method blocks until the shard reaches the `Active` state
    /// (successfully identified) or the `FatallyClosed` state (failed).
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Shard successfully identified
    /// * `Err(InitShardError)` - Shard failed to identify
    #[must_use]
    pub async fn identified(&self) -> Result<(), Arc<InitShardError>> {
        let mut receiver = self.inner.state.subscribe();
        let _ = receiver
            .wait_for(|state| matches!(state, ShardState::Active | ShardState::FatallyClosed))
            .await;

        match self.inner.error.read().await.as_ref() {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    /// Reconfigures the shard with updated configuration.
    ///
    /// This closes the existing connection and recreates the shard with
    /// the current configuration from the config object.
    pub async fn reconfigure(&self) -> Result<(), ReconfigureShardError> {
        let (tx, rx) = oneshot::channel();
        self.send_to_daemon(ShardDaemonMessage::Reconfigure { response: Some(tx) })
            .map_err(|_| ReconfigureShardError {
                id: self.id(),
                kind: ReconfigureShardErrorType::Closed,
            })?;

        rx.await
            .map_err(|_| ReconfigureShardError {
                id: self.id(),
                kind: ReconfigureShardErrorType::Closed,
            })?
            .map_err(|inner| ReconfigureShardError {
                id: self.id(),
                kind: ReconfigureShardErrorType::Init(inner),
            })
    }

    /// Sends a shut down signal to the shard daemon without waiting for it to
    /// fully shut down.
    ///
    /// This is a "fire-and-forget" method. It returns immediately after sending
    /// the signal.
    ///
    /// # Returns
    ///
    /// Returns `true` if the signal was sent successfully. Returns `false` if
    /// the daemon was already terminated (i.e., the communication channel
    /// was closed).
    pub(crate) fn shutdown(self) -> bool {
        self.inner.shutdown_tx.send(None).is_ok()
    }

    /// Sends a termination signal to the shard daemon and waits for it to
    /// confirm completion.
    ///
    /// Use this when you need to ensure the shard is fully shut down before
    /// proceeding.
    ///
    /// # Returns
    ///
    /// Returns `true` if the daemon shut down gracefully and sent a
    /// confirmation. Returns `false` if the communication channel was closed
    /// before a confirmation was received, which may indicate the daemon was
    /// already gone or terminated unexpectedly.
    pub(crate) async fn shutdown_and_wait(self) -> bool {
        let (tx, rx) = oneshot::channel();
        let sent = self.inner.shutdown_tx.send(Some(tx)).is_ok();
        if !sent {
            return false;
        }

        rx.await.is_ok()
    }
}

impl ShardHandle {
    /// Attempts to send the daemon message to its running daemon.
    fn send_to_daemon(&self, message: ShardDaemonMessage) -> Result<(), ShardTerminated> {
        self.inner
            .daemon_tx
            .send(message)
            .map_err(|_| ShardTerminated(self.id()))
    }

    /// Sets the current error.
    ///
    /// Called by the daemon when a fatal error occurs.
    async fn set_error(&self, error: Arc<InitShardError>) {
        *self.inner.error.write().await = Some(error);
    }

    /// Clears the current error.
    ///
    /// Called by the daemon when the shard successfully connects.
    async fn clear_error(&self) {
        *self.inner.error.write().await = None;
    }

    /// Updates the handle with the current runner state.
    ///
    /// This is called internally by the daemon after processing messages.
    fn update(&self, runner: &ShardRunner) {
        let shard = runner.shard();
        if shard.id() != self.id() {
            return;
        }

        self.inner.latency.store(shard.latency().average());

        let new_state = shard.state();
        let current_state = *self.inner.state.borrow();

        if new_state != current_state {
            self.inner.state.send_replace(new_state);
        }
    }
}

impl std::fmt::Debug for ShardHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardHandle")
            .field("id", &self.id())
            .field("latency", &self.latency())
            .field("state", &self.state())
            .finish()
    }
}
