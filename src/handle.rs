use crossbeam::atomic::AtomicCell;
use pin_project_lite::pin_project;
use thiserror::Error;
use tokio::sync::oneshot;
use tokio::sync::{Notify, futures::Notified};
use twilight_gateway::{CloseFrame, Command, Shard, ShardId, ShardState};

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use crate::AnyThreadSafeQueue;
use crate::runner::ShardRunnerMessage;

#[derive(Clone)]
pub struct ShardHandle(Arc<ShardHandleInner>);

pub(crate) struct ShardHandleInner {
    id: ShardId,

    /// A bounded communication send channel to the runner to close a shard.
    close_tx: flume::Sender<(CloseFrame<'static>, Option<oneshot::Sender<()>>)>,

    /// Communication layer that bridges between the controller
    /// or the client (outside this crate), and its runner.
    runner_tx: flume::Sender<ShardRunnerMessage>,

    /// Current state of a shard.
    ///
    /// It may be mutated in any time by the shard runner.
    state: AtomicCell<ShardState>,

    /// This allows to notify other threads that a shard's
    /// status has changed through [`status_changed(...)`] function.
    ///
    /// [`status_changed(...)`]: ShardHandle::status_changed
    state_changed: Notify,
}

impl ShardHandle {
    #[must_use]
    pub(crate) fn new(
        shard: &Shard<AnyThreadSafeQueue>,
        close_tx: flume::Sender<(CloseFrame<'static>, Option<oneshot::Sender<()>>)>,
        runner_tx: flume::Sender<ShardRunnerMessage>,
    ) -> Self {
        let inner: ShardHandleInner = ShardHandleInner {
            id: shard.id(),
            close_tx,
            runner_tx,
            state: AtomicCell::new(shard.state()),
            state_changed: Notify::new(),
        };
        Self(Arc::new(inner))
    }

    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.0.runner_tx.is_disconnected()
    }

    #[must_use]
    pub fn id(&self) -> ShardId {
        self.0.id
    }

    #[must_use]
    pub fn state(&self) -> ShardState {
        self.0.state.load()
    }

    #[must_use]
    pub fn status_changed(&self) -> StateChangedFuture<'_> {
        StateChangedFuture {
            future: self.0.state_changed.notified(),
            state: &self.0.state,
        }
    }
}

// It is stored like this to make it cheap for the memory.
#[derive(Debug, Error)]
#[error("Failed to send command to a closed shard")]
pub struct ChannelClosedError(flume::SendError<()>);

#[derive(Debug, Error)]
#[error("Failed to request shutdown to a shard")]
pub struct QueueShutdownError(flume::SendError<()>);

impl ShardHandle {
    /// Send a command to the associated shard.
    #[allow(clippy::missing_panics_doc)]
    pub fn command(&self, command: &impl Command) -> Result<(), ChannelClosedError> {
        let json = serde_json::to_string(command).expect("serialization cannot fail");
        let result = self.0.runner_tx.send(ShardRunnerMessage::Command { json });
        result.map_err(|_| ChannelClosedError(flume::SendError(())))
    }

    /// Gracefully shuts down the associated shard by sending a WebSocket close frame.
    ///
    /// This function initiates a clean shutdown sequence for the shard, ensuring that
    /// any necessary cleanup is performed and the connection is properly closed.
    ///
    /// Unlike the [`ShardHandle::queue_shutdown`], this method allows you to wait
    /// for the shard to shut down completely.
    ///
    /// It returns `true` if the shard shuts down successfully, or `false` if the shutdown
    /// process fails if the queue is full.
    pub async fn close(&self, frame: CloseFrame<'static>) -> Result<bool, QueueShutdownError> {
        let (tx, rx) = oneshot::channel();

        match self.0.close_tx.try_send((frame, Some(tx))) {
            Ok(()) => {}
            Err(flume::TrySendError::Full(..)) => return Ok(false),
            Err(..) => return Err(QueueShutdownError(flume::SendError(()))),
        };

        _ = rx.await;
        Ok(true)
    }

    /// Queues the associated shard to shut down by sending a Websocket close frame to the shard.
    pub fn queue_close(&self, frame: CloseFrame<'static>) -> Result<(), QueueShutdownError> {
        match self.0.close_tx.try_send((frame, None)) {
            Ok(()) => Ok(()),
            Err(flume::TrySendError::Full(..)) => Ok(()),
            Err(..) => return Err(QueueShutdownError(flume::SendError(()))),
        }
    }
}

impl ShardHandle {
    pub(super) fn change_state(&self, new_state: ShardState) {
        self.0.state.store(new_state);
        self.0.state_changed.notify_waiters();
    }

    pub fn send_to_runner(&self, message: ShardRunnerMessage) {
        _ = self.0.runner_tx.send(message);
    }
}

pin_project! {
    pub struct StateChangedFuture<'a> {
        #[pin]
        future: Notified<'a>,
        state: &'a AtomicCell<ShardState>,
    }
}

impl Future for StateChangedFuture<'_> {
    type Output = ShardState;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        ready!(this.future.poll(cx));
        Poll::Ready(this.state.load())
    }
}

impl fmt::Debug for ShardHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardHandle")
            .field("id", &self.id())
            .finish()
    }
}

impl PartialEq for ShardHandle {
    fn eq(&self, other: &Self) -> bool {
        self.id().eq(&other.id())
    }
}

impl Eq for ShardHandle {}
