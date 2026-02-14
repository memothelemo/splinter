use crossbeam::atomic::AtomicCell;
use either::Either::{self, *};
use tokio::sync::{oneshot, watch};
use twilight_gateway::{CloseFrame, Command, MessageSender, Shard, ShardId, ShardState};

use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::error::{
    Blocked, CloseShardError, InitShardError, ReconfigureShardError, SendCommandError,
};
use crate::queue::ThreadSafeQueue;

#[derive(Clone)]
pub struct ShardHandle(Arc<ShardHandleInner>);

impl ShardHandle {
    #[must_use]
    pub fn id(&self) -> ShardId {
        self.0.id
    }

    #[must_use]
    pub async fn identified(&self) -> bool {
        let mut receiver = self.inner().state.subscribe();
        receiver
            .wait_for(|state| *state == ShardState::Active)
            .await
            .is_ok()
    }

    #[must_use]
    pub fn latency(&self) -> Option<Duration> {
        self.0.latency.load()
    }

    #[must_use]
    pub fn state(&self) -> ShardState {
        *self.0.state.subscribe().borrow()
    }
}

impl ShardHandle {
    pub fn command(&self, command: &impl Command) -> Result<(), SendCommandError> {
        self.0.sender.command(command).map_err(|_| SendCommandError)
    }

    pub async fn close(&self, frame: CloseFrame<'static>) -> Result<bool, CloseShardError> {
        let (tx, rx) = oneshot::channel();
        match self.0.close_tx.try_send((frame, Some(tx))) {
            Ok(()) => {}
            Err(flume::TrySendError::Full(..)) => return Ok(false),
            Err(..) => return Err(CloseShardError),
        };
        rx.await.map(|_| true).map_err(|_| CloseShardError)
    }

    pub fn queue_close(&self, frame: CloseFrame<'static>) -> Result<(), CloseShardError> {
        match self.0.close_tx.try_send((frame, None)) {
            Ok(()) => Ok(()),
            Err(flume::TrySendError::Full(..)) => Ok(()),
            Err(..) => Err(CloseShardError),
        }
    }

    pub async fn reconfigure(&self) -> Result<bool, ReconfigureShardError> {
        let (tx, rx) = oneshot::channel();
        match self.0.reconfigure_tx.try_send(tx) {
            Ok(()) => {}
            Err(flume::TrySendError::Full(..)) => return Ok(false),
            Err(..) => return Err(ReconfigureShardError::Closed),
        };
        rx.await
            .map_err(|_| ReconfigureShardError::Closed)?
            .map_err(|either| match either {
                Left(init) => ReconfigureShardError::Init(init),
                Right(blocked) => ReconfigureShardError::Blocked(blocked),
            })
            .map(|_| true)
    }
}

impl ShardHandle {
    #[must_use]
    pub(crate) fn new(
        shard: &Shard<ThreadSafeQueue>,
        close_tx: flume::Sender<(CloseFrame<'static>, Option<oneshot::Sender<()>>)>,
        reconfigure_tx: flume::Sender<oneshot::Sender<Result<(), Either<InitShardError, Blocked>>>>,
    ) -> Self {
        let (state, _) = watch::channel(shard.state());
        let inner: ShardHandleInner = ShardHandleInner {
            id: shard.id(),
            close_tx,
            reconfigure_tx,
            sender: shard.sender(),

            latency: AtomicCell::new(None),
            state,
        };

        Self(Arc::new(inner))
    }

    #[allow(private_interfaces)]
    #[must_use]
    pub(crate) fn inner(&self) -> &ShardHandleInner {
        &self.0
    }
}

pub(crate) struct ShardHandleInner {
    id: ShardId,
    close_tx: flume::Sender<(CloseFrame<'static>, Option<oneshot::Sender<()>>)>,
    reconfigure_tx: flume::Sender<oneshot::Sender<Result<(), Either<InitShardError, Blocked>>>>,

    // Seems redundant because we already implemented our custom close channel
    // but we're lazy to implement custom command channel.
    sender: MessageSender,

    pub(crate) latency: AtomicCell<Option<Duration>>,
    pub(crate) state: watch::Sender<ShardState>,
}

impl fmt::Debug for ShardHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardHandle")
            .field("id", &self.0.id)
            .field("latency", &self.0.latency.load())
            .finish()
    }
}
