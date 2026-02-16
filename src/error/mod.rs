use thiserror::Error;
use twilight_gateway::{CloseFrame, ShardId};

use std::error::Error;
use std::fmt;
use std::sync::Arc;

/// An error that can occur when reconfiguring a shard.
#[derive(Debug)]
pub struct ReconfigureShardError {
    pub(crate) id: ShardId,
    pub(crate) kind: ReconfigureShardErrorType,
}

/// The specific type of error that occurred during shard reconfiguration.
#[derive(Debug)]
pub enum ReconfigureShardErrorType {
    /// The shard was already closed or terminated when reconfiguration was
    /// attempted.
    Closed,

    /// The shard failed to initialize after being reconfigured.
    Init(Arc<InitShardError>),
}

impl ReconfigureShardError {
    /// The associated ID of the shard that this error occurred.
    #[must_use]
    pub const fn id(&self) -> ShardId {
        self.id
    }

    /// Immutable reference to the type of error that occurred.
    #[must_use = "retrieving the type has no effect if left unused"]
    pub const fn kind(&self) -> &ReconfigureShardErrorType {
        &self.kind
    }

    /// Consume the error, returning the source error if there is any.
    #[must_use = "consuming the error and retrieving the source has no effect if left unused"]
    pub fn into_source(self) -> Option<Arc<dyn Error + Send + Sync>> {
        match self.kind {
            ReconfigureShardErrorType::Closed => None,
            ReconfigureShardErrorType::Init(source) => Some(source),
        }
    }
}

impl fmt::Display for ReconfigureShardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &self.kind {
            ReconfigureShardErrorType::Closed => f.write_str("tried to reconfigure closed shard"),
            ReconfigureShardErrorType::Init(..) => {
                f.write_str("failed to initialize reconfigured shard")
            }
        }
    }
}

impl Error for ReconfigureShardError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match &self.kind {
            ReconfigureShardErrorType::Closed => None,
            ReconfigureShardErrorType::Init(source) => Some(&*source as &(dyn Error + 'static)),
        }
    }
}

/// An error that occur when a shard is terminated while trying to
/// perform an operation typically from a [shard handle].
///
/// [shard handle]: crate::shard_manager::ShardHandle
#[derive(Debug, Error)]
#[error("shard {} is terminated", .0)]
pub struct ShardTerminated(pub(crate) ShardId);

impl ShardTerminated {
    /// The associated ID of the shard that this error occurred.
    #[must_use]
    pub const fn id(&self) -> ShardId {
        self.0
    }
}

/// An error that can occur when initializing or resuming a shard session.
#[derive(Debug)]
pub struct InitShardError {
    pub(crate) id: ShardId,
    pub(crate) kind: InitShardErrorType,
}

/// The specific type of error that occurred during shard initialization.
#[derive(Debug)]
pub enum InitShardErrorType {
    /// An error occurred while trying to connect to the gateway WebSocket.
    Connect(Box<dyn Error + Send + Sync>),

    /// The gateway closed the connection with a non-resumable close frame.
    Gateway(CloseFrame<'static>),
}

impl InitShardError {
    /// The associated ID of the shard that this error occurred.
    #[must_use]
    pub const fn id(&self) -> ShardId {
        self.id
    }

    /// Immutable reference to the type of error that occurred.
    #[must_use = "retrieving the type has no effect if left unused"]
    pub const fn kind(&self) -> &InitShardErrorType {
        &self.kind
    }

    /// Consume the error, returning the source error if there is any.
    #[must_use = "consuming the error and retrieving the source has no effect if left unused"]
    pub fn into_source(self) -> Option<Box<dyn Error + Send + Sync>> {
        match self.kind {
            InitShardErrorType::Connect(source) => Some(source),
            InitShardErrorType::Gateway(..) => None,
        }
    }
}

impl fmt::Display for InitShardError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind() {
            InitShardErrorType::Connect(..) => f.write_str("failed to reconnect to the gateway"),
            InitShardErrorType::Gateway(frame) => {
                write!(f, "gateway closed with code {}", frame.code)
            }
        }
    }
}

impl Error for InitShardError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self.kind() {
            InitShardErrorType::Connect(source) => Some(&**source as &(dyn Error + 'static)),
            InitShardErrorType::Gateway(..) => None,
        }
    }
}
