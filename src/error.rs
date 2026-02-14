use std::error::Error;
use std::fmt::Display;
use twilight_gateway::{CloseFrame, ShardId};

#[derive(Debug, thiserror::Error)]
#[error("tried to close shard with a closed shard")]
pub struct CloseShardError;

#[derive(Debug, thiserror::Error)]
#[error("cannot perform operation while shard is already starting")]
pub struct Blocked;

#[derive(Debug, thiserror::Error)]
#[error("tried to send command to a closed shard")]
pub struct SendCommandError;

#[derive(Debug, thiserror::Error)]
pub enum ReconfigureShardError {
    #[error(transparent)]
    Blocked(Blocked),

    #[error("tried to reconfigure shard with a closed shard")]
    Closed,

    #[error(transparent)]
    Init(InitShardError),
}

#[derive(Debug)]
pub struct InitShardError {
    pub(crate) id: ShardId,
    pub(crate) kind: InitShardErrorType,
}

#[derive(Debug)]
pub enum InitShardErrorType {
    Connect(Box<dyn Error + Send + Sync>),
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

    /// Consume the error, returning the owned error type and the source error.
    #[must_use = "consuming the error into its parts has no effect if left unused"]
    pub fn into_parts(self) -> (InitShardErrorType, Option<Box<dyn Error + Send + Sync>>) {
        (self.kind, None)
    }
}

impl InitShardError {
    #[must_use]
    pub(crate) fn connect(id: ShardId, error: Box<dyn Error + Send + Sync>) -> Self {
        Self {
            id,
            kind: InitShardErrorType::Connect(error),
        }
    }
}

impl Display for InitShardError {
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
