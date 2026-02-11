use thiserror::Error;
use twilight_gateway::{CloseFrame, ShardId, error::ReceiveMessageError};

#[derive(Debug, Error)]
#[error("Failed to initialize shard {}", self.id())]
pub enum InitShardError {
    Gateway {
        frame: Option<CloseFrame<'static>>,
        id: ShardId,
    },

    Reconnect {
        #[source]
        error: ReceiveMessageError,
        id: ShardId,
    },
}

impl InitShardError {
    #[must_use]
    pub const fn id(&self) -> ShardId {
        match self {
            Self::Gateway { id, .. } => *id,
            Self::Reconnect { id, .. } => *id,
        }
    }

    #[must_use]
    pub const fn close_frame(&self) -> Option<&CloseFrame<'static>> {
        match self {
            Self::Gateway { frame, .. } => frame.as_ref(),
            Self::Reconnect { .. } => None,
        }
    }

    #[track_caller]
    pub(crate) fn from_fatal_close(
        id: ShardId,
        frame: &Option<CloseFrame<'static>>,
    ) -> Option<Self> {
        let code = frame
            .as_ref()
            .map(|v| v.code)
            .unwrap_or(CloseFrame::NORMAL.code);

        // These codes are not reconnectable. If the code isn't,
        // then it returns `None` instead.
        match code {
            4004 | 4010..4014 => Some(InitShardError::Gateway {
                frame: frame.clone(),
                id,
            }),
            _ => None,
        }
    }
}
