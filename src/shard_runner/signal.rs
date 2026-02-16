use twilight_gateway::CloseFrame;
use twilight_gateway::error::ReceiveMessageError;

use crate::error::InitShardError;

/// A signal from the [`ShardRunner`] indicating an event that requires a
/// change in its control flow.
///
/// A `ShardSignal` is the primary control mechanism returned by the
/// [`ShardRunner::recv`] method. Unlike a [`GatewayEvent`], which represents
/// data from Discord (like a new message), a `ShardSignal` represents the
/// internal state of the runner itself.
///
/// [`GatewayEvent`]: twilight_gateway::Event
/// [`ShardRunner`]: super::ShardRunner
/// [`ShardRunner::recv`]: crate::shard_runner::ShardRunner
#[derive(Debug)]
pub enum ShardSignal {
    /// The shard has received `Hello` event from Discord.
    Hello,

    /// The shard has successfully connected and finished the gateway handshake.
    ///
    /// This signal indicates that the shard is online and ready to send commands
    /// and receive events from Discord.
    Connected,

    /// A fatal error occurred that cannot be recovered from. The shard
    /// should be terminated and recreated from scratch.
    ///
    /// This error usually occurs during the initialization or resuming
    /// stage of the shard with a possibly during its active session.
    Fatal {
        /// The specific initialization error that caused the failure.
        error: InitShardError,
    },

    /// The shard needs to reconnect to the gateway.
    ///
    /// If [`ShardRunner::recv`] is called after the runner sent this signal,
    /// it will try to reconnect automatically if the retry strategy associated
    /// to the runner agrees to.
    ///
    /// [`ShardRunner::recv`]: crate::shard_runner::ShardRunner::recv
    Reconnect {
        /// The reason for the disconnection.
        cause: ReconnectCause,
    },

    /// An error occurred while deserializing a gateway event or processing a
    /// raw websocket message.
    ///
    /// This is generally a non-fatal error, and the `run` loop can typically
    /// log the error and continue running.
    ReceiveError {
        /// The underlying receive message error from [`twilight-gateway`].
        error: ReceiveMessageError,
    },
}

/// The cause of how [`ShardSignal::Reconnect`] received in the runner.
#[derive(Debug)]
pub enum ReconnectCause {
    /// The gateway sent a close frame.
    ConnectionClosed(CloseFrame<'static>),

    /// A reconnection error occurred.
    NetworkError(ReceiveMessageError),

    /// Manual reconnection was requested.
    Manual,

    /// Session invalidated, need to re-identify.
    SessionInvalidated,

    /// Heartbeat acknowledgment was not received in time.
    HeartbeatTimeout,
}

impl ReconnectCause {
    /// Checks if the reconnection was triggered by an occasional or unexpected event,
    /// such as a session invalidation, heartbeat timeout, or a manual request.
    #[must_use]
    pub(crate) fn is_occasional(&self) -> bool {
        matches!(
            self,
            Self::Manual | Self::SessionInvalidated | Self::HeartbeatTimeout
        )
    }
}

impl std::fmt::Display for ReconnectCause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ConnectionClosed(frame) => {
                write!(f, "gateway closed with code {}", frame.code)?;
                if !frame.reason.is_empty() {
                    write!(f, " ({:?})", frame.reason)?;
                }
                Ok(())
            }
            // twilight's ReceiveMessageError is a bit ambiguous so we need an actual cause
            Self::NetworkError(error) => {
                use std::error::Error;
                if let Some(source) = error.source() {
                    std::fmt::Display::fmt(source, f)
                } else {
                    std::fmt::Display::fmt(&error, f)
                }
            }
            Self::Manual => f.write_str("manual reconnection requested"),
            Self::SessionInvalidated => f.write_str("session invalidated"),
            Self::HeartbeatTimeout => f.write_str("heartbeat acknowledgment timeout"),
        }
    }
}
