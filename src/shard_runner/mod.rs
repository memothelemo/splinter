pub(crate) mod daemon;
pub use self::daemon::ShardDaemon;

/// This module contains the definition of [`ShardSignal`] along with its subtypes.
pub mod signal;
pub use self::signal::*;

use futures::StreamExt;
use std::fmt;
use std::sync::Arc;
use twilight_gateway::error::{ReceiveMessageError, ReceiveMessageErrorType};
use twilight_gateway::{CloseFrame, EventType, Shard, ShardId, ShardState};
use twilight_gateway::{Event as GatewayEvent, Message as WsMessage};
use twilight_model::gateway::CloseCode;

use crate::config::ShardConfig;
use crate::error::{InitShardError, InitShardErrorType};
use crate::queue::ArcQueue;
use crate::util::{extract_event_type, has_fatal_error_code};

/// Manages the lifecycle of a single Discord gateway shard.
///
/// The `ShardRunner` is the core engine responsible for a shard's connection to
/// Discord. It wraps a [`twilight_gateway::Shard`] and automates the complex
/// process of connecting, receiving events, handling disconnects, and attempting
/// to reconnect according to a configurable strategy.
///
/// ### Design
///
/// This runner is designed as a stateful object that must be actively driven by an
/// external loop. It does not spawn any background tasks on its own. The primary
/// way to interact with the runner is to repeatedly call the [`recv`] method.
///
/// [`recv`]: ShardRunner::recv
pub struct ShardRunner {
    /// Shard configuration
    config: Arc<ShardConfig>,

    /// The current fake state of the shard. This is to handle connection
    /// fatal errors without relying on twilight's one which its shard
    /// will reconnect indefinitely.
    fake_state: ShardState,

    /// Total number of reconnection attempts made.
    ///
    /// It will revert back to `zero` once it is identified.
    reconnect_attempts: usize,

    /// The actual Shard object that this runner is holding.
    shard: Shard<ArcQueue>,
}

// ========== CONSTRUCTORS ========== //
impl ShardRunner {
    /// Creates a new shard runner with the given configuration.
    #[must_use]
    pub async fn new(id: ShardId, config: Arc<ShardConfig>) -> Self {
        let shard = Self::create_shard(id, &config).await;
        let state = shard.state();

        Self {
            config,
            reconnect_attempts: 0,
            shard,
            fake_state: state,
        }
    }

    /// Creates a [`twilight_gateway::Shard`] object from the library's own shard configuration.
    async fn create_shard(id: ShardId, config: &ShardConfig) -> Shard<ArcQueue> {
        let mut builder = twilight_gateway::ConfigBuilder::new(
            config.token.read().await.to_string(),
            config.intents,
        );

        if let Some(url) = config.resume_url.read().await.as_ref() {
            builder = builder.resume_url(url.to_string());
        }

        let gateway_config = builder.queue(config.queue.clone()).build();
        Shard::with_config(id, gateway_config)
    }
}

// ========== FUNCTIONS ========== //
impl ShardRunner {
    /// Closes the shard connection with a close frame.
    pub async fn close(&mut self, frame: CloseFrame<'static>) {
        // Don't try to close if the shard is already fatally closed
        if matches!(self.shard.state(), ShardState::FatallyClosed) {
            return;
        }

        tracing::debug!(
            shard.id = %self.shard.id(),
            ?frame,
            "closing shard connection"
        );

        self.shard.close(frame);

        // Wait for the connection to close
        let _ = self.shard.next().await;
    }

    /// Reconfigures the shard with updated configuration.
    ///
    /// This will close the existing connection and recreate the shard
    /// with the new configuration.
    ///
    /// However, this function will reset the shard session, so it needs to
    /// reidentify to Discord since intents and token may be changed.
    pub async fn reconfigure(&mut self) {
        tracing::debug!(
            shard.id = ?self.shard.id(),
            "reconfiguring shard"
        );

        // Close existing connection if it hasn't disconnected yet.
        if !matches!(self.shard.state(), ShardState::Disconnected { .. }) {
            self.close(CloseFrame::NORMAL).await;
        }

        // Create a new fresh shard with possibly new config
        self.shard = Self::create_shard(self.shard.id(), &self.config).await;
        self.fake_state = self.shard.state();
    }

    /// Receives the next event or signal from the shard.
    ///
    /// This is the main event loop method.
    ///
    /// It returns a tuple of [`twilight_gateway::Event`] and [`ShardSignal`], both
    /// optional to be processed with a custom runner behavior if desired.
    ///
    /// One of the values may return [`Some(...)`]. If both are [`None`], it means
    /// that the associated shard has fatally closed and cannot be recovered unless
    /// [reconfigured]. Therefore, it must have a logic to whether to stay the runner
    /// in idle or terminate indefinitely.
    ///
    /// You can determine whether if it is fatally closed through
    /// [`ShardRunner::is_fatally_closed`].
    ///
    /// [`Some(...)`]: core::option::Option::Some
    /// [reconfigured]: ShardRunner::reconfigure
    #[tracing::instrument(skip_all, fields(
        ?self.reconnect_attempts,
        self.shard.id = %self.shard.id(),
    ))]
    pub async fn recv(&mut self) -> (Option<ShardSignal>, Option<GatewayEvent>) {
        const UNKNOWN_REASON: CloseFrame<'static> = CloseFrame::new(4000, "unknown reason");

        loop {
            let message = match self.shard.next().await {
                Some(Ok(message)) => message,
                Some(Err(error)) if matches!(error.kind(), ReceiveMessageErrorType::Reconnect) => {
                    let cause = ReconnectCause::NetworkError(error);
                    return (Some(self.attempt_reconnect(cause)), None);
                }
                // Decompression error is the remaining receive message error type
                // to be checked in this function here. This is due to maybe Discord/Twilight
                // has unexpected behavior in the decompression process.
                Some(Err(error)) => return (Some(ShardSignal::ReceiveError { error }), None),
                None => return (None, None),
            };

            self.fake_state = self.shard.state();
            match message {
                WsMessage::Close(frame) => {
                    let frame = frame.unwrap_or_else(|| UNKNOWN_REASON);

                    // Dissect the error code so we can specifically pick which
                    // cause that made Discord sent this close frame.
                    let cause = match CloseCode::try_from(frame.code) {
                        Ok(CloseCode::SessionTimedOut) => ReconnectCause::SessionInvalidated,
                        // Twilight will send 1006 close code if the connection is failed or "zombied".
                        // https://docs.rs/crate/twilight-gateway/latest/source/src/shard.rs#877
                        _ if frame.code == 1006 && self.shard.session().is_some() => {
                            ReconnectCause::HeartbeatTimeout
                        }
                        _ => ReconnectCause::ConnectionClosed(frame),
                    };

                    return (Some(self.attempt_reconnect(cause)), None);
                }
                WsMessage::Text(json) => match self.extract_gateway_event(json) {
                    Ok((event, Some(EventType::GatewayHello))) => {
                        return (Some(ShardSignal::Hello), event);
                    }

                    Ok((event, Some(EventType::Ready | EventType::Resumed))) => {
                        if let Some(strategy) = self.config.reconnect_strategy.as_ref() {
                            strategy.reset();
                        }
                        return (Some(ShardSignal::Connected), event);
                    }

                    Ok((event, ..)) => {
                        if event.is_some() {
                            return (None, event);
                        }
                    }

                    // Deserialization errors make no deal with the shard connection,
                    // maybe twilight is not familiar with the new event.
                    Err(error) => return (Some(ShardSignal::ReceiveError { error }), None),
                },
            };
        }
    }

    /// Resets the total reconnection attempts.
    pub fn reset_reconnect_attempts(&mut self) {
        self.reconnect_attempts = 0;
    }
}

// ========== GETTERS ========== //
impl ShardRunner {
    /// Gets the current shard configuration associated to this runner.
    #[must_use]
    pub fn config(&self) -> &ShardConfig {
        &self.config
    }

    /// Returns a boolean whether the shard is fatally closed.
    #[must_use]
    pub fn is_fatally_closed(&self) -> bool {
        matches!(self.fake_state, ShardState::FatallyClosed)
    }

    /// The current state of [`ShardRunner`].
    #[must_use]
    pub fn state(&self) -> ShardState {
        self.fake_state
    }

    /// Gets the current [`Shard`] of the runner.
    #[must_use]
    pub fn shard(&self) -> &Shard<ArcQueue> {
        &self.shard
    }
}

// ========== INTERNAL FUNCTIONS ========== //
impl ShardRunner {
    fn attempt_reconnect(&mut self, reason: ReconnectCause) -> ShardSignal {
        if reason.is_occasional() {
            return ShardSignal::Reconnect { cause: reason };
        }

        let is_fatal = match &reason {
            ReconnectCause::ConnectionClosed(frame) => has_fatal_error_code(&frame),
            _ => false,
        };

        let treat_as_fatal = self
            .config
            .reconnect_strategy
            .as_ref()
            .map(|v| !v.should_reconnect(self.shard.id(), self.reconnect_attempts, &reason))
            .unwrap_or(false);

        if treat_as_fatal || is_fatal {
            let id = self.shard.id();
            let kind = match reason {
                ReconnectCause::ConnectionClosed(frame) => InitShardErrorType::Gateway(frame),
                ReconnectCause::NetworkError(error) => InitShardErrorType::Connect(Box::new(error)),
                _ if reason.is_occasional() => unreachable!(),
                _ => panic!("{reason:?} case is not handled!"),
            };

            self.fake_state = ShardState::FatallyClosed;
            return ShardSignal::Fatal {
                error: InitShardError { id, kind },
            };
        }

        if reason.is_occasional() {
            tracing::debug!(cause = %reason, "got disconnected from the gateway");
        } else {
            // Safely add attempts so we don't get an integer overflow error.
            if let Some(new_value) = self.reconnect_attempts.checked_add(1) {
                self.reconnect_attempts = new_value;
            } else {
                self.reconnect_attempts = 1;
            }

            tracing::debug!(
                cause = %reason,
                attempts = ?self.reconnect_attempts,
                "failed to reconnect the gateway"
            );
        }

        ShardSignal::Reconnect { cause: reason }
    }

    fn extract_gateway_event(
        &self,
        json: String,
    ) -> Result<(Option<GatewayEvent>, Option<EventType>), ReceiveMessageError> {
        let event_type = extract_event_type(&json);
        let event = twilight_gateway::parse(json, self.config.event_type_flags)?.map(Into::into);
        Ok((event, event_type))
    }
}

impl fmt::Debug for ShardRunner {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardRunner")
            .field("config", &self.config)
            .field("reconnect_attempts", &self.reconnect_attempts)
            .field("shard", &self.shard)
            .finish()
    }
}
