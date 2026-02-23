use std::fmt;
use std::sync::Arc;
use tokio::sync::RwLock;
use twilight_gateway::{EventTypeFlags, Intents, ShardId};

use crate::queue::{ArcQueue, ThreadSafeQueue};
use crate::shard_runner::ReconnectCause;

/// Provided [reconnect strategies] made by the library.
///
/// [reconnect strategies]: ReconnectStrategy
pub mod reconnect_strategies;

/// Shard configuration that allows to be shared across all shards.
///
/// This structure contains the essential configuration needed to establish and maintain
/// Discord gateway connections. It is designed to be shared safely across multiple
/// shards and their associated runners.
///
/// # Thread Safety
///
/// All reconfigured fields are wrapped in [`RwLock`] to allow safe concurrent access
/// from multiple shard runners. The configuration can be updated at runtime
/// without recreating the entire shard infrastructure.
///
/// # Example
///
/// ```rust,no_run
/// ShardConfig {
///     event_type_flags: EventTypeFlags::all(),
///     intents: Intents::GUILD_MESSAGES | Intents::MESSAGE_CONTENT,
///     queue: ThreadSafeQueue::new(/* your queue implementation */),
///     max_attempts: RwLock::new(Some(5)),
///     resume_url: RwLock::new(None),
///     token: RwLock::new("your_bot_token".to_string()),
/// }
/// ```
pub struct ShardConfig {
    /// Event type flags that determine which Discord events should be processed.
    ///
    /// This field controls event filtering at the gateway level, allowing you to
    /// reduce bandwidth and processing overhead by ignoring unnecessary events.
    pub event_type_flags: EventTypeFlags,

    /// Gateway intents that specify which events the bot wants to receive.
    ///
    /// Intents are required by Discord to receive certain privileged events.
    /// This field is used during the initial gateway handshake and connection
    /// establishment.
    pub intents: Intents,

    /// Thread-safe queue implementation for managing gateway command rate limiting.
    ///
    /// This queue handles the rate limiting of outbound commands to Discord's
    /// gateway, ensuring compliance with Discord's rate limit requirements.
    /// The same queue instance is shared across all shards.
    ///
    /// [`InMemoryQueue`] can be wrapped with [`ArcQueue`]. When using a custom
    /// queue, be sure that the queue is implemented with [`Sync`] and [`Send`].
    ///
    /// [`InMemoryQueue`]: twilight_gateway::queue::InMemoryQueue
    pub queue: ArcQueue,

    /// Custom reconnect strategy if one of the shards failed to reconnect to
    /// the gateway as soon as possible. This allows the runner to determine
    /// whether to continue reconnecting to the gateway or give up.
    ///
    /// Reconnect strategy is created through a struct that must be implemented
    /// with [`ReconnectStrategy`] trait. There are reconnect strategies provided
    /// in this library with the [`reconnect_strategies`] module.
    ///
    /// If this field is not set, it will try to reconnect to the gateway indefinitely.
    ///
    /// [`ReconnectStrategy`]: crate::config::ReconnectStrategy
    pub reconnect_strategy: Option<Arc<dyn ReconnectStrategy>>,

    /// Custom resume URL for gateway connections.
    ///
    /// If provided, shards will use this URL for initial connections instead of
    /// the default Discord gateway URL. This is typically used for proxying
    /// or custom gateway implementations.
    pub resume_url: RwLock<Option<String>>,

    /// Discord bot token used for authentication.
    ///
    /// This token is required for all gateway connections and is used during
    /// the initial handshake process.
    pub token: RwLock<String>,
}

impl ShardConfig {
    /// Creates a new configuration with the specified token, thread safe
    /// queue and intents with provided defaults listed below:
    /// - All event types are enabled
    /// - Maximum of 3 reconnection attempts
    /// - No custom resume URL
    #[allow(private_bounds)]
    #[must_use]
    pub fn new(token: String, intents: Intents, queue: impl ThreadSafeQueue) -> Self {
        use self::reconnect_strategies::MaxAttempts;

        Self {
            event_type_flags: EventTypeFlags::all(),
            intents,
            queue: ArcQueue::new(queue),
            reconnect_strategy: Some(Arc::new(MaxAttempts::default())),
            resume_url: RwLock::new(None),
            token: RwLock::new(token),
        }
    }

    /// Updates the bot token at runtime.
    ///
    /// This method allows for token rotation without recreating the entire
    /// configuration. The new token will be used for future shard connections
    /// and reconnections.
    pub async fn set_token(&self, token: String) {
        *self.token.write().await = token;
    }

    /// Sets a custom resume URL for gateway connections.
    ///
    /// If `url` is set to [`None`], it will use the default resume url
    /// provided by [`twilight_gateway`].
    pub async fn set_resume_url(&self, url: Option<String>) {
        *self.resume_url.write().await = url;
    }
}

/// Defines a strategy for handling shard reconnection attempts.
///
/// This trait allows for creating custom, pluggable logic to decide whether a
/// disconnected shard should try to reconnect based on the number of attempts
/// and the reason for the disconnect.
pub trait ReconnectStrategy: std::fmt::Debug + Send + Sync {
    /// Determines whether a shard should attempt to reconnect.
    fn should_reconnect(&self, id: ShardId, attempts: usize, cause: &ReconnectCause) -> bool;

    /// Resets any internal state of the strategy.
    ///
    /// This is called by the runner when a shard successfully connects and
    /// completes its handshake, resetting any attempt counters or backoff state.
    fn reset(&self) {}
}

impl fmt::Debug for ShardConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct Redacted;

        impl fmt::Debug for Redacted {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str("<redacted>")
            }
        }

        f.debug_struct("ShardConfig")
            .field("event_type_flags", &self.event_type_flags)
            .field("intents", &self.intents)
            .field("resume_url", &self.resume_url)
            .field("token", &Redacted)
            .finish_non_exhaustive()
    }
}
