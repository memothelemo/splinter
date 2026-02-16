use twilight_gateway::ShardId;

use crate::config::ReconnectStrategy;
use crate::shard_runner::ReconnectCause;

/// A strategy that allows reconnecting up to a maximum number of attempts.
///
/// After the number of `attempts` exceeds the configured maximum,
/// `should_reconnect` will return `false`.
pub struct MaxAttempts(usize);

impl MaxAttempts {
    /// Creates a new `MaxAttempts` strategy with the given limit.
    #[must_use]
    pub const fn new(max: usize) -> Self {
        Self(max)
    }
}

impl ReconnectStrategy for MaxAttempts {
    fn should_reconnect(&self, _id: ShardId, attempts: usize, _cause: &ReconnectCause) -> bool {
        attempts < self.0
    }
}

impl Default for MaxAttempts {
    /// It creates [`MaxAttempts`] object with three maximum attempts as default.
    ///
    /// ```rust,no_run
    /// use splinter::config::reconnect_strategies::MaxAttempts;
    ///
    /// let strategy = MaxAttempts::new(3);
    /// ```
    fn default() -> Self {
        Self::new(3)
    }
}

impl std::fmt::Debug for MaxAttempts {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("MaxAttempts").field(&self.0).finish()
    }
}

/// A strategy that always allows reconnection, regardless of the number of
/// attempts or the cause.
#[derive(Debug, Clone, Default)]
pub struct AlwaysReconnect;

impl ReconnectStrategy for AlwaysReconnect {
    fn should_reconnect(&self, _id: ShardId, _attempts: usize, _cause: &ReconnectCause) -> bool {
        true
    }
}

/// A strategy that never allows reconnection.
#[derive(Debug, Clone, Default)]
pub struct NeverReconnect;

impl ReconnectStrategy for NeverReconnect {
    fn should_reconnect(&self, _id: ShardId, _attempts: usize, _cause: &ReconnectCause) -> bool {
        false
    }
}
