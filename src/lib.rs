//! # Splinter
mod util;

/// Configuration structures for Discord shard management.
///
/// This module provides configuration options for managing Discord gateway shards,
/// including connection settings, event filtering, and reconnection behavior.
pub mod config;
pub use self::config::ReconnectStrategy;
pub use self::config::ShardConfig;

/// Defines all custom error types used throughout the library.
pub mod error;

/// Provides a thread-safe, type-erased queue structures for managing
/// gateway command rate-limiting with [`ArcQueue`] struct.
///
/// [`ArcQueue`]: crate::queue::ArcQueue
pub mod queue;
pub use self::queue::ArcQueue;

/// Defines and provides centralized event streaming for receiving gateway
/// event across all managed shards done by [`ShardManager`].
///
/// [`ShardManager`]: crate::shard_manager::ShardManager
pub mod stream;
pub use self::stream::ShardEventStream;

/// It contains the [`ShardManager`], the high-level and simplified
/// shard orchestrator for managing number of shards.
///
/// [`ShardManager`]: crate::shard_manager::ShardManager
pub mod shard_manager;
pub use self::shard_manager::{ShardHandle, ShardManager, ShardingRange};

/// Provides the low-level, technical structures for running a single shard's
/// connection and managing its lifecycle.
///
/// This includes the [`ShardRunner`], which it allows for direct communication
/// with the [shard] while at the same providing simplicity and serving as building
/// blocks for implementing custom shard managers/orchestrators.
///
/// [`ShardDaemon`] manages the [`ShardRunner`] in a background task.
///
/// [`ShardRunner`]: crate::shard_manager::ShardManager
/// [shard]: twilight_gateway::Shard
pub mod shard_runner;
pub use self::shard_runner::{ReconnectCause, ShardRunner, ShardSignal};
