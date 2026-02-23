/// This module contains the implementation and definition of [`ShardingRange`]
/// along with its [error] if it fails to meet the criteria when creating
/// [`ShardingRange`].
///
/// [error]: crate::shard_manager::range::ShardingRangeError
pub mod range;
pub use self::range::ShardingRange;
pub use crate::shard_runner::daemon::ShardHandle;

use crossbeam::atomic::AtomicCell;
use tokio::sync::RwLock;
use twilight_gateway::{CloseFrame, ShardId};

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use crate::config::ShardConfig;
use crate::error::InitShardError;
use crate::shard_runner::daemon::ShardDaemon;
use crate::stream::{ShardEventStream, ShardEventStreamItem};

/// Orchestrates a fleet of shards, managing their lifecycles via [`ShardDaemon`]s.
///
/// This is the primary entry point for running a sharded bot. It is responsible
/// for creating and spawning shard daemons based on a configurable range and
/// provides a unified view and control over all active shards.
pub struct ShardManager {
    config: Arc<ShardConfig>,
    event_stream_tx: flume::Sender<ShardEventStreamItem>,
    range: AtomicCell<ShardingRange>,
    shards: RwLock<HashMap<ShardId, ShardHandle>>,
}

impl ShardManager {
    /// Creates a new `ShardManager` and its associated event stream.
    ///
    /// The manager is returned wrapped in an [`Arc`] for safe sharing across
    /// threads. The [`ShardEventStream`] can be consumed to receive events from
    /// all shards managed by this manager.
    #[must_use]
    pub fn new(config: ShardConfig, range: ShardingRange) -> (Arc<Self>, ShardEventStream) {
        let (event_stream, event_stream_tx) = ShardEventStream::new();
        let manager = Arc::new(Self {
            config: Arc::new(config),
            event_stream_tx,
            range: AtomicCell::new(range),
            shards: RwLock::new(HashMap::new()),
        });
        (manager, event_stream)
    }
}

impl ShardManager {
    /// Returns the current [`ShardConfig`] of the shard manager.
    #[must_use]
    pub fn config(&self) -> &ShardConfig {
        &self.config
    }

    /// Returns a list of all [shard handles] that may have been initialized or identified.
    ///
    /// [shard handles]: ShardHandle
    #[must_use]
    pub async fn shards(&self) -> Vec<ShardHandle> {
        self.shards.read().await.values().cloned().collect()
    }

    /// Gets the total number of shards need to be booted.
    #[must_use]
    pub fn total(&self) -> u32 {
        let range = self.range.load();
        (range.to() - range.from()) + 1
    }
}

impl ShardManager {
    /// Updates the range of shards managed by this manager.
    ///
    /// This will gracefully shut down all currently running shards before
    /// starting a new set of shards based on the provided range.
    pub async fn set_range(&self, range: ShardingRange) -> Result<(), Arc<InitShardError>> {
        let old_range = self.range.load();
        if old_range == range {
            return Ok(());
        }
        self.shutdown_all().await;

        self.range.store(range);
        self.start_all().await
    }

    /// Signals all active shards to shut down without waiting for
    /// them to complete.
    ///
    /// This is useful for quick cleanup when you don't need to guarantee
    /// that all active shards to be fully terminated.
    pub async fn shutdown_in_background(&self) {
        let mut shards = self.shards.write().await;
        let total = shards.len();
        tracing::debug!("shutting down {total} active shard(s)...");

        for (id, shard) in shards.drain() {
            tracing::trace!("shutting down shard {id}...");
            shard.shutdown();
        }
    }

    /// Gracefully shuts down all active shards and waits for them
    /// all to terminate.
    pub async fn shutdown_all(&self) {
        let mut shards = self.shards.write().await;
        let total = shards.len();
        tracing::debug!("shutting down {total} active shard(s)...");

        let tasks = shards.drain().map(|(_, shard)| shard.shutdown_and_wait());
        futures::future::join_all(tasks).await;

        tracing::debug!("successfully shut down {total} shard(s)...");
    }

    /// Spawns all shards within the configured range in the background.
    ///
    /// It returns immediately with a list of handles to the shards, but
    /// it does not wait for the shards to connect or identify
    #[tracing::instrument(skip_all, level = "debug", fields(range = ?self.range.load()))]
    pub async fn spawn_all(&self) -> Vec<ShardHandle> {
        let range = self.range.load();
        let total = range.total();
        tracing::debug!("spawning {total} shard(s)...");

        let tasks = (range.from()..=range.to())
            .map(|id| ShardId::new(id, range.total()))
            .map(|id| self.spawn(id));

        let handles = futures::future::join_all(tasks).await;
        tracing::debug!("spawned {total} shard(s)");

        handles
    }

    /// Spawns all shards and waits for them all to successfully identify.
    ///
    /// This is the recommended method for starting a bot, as it ensures that
    /// all shards are online and ready before proceeding.
    #[tracing::instrument(skip_all, level = "debug", fields(range = ?self.range.load()))]
    pub async fn start_all(&self) -> Result<(), Arc<InitShardError>> {
        let range = self.range.load();
        let total = range.total();
        tracing::debug!("booting {total} shard(s)...");

        // Spawn all handles so they can be processed in background
        // (it's just such a tinny bit optimization but it helps)
        let tasks = (range.from()..=range.to())
            .map(|id| ShardId::new(id, range.total()))
            .map(|id| self.spawn(id));

        let handles = futures::future::join_all(tasks).await;
        let tasks = handles.iter().map(|v| v.identified());

        futures::future::try_join_all(tasks).await?;
        tracing::debug!("initialized {total} shard(s)");
        Ok(())
    }
}

impl ShardManager {
    /// Spawns a single shard with a given ID.
    ///
    /// This creates the daemon, runs it in a background task, and stores its
    /// handle in the manager.
    #[tracing::instrument(skip_all, level = "debug", fields(%id))]
    pub async fn spawn(&self, id: ShardId) -> ShardHandle {
        let config = self.config.clone();
        let event_stream_tx = self.event_stream_tx.clone();
        let handle = ShardDaemon::spawn(id, config, event_stream_tx).await;
        self.insert_shard_to_map(handle.clone()).await;

        handle
    }
}

impl ShardManager {
    async fn insert_shard_to_map(&self, shard: ShardHandle) {
        let mut shards = self.shards.write().await;
        if let Some(handle) = shards.insert(shard.id(), shard) {
            // If it got disconnected by other than NORMAL, the runner will not terminate.
            _ = handle.close(CloseFrame::NORMAL);
        }
    }
}

impl fmt::Debug for ShardManager {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardManager")
            .field("config", &self.config)
            .field("range", &self.range)
            .field("shards", &self.shards)
            .finish_non_exhaustive()
    }
}
