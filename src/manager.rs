use crossbeam::atomic::AtomicCell;
use either::Either;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, oneshot};
use twilight_gateway::{CloseFrame, ShardId};

use crate::config::CommonShardConfig;
use crate::error::{Blocked, CloseShardError, InitShardError};
use crate::handle::ShardHandle;
use crate::range::ShardingRange;
use crate::runner::ShardRunner;

pub struct ShardManager {
    config: Arc<CommonShardConfig>,
    range: AtomicCell<ShardingRange>,
    shards: RwLock<HashMap<ShardId, ShardHandle>>,
}

impl ShardManager {
    #[must_use]
    pub fn new(config: CommonShardConfig, range: ShardingRange) -> Arc<Self> {
        Arc::new(Self {
            config: Arc::new(config),
            range: AtomicCell::new(range),
            shards: RwLock::new(HashMap::new()),
        })
    }
}

impl ShardManager {
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
    pub async fn close_all(&self) -> Result<(), CloseShardError> {
        let mut shards = self.shards.write().await;
        let total = shards.len();
        tracing::debug!("closing down {total} active shard(s)...");

        for (id, shard) in shards.drain() {
            tracing::debug!("closing shard {id}...");
            shard.close(CloseFrame::NORMAL).await?;
        }

        tracing::debug!("successfully closed {total} shard(s)...");
        Ok(())
    }

    #[tracing::instrument(skip_all, level = "debug", fields(range = ?self.range.load()))]
    pub async fn spawn_all(&self) -> Result<Vec<ShardHandle>, InitShardError> {
        let range = self.range.load();
        let total = range.total();
        tracing::debug!("spawning {total} shard(s)...");

        let tasks = (range.from()..=range.to())
            .map(|id| ShardId::new(id, range.total()))
            .map(|id| self.spawn(id));

        let handles = futures::future::try_join_all(tasks).await?;
        tracing::debug!("spawned {total} shard(s)");

        Ok(handles)
    }

    #[tracing::instrument(skip_all, level = "debug", fields(range = ?self.range.load()))]
    pub async fn start_all(&self) -> Result<(), InitShardError> {
        let range = self.range.load();
        let total = range.total();
        tracing::debug!("booting {total} shard(s)...");

        let tasks = (range.from()..=range.to())
            .map(|id| ShardId::new(id, range.total()))
            .map(|id| self.boot(id));

        futures::future::try_join_all(tasks).await?;
        tracing::debug!("initialized {total} shard(s)");
        Ok(())
    }
}

impl ShardManager {
    #[tracing::instrument(skip_all, level = "debug", fields(%id))]
    pub async fn boot(&self, id: ShardId) -> Result<ShardHandle, InitShardError> {
        let mut runner = ShardRunner::new(id, self.config.clone()).await;
        let (tx, rx) = oneshot::channel();
        let handle = runner.handle();
        runner.identified_tx = Some(tx);
        runner.spawn();

        self.insert_shard_to_map(handle.clone()).await;
        rx.await
            .map_err(|error| InitShardError::connect(id, Box::new(error)))?
            .map_err(|error| Self::into_init_error(id, error))?;

        Ok(handle)
    }

    #[tracing::instrument(skip_all, level = "debug", fields(%id))]
    pub async fn spawn(&self, id: ShardId) -> Result<ShardHandle, InitShardError> {
        let runner = ShardRunner::new(id, self.config.clone()).await;
        let handle = runner.handle();
        self.insert_shard_to_map(handle.clone()).await;
        runner.spawn();

        Ok(handle)
    }
}

impl ShardManager {
    async fn insert_shard_to_map(&self, shard: ShardHandle) {
        let mut shards = self.shards.write().await;
        if let Some(handle) = shards.insert(shard.id(), shard) {
            // If it got disconnected by other than NORMAL, the runner will not terminate.
            _ = handle.queue_close(CloseFrame::NORMAL);
        }
    }

    fn into_init_error(id: ShardId, error: Either<InitShardError, Blocked>) -> InitShardError {
        error
            .map_right(|error| InitShardError::connect(id, Box::new(error)))
            .into_inner()
    }
}

impl Drop for ShardManager {
    // Abort all shards
    fn drop(&mut self) {
        let mut shards = self
            .shards
            .try_write()
            .expect("only this is using the shards map");

        // Are any shards running in the background?
        let total = shards.len();
        if total == 0 {
            tracing::debug!("manager dropped; no active shard(s) are running");
            return;
        }

        tracing::warn!(
            "manager dropped without shutting it down first; \
            aborting {total} active shard(s)..."
        );

        for (_, shard) in shards.drain() {
            _ = shard.queue_close(CloseFrame::NORMAL);
        }
        tracing::debug!("aborted {total} shard(s)...");
    }
}
