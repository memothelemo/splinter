use crossbeam::atomic::AtomicCell;
use either::Either::*;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, oneshot};
use twilight_gateway::{CloseFrame, ShardId};

use crate::config::CommonShardConfig;
use crate::controller::{ShardController, ShardControllerMessage};
use crate::error::{CloseShardError, InitShardError, InitShardErrorType};
use crate::handle::ShardHandle;
use crate::range::ShardingRange;

pub struct ShardManager {
    controller: flume::Sender<ShardControllerMessage>,
    range: AtomicCell<ShardingRange>,
    shards: Arc<RwLock<HashMap<ShardId, ShardHandle>>>,
}

impl ShardManager {
    #[must_use]
    pub fn new(config: CommonShardConfig, range: ShardingRange) -> Arc<Self> {
        let config = Arc::new(config);
        let shards = Arc::new(RwLock::new(HashMap::new()));

        let mut controller = ShardController {
            config,
            shards: shards.clone(),
        };

        let (controller_tx, controller_rx) = flume::unbounded();
        tokio::spawn(async move {
            tracing::debug!("spawned shard controller");
            controller.run(controller_rx).await;
            tracing::debug!("shard controller closed");
        });

        Arc::new(Self {
            controller: controller_tx,
            range: AtomicCell::new(range),
            shards,
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
    #[tracing::instrument(skip_all, level = "debug", fields(range = ?self.range.load()))]
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
    async fn boot(&self, id: ShardId) -> Result<ShardHandle, InitShardError> {
        let (handle_tx, handle_rx) = oneshot::channel();
        let (return_tx, return_rx) = oneshot::channel();
        self.send_to_controller(ShardControllerMessage::SpawnShard {
            id,
            handle_tx: Some(handle_tx),
            return_tx: Some(return_tx),
        });

        // Wait for the handle, then the result whether it succeeded or not.
        let handle = handle_rx
            .await
            .map_err(|_| InitShardError::controller_closed(id))?;

        let result = return_rx
            .await
            .map_err(|_| InitShardError::controller_closed(id))?;

        match result {
            Ok(..) => Ok(handle),
            Err(Left(error)) => Err(error),
            Err(Right(error)) => Err(InitShardError {
                id,
                kind: InitShardErrorType::Connect(Box::new(error)),
            }),
        }
    }

    async fn spawn(&self, id: ShardId) -> Result<ShardHandle, InitShardError> {
        let (handle_tx, handle_rx) = oneshot::channel();
        self.send_to_controller(ShardControllerMessage::SpawnShard {
            id,
            handle_tx: Some(handle_tx),
            return_tx: None,
        });

        handle_rx.await.map_err(|_| InitShardError {
            id,
            kind: InitShardErrorType::ControllerClosed,
        })
    }
}

impl ShardManager {
    fn send_to_controller(&self, message: ShardControllerMessage) {
        if let Err(error) = self.controller.send(message) {
            tracing::warn!(?error, "failed to send message to the controller");
        }
    }
}
