use either::Either;
use tokio::sync::{RwLock, oneshot};
use twilight_gateway::{CloseFrame, ShardId};

use std::collections::HashMap;
use std::sync::Arc;

use crate::config::CommonShardConfig;
use crate::error::{Blocked, InitShardError};
use crate::handle::ShardHandle;
use crate::runner::ShardRunner;

pub struct ShardController {
    pub config: Arc<CommonShardConfig>,
    pub shards: Arc<RwLock<HashMap<ShardId, ShardHandle>>>,
}

impl ShardController {
    #[must_use]
    pub fn new(config: Arc<CommonShardConfig>) -> Self {
        Self {
            config,
            shards: Arc::default(),
        }
    }

    pub async fn abort_all(&mut self) {
        let mut shards = self.shards.write().await;
        tracing::debug!(shards.len = ?shards.len(), "aborting all active shard(s)");

        for (_, shard) in shards.drain() {
            _ = shard.queue_close(CloseFrame::NORMAL);
        }
    }

    pub async fn boot(
        &mut self,
        id: ShardId,
        identified_tx: Option<oneshot::Sender<Result<(), Either<InitShardError, Blocked>>>>,
    ) -> ShardHandle {
        let mut runner = ShardRunner::new(id, self.config.clone()).await;
        let handle = runner.handle();
        runner.identified_tx = identified_tx;
        runner.spawn();

        // Close the orphan shard if it exists
        let mut shards = self.shards.write().await;
        if let Some(orphaned) = shards.insert(id, handle.clone()) {
            _ = orphaned.queue_close(CloseFrame::NORMAL);
        }

        tracing::debug!("booted shard {id}");
        handle
    }
}

impl ShardController {
    #[must_use]
    pub fn config(&self) -> &CommonShardConfig {
        &self.config
    }

    #[must_use]
    pub fn shards(&self) -> &RwLock<HashMap<ShardId, ShardHandle>> {
        &self.shards
    }
}

/// A channel message type used to communicate to the
/// controller from the manager.
#[derive(Debug)]
pub(crate) enum ShardControllerMessage {
    SpawnShard {
        id: ShardId,
        handle_tx: Option<oneshot::Sender<ShardHandle>>,
        return_tx: Option<oneshot::Sender<Result<(), Either<InitShardError, Blocked>>>>,
    },
}

impl ShardController {
    pub(crate) async fn run(&mut self, rx: flume::Receiver<ShardControllerMessage>) {
        loop {
            let Ok(message) = rx.recv_async().await else {
                tracing::debug!("shard manager got dropped");
                self.abort_all().await;
                break;
            };

            tracing::debug!(?message, "received message");
            match message {
                ShardControllerMessage::SpawnShard {
                    id,
                    handle_tx,
                    return_tx,
                } => {
                    let handle = self.boot(id, return_tx).await;
                    if let Some(tx) = handle_tx {
                        _ = tx.send(handle);
                    }
                }
            }
        }
    }
}
