use tokio::sync::{Mutex, RwLock, oneshot};
use tracing::{debug, trace};
use twilight_gateway::{CloseFrame, ShardId, ShardState};

use std::collections::HashMap;
use std::sync::Arc;

use crate::error::InitShardError;
use crate::handle::ShardHandle;
use crate::manager::ShardManagerConfig;
use crate::runner::{ShardRunner, ShardRunnerMessage};

const GOT_RECONNECTED_FRAME: CloseFrame<'static> = CloseFrame::new(4000, "got reconnected");

/// A shard controller is a simple loop that runs indefinitely to facilitates
/// and controls all shards. It runs in its own thread, due to the indefinite loop.
pub struct ShardController {
    pub config: Arc<RwLock<ShardManagerConfig>>,
    pub rx: flume::Receiver<ShardControllerMessage>,
    pub shards: Arc<Mutex<HashMap<ShardId, ShardHandle>>>,
}

#[derive(Debug)]
pub enum ShardControllerMessage {
    BootShard {
        id: ShardId,
        tx: Option<oneshot::Sender<Result<(), InitShardError>>>,
    },

    ReconfigureAll {
        tx: Option<oneshot::Sender<Result<usize, InitShardError>>>,
    },
}

impl ShardController {
    #[must_use]
    pub fn new(
        config: Arc<RwLock<ShardManagerConfig>>,
        shards: Arc<Mutex<HashMap<ShardId, ShardHandle>>>,
    ) -> (Self, flume::Sender<ShardControllerMessage>) {
        let (tx, rx) = flume::unbounded();
        let controller = Self { config, rx, shards };
        (controller, tx)
    }

    #[must_use]
    pub(crate) fn spawn(
        config: Arc<RwLock<ShardManagerConfig>>,
        shards: Arc<Mutex<HashMap<ShardId, ShardHandle>>>,
    ) -> flume::Sender<ShardControllerMessage> {
        let (mut controller, message_tx) = Self::new(config, shards);
        tokio::spawn(async move {
            debug!("spawned shard controller");
            controller.run().await;
            debug!("shard controller closed");
        });
        message_tx
    }

    pub async fn run(&mut self) {
        loop {
            let Ok(message) = self.rx.recv_async().await else {
                debug!("shard manager got dropped; aborting all active shard(s)");
                self.abort_all().await;
                break;
            };

            trace!(?message, "received message");
            match message {
                ShardControllerMessage::BootShard { id, tx } => {
                    self.boot(id, tx).await;
                }
                ShardControllerMessage::ReconfigureAll { tx } => {
                    let result = self.reconfigure().await;
                    if let Some(tx) = tx {
                        _ = tx.send(result);
                    }
                }
            }
        }
    }
}

impl ShardController {
    pub async fn abort_all(&mut self) {
        // Drain all of the handles in entire shards map
        let mut shards_map = self.shards.lock().await;

        let shards = shards_map.drain().collect::<Vec<_>>();
        for (id, shard) in shards {
            debug!("aborting shard {id}");
            _ = shard.queue_close(CloseFrame::NORMAL);
        }
    }

    pub async fn boot(
        &mut self,
        id: ShardId,
        tx: Option<oneshot::Sender<Result<(), InitShardError>>>,
    ) -> ShardHandle {
        let mut runner = ShardRunner::new(self.config.clone(), id).await;
        let handle = runner.handle();

        runner.identified_tx = tx;
        runner.run_in_background();

        let mut shards = self.shards.lock().await;
        if let Some(handle) = shards.insert(id, handle.clone()) {
            let _ = handle.close(GOT_RECONNECTED_FRAME);
        }

        handle
    }

    pub async fn reconfigure(&mut self) -> Result<usize, InitShardError> {
        let shards_map = self.shards.lock().await;

        // Alert all shards that a total reconfigure is requested
        // but we need to wait for them one by one to be reidentified.
        let tasks = shards_map
            .values()
            .map(|shard| {
                let (tx, rx) = oneshot::channel();
                let id = shard.id();
                let message = ShardRunnerMessage::Reconfigure { tx: Some(tx) };
                shard.send_to_runner(message);

                async move {
                    if let Ok(result) = rx.await {
                        result.map(|_| Some(id))
                    } else {
                        Ok(None)
                    }
                }
            })
            .collect::<Vec<_>>();

        debug!("reconfiguring {} shard(s)", tasks.len());

        let mut reconfigured = 0;
        for task in tasks {
            match task.await {
                Ok(Some(id)) => {
                    debug!("reconfigured shard {id}");
                    reconfigured += 1;
                }
                Ok(None) => {}
                Err(error) => return Err(error),
            }
        }

        drop(shards_map);
        Ok(reconfigured)
    }

    pub async fn reshard(&mut self) -> Result<usize, InitShardError> {
        let mut shards_map = self.shards.lock().await;

        // Select only active shards that need to be resharded.
        let shards = shards_map
            .values()
            .filter_map(|v| matches!(v.state(), ShardState::Active).then_some(v.id()))
            .collect::<Vec<_>>();

        // We have to be careful in resharding because disconnecting all
        // shards at once will result the bot becomes suddenly offline,
        // so reconnect all of the active shards one by one.
        let resharded = shards.len();
        let tasks = shards.into_iter().map(|id| {
            let (tx, rx) = oneshot::channel();
            let config = self.config.clone();
            async move {
                let mut runner = ShardRunner::new(config, id).await;
                runner.identified_tx = Some(tx);

                let handle = runner.handle();
                runner.run_in_background();

                if let Ok(result) = rx.await {
                    (result.map(|_| Some(handle)), id)
                } else {
                    (Ok(None), id)
                }
            }
        });

        debug!("resharding {resharded} shard(s)");

        let mut resharded = 0;
        for task in tasks {
            let (result, id) = task.await;
            match result {
                Ok(Some(handle)) => {
                    debug!("restarted shard {id}");
                    resharded += 1;
                    if let Some(old) = shards_map.insert(id, handle) {
                        _ = old.close(GOT_RECONNECTED_FRAME);
                    }
                }
                Ok(None) => {}
                Err(error) => return Err(error),
            }
        }

        Ok(resharded)
    }
}
