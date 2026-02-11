use crossbeam::atomic::AtomicCell;
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use tokio::sync::{Mutex, RwLock, oneshot};
use tracing::debug;
use twilight_gateway::{EventTypeFlags, Intents, ShardId};

use std::collections::HashMap;
use std::ops::RangeInclusive;
use std::sync::Arc;

use crate::controller::{ShardController, ShardControllerMessage};
use crate::event::EventStreamItem;
use crate::handle::ShardHandle;
use crate::range::ShardingRange;
use crate::{AnyThreadSafeQueue, InitShardError};

pub struct ShardManager {
    config: Arc<RwLock<ShardManagerConfig>>,
    controller: flume::Sender<ShardControllerMessage>,
    range: AtomicCell<ShardingRange>,
    shards: Arc<Mutex<HashMap<ShardId, ShardHandle>>>,
}

/// Configuration required to initialize a [`ShardManager`].
pub struct ShardManagerConfig {
    pub event_stream_tx: flume::Sender<EventStreamItem>,
    pub event_type_flags: EventTypeFlags,
    pub intents: Intents,
    pub queue: AnyThreadSafeQueue,
    pub resume_url: Option<String>,
    pub token: String,

    /// Maximum number of reconnection attempts allowed before
    /// treating the error as fatal.
    ///
    /// `None` allows unlimited retry attempts.
    pub max_attempts: Option<usize>,
}

impl ShardManager {
    /// Creates a new shard manager with an explicitly provided sharding range.
    ///
    /// This constructor performs no network requests and assumes the
    /// caller already knows the correct sharding layout.
    #[must_use]
    pub fn new(config: ShardManagerConfig, range: ShardingRange) -> Arc<Self> {
        let config = Arc::new(RwLock::new(config));
        let shards = Arc::new(Mutex::new(HashMap::new()));

        let controller = ShardController::spawn(config.clone(), shards.clone());
        let manager: Arc<ShardManager> = Arc::new(Self {
            config,
            controller,
            range: AtomicCell::new(range),
            shards,
        });

        Arc::clone(&manager)
    }

    // TODO: cancellation support
    pub async fn boot_all(&self) -> Result<(), InitShardError> {
        let range = self.range.load();
        let total = range.total();
        debug!("booting {total} shard(s)...");

        let mut futures = FuturesUnordered::new();
        self.queue_boot_multiple(range.from()..=range.to(), range.total())
            .into_iter()
            .map(|(id, v)| async move {
                v.await
                    .map_err(|_| InitShardError::Gateway { frame: None, id })
                    .flatten()
            })
            .for_each(|fut| futures.push(fut));

        while let Some(entry) = futures.next().await {
            if let Err(error) = entry {
                return Err(error);
            }
        }

        debug!("initialized {total} shard(s)");
        Ok(())
    }

    pub async fn update(&self, config: ShardManagerConfig) -> Result<usize, InitShardError> {
        *self.config.write().await = config;

        let (tx, rx) = oneshot::channel();
        let message = ShardControllerMessage::ReconfigureAll { tx: Some(tx) };
        _ = self.controller.send(message);

        rx.await.expect("shard controller should not be terminated")
    }
}

impl ShardManager {
    /// Gets the current event stream sender of the associated shard manager.
    ///
    /// [`EventStream`]: crate::event::EventStream
    #[must_use]
    pub async fn event_stream(&self) -> flume::Sender<EventStreamItem> {
        self.config.read().await.event_stream_tx.clone()
    }

    /// Returns a list of all [shard handles] that may have been initialized or identified.
    ///
    /// [shard handles]: ShardHandle
    #[must_use]
    pub async fn shards(&self) -> Vec<ShardHandle> {
        self.shards.lock().await.values().cloned().collect()
    }

    /// Gets the total number of shards need to be booted.
    #[must_use]
    pub fn total(&self) -> u32 {
        let range = self.range.load();
        (range.to() - range.from()) + 1
    }
}

impl ShardManager {
    fn queue_boot(&self, id: ShardId, tx: oneshot::Sender<Result<(), InitShardError>>) {
        let message = ShardControllerMessage::BootShard { id, tx: Some(tx) };
        self.controller
            .send(message)
            .expect("shard controller should not be terminated");
    }

    fn queue_boot_multiple(
        &self,
        range: RangeInclusive<u32>,
        total: u32,
    ) -> Vec<(ShardId, oneshot::Receiver<Result<(), InitShardError>>)> {
        range
            .map(|id| {
                let (tx, rx) = oneshot::channel();
                let id = ShardId::new(id, total);
                self.queue_boot(id, tx);
                (id, rx)
            })
            .collect::<Vec<_>>()
    }
}
