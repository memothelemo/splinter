pub mod controller;
pub mod error;
pub mod event;
pub mod handle;
pub mod manager;
pub mod range;
pub mod runner;

pub use self::error::*;
pub use self::event::{EventStream, EventStreamItem};
pub use self::handle::ShardHandle;
pub use self::manager::{ShardManager, ShardManagerConfig};
pub use self::range::ShardingRange;

use std::sync::Arc;
use twilight_gateway::queue::Queue;

#[derive(Clone)]
pub struct AnyThreadSafeQueue(pub Arc<dyn ThreadSafeQueue>);

impl AnyThreadSafeQueue {
    #[must_use]
    pub fn new(queue: impl ThreadSafeQueue + 'static) -> Self {
        Self(Arc::new(queue))
    }
}

impl Queue for AnyThreadSafeQueue {
    fn enqueue(&self, id: u32) -> tokio::sync::oneshot::Receiver<()> {
        self.0.enqueue(id)
    }
}

pub trait ThreadSafeQueue: Queue + Send + Sync {}

impl<Q: Queue + Send + Sync + 'static> ThreadSafeQueue for Q {}
