use std::any::Any;
use std::sync::Arc;
use twilight_gateway::queue::Queue;

#[derive(Clone)]
pub struct ThreadSafeQueue(Arc<dyn AnyThreadSafeQueue>);

impl ThreadSafeQueue {
    #[must_use]
    pub fn new(queue: impl AnyThreadSafeQueue + 'static) -> Self {
        Self(Arc::new(queue))
    }

    #[must_use]
    pub fn downcast<T: AnyThreadSafeQueue + 'static>(&self) -> Option<Arc<T>> {
        Arc::downcast(self.0.clone()).ok()
    }
}

impl Queue for ThreadSafeQueue {
    fn enqueue(&self, id: u32) -> tokio::sync::oneshot::Receiver<()> {
        self.0.enqueue(id)
    }
}

pub trait AnyThreadSafeQueue: Queue + Any + Send + Sync + 'static {}

impl<T: Queue + Any + Send + Sync + 'static> AnyThreadSafeQueue for T {}
