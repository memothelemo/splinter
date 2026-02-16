use std::any::Any;
use std::fmt;
use std::sync::Arc;
use twilight_gateway::queue::Queue;

/// A thread-safe, type-erased queue wrapper.
///
/// Wraps any `Queue` implementation in an `Arc` for cheap cloning and sharing
/// across threads while hiding the concrete queue type.
#[derive(Clone)]
pub struct ArcQueue {
    inner: Arc<dyn ThreadSafeQueue>,
}

#[allow(private_bounds)]
impl ArcQueue {
    /// Creates a new shared queue from any `Queue` implementation.
    ///
    /// The concrete type is erased, allowing different queue implementations
    /// to be used interchangeably.
    #[must_use]
    pub fn new<Q>(queue: Q) -> Self
    where
        Q: ThreadSafeQueue + 'static,
    {
        Self {
            inner: Arc::new(queue),
        }
    }

    /// Attempts to downcast to the original concrete queue type.
    ///
    /// Returns `Some(Arc<T>)` if the inner queue is of type `T`,
    /// otherwise returns `None`.
    #[must_use]
    pub fn downcast<T>(&self) -> Option<Arc<T>>
    where
        T: ThreadSafeQueue + 'static,
    {
        Arc::downcast(self.inner.clone()).ok()
    }
}

impl Queue for ArcQueue {
    fn enqueue(&self, id: u32) -> tokio::sync::oneshot::Receiver<()> {
        self.inner.enqueue(id)
    }
}

impl fmt::Debug for ArcQueue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ArcQueue").finish_non_exhaustive()
    }
}

/// Trait for queues that can be used in a type-erased, thread-safe context.
///
/// Automatically implemented for all types that implement `Queue + Send + Sync + 'static`.
pub(crate) trait ThreadSafeQueue: Queue + Any + Send + Sync {}

impl<T> ThreadSafeQueue for T where T: Queue + Any + Send + Sync + 'static {}

#[cfg(test)]
mod tests {
    use crate::queue::ArcQueue;
    use static_assertions::assert_impl_all;

    assert_impl_all!(ArcQueue: Send, Sync);
}
