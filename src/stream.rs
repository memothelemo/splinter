use flume::r#async::RecvStream;
use std::pin::Pin;
use std::task::{Context, Poll};
use twilight_gateway::Event;

use crate::shard_manager::ShardHandle;

/// A tuple containing the handle of the shard that produced
/// an event and the [`twilight_gateway::Event`] itself.
///
/// This is the item type yielded by the [`ShardEventStream`].
pub type ShardEventStreamItem = (ShardHandle, Event);

/// An asynchronous stream of events from all shards managed by a [`ShardManager`].
///
/// This stream can be used as the central event source for a bot. It receives
/// events from all running shards and yields them along with a handle to the
/// originating shard.
pub struct ShardEventStream {
    rx: RecvStream<'static, ShardEventStreamItem>,
}

impl ShardEventStream {
    /// Creates a new `ShardEventStream` and the `flume::Sender`
    /// used to push events into it.
    ///
    /// This is used internally by the [`ShardManager`] to construct
    /// the event stream.
    #[must_use]
    pub fn new() -> (Self, flume::Sender<ShardEventStreamItem>) {
        let (tx, rx) = flume::unbounded();
        let rx = rx.into_stream();
        (Self { rx }, tx)
    }
}

impl std::fmt::Debug for ShardEventStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ShardEventStream").finish_non_exhaustive()
    }
}

impl futures::Stream for ShardEventStream {
    type Item = ShardEventStreamItem;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.rx).poll_next(cx)
    }
}
