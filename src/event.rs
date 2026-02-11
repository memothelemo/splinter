use futures::StreamExt;
use std::pin::Pin;
use std::task::{Context, Poll};
use twilight_gateway::Event;

use crate::handle::ShardHandle;

pub type EventStreamItem = (ShardHandle, Event);

#[derive(Debug)]
pub struct EventStream {
    rx: flume::r#async::RecvStream<'static, EventStreamItem>,
}

impl EventStream {
    #[must_use]
    pub fn new() -> (Self, flume::Sender<EventStreamItem>) {
        let (tx, rx) = flume::unbounded();
        let rx = rx.into_stream();
        (Self { rx }, tx)
    }

    #[must_use]
    pub async fn next_event(&mut self) -> Option<EventStreamItem> {
        self.next().await
    }
}

impl futures::Stream for EventStream {
    type Item = EventStreamItem;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.rx.poll_next_unpin(cx)
    }
}
