use std::pin::Pin;
use std::task::{Context, Poll};

use pin_project_lite::pin_project;
use twilight_gateway::{CloseFrame, EventType};
use twilight_model::gateway::CloseCode;
use twilight_model::gateway::event::GatewayEventDeserializer;

pin_project! {
    /// This type runs the future if it exists and returns the value
    /// as expected but it will yield indefinitely if this struct
    /// has no value inside.
    #[derive(Debug)]
    pub struct Optional<F> {
        #[pin]
        future: Option<F>,
    }
}

/// This trait allows to conveniently utilize [`Optional`] without
/// having to initialize it using the type itself by calling
/// `optional(...)` method.
pub trait OptionalExt {
    type Future: Future;

    fn optional(self) -> Optional<Self::Future>;
}

impl<F: Future> OptionalExt for Option<F> {
    type Future = F;

    fn optional(self) -> Optional<Self::Future> {
        Optional { future: self }
    }
}

impl<F: Future> Future for Optional<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        match self.project().future.as_pin_mut() {
            Some(f) => f.poll(cx),
            None => Poll::Pending,
        }
    }
}

#[must_use]
pub fn has_fatal_error_code(frame: &CloseFrame<'_>) -> bool {
    !CloseCode::try_from(frame.code)
        .map(CloseCode::can_reconnect)
        .unwrap_or(true)
}

#[must_use]
pub fn extract_event_type(event: &str) -> Option<EventType> {
    let deserializer = GatewayEventDeserializer::from_json(event)?;
    let event_type = deserializer.event_type()?;
    EventType::try_from(event_type).ok()
}
