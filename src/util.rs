use twilight_gateway::{CloseFrame, EventType};
use twilight_model::gateway::event::GatewayEventDeserializer;

#[must_use]
pub fn has_fatal_error_code(frame: &CloseFrame<'_>) -> bool {
    // https://docs.discord.com/developers/topics/opcodes-and-status-codes
    frame.code == 4004 || (4010..=4014).contains(&frame.code)
}

#[must_use]
pub fn extract_event_type(event: &str) -> Option<EventType> {
    let deserializer = GatewayEventDeserializer::from_json(event)?;
    let event_type = deserializer.event_type()?;
    EventType::try_from(event_type).ok()
}
