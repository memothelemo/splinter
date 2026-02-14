use tokio::sync::RwLock;
use twilight_gateway::{EventTypeFlags, Intents};

use crate::queue::ThreadSafeQueue;

pub struct CommonShardConfig {
    pub event_type_flags: EventTypeFlags,
    pub intents: Intents,
    pub queue: ThreadSafeQueue,

    /// Maximum number of reconnection attempts allowed before
    /// treating the error as fatal.
    ///
    /// `None` allows unlimited retry attempts.
    pub max_attempts: RwLock<Option<usize>>,

    pub resume_url: RwLock<Option<String>>,
    pub token: RwLock<String>,
}
