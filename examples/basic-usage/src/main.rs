use splinter::{CommonShardConfig, ShardManager, ShardingRange, ThreadSafeQueue};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use twilight_gateway::{queue::InMemoryQueue, EventTypeFlags, Intents};

#[tokio::main]
async fn main() {
    example_common::init_tracing("splinter=debug,info");

    let config = CommonShardConfig {
        event_type_flags: EventTypeFlags::all(),
        intents: Intents::GUILDS | Intents::MESSAGE_CONTENT,
        queue: ThreadSafeQueue::new(InMemoryQueue::default()),
        max_attempts: RwLock::new(Some(3)),
        resume_url: RwLock::new(None),
        token: RwLock::new(example_common::require_token()),
    };

    tracing::info!("Press CTRL+C to exit the program");

    let cancel_token = CancellationToken::new();
    tokio::spawn({
        let cancel_token = cancel_token.clone();
        async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                tracing::warn!("CTRL+C has triggered; closing program...");
                cancel_token.cancel();
            }
        }
    });

    let manager = ShardManager::new(config, ShardingRange::new(0, 1, 2));
    if let Some(result) = cancel_token.run_until_cancelled(manager.start_all()).await {
        result.unwrap();
        tracing::info!("done waiting for shards to start");
    }

    cancel_token.cancelled().await;

    tracing::info!("closing all shards");
    manager.close_all().await.unwrap();
}
