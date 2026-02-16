use splinter::config::reconnect_strategies::MaxAttempts;
use splinter::shard_runner::ShardRunner;
use splinter::{ArcQueue, ShardConfig};
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use twilight_gateway::{queue::InMemoryQueue, EventTypeFlags, Intents};
use twilight_gateway::{CloseFrame, ShardId};

#[tokio::main]
async fn main() {
    example_common::init_tracing("splinter=debug,info");

    let config = ShardConfig {
        event_type_flags: EventTypeFlags::all(),
        intents: Intents::GUILDS | Intents::MESSAGE_CONTENT,
        queue: ArcQueue::new(InMemoryQueue::default()),
        reconnect_strategy: Some(Arc::new(MaxAttempts::new(2))),
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

    let mut runner = ShardRunner::new(ShardId::ONE, Arc::new(config)).await;
    loop {
        if runner.is_fatally_closed() {
            break;
        }

        let Some((signal, event)) = cancel_token.run_until_cancelled(runner.recv()).await else {
            break;
        };

        if let Some(event) = event {
            tracing::info!(
                event.kind = ?event.kind(),
                shard.id = %ShardId::ONE,
                "received event"
            );
        }

        if let Some(signal) = signal {
            tracing::info!(?signal, "received signal");
        }
    }

    if !runner.is_fatally_closed() {
        runner.close(CloseFrame::NORMAL).await;
    }
}
