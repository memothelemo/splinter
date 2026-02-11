use anyhow::{Context, Result};
use dotenvy::dotenv;
use splinter::{
    AnyThreadSafeQueue, EventStream, ShardHandle, ShardManager, ShardManagerConfig, ShardingRange,
};
use std::{sync::Arc, time::Duration};
use tracing::info;
use tracing_subscriber::{filter::LevelFilter, EnvFilter};
use twilight_gateway::{queue::InMemoryQueue, Event, EventTypeFlags, Intents};

async fn handle_event(http: Arc<twilight_http::Client>, shard: ShardHandle, event: Event) {
    match event {
        Event::MessageCreate(message) => {
            if message.content.to_lowercase().contains("hi") {
                _ = http
                    .create_message(message.channel_id)
                    .reply(message.id)
                    .content(&format!("Hello from shard `{}`!", shard.id()))
                    .await;
            }
        }
        Event::Ready(info) => {
            info!("identified as {} ({})", info.user.name, info.user.id);
        }
        _ => {}
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let filter = EnvFilter::builder()
        .with_default_directive(LevelFilter::INFO.into())
        .parse(dotenvy::var("RUST_LOG").unwrap_or_else(|_| "splinter=debug,info".to_string()))
        .unwrap();

    tracing_subscriber::fmt().with_env_filter(filter).init();
    _ = dotenv().ok();

    let token = dotenvy::var("TOKEN").context("`TOKEN` is required to run this example")?;
    let http = twilight_http::Client::new(token.to_string());

    let (mut stream, tx) = EventStream::new();
    let config = ShardManagerConfig {
        event_stream_tx: tx,
        event_type_flags: EventTypeFlags::all(),
        intents: Intents::GUILDS | Intents::GUILD_MESSAGES | Intents::MESSAGE_CONTENT,
        max_attempts: None,
        queue: AnyThreadSafeQueue::new(InMemoryQueue::default()),
        resume_url: None,
        token,
    };

    let manager = ShardManager::new(config, ShardingRange::new(0, 0, 1));
    tokio::spawn(async move {
        let http = Arc::new(http);
        while let Some((shard, event)) = stream.next_event().await {
            tracing::info!(event.kind = ?event.kind(), shard.id = %shard.id());
            tokio::spawn(handle_event(http.clone(), shard, event));
        }
    });

    info!("connecting to Discord...");
    manager.boot_all().await?;

    info!("successfully connected to Discord; press CTRL+C to exit the program");
    _ = tokio::signal::ctrl_c().await;

    info!("shutting down all shards...");
    drop(manager);
    tokio::time::sleep(Duration::from_secs(5)).await;

    Ok(())
}
