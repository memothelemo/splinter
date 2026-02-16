use tracing_subscriber::EnvFilter;

#[must_use]
pub fn require_token() -> String {
    dotenvy::var("TOKEN").unwrap()
}

pub fn init_tracing(directives: &str) {
    let filter = EnvFilter::builder().parse(directives).unwrap();
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .compact()
        .init();
}
