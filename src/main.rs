use std::sync::Arc;

use fiducia_lambda_service::child_runner::ChildRunner;
use fiducia_lambda_service::config::Config;
use fiducia_lambda_service::coord::Coordinator;
use fiducia_lambda_service::http::AppState;
use fiducia_lambda_service::metrics::Metrics;
use fiducia_lambda_service::nats::Nats;
use fiducia_lambda_service::workflow::{Engine, Store};
use tracing::info;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    let config = Config::from_env();
    let instance_id = uuid::Uuid::new_v4().to_string();
    info!(
        host = %config.host,
        port = config.port,
        postgres = config.database_url.is_some(),
        nats = config.nats_url.is_some(),
        fiducia_node = config.fiducia_base_url.is_some(),
        %instance_id,
        "starting fiducia-lambda-service"
    );

    let metrics = Arc::new(Metrics::default());
    let nats = Arc::new(Nats::new(&config));
    // fiducia-node coordination is OPTIONAL: only active when FIDUCIA_BASE_URL is
    // set. Absent → the workflow engine runs single-node with permissive leases.
    let coord = Coordinator::new(config.fiducia_base_url.as_deref(), instance_id.clone());
    coord.register_service().await;

    let child = ChildRunner::new(config.clone(), metrics.clone(), nats.clone());
    let store = Arc::new(Store::new(config.clone()));
    let engine = Engine::new(store, child.clone(), coord, nats.clone(), config.clone());
    engine.start();

    let state = AppState {
        config: Arc::new(config.clone()),
        child,
        engine,
    };

    let addr = std::net::SocketAddr::new(config.host, config.port);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(%addr, "listening");

    let app = fiducia_lambda_service::router(state);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,fiducia_lambda_service=debug"));
    let json = std::env::var("LOG_FORMAT").map(|v| v == "json").unwrap_or(false);
    let builder = tracing_subscriber::fmt().with_env_filter(filter);
    if json {
        builder.json().init();
    } else {
        builder.init();
    }
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
    info!("shutdown signal received");
}
