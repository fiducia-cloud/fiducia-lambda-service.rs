use std::sync::Arc;

use fiducia_lambda_service::child_runner::ChildRunner;
use fiducia_lambda_service::config::Config;
use fiducia_lambda_service::coord::Coordinator;
use fiducia_lambda_service::http::AppState;
use fiducia_lambda_service::metrics::Metrics;
use fiducia_lambda_service::nats::Nats;
use fiducia_lambda_service::workflow::{Engine, Store};
use tracing::info;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _telemetry = fiducia_telemetry::init("fiducia-lambda-service");
    run().await
}

async fn run() -> anyhow::Result<()> {
    let config = Config::from_env()?;
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
    let nats = Arc::new(Nats::new(&config, metrics.clone()));
    // Local authority requires an explicit development opt-in. Once a node is
    // configured, startup requires its internal credentials and registration
    // must succeed so a broken authority boundary cannot degrade silently.
    let coord = Coordinator::new(
        config.fiducia_base_url.as_deref(),
        config.fiducia_node_internal_secret.as_deref(),
        &config.fiducia_node_org_id,
        config.fiducia_service_address.as_deref(),
        instance_id.clone(),
        config.allow_local_coordination,
    )
    .map_err(anyhow::Error::msg)?;
    let addr = std::net::SocketAddr::new(config.host, config.port);
    let listener = tokio::net::TcpListener::bind(addr).await?;
    coord.register_service().await.map_err(anyhow::Error::msg)?;

    let child = ChildRunner::new(config.clone(), metrics.clone(), nats.clone());
    let store = Arc::new(Store::new(config.clone()));
    let engine = Engine::new(
        store,
        child.clone(),
        coord.clone(),
        nats.clone(),
        config.clone(),
    );
    engine.start();

    let state = AppState {
        config: Arc::new(config.clone()),
        child,
        engine,
        coord: coord.clone(),
    };

    info!(%addr, "listening");

    let (registration_shutdown, registration_shutdown_rx) = tokio::sync::watch::channel(false);
    let registration_task = coord.enabled().then(|| {
        let coordinator = coord.clone();
        tokio::spawn(async move {
            coordinator
                .maintain_service_registration(registration_shutdown_rx)
                .await;
        })
    });

    let app = fiducia_lambda_service::router(state);
    let serve_result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await;
    let _ = registration_shutdown.send(true);
    if let Some(task) = registration_task {
        if let Err(error) = task.await {
            tracing::warn!(%error, "service registration heartbeat task failed during shutdown");
        }
    }
    if let Err(error) = coord.deregister_service().await {
        tracing::warn!(%error, "failed to deregister fiducia-node service instance at shutdown");
    }
    serve_result?;
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        let terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate());
        match terminate {
            Ok(mut terminate) => {
                tokio::select! {
                    _ = tokio::signal::ctrl_c() => {}
                    _ = terminate.recv() => {}
                }
            }
            Err(error) => {
                tracing::warn!(%error, "failed to install SIGTERM handler; waiting for Ctrl-C");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
    info!("shutdown signal received");
}
