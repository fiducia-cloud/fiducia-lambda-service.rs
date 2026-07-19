//! HTTP surface — the axum port of `http_server.gleam`. Same routes, same
//! three-header auth (`x-server-auth` / `x-lambda-runner-auth` / `x-agent-auth`),
//! and the same error→status mapping the Gleam `workflow_error_status/1` used.

use std::sync::Arc;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use axum::Router;

use crate::api_docs;
use crate::child_runner::ChildRunner;
use crate::config::{Config, DEFAULT_NODEJS_HOST_COMMAND};
use crate::coord::Coordinator;
use crate::workflow::Engine;

/// Shared application state handed to every handler.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub child: Arc<ChildRunner>,
    pub engine: Engine,
    pub coord: Coordinator,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/", get(|| async { Redirect::temporary("/home") }))
        .route("/home", get(home))
        .route("/healthz", get(healthz))
        .route("/readyz", get(healthz))
        .route("/metrics", get(metrics))
        .route("/docs/api", get(api_docs::html))
        .route("/api/docs", get(api_docs::html))
        .route("/api/docs.json", get(api_docs::json))
        .route("/invoke/{function_id}", post(invoke))
        .route("/check", post(check))
        .route("/destroy/{reuse_key}", post(destroy))
        .route("/workflows/start", post(workflow_start))
        .route("/workflows/runs", get(workflow_list))
        .route("/workflows/runs/{run_id}", get(workflow_get))
        .route("/workflows/runs/{run_id}/signal", post(workflow_signal))
        .route("/workflows/runs/{run_id}/cancel", post(workflow_cancel))
        .fallback(not_found)
        // Enforce the configured max body on every route, so a large POST is
        // rejected before it is buffered into memory (DoS guard).
        .layer(DefaultBodyLimit::max(state.config.max_body_bytes))
        // Fleet convention: hardening layers last — catch-panic outermost so a
        // panicking handler becomes a 500 instead of a dropped connection.
        .layer(tower_http::trace::TraceLayer::new_for_http())
        .layer(tower_http::catch_panic::CatchPanicLayer::new())
        .with_state(state)
}

// ─── helpers ────────────────────────────────────────────────────────────────

fn json_response(status: StatusCode, body: String) -> Response {
    (status, [(header::CONTENT_TYPE, "application/json")], body).into_response()
}

fn ok_err(err: &str) -> String {
    format!(
        "{{\"ok\":false,\"error\":\"{}\"}}",
        crate::runtime::json_escape(err)
    )
}

/// Resolve the shared auth secret and return an HTTP error when access is denied.
fn authorization_error(config: &Config, headers: &HeaderMap) -> Option<Response> {
    let Some(secret) = config.server_auth_secret.as_deref() else {
        return Some(json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            ok_err("SERVER_AUTH_SECRET is not configured"),
        ));
    };
    let presented = ["x-server-auth", "x-lambda-runner-auth", "x-agent-auth"]
        .iter()
        .filter_map(|h| headers.get(*h))
        .filter_map(|v| v.to_str().ok())
        .any(|v| crate::util::constant_time_eq(v.as_bytes(), secret.as_bytes()));
    if presented {
        None
    } else {
        Some(json_response(
            StatusCode::UNAUTHORIZED,
            ok_err("unauthorized"),
        ))
    }
}

/// Map a workflow/engine error string to an HTTP status (`workflow_error_status`).
fn workflow_error_status(err: &str) -> StatusCode {
    if err.contains("not found") {
        StatusCode::NOT_FOUND
    } else if err.contains("not cancelable")
        || err.contains("not running")
        || err.contains("already claimed")
    {
        StatusCode::CONFLICT
    } else if err.contains("required")
        || err.contains("invalid")
        || err.contains("must")
        || err.contains("not active")
    {
        StatusCode::BAD_REQUEST
    } else {
        StatusCode::BAD_GATEWAY
    }
}

fn workflow_error_response(err: &str) -> Response {
    json_response(workflow_error_status(err), ok_err(err))
}

// ─── service routes ─────────────────────────────────────────────────────────

async fn home() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        api_docs::HOME_HTML,
    )
}

async fn healthz(State(st): State<AppState>) -> impl IntoResponse {
    let registration = st.coord.registration_status();
    let body = serde_json::json!({
        "ok": true,
        "degraded": registration.configured && !registration.healthy,
        "service": "fiducia-lambda-service",
        "authConfigured": st.config.server_auth_configured(),
        "postgresConfigured": st.config.database_url.is_some(),
        "natsConfigured": st.config.nats_url.is_some(),
        "workflowEngineEnabled": st.engine.enabled(),
        "fiduciaNodeConfigured": registration.configured,
        "fiduciaRegistrationHealthy": registration.configured.then_some(registration.healthy),
    })
    .to_string();
    json_response(StatusCode::OK, body)
}

async fn metrics(State(st): State<AppState>) -> impl IntoResponse {
    let active = st.child.active_workers().await;
    let body = format!(
        "{}\n{}\n{}",
        st.child.metrics_text(active),
        st.engine.metrics_text(),
        st.coord.registration_metrics_text(),
    );
    (
        [(
            header::CONTENT_TYPE,
            "text/plain; version=0.0.4; charset=utf-8",
        )],
        body,
    )
}

async fn not_found() -> impl IntoResponse {
    json_response(StatusCode::NOT_FOUND, ok_err("not-found"))
}

// ─── invoke / check / destroy ───────────────────────────────────────────────

async fn invoke(
    State(st): State<AppState>,
    Path(function_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Some(resp) = authorization_error(&st.config, &headers) {
        return resp;
    }
    let Ok(payload) = std::str::from_utf8(&body) else {
        return json_response(StatusCode::BAD_REQUEST, ok_err("body-not-utf8"));
    };
    let request = crate::runtime::normalize_request_payload(payload);
    match st
        .child
        .invoke(
            DEFAULT_NODEJS_HOST_COMMAND,
            &function_id,
            &request,
            st.config.child_idle_ms,
            st.config.child_timeout_ms,
        )
        .await
    {
        Ok(output) => json_response(
            StatusCode::OK,
            format!(
                "{{\"ok\":true,\"output\":\"{}\"}}",
                crate::runtime::json_escape(&output)
            ),
        ),
        Err(err) => json_response(StatusCode::BAD_GATEWAY, ok_err(&err)),
    }
}

async fn check(State(st): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    if let Some(resp) = authorization_error(&st.config, &headers) {
        return resp;
    }
    let Ok(payload) = std::str::from_utf8(&body) else {
        return json_response(StatusCode::BAD_REQUEST, ok_err("body-not-utf8"));
    };
    match st
        .child
        .check_definition(
            DEFAULT_NODEJS_HOST_COMMAND,
            payload,
            st.config.child_timeout_ms,
        )
        .await
    {
        Ok(output) => {
            // The child reports validity in-band; a `"ok":false` body → 422.
            let status = if output.contains("\"ok\":false") {
                StatusCode::UNPROCESSABLE_ENTITY
            } else {
                StatusCode::OK
            };
            json_response(status, output)
        }
        Err(err) => json_response(StatusCode::BAD_GATEWAY, ok_err(&err)),
    }
}

async fn destroy(
    State(st): State<AppState>,
    Path(reuse_key): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(resp) = authorization_error(&st.config, &headers) {
        return resp;
    }
    match st.child.destroy(&reuse_key).await {
        Ok(message) => json_response(
            StatusCode::OK,
            format!(
                "{{\"ok\":true,\"message\":\"{}\"}}",
                crate::runtime::json_escape(&message)
            ),
        ),
        Err(err) => json_response(StatusCode::BAD_GATEWAY, ok_err(&err)),
    }
}

// ─── workflow routes ────────────────────────────────────────────────────────

async fn workflow_start(State(st): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    if let Some(resp) = authorization_error(&st.config, &headers) {
        return resp;
    }
    let Ok(payload) = std::str::from_utf8(&body) else {
        return json_response(StatusCode::BAD_REQUEST, ok_err("body-not-utf8"));
    };
    match st.engine.start_run_from_body(payload).await {
        Ok(run) => json_response(
            StatusCode::CREATED,
            format!("{{\"ok\":true,\"run\":{run}}}"),
        ),
        Err(err) => workflow_error_response(&err),
    }
}

async fn workflow_signal(
    State(st): State<AppState>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Some(resp) = authorization_error(&st.config, &headers) {
        return resp;
    }
    let Ok(payload) = std::str::from_utf8(&body) else {
        return json_response(StatusCode::BAD_REQUEST, ok_err("body-not-utf8"));
    };
    match st.engine.signal_from_body(&run_id, payload).await {
        Ok(run) => json_response(StatusCode::OK, format!("{{\"ok\":true,\"run\":{run}}}")),
        Err(err) => workflow_error_response(&err),
    }
}

async fn workflow_cancel(
    State(st): State<AppState>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(resp) = authorization_error(&st.config, &headers) {
        return resp;
    }
    match st.engine.cancel_run(&run_id) {
        Ok(run) => json_response(StatusCode::OK, format!("{{\"ok\":true,\"run\":{run}}}")),
        Err(err) => workflow_error_response(&err),
    }
}

async fn workflow_get(
    State(st): State<AppState>,
    Path(run_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(resp) = authorization_error(&st.config, &headers) {
        return resp;
    }
    match st.engine.get_run(&run_id) {
        // get_run already returns a wrapped {"ok":true,"run":...,"steps":...} body.
        Ok(body) => json_response(StatusCode::OK, body),
        Err(err) => workflow_error_response(&err),
    }
}

#[derive(serde::Deserialize)]
struct ListQuery {
    definition: Option<String>,
    limit: Option<i64>,
}

async fn workflow_list(
    State(st): State<AppState>,
    headers: HeaderMap,
    Query(q): Query<ListQuery>,
) -> Response {
    if let Some(resp) = authorization_error(&st.config, &headers) {
        return resp;
    }
    let definition = q.definition.unwrap_or_default();
    let limit = q.limit.unwrap_or(100);
    match st.engine.list_runs(&definition, limit) {
        Ok(runs) => json_response(StatusCode::OK, format!("{{\"ok\":true,\"runs\":{runs}}}")),
        Err(err) => workflow_error_response(&err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn duplicate_distributed_idempotency_claim_is_a_conflict() {
        assert_eq!(
            workflow_error_status("workflow idempotency key already claimed"),
            StatusCode::CONFLICT
        );
    }
}
