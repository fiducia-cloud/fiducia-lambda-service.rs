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
use crate::function_control::{self, FunctionControlError, FunctionDefinitionInput};
use crate::metrics::Metrics;

/// Shared application state handed to every handler.
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub child: Arc<ChildRunner>,
    pub engine: Engine,
    pub coord: Coordinator,
    pub metrics: Arc<Metrics>,
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
        .route(
            "/v1/functions",
            get(function_list).post(function_create),
        )
        .route(
            "/v1/functions/{function_id}",
            get(function_get).put(function_update).delete(function_delete),
        )
        .route(
            "/v1/functions/{function_id}/check",
            post(function_check),
        )
        .route(
            "/v1/functions/{function_id}/pause",
            post(function_pause),
        )
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

fn sensitive_json_response(status: StatusCode, body: String) -> Response {
    (
        status,
        [
            (header::CONTENT_TYPE, "application/json; charset=utf-8"),
            (header::CACHE_CONTROL, "no-store"),
            (header::X_CONTENT_TYPE_OPTIONS, "nosniff"),
        ],
        body,
    )
        .into_response()
}

fn organization_id(headers: &HeaderMap) -> Result<String, Response> {
    let Some(value) = headers
        .get(function_control::ORG_HEADER)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Err(json_response(
            StatusCode::BAD_REQUEST,
            ok_err("x-fiducia-org-id is required"),
        ));
    };
    if !function_control::valid_org_id(value) {
        return Err(json_response(
            StatusCode::BAD_REQUEST,
            ok_err("x-fiducia-org-id is invalid"),
        ));
    }
    Ok(value.to_string())
}

fn function_control_error(error: FunctionControlError) -> Response {
    match error {
        FunctionControlError::Invalid(reason) => {
            json_response(StatusCode::BAD_REQUEST, ok_err(&reason))
        }
        FunctionControlError::NotFound => {
            json_response(StatusCode::NOT_FOUND, ok_err("function not found"))
        }
        FunctionControlError::Conflict => {
            json_response(StatusCode::CONFLICT, ok_err("function changed during check"))
        }
        FunctionControlError::Unavailable(detail) => {
            tracing::error!(%detail, "lambda function definition store unavailable");
            json_response(
                StatusCode::SERVICE_UNAVAILABLE,
                ok_err("function definition store unavailable"),
            )
        }
    }
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

#[tracing::instrument(
    name = "fiducia.lambda.invoke",
    skip_all,
    fields(fiducia.org_id = tracing::field::Empty, function.id = %function_id)
)]
async fn invoke(
    State(st): State<AppState>,
    Path(function_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Some(resp) = authorization_error(&st.config, &headers) {
        st.metrics.tenant_auth_rejections_total(1);
        return resp;
    }
    let org_id = match organization_id(&headers) {
        Ok(org_id) => org_id,
        Err(response) => {
            st.metrics.tenant_auth_rejections_total(1);
            return response;
        }
    };
    tracing::Span::current().record("fiducia.org_id", tracing::field::display(&org_id));
    let Ok(payload) = std::str::from_utf8(&body) else {
        return json_response(StatusCode::BAD_REQUEST, ok_err("body-not-utf8"));
    };
    let request = crate::runtime::normalize_request_payload(payload);
    match st
        .child
        .invoke_for_org(
            DEFAULT_NODEJS_HOST_COMMAND,
            &function_id,
            &org_id,
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
        Err(err) if err.contains("not found") => {
            st.metrics.function_invocation_failures_total(1);
            json_response(StatusCode::NOT_FOUND, ok_err("function not found"))
        }
        Err(err) => {
            st.metrics.function_invocation_failures_total(1);
            tracing::warn!(function.id = %function_id, %err, "tenant function invocation failed");
            json_response(StatusCode::BAD_GATEWAY, ok_err("function invocation failed"))
        }
    }
}


#[derive(serde::Deserialize)]
struct FunctionListQuery {
    limit: Option<u16>,
}

#[tracing::instrument(
    name = "fiducia.lambda.function.list",
    skip_all,
    fields(fiducia.org_id = tracing::field::Empty)
)]
async fn function_list(
    State(st): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<FunctionListQuery>,
) -> Response {
    if let Some(response) = authorization_error(&st.config, &headers) {
        st.metrics.tenant_auth_rejections_total(1);
        return response;
    }
    let org_id = match organization_id(&headers) {
        Ok(org_id) => org_id,
        Err(response) => {
            st.metrics.tenant_auth_rejections_total(1);
            return response;
        }
    };
    tracing::Span::current().record("fiducia.org_id", tracing::field::display(&org_id));
    match function_control::list(&st.config, &org_id, query.limit).await {
        Ok(functions) => sensitive_json_response(
            StatusCode::OK,
            serde_json::json!({ "ok": true, "functions": functions }).to_string(),
        ),
        Err(error) => function_control_error(error),
    }
}

#[tracing::instrument(
    name = "fiducia.lambda.function.get",
    skip_all,
    fields(fiducia.org_id = tracing::field::Empty, function.id = %function_id)
)]
async fn function_get(
    State(st): State<AppState>,
    Path(function_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(response) = authorization_error(&st.config, &headers) {
        st.metrics.tenant_auth_rejections_total(1);
        return response;
    }
    let org_id = match organization_id(&headers) {
        Ok(org_id) => org_id,
        Err(response) => {
            st.metrics.tenant_auth_rejections_total(1);
            return response;
        }
    };
    tracing::Span::current().record("fiducia.org_id", tracing::field::display(&org_id));
    let id = match function_control::validate_function_id(&function_id) {
        Ok(id) => id,
        Err(error) => return function_control_error(error),
    };
    match function_control::get(&st.config, &org_id, id).await {
        Ok(function) => sensitive_json_response(
            StatusCode::OK,
            serde_json::json!({ "ok": true, "function": function }).to_string(),
        ),
        Err(error) => function_control_error(error),
    }
}

fn parse_function_input(body: &Bytes) -> Result<FunctionDefinitionInput, Response> {
    serde_json::from_slice(body).map_err(|error| {
        tracing::debug!(%error, "rejected invalid function definition request");
        json_response(
            StatusCode::BAD_REQUEST,
            ok_err("invalid function definition JSON"),
        )
    })
}

#[tracing::instrument(
    name = "fiducia.lambda.function.create",
    skip_all,
    fields(fiducia.org_id = tracing::field::Empty, function.runtime = tracing::field::Empty)
)]
async fn function_create(
    State(st): State<AppState>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Some(response) = authorization_error(&st.config, &headers) {
        st.metrics.tenant_auth_rejections_total(1);
        return response;
    }
    let org_id = match organization_id(&headers) {
        Ok(org_id) => org_id,
        Err(response) => {
            st.metrics.tenant_auth_rejections_total(1);
            return response;
        }
    };
    tracing::Span::current().record("fiducia.org_id", tracing::field::display(&org_id));
    let input = match parse_function_input(&body) {
        Ok(input) => input,
        Err(response) => return response,
    };
    tracing::Span::current().record("function.runtime", tracing::field::display(&input.runtime));
    match function_control::create(&st.config, &org_id, input).await {
        Ok(function) => {
            st.metrics.function_definitions_created_total(1);
            tracing::info!(function.id = %function.id, "customer function definition created in draft state");
            sensitive_json_response(
                StatusCode::CREATED,
                serde_json::json!({ "ok": true, "function": function }).to_string(),
            )
        }
        Err(error) => function_control_error(error),
    }
}

#[tracing::instrument(
    name = "fiducia.lambda.function.update",
    skip_all,
    fields(fiducia.org_id = tracing::field::Empty, function.id = %function_id)
)]
async fn function_update(
    State(st): State<AppState>,
    Path(function_id): Path<String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    if let Some(response) = authorization_error(&st.config, &headers) {
        st.metrics.tenant_auth_rejections_total(1);
        return response;
    }
    let org_id = match organization_id(&headers) {
        Ok(org_id) => org_id,
        Err(response) => {
            st.metrics.tenant_auth_rejections_total(1);
            return response;
        }
    };
    tracing::Span::current().record("fiducia.org_id", tracing::field::display(&org_id));
    let id = match function_control::validate_function_id(&function_id) {
        Ok(id) => id,
        Err(error) => return function_control_error(error),
    };
    let input = match parse_function_input(&body) {
        Ok(input) => input,
        Err(response) => return response,
    };
    match function_control::update(&st.config, &org_id, id, input).await {
        Ok(function) => {
            st.metrics.function_definitions_updated_total(1);
            tracing::info!(function.id = %function.id, "customer function definition updated and reset to draft");
            sensitive_json_response(
                StatusCode::OK,
                serde_json::json!({ "ok": true, "function": function }).to_string(),
            )
        }
        Err(error) => function_control_error(error),
    }
}

#[tracing::instrument(
    name = "fiducia.lambda.function.delete",
    skip_all,
    fields(fiducia.org_id = tracing::field::Empty, function.id = %function_id)
)]
async fn function_delete(
    State(st): State<AppState>,
    Path(function_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(response) = authorization_error(&st.config, &headers) {
        st.metrics.tenant_auth_rejections_total(1);
        return response;
    }
    let org_id = match organization_id(&headers) {
        Ok(org_id) => org_id,
        Err(response) => {
            st.metrics.tenant_auth_rejections_total(1);
            return response;
        }
    };
    tracing::Span::current().record("fiducia.org_id", tracing::field::display(&org_id));
    let id = match function_control::validate_function_id(&function_id) {
        Ok(id) => id,
        Err(error) => return function_control_error(error),
    };
    match function_control::delete(&st.config, &org_id, id).await {
        Ok(()) => {
            st.metrics.function_definitions_deleted_total(1);
            tracing::info!(function.id = %function_id, "customer function definition soft-deleted");
            sensitive_json_response(
                StatusCode::OK,
                serde_json::json!({ "ok": true, "deleted": true }).to_string(),
            )
        }
        Err(error) => function_control_error(error),
    }
}

#[tracing::instrument(
    name = "fiducia.lambda.function.pause",
    skip_all,
    fields(fiducia.org_id = tracing::field::Empty, function.id = %function_id)
)]
async fn function_pause(
    State(st): State<AppState>,
    Path(function_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(response) = authorization_error(&st.config, &headers) {
        st.metrics.tenant_auth_rejections_total(1);
        return response;
    }
    let org_id = match organization_id(&headers) {
        Ok(org_id) => org_id,
        Err(response) => {
            st.metrics.tenant_auth_rejections_total(1);
            return response;
        }
    };
    tracing::Span::current().record("fiducia.org_id", tracing::field::display(&org_id));
    let id = match function_control::validate_function_id(&function_id) {
        Ok(id) => id,
        Err(error) => return function_control_error(error),
    };
    match function_control::pause(&st.config, &org_id, id).await {
        Ok(function) => {
            st.metrics.function_definitions_paused_total(1);
            sensitive_json_response(
                StatusCode::OK,
                serde_json::json!({ "ok": true, "function": function }).to_string(),
            )
        }
        Err(error) => function_control_error(error),
    }
}

#[tracing::instrument(
    name = "fiducia.lambda.function.check",
    skip_all,
    fields(fiducia.org_id = tracing::field::Empty, function.id = %function_id)
)]
async fn function_check(
    State(st): State<AppState>,
    Path(function_id): Path<String>,
    headers: HeaderMap,
) -> Response {
    if let Some(response) = authorization_error(&st.config, &headers) {
        st.metrics.tenant_auth_rejections_total(1);
        return response;
    }
    let org_id = match organization_id(&headers) {
        Ok(org_id) => org_id,
        Err(response) => {
            st.metrics.tenant_auth_rejections_total(1);
            return response;
        }
    };
    tracing::Span::current().record("fiducia.org_id", tracing::field::display(&org_id));
    let id = match function_control::validate_function_id(&function_id) {
        Ok(id) => id,
        Err(error) => return function_control_error(error),
    };
    let record = match function_control::get(&st.config, &org_id, id).await {
        Ok(record) => record,
        Err(error) => return function_control_error(error),
    };
    st.metrics.function_definition_checks_total(1);
    let definition = function_control::invocation_definition(&record).to_string();
    match st
        .child
        .check_definition(
            DEFAULT_NODEJS_HOST_COMMAND,
            &definition,
            st.config.child_timeout_ms,
        )
        .await
    {
        Ok(output) if output.contains("\"ok\":false") => {
            st.metrics.function_definition_check_failures_total(1);
            sensitive_json_response(StatusCode::UNPROCESSABLE_ENTITY, output)
        }
        Ok(output) => match function_control::activate_checked(&st.config, &org_id, &record).await {
            Ok(function) => {
                tracing::info!(function.id = %function.id, "checked customer function activated");
                let check = serde_json::from_str::<serde_json::Value>(&output)
                    .unwrap_or_else(|_| serde_json::json!({ "output": output }));
                sensitive_json_response(
                    StatusCode::OK,
                    serde_json::json!({
                        "ok": true,
                        "check": check,
                        "function": function,
                    })
                    .to_string(),
                )
            }
            Err(error) => function_control_error(error),
        },
        Err(error) => {
            st.metrics.function_definition_check_failures_total(1);
            tracing::warn!(%error, function.id = %function_id, "customer function check failed");
            json_response(
                StatusCode::BAD_GATEWAY,
                ok_err("function check failed"),
            )
        }
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
