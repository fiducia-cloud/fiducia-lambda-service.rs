//! `/docs/api`, `/api/docs`, and `/api/docs.json` — a compact, self-describing
//! route listing. The Gleam service shipped a generated blob; here we render the
//! same route set the Rust router actually serves.

use axum::http::header;
use axum::response::IntoResponse;

const ROUTES: &[(&str, &str, &str)] = &[
    ("GET", "/", "Home redirect."),
    ("GET", "/home", "Service home page."),
    ("GET", "/healthz", "Health check."),
    ("GET", "/metrics", "Prometheus metrics."),
    ("GET", "/docs/api", "Human-readable API docs."),
    ("GET", "/api/docs", "Human-readable API docs."),
    ("GET", "/api/docs.json", "Machine-readable route metadata."),
    (
        "POST",
        "/invoke/:function_id",
        "Invoke a stored function by id or slug.",
    ),
    (
        "POST",
        "/check",
        "Validate a function definition (check-only run).",
    ),
    (
        "POST",
        "/destroy/:reuse_key",
        "Tear down a warm child worker.",
    ),
    ("POST", "/workflows/start", "Start a workflow run."),
    ("GET", "/workflows/runs", "List workflow runs."),
    (
        "GET",
        "/workflows/runs/:run_id",
        "Get a run with its steps.",
    ),
    (
        "POST",
        "/workflows/runs/:run_id/signal",
        "Deliver a signal to a run.",
    ),
    ("POST", "/workflows/runs/:run_id/cancel", "Cancel a run."),
];

pub async fn html() -> impl IntoResponse {
    let mut rows = String::new();
    for (method, path, purpose) in ROUTES {
        rows.push_str(&format!(
            "<tr><td><code>{method}</code></td><td><code>{path}</code></td><td>{purpose}</td></tr>"
        ));
    }
    let body = format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\">\
<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
<title>fiducia-lambda-service API docs</title>\
<style>body{{font:14px/1.5 system-ui;margin:24px;color:#17202a}}table{{border-collapse:collapse;width:100%}}\
th,td{{padding:8px 10px;border-bottom:1px solid #d8dee6;text-align:left}}code{{background:#eef2f6;padding:2px 5px;border-radius:5px}}</style>\
</head><body><h1>fiducia-lambda-service API</h1><table><thead><tr><th>Method</th><th>Path</th><th>Purpose</th></tr></thead><tbody>{rows}</tbody></table></body></html>"
    );
    ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], body)
}

pub async fn json() -> impl IntoResponse {
    let routes: Vec<_> = ROUTES
        .iter()
        .map(|(m, p, purpose)| serde_json::json!({ "method": m, "path": p, "purpose": purpose }))
        .collect();
    let body = serde_json::json!({
        "ok": true,
        "service": "fiducia-lambda-service",
        "language": "rust",
        "routeCount": ROUTES.len(),
        "routes": routes,
    });
    (
        [(header::CONTENT_TYPE, "application/json; charset=utf-8")],
        body.to_string(),
    )
}

pub const HOME_HTML: &str = "<!doctype html><html><head><meta charset=\"utf-8\"/><title>fiducia lambda service</title>\
<style>body{font-family:system-ui;margin:24px;line-height:1.45}code{background:#f1f5f9;padding:2px 5px;border-radius:4px}</style></head>\
<body><h1>fiducia lambda service</h1><p>Health: <code>/healthz</code></p><p>Metrics: <code>/metrics</code></p>\
<p>Invocation endpoint: <code>POST /invoke/:function_id</code>. The child runner loads the active function definition from Postgres and manages reusable child processes; workflows run on the durable engine.</p></body></html>";
