//! Durable run/step store. Port of `workflow_store.erl`'s state machine.
//!
//! The Erlang store persisted every transition to Postgres (`FOR UPDATE SKIP
//! LOCKED` claims across replicas). This port keeps the identical *state
//! machine and commit contract* but backs it with an in-memory map guarded by a
//! `Mutex`; cross-replica leasing is provided at a higher layer by fiducia-node
//! ([`crate::coord`]). The commit methods return the same three-way outcome the
//! engine branches on: `Committed(run_json)` | `Conflict(run_id)` | `Err`.
//!
//! Definitions are resolved when a run is created: an inline `{ "steps": [...] }`
//! object is used directly; otherwise the definition is loaded from Postgres by
//! id/slug (same psql path as lambda definitions).

use std::collections::HashMap;

use parking_lot::Mutex;
use serde_json::{json, Value};
use uuid::Uuid;

use crate::config::Config;

/// Cap on undelivered signals held for a single run (memory-DoS guard).
const MAX_PENDING_SIGNALS: usize = 1024;

/// Outcome of a commit, mirroring `{ok,Json} | {conflict,RunId} | {error,_}`.
pub enum Commit {
    Committed(String),
    Conflict(String),
    Err(String),
}

/// A step definition parsed from the workflow definition.
#[derive(Clone)]
pub struct Run {
    pub id: String,
    pub definition_ref: String,
    pub status: String, // pending|running|waiting|completed|failed|canceled
    pub input: Value,
    pub context: serde_json::Map<String, Value>,
    pub steps: Vec<Value>,
    pub default_retry: Value,
    pub current_step_index: usize,
    pub attempt: i64,
    pub signals: Vec<Value>, // {name,payload}
    pub wait_deadline_ms: Option<i64>,
    pub next_run_at_ms: i64,
    pub lease_until_ms: i64,
    pub idempotency_key: Option<String>,
    pub error: Option<String>,
    pub step_runs: Vec<Value>,
    pub created_at: i64,
    pub updated_at: i64,
    /// Optimistic-concurrency version; every mutation bumps it and claim_due
    /// snapshots it so a concurrent cancel is detected as a conflict.
    pub version: u64,
}

impl Run {
    fn to_json(&self) -> Value {
        json!({
            "id": self.id,
            "definitionRef": self.definition_ref,
            "status": self.status,
            "input": self.input,
            "context": Value::Object(self.context.clone()),
            "steps": self.steps,
            "defaultRetry": self.default_retry,
            "currentStepIndex": self.current_step_index,
            "attempt": self.attempt,
            "signals": self.signals,
            "waitDeadlineMs": self.wait_deadline_ms,
            "idempotencyKey": self.idempotency_key,
            "error": self.error,
            "createdAt": self.created_at,
            "updatedAt": self.updated_at,
        })
    }
}

/// A leased snapshot handed to a worker (`claim_due` result element). Carries
/// everything `do_process_run` needs plus `nowMs` for deadline checks.
#[derive(Clone)]
pub struct RunView {
    pub json: Value,
    pub version: u64,
}

pub struct Store {
    runs: Mutex<HashMap<String, Run>>,
    config: Config,
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

impl Store {
    pub fn new(config: Config) -> Self {
        Store {
            runs: Mutex::new(HashMap::new()),
            config,
        }
    }

    /// The store is "available" when a database is configured OR runs are being
    /// created inline. We treat inline mode as always-available so the engine
    /// runs in single-node/test deployments.
    pub fn available(&self) -> bool {
        true
    }

    // ─── creation / external API ────────────────────────────────────────────

    pub async fn create_run(
        &self,
        def_ref: &str,
        input_json: &str,
        idempotency_key: &str,
    ) -> Result<String, String> {
        let input: Value = serde_json::from_str(input_json).unwrap_or(Value::Null);
        let (steps, default_retry) = self.resolve_definition(def_ref).await?;
        if steps.is_empty() {
            return Err("workflow definition has no steps".into());
        }

        // Idempotency: an existing run with the same key is returned as-is.
        if !idempotency_key.is_empty() {
            let runs = self.runs.lock();
            if let Some(existing) = runs
                .values()
                .find(|r| r.idempotency_key.as_deref() == Some(idempotency_key))
            {
                return Ok(existing.to_json().to_string());
            }
        }

        let now = now_ms();
        let run = Run {
            id: Uuid::new_v4().to_string(),
            definition_ref: def_ref.to_string(),
            status: "pending".into(),
            input,
            context: serde_json::Map::new(),
            steps,
            default_retry,
            current_step_index: 0,
            attempt: 0,
            signals: Vec::new(),
            wait_deadline_ms: None,
            next_run_at_ms: now,
            lease_until_ms: 0,
            idempotency_key: (!idempotency_key.is_empty()).then(|| idempotency_key.to_string()),
            error: None,
            step_runs: Vec::new(),
            created_at: now,
            updated_at: now,
            version: 0,
        };
        let out = run.to_json().to_string();
        self.runs.lock().insert(run.id.clone(), run);
        Ok(out)
    }

    pub fn deliver_signal(
        &self,
        run_id: &str,
        name_json: &str,
        payload_json: &str,
    ) -> Result<String, String> {
        let name: Value = serde_json::from_str(name_json).unwrap_or(Value::Null);
        let payload: Value = serde_json::from_str(payload_json).unwrap_or(Value::Null);
        let mut runs = self.runs.lock();
        let run = runs.get_mut(run_id).ok_or("workflow run not found")?;
        if matches!(run.status.as_str(), "completed" | "failed" | "canceled") {
            return Err("workflow run is not active".into());
        }
        // Bound the pending-signal buffer so a caller cannot grow a run's memory
        // without bound by spamming signals a `waitSignal` step never consumes.
        if run.signals.len() >= MAX_PENDING_SIGNALS {
            return Err("workflow run has too many undelivered signals".into());
        }
        run.signals.push(json!({ "name": name, "payload": payload }));
        run.next_run_at_ms = now_ms();
        run.lease_until_ms = 0; // make immediately claimable
        run.updated_at = now_ms();
        run.version += 1;
        Ok(run.to_json().to_string())
    }

    pub fn cancel_run(&self, run_id: &str) -> Commit {
        let mut runs = self.runs.lock();
        let Some(run) = runs.get_mut(run_id) else {
            return Commit::Err("workflow run not found".into());
        };
        if matches!(run.status.as_str(), "completed" | "failed" | "canceled") {
            return Commit::Conflict(run_id.to_string());
        }
        run.status = "canceled".into();
        run.updated_at = now_ms();
        run.version += 1;
        Commit::Committed(run.to_json().to_string())
    }

    pub fn get_run_with_steps(&self, run_id: &str) -> Result<String, String> {
        let runs = self.runs.lock();
        let run = runs.get(run_id).ok_or("workflow run not found")?;
        Ok(json!({
            "ok": true,
            "run": run.to_json(),
            "steps": run.step_runs,
        })
        .to_string())
    }

    pub fn list_runs(&self, def_ref: &str, limit: i64) -> Result<String, String> {
        let runs = self.runs.lock();
        let mut items: Vec<&Run> = runs
            .values()
            .filter(|r| def_ref.is_empty() || r.definition_ref == def_ref)
            .collect();
        items.sort_by(|a, b| b.created_at.cmp(&a.created_at));
        let limited: Vec<Value> = items
            .into_iter()
            .take(limit.max(0) as usize)
            .map(|r| r.to_json())
            .collect();
        Ok(Value::Array(limited).to_string())
    }

    /// Evict terminal runs (`completed|failed|canceled`) whose last update is
    /// older than `retain_ms`. The Erlang store lived in Postgres and never held
    /// runs in memory; this in-memory port would otherwise grow without bound, so
    /// a periodic sweep reclaims finished runs. Returns the number evicted.
    pub fn gc_terminal(&self, retain_ms: i64) -> usize {
        let cutoff = now_ms() - retain_ms.max(0);
        let mut runs = self.runs.lock();
        let before = runs.len();
        runs.retain(|_, r| {
            !(matches!(r.status.as_str(), "completed" | "failed" | "canceled")
                && r.updated_at < cutoff)
        });
        before - runs.len()
    }

    // ─── scheduler ──────────────────────────────────────────────────────────

    /// Atomically lease up to `limit` due runs (`claim_due/2`). A run is due when
    /// it is not terminal, its `next_run_at` has passed, and it is not currently
    /// leased. Leasing sets `lease_until = now + lease_ms` and flips it to
    /// `running` so a peer scheduler skips it.
    pub fn claim_due(&self, limit: usize, lease_ms: i64) -> Vec<RunView> {
        let now = now_ms();
        let mut runs = self.runs.lock();
        let mut due: Vec<String> = runs
            .values()
            .filter(|r| {
                !matches!(r.status.as_str(), "completed" | "failed" | "canceled")
                    && r.next_run_at_ms <= now
                    && r.lease_until_ms <= now
            })
            .map(|r| r.id.clone())
            .collect();
        due.sort();
        due.truncate(limit);

        let mut views = Vec::new();
        for id in due {
            if let Some(run) = runs.get_mut(&id) {
                run.lease_until_ms = now + lease_ms;
                if run.status == "pending" {
                    run.status = "running".into();
                }
                run.version += 1;
                let mut j = run.to_json();
                // Enrich with fields the worker reads directly.
                j["nowMs"] = json!(now);
                j["context"] = Value::Object(run.context.clone());
                views.push(RunView {
                    json: j,
                    version: run.version,
                });
            }
        }
        views
    }

    // ─── commit operations (guarded by version) ─────────────────────────────

    fn guard<'a>(
        runs: &'a mut HashMap<String, Run>,
        run_id: &str,
        expected_version: u64,
    ) -> Result<&'a mut Run, Commit> {
        match runs.get_mut(run_id) {
            None => Err(Commit::Err("workflow run not found".into())),
            Some(run) => {
                if run.status == "canceled" {
                    Err(Commit::Conflict(run_id.to_string()))
                } else if run.version != expected_version {
                    Err(Commit::Conflict(run_id.to_string()))
                } else {
                    Ok(run)
                }
            }
        }
    }

    pub fn succeed_complete(
        &self,
        run_id: &str,
        version: u64,
        step_row: Option<Value>,
        _output_json: &str,
        context_json: &str,
    ) -> Commit {
        let mut runs = self.runs.lock();
        let run = match Self::guard(&mut runs, run_id, version) {
            Ok(r) => r,
            Err(c) => return c,
        };
        if let Some(sr) = step_row {
            run.step_runs.push(sr);
        }
        run.context = parse_obj(context_json);
        run.status = "completed".into();
        run.error = None;
        run.lease_until_ms = 0;
        run.updated_at = now_ms();
        run.version += 1;
        Commit::Committed(run.to_json().to_string())
    }

    pub fn succeed_advance(
        &self,
        run_id: &str,
        version: u64,
        step_row: Value,
        next_idx: usize,
        context_json: &str,
    ) -> Commit {
        let mut runs = self.runs.lock();
        let run = match Self::guard(&mut runs, run_id, version) {
            Ok(r) => r,
            Err(c) => return c,
        };
        run.step_runs.push(step_row);
        run.context = parse_obj(context_json);
        run.current_step_index = next_idx;
        run.attempt = 0;
        run.status = "running".into();
        run.next_run_at_ms = now_ms();
        run.lease_until_ms = 0;
        run.updated_at = now_ms();
        run.version += 1;
        Commit::Committed(run.to_json().to_string())
    }

    pub fn fail_retry(
        &self,
        run_id: &str,
        version: u64,
        step_row: Value,
        new_attempt: i64,
        backoff_ms: i64,
        err: &str,
    ) -> Commit {
        let mut runs = self.runs.lock();
        let run = match Self::guard(&mut runs, run_id, version) {
            Ok(r) => r,
            Err(c) => return c,
        };
        run.step_runs.push(step_row);
        run.attempt = new_attempt;
        run.error = Some(err.to_string());
        run.status = "running".into();
        run.next_run_at_ms = now_ms() + backoff_ms.max(0);
        run.lease_until_ms = 0;
        run.updated_at = now_ms();
        run.version += 1;
        Commit::Committed(run.to_json().to_string())
    }

    pub fn fail_terminal(&self, run_id: &str, version: u64, step_row: Value, err: &str) -> Commit {
        let mut runs = self.runs.lock();
        let run = match Self::guard(&mut runs, run_id, version) {
            Ok(r) => r,
            Err(c) => return c,
        };
        run.step_runs.push(step_row);
        run.status = "failed".into();
        run.error = Some(err.to_string());
        run.lease_until_ms = 0;
        run.updated_at = now_ms();
        run.version += 1;
        Commit::Committed(run.to_json().to_string())
    }

    pub fn enter_sleep(
        &self,
        run_id: &str,
        version: u64,
        step_row: Value,
        duration_ms: i64,
        next_idx: usize,
    ) -> Commit {
        let mut runs = self.runs.lock();
        let run = match Self::guard(&mut runs, run_id, version) {
            Ok(r) => r,
            Err(c) => return c,
        };
        run.step_runs.push(step_row);
        run.current_step_index = next_idx;
        run.attempt = 0;
        run.status = "running".into();
        run.next_run_at_ms = now_ms() + duration_ms.max(0);
        run.lease_until_ms = 0;
        run.updated_at = now_ms();
        run.version += 1;
        Commit::Committed(run.to_json().to_string())
    }

    pub fn consume_signal(
        &self,
        run_id: &str,
        version: u64,
        step_row: Value,
        next_idx: usize,
        context_json: &str,
        position: usize,
    ) -> Commit {
        let mut runs = self.runs.lock();
        let run = match Self::guard(&mut runs, run_id, version) {
            Ok(r) => r,
            Err(c) => return c,
        };
        run.step_runs.push(step_row);
        if position < run.signals.len() {
            run.signals.remove(position);
        }
        run.context = parse_obj(context_json);
        run.current_step_index = next_idx;
        run.attempt = 0;
        run.status = "running".into();
        run.wait_deadline_ms = None;
        run.next_run_at_ms = now_ms();
        run.lease_until_ms = 0;
        run.updated_at = now_ms();
        run.version += 1;
        Commit::Committed(run.to_json().to_string())
    }

    /// Park a wait step: set status `waiting`, compute the deadline, and stop
    /// scheduling until the deadline (or an incoming signal re-arms it).
    pub fn park_wait(&self, run_id: &str, version: u64, deadline: Option<i64>) -> Commit {
        let mut runs = self.runs.lock();
        let run = match Self::guard(&mut runs, run_id, version) {
            Ok(r) => r,
            Err(c) => return c,
        };
        run.status = "waiting".into();
        run.wait_deadline_ms = deadline.map(|d| now_ms() + d);
        run.next_run_at_ms = run.wait_deadline_ms.unwrap_or(i64::MAX);
        run.lease_until_ms = 0;
        run.updated_at = now_ms();
        run.version += 1;
        Commit::Committed(run.to_json().to_string())
    }

    /// Re-park a waiting run woken by a non-matching signal: keep the deadline.
    pub fn repark_wait(&self, run_id: &str, version: u64) -> Commit {
        let mut runs = self.runs.lock();
        let run = match Self::guard(&mut runs, run_id, version) {
            Ok(r) => r,
            Err(c) => return c,
        };
        run.status = "waiting".into();
        run.next_run_at_ms = run.wait_deadline_ms.unwrap_or(i64::MAX);
        run.lease_until_ms = 0;
        run.updated_at = now_ms();
        run.version += 1;
        Commit::Committed(run.to_json().to_string())
    }

    // ─── definition resolution ──────────────────────────────────────────────

    /// Resolve a definition reference to `(steps, defaultRetry)`. Inline JSON
    /// object refs are parsed directly; bare id/slug refs are loaded from the
    /// `workflow_definitions` view via psql.
    async fn resolve_definition(&self, def_ref: &str) -> Result<(Vec<Value>, Value), String> {
        let trimmed = def_ref.trim();
        if trimmed.starts_with('{') {
            let obj: Value =
                serde_json::from_str(trimmed).map_err(|e| format!("invalid inline definition: {e}"))?;
            return Ok(extract_steps(&obj));
        }
        // Load from Postgres.
        let db = self
            .config
            .database_url
            .as_deref()
            .ok_or("LAMBDA_DATABASE_URL is required to resolve workflow definitions")?;
        let json = load_workflow_definition(db, trimmed).await?;
        let obj: Value = serde_json::from_str(&json)
            .map_err(|e| format!("workflow definition is not valid JSON: {e}"))?;
        Ok(extract_steps(&obj))
    }
}

fn parse_obj(json: &str) -> serde_json::Map<String, Value> {
    match serde_json::from_str::<Value>(json) {
        Ok(Value::Object(m)) => m,
        _ => serde_json::Map::new(),
    }
}

fn extract_steps(obj: &Value) -> (Vec<Value>, Value) {
    let steps = obj
        .get("steps")
        .or_else(|| obj.get("definition").and_then(|d| d.get("steps")))
        .and_then(|s| s.as_array())
        .cloned()
        .unwrap_or_default();
    let default_retry = obj
        .get("defaultRetry")
        .or_else(|| obj.get("definition").and_then(|d| d.get("defaultRetry")))
        .cloned()
        .unwrap_or_else(|| json!({}));
    (steps, default_retry)
}

/// Load a workflow definition (steps) by id/slug from Postgres.
async fn load_workflow_definition(database_url: &str, identifier: &str) -> Result<String, String> {
    use crate::runtime::{identifier_kind, IdentifierKind};
    let where_clause = match identifier_kind(identifier) {
        IdentifierKind::Uuid => format!("id = '{identifier}'"),
        IdentifierKind::Slug => format!("slug = '{identifier}'"),
        IdentifierKind::Invalid => {
            return Err("valid workflow definition UUID or slug is required".into())
        }
    };
    let sql = format!(
        "select jsonb_build_object('id', id, 'slug', slug, 'steps', steps_json::jsonb, 'defaultRetry', default_retry_json::jsonb)::text \
from workflow_definitions where {where_clause} and is_soft_deleted = false limit 1"
    );
    let child = tokio::process::Command::new("psql")
        .arg(database_url)
        .args(["-X", "-q", "-At", "-v", "ON_ERROR_STOP=1", "-c", &sql])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| format!("psql executable not found: {e}"))?;
    let out = tokio::time::timeout(std::time::Duration::from_secs(5), child.wait_with_output())
        .await
        .map_err(|_| "workflow definition query timed out".to_string())?
        .map_err(|e| format!("psql failed: {e}"))?;
    if !out.status.success() {
        return Err(format!(
            "psql exited {}: {}",
            out.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&out.stderr).trim()
        ));
    }
    let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if text.is_empty() {
        Err(format!("workflow definition not found: {identifier}"))
    } else {
        Ok(text)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::Config;

    fn cfg() -> Config {
        Config {
            host: "127.0.0.1".parse().unwrap(),
            port: 0,
            max_body_bytes: 1024,
            database_url: None,
            server_auth_secret: None,
            nats_url: None,
            workflow_event_subject: "x".into(),
            fiducia_base_url: None,
            child_idle_ms: 1000,
            child_timeout_ms: 1000,
        }
    }

    fn inline(steps: Value) -> String {
        json!({ "steps": steps }).to_string()
    }

    fn run_id(json: &str) -> String {
        serde_json::from_str::<Value>(json).unwrap()["id"]
            .as_str()
            .unwrap()
            .to_string()
    }

    #[tokio::test]
    async fn create_run_parses_inline_steps() {
        let store = Store::new(cfg());
        let def = inline(json!([{"type":"sleep","name":"w","durationMs":0}]));
        let v: Value =
            serde_json::from_str(&store.create_run(&def, "null", "").await.unwrap()).unwrap();
        assert_eq!(v["status"], "pending");
        assert_eq!(v["steps"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn empty_definition_is_rejected() {
        let store = Store::new(cfg());
        assert!(store.create_run(&inline(json!([])), "null", "").await.is_err());
    }

    #[tokio::test]
    async fn claim_leases_then_advance_progresses() {
        let store = Store::new(cfg());
        let def = inline(json!([{"type":"sleep","name":"a"},{"type":"sleep","name":"b"}]));
        let id = run_id(&store.create_run(&def, "null", "").await.unwrap());

        let claimed = store.claim_due(10, 60_000);
        assert_eq!(claimed.len(), 1);
        let version = claimed[0].version;
        // A leased run is not re-claimed until the lease lapses.
        assert!(store.claim_due(10, 60_000).is_empty());

        match store.succeed_advance(&id, version, json!({"index":0}), 1, "{}") {
            Commit::Committed(_) => {}
            _ => panic!("advance should commit"),
        }
        let after: Value =
            serde_json::from_str(&store.get_run_with_steps(&id).unwrap()).unwrap();
        assert_eq!(after["run"]["currentStepIndex"], 1);
    }

    #[tokio::test]
    async fn stale_version_commit_conflicts() {
        let store = Store::new(cfg());
        let def = inline(json!([{"type":"sleep"},{"type":"sleep"}]));
        let id = run_id(&store.create_run(&def, "null", "").await.unwrap());
        let version = store.claim_due(10, 60_000)[0].version;
        // First commit succeeds and bumps the version.
        assert!(matches!(
            store.succeed_advance(&id, version, json!({}), 1, "{}"),
            Commit::Committed(_)
        ));
        // Re-using the now-stale version conflicts (optimistic concurrency).
        assert!(matches!(
            store.succeed_advance(&id, version, json!({}), 1, "{}"),
            Commit::Conflict(_)
        ));
    }

    #[tokio::test]
    async fn cancel_then_commit_conflicts() {
        let store = Store::new(cfg());
        let id = run_id(&store.create_run(&inline(json!([{"type":"sleep"}])), "null", "").await.unwrap());
        let version = store.claim_due(10, 60_000)[0].version;
        assert!(matches!(store.cancel_run(&id), Commit::Committed(_)));
        assert!(matches!(store.cancel_run(&id), Commit::Conflict(_)), "double cancel");
        assert!(matches!(
            store.succeed_complete(&id, version, None, "{}", "{}"),
            Commit::Conflict(_)
        ));
    }

    #[tokio::test]
    async fn signal_delivery_is_bounded() {
        let store = Store::new(cfg());
        let id = run_id(&store.create_run(&inline(json!([{"type":"waitSignal","signalName":"go"}])), "null", "").await.unwrap());
        for _ in 0..MAX_PENDING_SIGNALS {
            store.deliver_signal(&id, "\"go\"", "null").unwrap();
        }
        assert!(
            store.deliver_signal(&id, "\"go\"", "null").is_err(),
            "over-cap signal rejected"
        );
    }

    #[tokio::test]
    async fn idempotency_key_returns_the_same_run() {
        let store = Store::new(cfg());
        let def = inline(json!([{"type":"sleep"}]));
        let a = run_id(&store.create_run(&def, "null", "idem-1").await.unwrap());
        let b = run_id(&store.create_run(&def, "null", "idem-1").await.unwrap());
        assert_eq!(a, b);
    }

    #[tokio::test]
    async fn gc_evicts_terminal_runs_only() {
        let store = Store::new(cfg());
        let done = run_id(&store.create_run(&inline(json!([{"type":"sleep"}])), "null", "").await.unwrap());
        let running = run_id(&store.create_run(&inline(json!([{"type":"sleep"}])), "null", "").await.unwrap());
        let v = store
            .claim_due(10, 60_000)
            .into_iter()
            .find(|view| view.json["id"] == done)
            .unwrap()
            .version;
        assert!(matches!(
            store.succeed_complete(&done, v, None, "{}", "{}"),
            Commit::Committed(_)
        ));
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        // retain 0 → the completed run is past the cutoff; the running one stays.
        assert_eq!(store.gc_terminal(0), 1);
        assert!(store.get_run_with_steps(&done).is_err());
        assert!(store.get_run_with_steps(&running).is_ok());
    }

    #[tokio::test]
    async fn list_runs_respects_limit_and_definition_filter() {
        let store = Store::new(cfg());
        let def = inline(json!([{"type":"sleep"}]));
        for _ in 0..3 {
            store.create_run(&def, "null", "").await.unwrap();
        }
        let listed: Value =
            serde_json::from_str(&store.list_runs("", 2).unwrap()).unwrap();
        assert_eq!(listed.as_array().unwrap().len(), 2, "limit honored");
    }
}
