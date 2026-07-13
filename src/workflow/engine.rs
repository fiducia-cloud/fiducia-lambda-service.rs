//! Workflow execution engine — port of `workflow_engine.erl`.
//!
//! A lightweight, Temporal-style durable job runner. A definition is an ordered
//! list of steps (`activity` | `sleep` | `waitSignal`); run state is a durable
//! step-state machine in [`Store`]. A background scheduler polls for due runs
//! and advances each by exactly one step per tick, persisting the transition.
//! Because the store is authoritative, a crash/restart resumes automatically.
//!
//! Per the messaging-architecture decision, cross-replica *authority* is a
//! fiducia-node lease: before advancing a run the worker acquires
//! `workflow/run/<id>` and stamps its fencing token onto the activity payload,
//! so a stale replica cannot double-drive a run. Lifecycle events publish to
//! JetStream.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde_json::{json, Value};

use crate::child_runner::ChildRunner;
use crate::config::Config;
use crate::coord::Coordinator;
use crate::messaging::MessageEnvelope;
use crate::nats::Nats;
use crate::workflow::store::{Commit, RunView, Store};

const DEFAULT_ACTIVITY_TIMEOUT_MS: i64 = 30_000;
const DEFAULT_ACTIVITY_IDLE_MS: u64 = 300_000;
const DEFAULT_MAX_ATTEMPTS: i64 = 3;
const MAX_ATTEMPTS_CAP: i64 = 1000;
const DEFAULT_BACKOFF_MS: i64 = 1000;
const DEFAULT_BACKOFF_FACTOR: f64 = 2.0;
const DEFAULT_MAX_BACKOFF_MS: i64 = 60_000;
const MAX_CONTEXT_BYTES: usize = 4_194_304;

#[derive(Default)]
pub struct WfMetrics {
    pub runs_started: AtomicU64,
    pub runs_completed: AtomicU64,
    pub runs_failed: AtomicU64,
    pub runs_canceled: AtomicU64,
    pub steps_succeeded: AtomicU64,
    pub steps_failed: AtomicU64,
    pub steps_retried: AtomicU64,
    pub timers_started: AtomicU64,
    pub waits_started: AtomicU64,
    pub signals_delivered: AtomicU64,
    pub signals_consumed: AtomicU64,
    pub claims_total: AtomicU64,
    pub commit_conflicts: AtomicU64,
    pub commit_errors: AtomicU64,
    pub worker_exceptions: AtomicU64,
}

impl WfMetrics {
    fn inc(a: &AtomicU64) {
        a.fetch_add(1, Ordering::Relaxed);
    }
    pub fn render(&self) -> String {
        let g = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let rows = [
            ("workflow_runs_started_total", g(&self.runs_started)),
            ("workflow_runs_completed_total", g(&self.runs_completed)),
            ("workflow_runs_failed_total", g(&self.runs_failed)),
            ("workflow_runs_canceled_total", g(&self.runs_canceled)),
            ("workflow_steps_succeeded_total", g(&self.steps_succeeded)),
            ("workflow_steps_failed_total", g(&self.steps_failed)),
            ("workflow_steps_retried_total", g(&self.steps_retried)),
            ("workflow_timers_started_total", g(&self.timers_started)),
            ("workflow_waits_started_total", g(&self.waits_started)),
            ("workflow_signals_delivered_total", g(&self.signals_delivered)),
            ("workflow_signals_consumed_total", g(&self.signals_consumed)),
            ("workflow_claims_total", g(&self.claims_total)),
            ("workflow_commit_conflicts_total", g(&self.commit_conflicts)),
            ("workflow_commit_errors_total", g(&self.commit_errors)),
            ("workflow_worker_exceptions_total", g(&self.worker_exceptions)),
        ];
        let mut out = String::from(
            "# HELP workflow_engine Workflow execution engine counters\n# TYPE workflow_runs_started_total counter\n",
        );
        for (name, v) in rows {
            out.push_str(&format!("{name} {v}\n"));
        }
        out
    }
}

/// The engine handle. Cloneable (all shared state is `Arc`) so the HTTP layer
/// can call the public API while the scheduler loop runs in the background.
#[derive(Clone)]
pub struct Engine {
    store: Arc<Store>,
    child: Arc<ChildRunner>,
    coord: Coordinator,
    nats: Arc<Nats>,
    config: Config,
    metrics: Arc<WfMetrics>,
    inflight: Arc<AtomicU64>,
    wake: Arc<tokio::sync::Notify>,
}

impl Engine {
    pub fn new(
        store: Arc<Store>,
        child: Arc<ChildRunner>,
        coord: Coordinator,
        nats: Arc<Nats>,
        config: Config,
    ) -> Self {
        Engine {
            store,
            child,
            coord,
            nats,
            config,
            metrics: Arc::new(WfMetrics::default()),
            inflight: Arc::new(AtomicU64::new(0)),
            wake: Arc::new(tokio::sync::Notify::new()),
        }
    }

    pub fn enabled(&self) -> bool {
        self.store.available()
            && !matches!(
                std::env::var("WORKFLOW_ENGINE_ENABLED").as_deref(),
                Ok("0") | Ok("false") | Ok("no")
            )
    }

    pub fn metrics_text(&self) -> String {
        self.metrics.render()
    }

    /// Launch the polling scheduler as a detached background task.
    pub fn start(&self) {
        if !self.enabled() {
            tracing::info!("workflow-engine disabled (WORKFLOW_ENGINE_ENABLED=0)");
            return;
        }
        let engine = self.clone();
        let poll_ms = env_int("WORKFLOW_POLL_MS", 1000, 50, 600_000) as u64;
        let max_inflight = env_int("WORKFLOW_MAX_INFLIGHT", 16, 1, 512) as u64;
        let lease_ms = env_int("WORKFLOW_LEASE_MS", 60_000, 1000, 600_000);
        let batch = env_int("WORKFLOW_CLAIM_BATCH", 25, 1, 200) as usize;
        tracing::info!(poll_ms, "workflow-engine enabled; polling");
        tokio::spawn(async move {
            loop {
                engine.claim_and_dispatch(max_inflight, lease_ms, batch).await;
                tokio::select! {
                    _ = tokio::time::sleep(Duration::from_millis(poll_ms)) => {}
                    _ = engine.wake.notified() => {}
                }
            }
        });

        // Periodic GC of finished runs so the in-memory store cannot grow without
        // bound. Retains terminal runs for a window so clients can still read the
        // final state via GET /workflows/runs/{id}.
        let store = self.store.clone();
        let retain_ms = env_int("WORKFLOW_RETAIN_TERMINAL_MS", 3_600_000, 60_000, 604_800_000);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(300)).await;
                let evicted = store.gc_terminal(retain_ms);
                if evicted > 0 {
                    tracing::debug!(evicted, "workflow-engine GC'd terminal runs");
                }
            }
        });
    }

    fn nudge(&self) {
        self.wake.notify_one();
    }

    // ─── public API (delegates to store; nudges scheduler) ──────────────────

    pub async fn start_run_from_body(&self, body: &str) -> Result<String, String> {
        let obj: Value = parse_object(body)?;
        // An inline `steps` array (or a `definition` object carrying one) is used
        // directly; otherwise the run references a stored definition by id/slug.
        let def_ref = if obj.get("steps").and_then(|s| s.as_array()).is_some()
            || obj
                .get("definition")
                .and_then(|d| d.get("steps"))
                .and_then(|s| s.as_array())
                .is_some()
        {
            obj.to_string()
        } else {
            let r = first_present(&obj, &["definitionId", "definitionSlug", "definition", "slug"]);
            if r.is_empty() {
                return Err("definitionId or definitionSlug is required".into());
            }
            r
        };
        let input = obj.get("input").cloned().unwrap_or(Value::Null).to_string();
        let idem = {
            let k = first_present(&obj, &["idempotencyKey", "idempotency_key"]);
            k
        };
        // fiducia-node idempotency gate (authority), in addition to the store's.
        if !idem.is_empty() {
            match self.coord.claim_idempotency(&idem).await {
                Ok(false) => tracing::info!(idem, "workflow start deduplicated by fiducia-node"),
                Ok(true) => {}
                Err(e) => tracing::warn!(error = %e, "idempotency claim unavailable; proceeding"),
            }
        }
        let run = self.store.create_run(&def_ref, &input, &idem).await?;
        WfMetrics::inc(&self.metrics.runs_started);
        self.nudge();
        self.publish_event(json!({ "event": "run.started", "run": raw(&run) })).await;
        Ok(run)
    }

    pub async fn signal_from_body(&self, run_id: &str, body: &str) -> Result<String, String> {
        let obj: Value = parse_object(body)?;
        let name = obj.get("name").cloned();
        let Some(name) = name else {
            return Err("signal name is required".into());
        };
        let payload = obj.get("payload").cloned().unwrap_or(Value::Null);
        let run = self
            .store
            .deliver_signal(run_id, &name.to_string(), &payload.to_string())?;
        WfMetrics::inc(&self.metrics.signals_delivered);
        self.nudge();
        Ok(run)
    }

    pub fn cancel_run(&self, run_id: &str) -> Result<String, String> {
        match self.store.cancel_run(run_id) {
            Commit::Committed(run) => {
                WfMetrics::inc(&self.metrics.runs_canceled);
                let ev = json!({ "event": "run.canceled", "runId": run_id });
                let nats = self.nats.clone();
                let subject = self.config.workflow_event_subject.clone();
                tokio::spawn(async move {
                    nats.publish_event(&subject, &MessageEnvelope::new("run.canceled", ev)).await;
                });
                Ok(run)
            }
            Commit::Conflict(_) => Err("workflow run is not cancelable".into()),
            Commit::Err(e) => Err(e),
        }
    }

    pub fn get_run(&self, run_id: &str) -> Result<String, String> {
        self.store.get_run_with_steps(run_id)
    }

    pub fn list_runs(&self, def_ref: &str, limit: i64) -> Result<String, String> {
        self.store.list_runs(def_ref, limit)
    }

    // ─── scheduling ─────────────────────────────────────────────────────────

    async fn claim_and_dispatch(&self, max_inflight: u64, lease_ms: i64, batch: usize) {
        let free = max_inflight.saturating_sub(self.inflight.load(Ordering::SeqCst));
        if free == 0 {
            return;
        }
        WfMetrics::inc(&self.metrics.claims_total);
        let runs = self.store.claim_due((free as usize).min(batch), lease_ms);
        for view in runs {
            self.inflight.fetch_add(1, Ordering::SeqCst);
            let engine = self.clone();
            tokio::spawn(async move {
                engine.process_run(view).await;
                engine.inflight.fetch_sub(1, Ordering::SeqCst);
            });
        }
    }

    /// Advance one run by exactly one step. Acquires the fiducia-node run lease
    /// first (authority); if another replica holds it, this tick is a no-op.
    async fn process_run(&self, view: RunView) {
        let run_id = view.json["id"].as_str().unwrap_or("").to_string();
        let lease = match self.coord.try_lease_run(&run_id).await {
            Ok(Some(lease)) => lease,
            Ok(None) => return, // another replica owns this run right now
            Err(e) => {
                tracing::warn!(error = %e, run_id, "run lease unavailable; proceeding single-node");
                crate::coord::RunLease {
                    key: format!("workflow/run/{run_id}"),
                    lock_id: run_id.clone(),
                    fencing_token: 0,
                }
            }
        };

        let result = self.do_process_run(&run_id, &view, lease.fencing_token).await;
        if let Err(err) = result {
            WfMetrics::inc(&self.metrics.worker_exceptions);
            tracing::error!(run_id, %err, "workflow step crashed");
        }
        self.coord.release_run(&lease).await;
    }

    async fn do_process_run(
        &self,
        run_id: &str,
        view: &RunView,
        fencing_token: u64,
    ) -> Result<(), String> {
        let run = &view.json;
        let steps = run["steps"].as_array().cloned().unwrap_or_default();
        let idx = run["currentStepIndex"].as_i64().unwrap_or(0) as usize;
        let total = steps.len();
        if idx >= total {
            self.complete_run(run_id, view).await;
            return Ok(());
        }
        let step = &steps[idx];
        match step_type(step).as_str() {
            "activity" => self.run_activity(run_id, view, step, idx, total, fencing_token).await,
            "sleep" => self.run_sleep(run_id, view, step, idx),
            "waitSignal" => self.run_wait_signal(run_id, view, step, idx),
            other => {
                self.terminal_failure(run_id, view, step, idx, &format!("unknown workflow step type: {other}"));
            }
        }
        Ok(())
    }

    async fn complete_run(&self, run_id: &str, view: &RunView) {
        let ctx = view.json["context"].clone();
        let ctx_json = ctx.to_string();
        let c = self
            .store
            .succeed_complete(run_id, view.version, None, &ctx_json, &ctx_json);
        self.handle_commit(c, run_id, |run| {
            WfMetrics::inc(&self.metrics.runs_completed);
            Some(json!({ "event": "run.completed", "run": raw(run) }))
        })
        .await;
    }

    // ── activity ──
    async fn run_activity(
        &self,
        run_id: &str,
        view: &RunView,
        step: &Value,
        idx: usize,
        total: usize,
        fencing_token: u64,
    ) {
        let function_ref = match activity_function_ref(step) {
            Ok(r) => r,
            Err(e) => return self.terminal_failure(run_id, view, step, idx, &e),
        };
        let ctx = view.json["context"].clone();
        let run_input = view.json["input"].clone();
        let step_input = step.get("input").cloned().unwrap_or(json!({}));
        let name = step_name(step, idx);
        // The activity payload carries the fencing token so an activity that
        // performs an external mutation can prove current authority.
        let payload = json!({
            "runId": run_id,
            "step": name,
            "input": step_input,
            "context": ctx,
            "runInput": run_input,
            "fencingToken": fencing_token,
        })
        .to_string();
        let timeout_ms = step.get("timeoutMs").and_then(|v| v.as_i64()).unwrap_or(DEFAULT_ACTIVITY_TIMEOUT_MS);
        let result = self
            .child
            .invoke(
                crate::config::DEFAULT_NODEJS_HOST_COMMAND,
                &function_ref,
                &payload,
                DEFAULT_ACTIVITY_IDLE_MS,
                timeout_ms.max(1000) as u64,
            )
            .await;
        match result {
            Ok(output) => {
                self.activity_success(run_id, view, step, idx, total, &function_ref, &step_input, &output)
                    .await
            }
            Err(err) => {
                self.activity_failure(run_id, view, step, idx, &function_ref, &step_input, &err)
                    .await
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    async fn activity_success(
        &self,
        run_id: &str,
        view: &RunView,
        step: &Value,
        idx: usize,
        total: usize,
        function_ref: &str,
        step_input: &Value,
        output: &str,
    ) {
        let name = step_name(step, idx);
        let output_value = decode_output(output);
        let mut ctx = view.json["context"].as_object().cloned().unwrap_or_default();
        ctx.insert(name.clone(), output_value.clone());
        let ctx_json = Value::Object(ctx).to_string();
        if ctx_json.len() > MAX_CONTEXT_BYTES {
            return self.terminal_failure(
                run_id,
                view,
                step,
                idx,
                &format!("workflow context exceeded {MAX_CONTEXT_BYTES} bytes"),
            );
        }
        let attempt = view.json["attempt"].as_i64().unwrap_or(0);
        let step_row = step_row(idx, &name, "activity", function_ref, attempt, "succeeded",
            Some(step_input.clone()), Some(output_value.clone()), None, None);
        if idx + 1 >= total {
            let c = self.store.succeed_complete(run_id, view.version, Some(step_row), &output_value.to_string(), &ctx_json);
            let name2 = name.clone();
            self.handle_commit(c, run_id, |run| {
                WfMetrics::inc(&self.metrics.steps_succeeded);
                WfMetrics::inc(&self.metrics.runs_completed);
                Some(json!({ "event": "run.completed", "run": raw(run), "step": name2 }))
            })
            .await;
        } else {
            let c = self.store.succeed_advance(run_id, view.version, step_row, idx + 1, &ctx_json);
            let name2 = name.clone();
            let run_id2 = run_id.to_string();
            self.handle_commit(c, run_id, move |_run| {
                WfMetrics::inc(&self.metrics.steps_succeeded);
                Some(json!({ "event": "step.succeeded", "runId": run_id2, "step": name2 }))
            })
            .await;
            self.nudge();
        }
    }

    async fn activity_failure(
        &self,
        run_id: &str,
        view: &RunView,
        step: &Value,
        idx: usize,
        function_ref: &str,
        step_input: &Value,
        error: &str,
    ) {
        let name = step_name(step, idx);
        let attempt = view.json["attempt"].as_i64().unwrap_or(0);
        let new_attempt = attempt + 1;
        let retry = retry_config(&view.json, step);
        let max_attempts = max_attempts(&retry);
        let step_row = step_row(idx, &name, "activity", function_ref, attempt, "failed",
            Some(step_input.clone()), None, Some(error.to_string()), None);
        if new_attempt < max_attempts {
            let backoff = backoff_ms(&retry, attempt);
            let c = self.store.fail_retry(run_id, view.version, step_row, new_attempt, backoff, error);
            self.handle_commit(c, run_id, |_run| {
                WfMetrics::inc(&self.metrics.steps_retried);
                None
            })
            .await;
        } else {
            self.terminal_failure_row(run_id, view.version, step_row, &name, error).await;
        }
    }

    // ── sleep ──
    fn run_sleep(&self, run_id: &str, view: &RunView, step: &Value, idx: usize) {
        let duration_ms = step.get("durationMs").and_then(|v| v.as_i64()).unwrap_or(0);
        let name = step_name(step, idx);
        let step_row = step_row(idx, &name, "sleep", "", 0, "succeeded",
            Some(json!({ "durationMs": duration_ms })), None, None, Some(0));
        let c = self.store.enter_sleep(run_id, view.version, step_row, duration_ms, idx + 1);
        let engine = self.clone();
        let run_id = run_id.to_string();
        tokio::spawn(async move {
            engine.handle_commit(c, &run_id, |_r| {
                WfMetrics::inc(&engine.metrics.timers_started);
                None
            })
            .await;
        });
    }

    // ── waitSignal ──
    fn run_wait_signal(&self, run_id: &str, view: &RunView, step: &Value, idx: usize) {
        let signal_name = step.get("signalName").and_then(|v| v.as_str()).unwrap_or("").to_string();
        let signals = view.json["signals"].as_array().cloned().unwrap_or_default();
        let name = step_name(step, idx);
        // find matching signal
        let mut matched: Option<(usize, Value)> = None;
        for (pos, sig) in signals.iter().enumerate() {
            if sig.get("name").and_then(|n| n.as_str()) == Some(signal_name.as_str()) {
                matched = Some((pos, sig.get("payload").cloned().unwrap_or(Value::Null)));
                break;
            }
        }
        let engine = self.clone();
        let run_id_s = run_id.to_string();
        if let Some((position, payload)) = matched {
            let mut ctx = view.json["context"].as_object().cloned().unwrap_or_default();
            ctx.insert(name.clone(), payload.clone());
            let ctx_json = Value::Object(ctx).to_string();
            let step_row = step_row(idx, &name, "waitSignal", "", 0, "succeeded",
                Some(json!({ "signalName": signal_name })), Some(payload), None, Some(0));
            let c = self.store.consume_signal(run_id, view.version, step_row, idx + 1, &ctx_json, position);
            tokio::spawn(async move {
                engine.handle_commit(c, &run_id_s, |_r| {
                    WfMetrics::inc(&engine.metrics.signals_consumed);
                    None
                })
                .await;
                engine.nudge();
            });
        } else {
            // No matching signal: park (first entry) or check deadline (waiting).
            let status = view.json["status"].as_str().unwrap_or("running");
            if status == "waiting" {
                let now_ms = view.json["nowMs"].as_i64().unwrap_or(0);
                match view.json["waitDeadlineMs"].as_i64() {
                    Some(deadline) if now_ms >= deadline => {
                        self.terminal_failure(run_id, view, step, idx, &format!("signal wait timeout: {signal_name}"));
                    }
                    _ => {
                        let c = self.store.repark_wait(run_id, view.version);
                        tokio::spawn(async move {
                            engine.handle_commit(c, &run_id_s, |_r| None).await;
                        });
                    }
                }
            } else {
                let deadline = match step.get("waitTimeoutMs").and_then(|v| v.as_i64()).unwrap_or(0) {
                    0 => None,
                    ms => Some(ms),
                };
                let c = self.store.park_wait(run_id, view.version, deadline);
                WfMetrics::inc(&self.metrics.waits_started);
                tokio::spawn(async move {
                    engine.handle_commit(c, &run_id_s, |_r| None).await;
                });
            }
        }
    }

    // ── failure helpers ──
    fn terminal_failure(&self, run_id: &str, view: &RunView, step: &Value, idx: usize, err: &str) {
        let name = step_name(step, idx);
        let step_row = step_row(idx, &name, &step_type(step), "", 0, "failed", None, None, Some(err.to_string()), None);
        let engine = self.clone();
        let run_id = run_id.to_string();
        let err = err.to_string();
        let version = view.version;
        tokio::spawn(async move {
            engine.terminal_failure_row(&run_id, version, step_row, &name, &err).await;
        });
    }

    async fn terminal_failure_row(&self, run_id: &str, version: u64, step_row: Value, name: &str, err: &str) {
        let c = self.store.fail_terminal(run_id, version, step_row, err);
        let name = name.to_string();
        let err = err.to_string();
        self.handle_commit(c, run_id, |run| {
            WfMetrics::inc(&self.metrics.steps_failed);
            WfMetrics::inc(&self.metrics.runs_failed);
            Some(json!({ "event": "run.failed", "run": raw(run), "step": name, "error": err }))
        })
        .await;
    }

    /// Apply the success handler on a committed transition; treat a conflict as a
    /// benign no-op; log hard errors. Publishes any event the handler returns.
    async fn handle_commit<F>(&self, commit: Commit, run_id: &str, on_ok: F)
    where
        F: FnOnce(&str) -> Option<Value>,
    {
        match commit {
            Commit::Committed(run_json) => {
                if let Some(ev) = on_ok(&run_json) {
                    self.publish_event(ev).await;
                }
            }
            Commit::Conflict(_) => {
                WfMetrics::inc(&self.metrics.commit_conflicts);
                tracing::debug!(run_id, "workflow commit skipped (run changed concurrently)");
            }
            Commit::Err(reason) => {
                WfMetrics::inc(&self.metrics.commit_errors);
                tracing::warn!(run_id, %reason, "workflow commit error");
            }
        }
    }

    async fn publish_event(&self, mut event: Value) {
        if let Value::Object(ref mut m) = event {
            m.insert("ts".into(), json!(chrono::Utc::now().to_rfc3339()));
        }
        let msg_type = event.get("event").and_then(|e| e.as_str()).unwrap_or("workflow.event").to_string();
        let envelope = MessageEnvelope::new(msg_type, event);
        self.nats.publish_event(&self.config.workflow_event_subject, &envelope).await;
    }
}

// ─── pure helpers (retry policy, step rows) ─────────────────────────────────

fn step_type(step: &Value) -> String {
    match step.get("type").and_then(|t| t.as_str()) {
        Some("") | None => "activity".to_string(),
        Some(t) => t.to_string(),
    }
}

fn step_name(step: &Value, idx: usize) -> String {
    match step.get("name").and_then(|n| n.as_str()) {
        Some(n) if !n.is_empty() => n.to_string(),
        _ => format!("step-{idx}"),
    }
}

fn activity_function_ref(step: &Value) -> Result<String, String> {
    if let Some(id) = step.get("functionId").and_then(|v| v.as_str()) {
        if !id.is_empty() {
            return Ok(id.to_string());
        }
    }
    if let Some(slug) = step.get("functionSlug").and_then(|v| v.as_str()) {
        if !slug.is_empty() {
            return Ok(slug.to_string());
        }
    }
    Err("activity step requires functionId or functionSlug".into())
}

#[allow(clippy::too_many_arguments)]
fn step_row(
    idx: usize,
    name: &str,
    ty: &str,
    function_ref: &str,
    attempt: i64,
    status: &str,
    input: Option<Value>,
    output: Option<Value>,
    error: Option<String>,
    duration_ms: Option<i64>,
) -> Value {
    let mut row = json!({
        "index": idx,
        "name": name,
        "type": ty,
        "functionRef": function_ref,
        "attempt": attempt,
        "status": status,
    });
    let m = row.as_object_mut().unwrap();
    if let Some(v) = input {
        m.insert("input".into(), v);
    }
    if let Some(v) = output {
        m.insert("output".into(), v);
    }
    if let Some(v) = error {
        m.insert("error".into(), json!(v));
    }
    if let Some(v) = duration_ms {
        m.insert("durationMs".into(), json!(v));
    }
    row
}

fn retry_config(run: &Value, step: &Value) -> Value {
    let default_retry = run.get("defaultRetry").cloned().unwrap_or(json!({}));
    let step_retry = step.get("retry").cloned().unwrap_or(json!({}));
    match (default_retry.as_object(), step_retry.as_object()) {
        (Some(d), Some(s)) => {
            let mut merged = d.clone();
            for (k, v) in s {
                merged.insert(k.clone(), v.clone());
            }
            Value::Object(merged)
        }
        (Some(d), None) => Value::Object(d.clone()),
        (None, Some(s)) => Value::Object(s.clone()),
        _ => json!({}),
    }
}

pub fn max_attempts(retry: &Value) -> i64 {
    let n = retry.get("maxAttempts").and_then(|v| v.as_i64()).unwrap_or(DEFAULT_MAX_ATTEMPTS);
    n.clamp(1, MAX_ATTEMPTS_CAP)
}

pub fn backoff_ms(retry: &Value, attempt: i64) -> i64 {
    let base = retry.get("backoffMs").and_then(|v| v.as_i64()).unwrap_or(DEFAULT_BACKOFF_MS).max(0) as f64;
    let factor = retry.get("backoffFactor").and_then(|v| v.as_f64()).unwrap_or(DEFAULT_BACKOFF_FACTOR).max(1.0);
    let max_backoff = retry.get("maxBackoffMs").and_then(|v| v.as_i64()).unwrap_or(DEFAULT_MAX_BACKOFF_MS);
    let exp = attempt.min(64) as f64;
    let scaled = base * factor.powf(exp);
    max_backoff.min(scaled.min(1.0e15) as i64)
}

fn decode_output(output: &str) -> Value {
    let trimmed = output.trim();
    if trimmed.is_empty() {
        return Value::Null;
    }
    serde_json::from_str(trimmed).unwrap_or_else(|_| Value::String(output.to_string()))
}

/// Decode an already-encoded run JSON string into a structured value for
/// embedding in an event (`raw/1`); degrade to the raw string on failure.
fn raw(json: &str) -> Value {
    serde_json::from_str(json).unwrap_or_else(|_| Value::String(json.to_string()))
}

fn parse_object(body: &str) -> Result<Value, String> {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return Err("request body is required".into());
    }
    match serde_json::from_str::<Value>(trimmed) {
        Ok(v @ Value::Object(_)) => Ok(v),
        Ok(_) => Err("request body must be a JSON object".into()),
        Err(_) => Err("invalid JSON body".into()),
    }
}

/// Read an integer env var, clamped to `[min, max]`, else `default`
/// (`env_int/4` in workflow_engine.erl).
fn env_int(name: &str, default: i64, min: i64, max: i64) -> i64 {
    match std::env::var(name).ok().and_then(|v| v.parse::<i64>().ok()) {
        Some(v) if v >= min && v <= max => v,
        _ => default,
    }
}

fn first_present(obj: &Value, keys: &[&str]) -> String {
    for k in keys {
        if let Some(s) = obj.get(*k).and_then(|v| v.as_str()) {
            if !s.is_empty() {
                return s.to_string();
            }
        }
    }
    String::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn max_attempts_is_clamped() {
        assert_eq!(max_attempts(&json!({})), DEFAULT_MAX_ATTEMPTS);
        assert_eq!(max_attempts(&json!({"maxAttempts": 0})), 1, "floor of 1");
        assert_eq!(max_attempts(&json!({"maxAttempts": 5})), 5);
        assert_eq!(
            max_attempts(&json!({"maxAttempts": 10_000})),
            MAX_ATTEMPTS_CAP,
            "ceiling"
        );
    }

    #[test]
    fn backoff_grows_geometrically_and_caps() {
        let retry = json!({"backoffMs": 1000, "backoffFactor": 2.0, "maxBackoffMs": 60_000});
        assert_eq!(backoff_ms(&retry, 0), 1000);
        assert_eq!(backoff_ms(&retry, 1), 2000);
        assert_eq!(backoff_ms(&retry, 2), 4000);
        // Large attempt is capped, never overflows.
        assert_eq!(backoff_ms(&retry, 1000), 60_000);
    }

    #[test]
    fn backoff_factor_below_one_is_treated_as_one() {
        let retry = json!({"backoffMs": 500, "backoffFactor": 0.1});
        // factor clamped to >= 1.0, so it never shrinks below base.
        assert_eq!(backoff_ms(&retry, 3), 500);
    }

    #[test]
    fn step_type_defaults_to_activity() {
        assert_eq!(step_type(&json!({})), "activity");
        assert_eq!(step_type(&json!({"type": ""})), "activity");
        assert_eq!(step_type(&json!({"type": "sleep"})), "sleep");
    }

    #[test]
    fn activity_ref_requires_a_function() {
        assert!(activity_function_ref(&json!({"functionId": "abc"})).is_ok());
        assert!(activity_function_ref(&json!({"functionSlug": "s"})).is_ok());
        assert!(activity_function_ref(&json!({})).is_err());
    }

    #[test]
    fn retry_config_merges_step_over_default() {
        let run = json!({"defaultRetry": {"maxAttempts": 3, "backoffMs": 100}});
        let step = json!({"retry": {"maxAttempts": 7}});
        let merged = retry_config(&run, &step);
        assert_eq!(merged["maxAttempts"], 7, "step overrides default");
        assert_eq!(merged["backoffMs"], 100, "default retained");
    }
}
