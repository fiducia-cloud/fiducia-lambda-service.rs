//! Reusable child-process pool — the Rust port of `lambda_child_runner.erl`.
//!
//! Each warm child is a long-lived `/bin/sh -c 'exec <command>'` process that
//! reads one line-framed JSON invocation on stdin and writes one line-framed
//! JSON result on stdout. Children are keyed by a *reuse key* (per-function or
//! per-runtime pool slot); a slot is reused only while the underlying command is
//! unchanged and the process is alive. Idle children are reaped after their idle
//! window, and every invocation is bounded by a hard timeout.
//!
//! The Erlang version used a `gen_server`-style manager process plus ETS tables;
//! here the pool is a `Mutex<HashMap>` guarding [`Worker`] handles, each owning a
//! Tokio child + framed stdio and reachable through a request channel. The
//! externally observable contract (reuse, idle-reap, byte cap, line framing,
//! metric names) is preserved.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, Command};
use tokio::sync::{mpsc, oneshot, Mutex};
use tokio::time::Instant;

use crate::config::Config;
use crate::metrics::Metrics;
use crate::nats::Nats;
use crate::runtime;

const MAX_RESULT_BYTES: usize = 1_048_576;

type InvokeReply = oneshot::Sender<Result<String, String>>;

/// A message to a live worker: an invocation payload plus a reply channel.
struct Invocation {
    payload: String,
    reply: InvokeReply,
}

/// Handle to one warm child process. The driver task owns the `Child`; this
/// struct just carries the request channel and reuse bookkeeping.
struct Worker {
    command: String,
    tx: mpsc::Sender<Invocation>,
    idle_ms: u64,
    last_used: Instant,
    /// Set once the driver task observes the child gone, so `ensure` respawns.
    alive: Arc<std::sync::atomic::AtomicBool>,
}

/// The pool: a keyed set of warm workers plus shared metrics and NATS handle
/// (the latter for container-pool dispatch).
pub struct ChildRunner {
    workers: Mutex<HashMap<String, Worker>>,
    metrics: Arc<Metrics>,
    nats: Arc<Nats>,
    config: Config,
}

impl ChildRunner {
    pub fn new(config: Config, metrics: Arc<Metrics>, nats: Arc<Nats>) -> Arc<Self> {
        Arc::new(Self {
            workers: Mutex::new(HashMap::new()),
            metrics,
            nats,
            config,
        })
    }

    pub async fn active_workers(&self) -> usize {
        self.workers.lock().await.len()
    }

    pub fn metrics_text(&self, active: usize) -> String {
        self.metrics.render(active)
    }

    // ─── public entrypoints (mirror the Gleam FFI surface) ──────────────────

    /// `POST /invoke/:function_id` — load the definition, then dispatch.
    pub async fn invoke(
        self: &Arc<Self>,
        fallback_command: &str,
        identifier: &str,
        payload: &str,
        idle_ms: u64,
        timeout_ms: u64,
    ) -> Result<String, String> {
        let request = runtime::normalize_request_payload(payload);
        self.reap_idle().await;
        let def = crate::definition::load_function_definition(&self.config, identifier).await?;
        self.invoke_loaded_definition(
            fallback_command,
            identifier,
            &def,
            &request,
            idle_ms,
            timeout_ms,
        )
        .await
    }

    /// `POST /check` — validate a definition by running a check-only invocation.
    pub async fn check_definition(
        self: &Arc<Self>,
        fallback_command: &str,
        def: &str,
        timeout_ms: u64,
    ) -> Result<String, String> {
        self.reap_idle().await;
        let def = runtime::normalize_request_payload(def);
        let command = runtime::command_for_definition(fallback_command, &def)?;
        let rt = runtime::runtime_from_definition(&def);
        let containerized = runtime::json_bool_field(&def, "containerized", false);
        let payload = runtime::check_payload(&def);
        let key = runtime::check_worker_key(&rt, containerized);
        self.invoke_worker(&command, &key, &payload, 30_000, timeout_ms.max(1000))
            .await
    }

    /// `POST /destroy/:reuse_key` — tear down a warm worker.
    pub async fn destroy(self: &Arc<Self>, reuse_key: &str) -> Result<String, String> {
        let mut workers = self.workers.lock().await;
        if workers.remove(reuse_key).is_some() {
            self.metrics.child_destroys_total(1);
            Ok("destroyed".into())
        } else {
            Ok("not-found".into())
        }
    }

    // ─── dispatch ───────────────────────────────────────────────────────────

    async fn invoke_loaded_definition(
        self: &Arc<Self>,
        fallback_command: &str,
        identifier: &str,
        def: &str,
        request: &str,
        idle_ms: u64,
        timeout_ms: u64,
    ) -> Result<String, String> {
        self.metrics.invocations_total(1);
        match self.pool_dispatch_target(def) {
            PoolTarget::Local => {
                self.invoke_loaded_definition_local(
                    fallback_command,
                    identifier,
                    def,
                    request,
                    idle_ms,
                    timeout_ms,
                )
                .await
            }
            PoolTarget::Dispatch { subject, slug } => {
                self.dispatch_via_pool(
                    &subject,
                    &slug,
                    fallback_command,
                    identifier,
                    def,
                    request,
                    idle_ms,
                    timeout_ms,
                )
                .await
            }
            PoolTarget::Error(reason) => Err(reason),
        }
    }

    /// Procure a warm worker from dd-container-pool over NATS; fall back to local
    /// execution on any transport/pool failure (unless disabled).
    #[allow(clippy::too_many_arguments)]
    async fn dispatch_via_pool(
        self: &Arc<Self>,
        subject: &str,
        slug: &str,
        fallback_command: &str,
        identifier: &str,
        def: &str,
        request: &str,
        idle_ms: u64,
        timeout_ms: u64,
    ) -> Result<String, String> {
        self.metrics.pool_dispatch_total(1);
        let timeout = runtime::timeout_ms_from_definition(def, timeout_ms);
        let payload = runtime::invocation_payload(identifier, def, request);
        match self
            .nats
            .pool_dispatch(subject, slug, identifier, &payload, timeout)
            .await
        {
            Ok(output) => Ok(output),
            Err(reason) => {
                self.metrics.pool_dispatch_failures_total(1);
                if runtime::env_bool("LAMBDA_POOL_FALLBACK_LOCAL", true) {
                    tracing::warn!(%reason, "lambda pool dispatch failed; falling back to local execution");
                    self.invoke_loaded_definition_local(
                        fallback_command,
                        identifier,
                        def,
                        request,
                        idle_ms,
                        timeout_ms,
                    )
                    .await
                } else {
                    Err(reason)
                }
            }
        }
    }

    async fn invoke_loaded_definition_local(
        self: &Arc<Self>,
        fallback_command: &str,
        identifier: &str,
        def: &str,
        request: &str,
        idle_ms: u64,
        timeout_ms: u64,
    ) -> Result<String, String> {
        let command = runtime::command_for_definition(fallback_command, def)?;
        let rt = runtime::runtime_from_definition(def);
        let containerized = runtime::json_bool_field(def, "containerized", false);
        let key = runtime::worker_key(identifier, def, &rt, containerized)?;
        let idle = runtime::idle_ms_from_definition(def, idle_ms);
        let timeout = runtime::timeout_ms_from_definition(def, timeout_ms);
        let payload = runtime::invocation_payload(identifier, def, request);
        self.invoke_worker(&command, &key, &payload, idle, timeout)
            .await
    }

    /// Ensure a warm worker for `reuse_key`, send it one invocation, and await a
    /// single line-framed result under `timeout_ms` (`invoke_worker/5`).
    async fn invoke_worker(
        self: &Arc<Self>,
        command: &str,
        reuse_key: &str,
        payload: &str,
        idle_ms: u64,
        timeout_ms: u64,
    ) -> Result<String, String> {
        let tx = self.ensure_worker(command, reuse_key, idle_ms).await?;
        let (reply_tx, reply_rx) = oneshot::channel();
        if tx
            .send(Invocation {
                payload: payload.to_string(),
                reply: reply_tx,
            })
            .await
            .is_err()
        {
            self.delete_worker(reuse_key).await;
            return Err("lambda child worker unavailable".into());
        }
        match tokio::time::timeout(Duration::from_millis(timeout_ms), reply_rx).await {
            Ok(Ok(Ok(data))) => {
                self.metrics.child_stdio_bytes_total(data.len() as u64);
                self.update_last_used(reuse_key).await;
                Ok(data)
            }
            Ok(Ok(Err(reason))) => {
                self.delete_worker(reuse_key).await;
                self.metrics.child_exits_total(1);
                Err(reason)
            }
            Ok(Err(_recv)) => {
                self.delete_worker(reuse_key).await;
                self.metrics.child_exits_total(1);
                Err("lambda child worker exited".into())
            }
            Err(_elapsed) => {
                self.delete_worker(reuse_key).await;
                self.metrics.invocation_timeouts_total(1);
                Err("lambda child process timed out".into())
            }
        }
    }

    // ─── worker lifecycle ───────────────────────────────────────────────────

    async fn ensure_worker(
        self: &Arc<Self>,
        command: &str,
        reuse_key: &str,
        idle_ms: u64,
    ) -> Result<mpsc::Sender<Invocation>, String> {
        {
            let mut workers = self.workers.lock().await;
            if let Some(w) = workers.get_mut(reuse_key) {
                if w.command == command && w.alive.load(std::sync::atomic::Ordering::SeqCst) {
                    self.metrics.child_reuses_total(1);
                    w.last_used = Instant::now();
                    return Ok(w.tx.clone());
                }
                // Command changed or child gone — drop and respawn.
                workers.remove(reuse_key);
            }
        }
        self.spawn_worker(command, reuse_key, idle_ms).await
    }

    async fn spawn_worker(
        self: &Arc<Self>,
        command: &str,
        reuse_key: &str,
        idle_ms: u64,
    ) -> Result<mpsc::Sender<Invocation>, String> {
        let mut child = Command::new("/bin/sh")
            .arg("-c")
            .arg(format!("exec {command}"))
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::inherit())
            .spawn()
            .map_err(|e| format!("failed to spawn child process: {e}"))?;

        let stdin = child.stdin.take().ok_or("child stdin unavailable")?;
        let stdout = child.stdout.take().ok_or("child stdout unavailable")?;
        let (tx, rx) = mpsc::channel::<Invocation>(16);
        let alive = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let alive_driver = alive.clone();

        tokio::spawn(worker_driver(child, stdin, stdout, rx, alive_driver));

        let mut workers = self.workers.lock().await;
        workers.insert(
            reuse_key.to_string(),
            Worker {
                command: command.to_string(),
                tx: tx.clone(),
                idle_ms,
                last_used: Instant::now(),
                alive,
            },
        );
        self.metrics.child_spawns_total(1);
        Ok(tx)
    }

    async fn update_last_used(&self, reuse_key: &str) {
        if let Some(w) = self.workers.lock().await.get_mut(reuse_key) {
            w.last_used = Instant::now();
        }
    }

    async fn delete_worker(&self, reuse_key: &str) {
        self.workers.lock().await.remove(reuse_key);
    }

    /// Reap workers idle past their window (`reap_idle/1`).
    async fn reap_idle(&self) {
        let now = Instant::now();
        let mut workers = self.workers.lock().await;
        let mut dead = Vec::new();
        for (key, w) in workers.iter() {
            let idle_for = now.saturating_duration_since(w.last_used).as_millis() as u64;
            let child_gone = !w.alive.load(std::sync::atomic::Ordering::SeqCst);
            if child_gone || idle_for > w.idle_ms {
                dead.push(key.clone());
            }
        }
        for key in dead {
            workers.remove(&key);
            self.metrics.child_destroys_total(1);
        }
    }

    // ─── pool routing (pool_dispatch_target/1 and helpers) ─────────────────

    fn pool_dispatch_target(&self, def: &str) -> PoolTarget {
        let pool_backed = runtime::json_bool_field(
            def,
            "poolBacked",
            runtime::env_bool("LAMBDA_POOL_DISPATCH_DEFAULT", false),
        );
        if !pool_backed {
            return PoolTarget::Local;
        }
        let rt = runtime::runtime_from_definition(def);
        let language = {
            let l = runtime::json_string_field(def, "poolLanguage");
            if l.is_empty() {
                rt
            } else {
                l
            }
        };
        if !runtime::safe_pool_language(&language) {
            return PoolTarget::Error(format!("invalid pool language token: {language}"));
        }
        let subject = {
            let def_subject = runtime::json_string_field(def, "poolSubject");
            if !def_subject.is_empty() {
                def_subject
            } else {
                let env_subject = runtime::env_binary("LAMBDA_POOL_SUBJECT", "");
                if !env_subject.is_empty() {
                    env_subject
                } else {
                    // dd.remote.container_pool.<language>.requests
                    format!("dd.remote.container_pool.{language}.requests")
                }
            }
        };
        if !runtime::safe_nats_subject(&subject) {
            return PoolTarget::Error("pool subject is not a valid NATS subject".into());
        }
        let slug = {
            let s = runtime::json_string_field(def, "poolSlug");
            if s.is_empty() {
                String::new()
            } else if runtime::safe_pool_slug(&s) {
                s
            } else {
                return PoolTarget::Error("poolSlug contains unsupported characters".into());
            }
        };
        PoolTarget::Dispatch { subject, slug }
    }
}

enum PoolTarget {
    Local,
    Dispatch { subject: String, slug: String },
    Error(String),
}

/// Owns one child process: pumps invocations in, reads one line-framed result
/// per request out, and flips `alive` to false when the child dies. Bounds each
/// result to [`MAX_RESULT_BYTES`], matching `worker_receive_result/4`.
async fn worker_driver(
    mut child: Child,
    mut stdin: ChildStdin,
    stdout: tokio::process::ChildStdout,
    mut rx: mpsc::Receiver<Invocation>,
    alive: Arc<std::sync::atomic::AtomicBool>,
) {
    let mut reader = BufReader::new(stdout);
    while let Some(inv) = rx.recv().await {
        // Write the request line.
        if stdin.write_all(inv.payload.as_bytes()).await.is_err()
            || stdin.write_all(b"\n").await.is_err()
            || stdin.flush().await.is_err()
        {
            let _ = inv
                .reply
                .send(Err("failed to write to lambda child".into()));
            break;
        }
        // Read exactly one line of result.
        let mut line = String::new();
        match reader.read_line(&mut line).await {
            Ok(0) => {
                // EOF: child exited without a result.
                let status = child.wait().await.ok();
                let _ = inv.reply.send(Err(format!(
                    "child exited with status {}",
                    status
                        .and_then(|s| s.code())
                        .map(|c| c.to_string())
                        .unwrap_or_else(|| "unknown".into())
                )));
                break;
            }
            Ok(n) if n > MAX_RESULT_BYTES => {
                let _ = inv
                    .reply
                    .send(Err("lambda child result exceeded byte limit".into()));
                break;
            }
            Ok(_) => {
                let trimmed = line.trim_end_matches(['\n', '\r']).to_string();
                let _ = inv.reply.send(Ok(trimmed));
            }
            Err(e) => {
                let _ = inv.reply.send(Err(format!("child read error: {e}")));
                break;
            }
        }
    }
    alive.store(false, std::sync::atomic::Ordering::SeqCst);
    let _ = child.start_kill();
}
