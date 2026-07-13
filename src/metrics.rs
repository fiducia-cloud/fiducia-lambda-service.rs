//! Prometheus-text metrics. The Erlang runner kept counters in an ETS table;
//! here they are process-global atomics, rendered to the same metric names so
//! existing scrape configs and dashboards keep working.

use std::sync::atomic::{AtomicU64, Ordering};

macro_rules! counters {
    ($($field:ident),+ $(,)?) => {
        #[derive(Default)]
        pub struct Metrics {
            $(pub $field: AtomicU64,)+
        }
        impl Metrics {
            $(
                pub fn $field(&self, by: u64) {
                    self.$field.fetch_add(by, Ordering::Relaxed);
                }
            )+
        }
    };
}

counters! {
    invocations_total,
    child_spawns_total,
    child_reuses_total,
    child_destroys_total,
    child_exits_total,
    invocation_timeouts_total,
    child_stdio_bytes_total,
    pool_dispatch_total,
    pool_dispatch_failures_total,
}

fn g(a: &AtomicU64) -> u64 {
    a.load(Ordering::Relaxed)
}

impl Metrics {
    /// Render the child-runner metrics block (`lambda_child_runner:metrics/0`).
    /// `active_workers` is passed in because it lives in the pool, not here.
    pub fn render(&self, active_workers: usize) -> String {
        let line = |name: &str, v: u64| {
            format!("{name}{{service=\"dd-fiducia-lambda-service\"}} {v}\n")
        };
        let mut out = String::new();
        let block = |out: &mut String, name: &str, help: &str, ty: &str, v: u64| {
            out.push_str(&format!("# HELP {name} {help}\n# TYPE {name} {ty}\n"));
            out.push_str(&line(name, v));
        };
        block(&mut out, "dd_lambda_runner_invocations_total", "Lambda invocations handled by the runner.", "counter", g(&self.invocations_total));
        block(&mut out, "dd_lambda_runner_child_spawns_total", "Child processes spawned by the runner.", "counter", g(&self.child_spawns_total));
        block(&mut out, "dd_lambda_runner_child_reuses_total", "Child process reuse hits.", "counter", g(&self.child_reuses_total));
        block(&mut out, "dd_lambda_runner_child_destroys_total", "Child processes destroyed by idle reaping or command changes.", "counter", g(&self.child_destroys_total));
        block(&mut out, "dd_lambda_runner_child_exits_total", "Child processes that exited during invocation.", "counter", g(&self.child_exits_total));
        block(&mut out, "dd_lambda_runner_invocation_timeouts_total", "Lambda child invocations that timed out.", "counter", g(&self.invocation_timeouts_total));
        block(&mut out, "dd_lambda_runner_child_stdio_bytes_total", "Bytes read from child process stdio.", "counter", g(&self.child_stdio_bytes_total));
        block(&mut out, "dd_lambda_runner_pool_dispatch_total", "Invocations dispatched to dd-container-pool over NATS.", "counter", g(&self.pool_dispatch_total));
        block(&mut out, "dd_lambda_runner_pool_dispatch_failures_total", "Container-pool dispatches that failed (before any local fallback).", "counter", g(&self.pool_dispatch_failures_total));
        block(&mut out, "dd_lambda_runner_active_workers", "Active reusable child processes.", "gauge", active_workers as u64);
        out
    }
}
