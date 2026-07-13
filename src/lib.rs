//! fiducia-lambda-service: a function runtime + durable workflow engine.
//!
//! Rust port of the Gleam/Erlang `gleam-lambda-runner`. Two subsystems share one
//! process:
//!
//!   * **child runner** ([`child_runner`]) — runs stored functions in reusable,
//!     sandboxed child processes (host or containerized), with idle reaping and
//!     optional dispatch to dd-container-pool over NATS.
//!   * **workflow engine** ([`workflow`]) — a Temporal-style durable step machine
//!     (`activity` / `sleep` / `waitSignal`) over a persistent [`workflow::Store`].
//!
//! Messaging is NATS (JetStream for durable lifecycle events); coordination is
//! **optional** fiducia-node ([`coord`]) reached through `fiducia-client`. See the
//! `messaging-architecture` project note.

pub mod api_docs;
pub mod child_runner;
pub mod config;
pub mod coord;
pub mod definition;
pub mod http;
pub mod messaging;
pub mod metrics;
pub mod nats;
pub mod runtime;
pub mod util;
pub mod workflow;

pub use config::Config;
pub use http::{router, AppState};
