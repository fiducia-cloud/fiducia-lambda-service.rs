//! The platform-standard message envelope.
//!
//! Keeping this as a re-export prevents the lambda service from drifting to a
//! private wire format. NATS is delivery; fiducia-node remains authority.

pub use fiducia_messaging::MessageEnvelope;
