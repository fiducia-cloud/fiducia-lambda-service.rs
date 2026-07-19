//! Coordination via **fiducia-node** (through the blocking `fiducia-client`).
//!
//! Per the messaging-architecture decision, NATS handles *delivery* and
//! fiducia-node handles *authority*. This module is the authority side:
//!
//!   * **service registration** — announce this runner instance so peers can
//!     discover it and detect staleness;
//!   * **idempotency** — dedup workflow starts by `idempotencyKey`;
//!   * **run leases + fencing tokens** — the workflow scheduler leases a run
//!     before advancing it, so only one replica (holding the current fencing
//!     token) ever drives a given run.
//!
//! `fiducia-client` is blocking (ureq); every call is dispatched to the blocking
//! pool so it never stalls the async runtime. Synthetic single-process mode
//! requires an explicit development opt-in; once a node is configured,
//! credentials and every authority decision fail closed.

use std::{sync::Arc, time::Duration};

use fiducia_client::{FiduciaClient, LockHandle, LockOptions};
use fiducia_interfaces as types;

const RUN_LEASE_TTL_MS: u64 = 60_000;
const MAX_RENEWAL_INTERVAL_MS: u64 = 20_000;

/// A held run lease: the caller-chosen holder plus fiducia-node's fencing token.
/// The token is monotonic — a stale holder's token is always lower than the
/// current one, so downstream mutations can reject it.
#[derive(Debug, Clone)]
pub struct RunLease {
    pub key: String,
    pub holder: String,
    pub fencing_token: u64,
    pub lease_expires_ms: Option<u64>,
    ttl_ms: u64,
    authoritative: bool,
}

impl RunLease {
    fn as_lock_handle(&self) -> LockHandle {
        LockHandle {
            keys: vec![self.key.clone()],
            holder: self.holder.clone(),
            fencing_token: self.fencing_token,
            fencing_tokens: Default::default(),
            lease_expires_ms: self.lease_expires_ms,
            ttl_ms: self.ttl_ms,
        }
    }
}

/// Thin async wrapper over the blocking fiducia client.
#[derive(Clone)]
pub struct Coordinator {
    inner: Option<Arc<FiduciaClient>>,
    instance_id: String,
    service_address: String,
}

impl Coordinator {
    pub fn new(
        base_url: Option<&str>,
        internal_secret: Option<&str>,
        org_id: &str,
        service_address: Option<&str>,
        instance_id: impl Into<String>,
        allow_local_coordination: bool,
    ) -> Result<Self, String> {
        let (inner, service_address) = match base_url {
            None if allow_local_coordination => (None, String::new()),
            None => {
                return Err(
                    "fiducia-node is required unless explicit local coordination is enabled"
                        .to_string(),
                );
            }
            Some(base_url) => {
                let secret = internal_secret
                    .map(str::trim)
                    .filter(|secret| !secret.is_empty())
                    .ok_or_else(|| {
                        "fiducia-node coordination requires an internal secret".to_string()
                    })?;
                let org = org_id.trim();
                if org.is_empty()
                    || org.len() > 128
                    || org
                        .chars()
                        .any(|character| character.is_whitespace() || character.is_control())
                {
                    return Err("fiducia-node coordination requires a valid org scope".to_string());
                }
                let mut client = FiduciaClient::internal(base_url.trim(), secret, org);
                client.request_timeout = Some(Duration::from_secs(5));
                let service_address = service_address
                    .map(str::trim)
                    .filter(|address| !address.is_empty())
                    .ok_or_else(|| {
                        "fiducia-node coordination requires a service address".to_string()
                    })?
                    .to_string();
                (Some(Arc::new(client)), service_address)
            }
        };
        Ok(Coordinator {
            inner,
            instance_id: instance_id.into(),
            service_address,
        })
    }

    pub fn enabled(&self) -> bool {
        self.inner.is_some()
    }

    /// Register this process before serving. A configured node is authoritative,
    /// so transport, authentication, and response-contract errors abort startup.
    pub async fn register_service(&self) -> Result<(), String> {
        let Some(client) = self.inner.clone() else {
            return Ok(());
        };
        let instance = self.instance_id.clone();
        let address = self.service_address.clone();
        let response = tokio::task::spawn_blocking(move || {
            client.service_register("fiducia-lambda-service", &instance, &address, 30_000, None)
        })
        .await
        .map_err(|error| format!("service registration task failed: {error}"))?
        .map_err(|error| format!("service registration request failed: {error:?}"))?;
        let registered = response
            .pointer("/result/output/registered")
            .and_then(|value| value.as_bool())
            .ok_or_else(|| "service registration response omitted registered".to_string())?;
        if !registered {
            return Err("fiducia-node rejected service registration".to_string());
        }
        let instance = response
            .pointer("/result/output/instance")
            .cloned()
            .ok_or_else(|| "service registration response omitted instance".to_string())?;
        serde_json::from_value::<types::ServiceInstance>(instance)
            .map_err(|error| format!("invalid service registration response: {error}"))?;
        tracing::info!(instance = %self.instance_id, "registered with fiducia-node");
        Ok(())
    }

    /// Claim an idempotency key for a workflow start. Returns:
    ///   * `Ok(true)`  — freshly claimed, caller should do the work;
    ///   * `Ok(false)` — already claimed/completed, caller should treat as dup;
    ///   * `Err`       — coordination unavailable or malformed; caller must stop.
    ///
    /// With no coordinator configured this always claims (`Ok(true)`).
    pub async fn claim_idempotency(&self, key: &str) -> Result<bool, String> {
        let Some(client) = self.inner.clone() else {
            return Ok(true);
        };
        let key = key.to_string();
        let holder = self.instance_id.clone();
        tokio::task::spawn_blocking(move || {
            match client.idempotency_claim(
                &key,
                Some(&holder),
                Some(60_000),
                None,
                serde_json::Value::Null,
            ) {
                Ok(v) => parse_idempotency_claim(&v),
                Err(e) => Err(format!("{e:?}")),
            }
        })
        .await
        .map_err(|e| e.to_string())?
    }

    /// Try to acquire the exclusive lease that authorizes advancing a run. The
    /// returned fencing token stamps any external mutation the run performs.
    /// In explicitly enabled single-process development mode this grants a
    /// synthetic lease. A configured coordinator never falls back to this path.
    pub async fn try_lease_run(&self, run_id: &str) -> Result<Option<RunLease>, String> {
        self.try_lease_run_with_ttl(run_id, RUN_LEASE_TTL_MS).await
    }

    async fn try_lease_run_with_ttl(
        &self,
        run_id: &str,
        ttl_ms: u64,
    ) -> Result<Option<RunLease>, String> {
        let key = format!("workflow/run/{run_id}");
        let Some(client) = self.inner.clone() else {
            return Ok(Some(RunLease {
                key: key.clone(),
                holder: self.instance_id.clone(),
                fencing_token: 1,
                lease_expires_ms: None,
                ttl_ms,
                authoritative: false,
            }));
        };
        let holder = self.instance_id.clone();
        let key2 = key.clone();
        let handle = tokio::task::spawn_blocking(move || {
            client.try_lock_handle(
                &[&key2],
                LockOptions {
                    ttl_ms,
                    holder: Some(holder),
                    ..LockOptions::default()
                },
            )
        })
        .await
        .map_err(|error| format!("run lease acquisition task failed: {error}"))?
        .map_err(|error| format!("run lease acquisition request failed: {error}"))?;
        handle
            .map(|handle| run_lease_from_handle(key, ttl_ms, handle))
            .transpose()
    }

    /// Renew until cancelled by the caller. Any transport, contract, or fenced
    /// authority failure ends the loop immediately so the workflow step can be
    /// cancelled before it performs another mutation.
    pub async fn maintain_run_lease(&self, lease: RunLease) -> Result<(), String> {
        if !lease.authoritative {
            return std::future::pending::<Result<(), String>>().await;
        }
        let interval_ms = (lease.ttl_ms / 3).clamp(1, MAX_RENEWAL_INTERVAL_MS);
        loop {
            tokio::time::sleep(Duration::from_millis(interval_ms)).await;
            self.renew_run_once(&lease).await?;
        }
    }

    async fn renew_run_once(&self, lease: &RunLease) -> Result<(), String> {
        let Some(client) = self.inner.clone() else {
            return if lease.authoritative {
                Err("authoritative run lease has no configured coordinator".to_string())
            } else {
                Ok(())
            };
        };
        let mut handle = lease.as_lock_handle();
        let expected_key = lease.key.clone();
        let expected_holder = lease.holder.clone();
        let expected_token = lease.fencing_token;
        let ttl_ms = lease.ttl_ms;
        let response =
            tokio::task::spawn_blocking(move || client.renew_lock(&mut handle, Some(ttl_ms)))
                .await
                .map_err(|error| format!("run lease renewal task failed: {error}"))?
                .map_err(|error| format!("run lease renewal request failed: {error:?}"))?;
        let output = response
            .pointer("/result/output")
            .cloned()
            .ok_or_else(|| "run lease renewal response omitted result.output".to_string())?;
        let renewed: types::LockRenewResponse = serde_json::from_value(output)
            .map_err(|error| format!("invalid run lease renewal response: {error}"))?;
        if !renewed.renewed {
            return Err(format!(
                "fiducia-node rejected run lease renewal: {:?}",
                renewed.reason
            ));
        }
        if renewed.holder.as_deref() != Some(expected_holder.as_str())
            || renewed
                .fencing_token
                .and_then(|token| u64::try_from(token).ok())
                != Some(expected_token)
            || renewed.keys.as_deref() != Some(std::slice::from_ref(&expected_key))
            || renewed.lease_expires_ms.is_none_or(|expiry| expiry <= 0)
        {
            return Err("run lease renewal response did not preserve the exact grant".to_string());
        }
        Ok(())
    }

    /// Release a previously-held run lease and surface every configured-node
    /// failure instead of silently discarding it.
    pub async fn release_run(&self, lease: &RunLease) -> Result<(), String> {
        let Some(client) = self.inner.clone() else {
            return Ok(());
        };
        let handle = lease.as_lock_handle();
        let expected_key = lease.key.clone();
        let response = tokio::task::spawn_blocking(move || client.release_lock(&handle))
            .await
            .map_err(|error| format!("run lease release task failed: {error}"))?
            .map_err(|error| format!("run lease release request failed: {error:?}"))?;
        let output = response
            .pointer("/result/output")
            .cloned()
            .ok_or_else(|| "run lease release response omitted result.output".to_string())?;
        let released: types::LockReleaseResponse = serde_json::from_value(output)
            .map_err(|error| format!("invalid run lease release response: {error}"))?;
        if !released.released {
            return Err(format!(
                "fiducia-node rejected exact run lease release: {:?}",
                released.reason
            ));
        }
        if released.keys.as_deref() != Some(std::slice::from_ref(&expected_key)) {
            return Err("run lease release response named a different grant".to_string());
        }
        Ok(())
    }
}

fn run_lease_from_handle(key: String, ttl_ms: u64, handle: LockHandle) -> Result<RunLease, String> {
    if handle.keys.as_slice() != std::slice::from_ref(&key)
        || handle.holder.trim().is_empty()
        || handle.fencing_token == 0
        || handle.lease_expires_ms.is_none_or(|expiry| expiry == 0)
    {
        return Err("acquired run lease did not contain an exact holder/fenced grant".to_string());
    }
    Ok(RunLease {
        key,
        holder: handle.holder,
        fencing_token: handle.fencing_token,
        lease_expires_ms: handle.lease_expires_ms,
        ttl_ms,
        authoritative: true,
    })
}

fn parse_idempotency_claim(response: &serde_json::Value) -> Result<bool, String> {
    response
        .pointer("/result/output/claimed")
        .and_then(|value| value.as_bool())
        .ok_or_else(|| "idempotency response omitted result.output.claimed".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::{
        io::{Read, Write},
        net::TcpListener,
        sync::mpsc,
        thread,
        time::Duration,
    };

    fn serve_once(
        status: &'static str,
        body: &'static str,
    ) -> (String, mpsc::Receiver<String>, thread::JoinHandle<()>) {
        serve_responses(vec![(status, body)])
    }

    fn serve_responses(
        responses: Vec<(&'static str, &'static str)>,
    ) -> (String, mpsc::Receiver<String>, thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let (request_tx, request_rx) = mpsc::channel();
        let server = thread::spawn(move || {
            for (status, body) in responses {
                let (mut stream, _) = listener.accept().unwrap();
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut request = Vec::new();
                let mut buffer = [0_u8; 1024];
                loop {
                    let count = stream.read(&mut buffer).unwrap();
                    if count == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..count]);
                    if request.windows(4).any(|window| window == b"\r\n\r\n") {
                        break;
                    }
                }
                let header_end = request
                    .windows(4)
                    .position(|window| window == b"\r\n\r\n")
                    .map(|position| position + 4)
                    .unwrap();
                let content_length = String::from_utf8_lossy(&request[..header_end])
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .and_then(|value| value.trim().parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                while request.len() < header_end + content_length {
                    let count = stream.read(&mut buffer).unwrap();
                    if count == 0 {
                        break;
                    }
                    request.extend_from_slice(&buffer[..count]);
                }
                request_tx
                    .send(String::from_utf8(request).unwrap())
                    .unwrap();
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
        });
        (format!("http://{address}"), request_rx, server)
    }

    fn request_json(request: &str) -> serde_json::Value {
        let (_, body) = request.split_once("\r\n\r\n").unwrap();
        serde_json::from_str(body).unwrap()
    }

    #[test]
    fn configured_coordinator_requires_secret_and_valid_org() {
        assert!(Coordinator::new(
            Some("http://node"),
            None,
            "fiducia-lambda-service",
            Some("http://lambda:8083"),
            "i",
            false,
        )
        .is_err());
        assert!(Coordinator::new(
            Some("http://node"),
            Some("secret"),
            "bad org",
            Some("http://lambda:8083"),
            "i",
            false,
        )
        .is_err());
        assert!(Coordinator::new(
            Some("http://node"),
            Some("secret"),
            "fiducia-lambda-service",
            None,
            "i",
            false,
        )
        .is_err());
        assert!(Coordinator::new(None, None, "fiducia-lambda-service", None, "i", false).is_err());
        assert!(Coordinator::new(None, None, "fiducia-lambda-service", None, "i", true).is_ok());
    }

    #[tokio::test]
    async fn registration_sends_normalized_internal_headers() {
        const BODY: &str = r#"{"committed":true,"result":{"output":{"registered":true,"instance":{"instance_id":"instance-1","address":"http://lambda-service:8083","lease_expires_ms":123,"metadata":{}}}}}"#;
        let (url, request_rx, server) = serve_once("200 OK", BODY);
        let coordinator = Coordinator::new(
            Some(&url),
            Some("  node-secret  "),
            "  fiducia-lambda-service  ",
            Some("  http://lambda-service:8083  "),
            "instance-1",
            false,
        )
        .unwrap();
        coordinator.register_service().await.unwrap();
        let request = request_rx
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .to_ascii_lowercase();
        assert!(request.starts_with("put /v1/services/fiducia-lambda-service/instances/instance-1"));
        assert!(request.contains("x-fiducia-internal-auth: node-secret\r\n"));
        assert!(request.contains("x-fiducia-org-id: fiducia-lambda-service\r\n"));
        assert!(request.contains(r#""address":"http://lambda-service:8083""#));
        server.join().unwrap();
    }

    #[tokio::test]
    async fn registration_propagates_node_auth_failure() {
        let (url, _request_rx, server) =
            serve_once("401 Unauthorized", r#"{"error":"unauthorized"}"#);
        let coordinator = Coordinator::new(
            Some(&url),
            Some("wrong-secret"),
            "fiducia-lambda-service",
            Some("http://lambda-service:8083"),
            "instance-1",
            false,
        )
        .unwrap();
        assert!(coordinator.register_service().await.is_err());
        server.join().unwrap();
    }

    #[test]
    fn idempotency_authority_envelopes_are_parsed_strictly() {
        assert_eq!(
            parse_idempotency_claim(&json!({"result":{"output":{"claimed":true}}})),
            Ok(true)
        );
        assert_eq!(
            parse_idempotency_claim(&json!({"result":{"output":{"claimed":false}}})),
            Ok(false)
        );
        assert!(parse_idempotency_claim(&json!({"claimed":true})).is_err());
    }

    #[tokio::test]
    async fn run_lease_uses_holder_token_renewal_and_exact_release() {
        const ACQUIRED: &str = r#"{"committed":true,"result":{"output":{"acquired":true,"keys":["workflow/run/1"],"holder":"instance-1","fencing_token":9,"lease_expires_ms":1000,"revision":1}}}"#;
        const RENEWED: &str = r#"{"committed":true,"result":{"output":{"renewed":true,"keys":["workflow/run/1"],"holder":"instance-1","fencing_token":9,"lease_expires_ms":2000,"revision":2}}}"#;
        const RELEASED: &str = r#"{"committed":true,"result":{"output":{"released":true,"keys":["workflow/run/1"],"promoted":[],"revision":3}}}"#;
        let (url, request_rx, server) = serve_responses(vec![
            ("200 OK", ACQUIRED),
            ("200 OK", RENEWED),
            ("200 OK", RELEASED),
        ]);
        let coordinator = Coordinator::new(
            Some(&url),
            Some("node-secret"),
            "fiducia-lambda-service",
            Some("http://lambda-service:8083"),
            "instance-1",
            false,
        )
        .unwrap();
        let lease = coordinator
            .try_lease_run_with_ttl("1", 90)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(lease.holder, "instance-1");
        assert_eq!(lease.fencing_token, 9);
        assert_eq!(lease.lease_expires_ms, Some(1000));

        coordinator.renew_run_once(&lease).await.unwrap();
        coordinator.release_run(&lease).await.unwrap();

        let acquire = request_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(acquire.starts_with("POST /v1/locks/acquire "));
        let acquire_body = request_json(&acquire);
        assert_eq!(acquire_body["keys"], json!(["workflow/run/1"]));
        assert_eq!(acquire_body["holder"], "instance-1");
        assert_eq!(acquire_body["ttl_ms"], 90);
        assert_eq!(acquire_body["wait"], false);
        assert!(acquire_body["request_id"]
            .as_str()
            .is_some_and(|value| value.starts_with("fdc-attempt-")));

        let renew = request_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(renew.starts_with("POST /v1/locks/renew "));
        assert_eq!(
            request_json(&renew),
            json!({
                "keys": ["workflow/run/1"],
                "holder": "instance-1",
                "fencing_token": 9,
                "ttl_ms": 90
            })
        );

        let release = request_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        assert!(release.starts_with("POST /v1/locks/release "));
        assert_eq!(
            request_json(&release),
            json!({"holder": "instance-1", "fencing_token": 9})
        );
        server.join().unwrap();
    }

    #[tokio::test]
    async fn renewal_supervisor_fails_closed_when_fenced_authority_is_lost() {
        const ACQUIRED: &str = r#"{"committed":true,"result":{"output":{"acquired":true,"keys":["workflow/run/long"],"holder":"instance-1","fencing_token":17,"lease_expires_ms":1000,"revision":1}}}"#;
        const LOST: &str = r#"{"committed":true,"result":{"output":{"renewed":false,"reason":"not_holder","revision":2}}}"#;
        let (url, _request_rx, server) =
            serve_responses(vec![("200 OK", ACQUIRED), ("200 OK", LOST)]);
        let coordinator = Coordinator::new(
            Some(&url),
            Some("node-secret"),
            "fiducia-lambda-service",
            Some("http://lambda-service:8083"),
            "instance-1",
            false,
        )
        .unwrap();
        let lease = coordinator
            .try_lease_run_with_ttl("long", 30)
            .await
            .unwrap()
            .unwrap();
        let result = tokio::time::timeout(
            Duration::from_millis(200),
            coordinator.maintain_run_lease(lease),
        )
        .await
        .expect("renewal must remain bounded");
        assert!(result.is_err());
        server.join().unwrap();
    }
}
