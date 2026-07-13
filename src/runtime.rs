//! Definition → shell-command resolution and the runtime allow-list. This is a
//! direct port of the pure helpers in `lambda_child_runner.erl`
//! (`command_for_definition`, `canonical_runtime`, `container_command`, the
//! `safe_*` validators, and the lightweight JSON field extractors).
//!
//! The JSON field extractors are deliberately regex-based, matching the Erlang
//! original: definitions are loaded straight from Postgres as opaque JSON text
//! and forwarded to the child unchanged, so we only ever need to peek at a few
//! top-level scalar fields (`runtime`, `containerized`, `reuseKey`, …) without a
//! full parse.

use regex::Regex;

/// Read an env var as a String, empty string when unset (Erlang `env_binary`).
pub fn env_binary(name: &str, default: &str) -> String {
    match std::env::var(name) {
        Ok(v) if !v.is_empty() => v,
        _ => default.to_string(),
    }
}

pub fn env_bool(name: &str, default: bool) -> bool {
    match env_binary(name, "").as_str() {
        "true" | "1" => true,
        "false" | "0" => false,
        _ => default,
    }
}

/// Comma-separated env list, each token canonicalised as a runtime.
pub fn csv_env(name: &str, default: &str) -> Vec<String> {
    env_binary(name, default)
        .split(',')
        .map(|t| t.trim())
        .filter(|t| !t.is_empty())
        .map(canonical_runtime)
        .collect()
}

/// Normalise the many runtime aliases down to the canonical token set
/// (`canonical_runtime/1`). An empty runtime defaults to `nodejs`.
pub fn canonical_runtime(runtime: &str) -> String {
    match runtime {
        "javascript" | "typescript" | "node" | "nodejs" => "nodejs",
        "python" | "python3" => "python3",
        "shell" | "bash" => "bash",
        "ruby" => "ruby",
        "go" | "golang" => "golang",
        "dart" => "dart",
        "erl" | "erlang" => "erlang",
        "ex" | "elixir" => "elixir",
        "jvm" | "java" => "java",
        "" => "nodejs",
        other => other,
    }
    .to_string()
}

pub fn supported_runtime(runtime: &str) -> bool {
    matches!(
        runtime,
        "nodejs" | "python3" | "ruby" | "bash" | "golang" | "dart" | "erlang" | "elixir" | "java"
    )
}

// ─── JSON field extractors (regex, top-level scalars only) ──────────────────

pub fn json_string_field(json: &str, field: &str) -> String {
    let pat = format!(r#""{}"\s*:\s*"((?:\\.|[^"])*)""#, regex::escape(field));
    Regex::new(&pat)
        .ok()
        .and_then(|re| re.captures(json))
        .and_then(|c| c.get(1))
        .map(|m| json_unescape(m.as_str()))
        .unwrap_or_default()
}

pub fn json_bool_field(json: &str, field: &str, default: bool) -> bool {
    let pat = format!(r#""{}"\s*:\s*(true|false)"#, regex::escape(field));
    match Regex::new(&pat).ok().and_then(|re| re.captures(json)) {
        Some(c) => c.get(1).map(|m| m.as_str() == "true").unwrap_or(default),
        None => default,
    }
}

pub fn json_int_field(json: &str, field: &str, default: i64) -> i64 {
    let pat = format!(r#""{}"\s*:\s*([0-9]+)"#, regex::escape(field));
    Regex::new(&pat)
        .ok()
        .and_then(|re| re.captures(json))
        .and_then(|c| c.get(1))
        .and_then(|m| m.as_str().parse().ok())
        .unwrap_or(default)
}

fn json_unescape(v: &str) -> String {
    v.replace("\\\"", "\"").replace("\\\\", "\\")
}

pub fn json_escape(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}

// ─── validators ─────────────────────────────────────────────────────────────

fn matches_full(re: &str, value: &str) -> bool {
    Regex::new(re).map(|r| r.is_match(value)).unwrap_or(false)
}

pub fn safe_reuse_key(k: &str) -> bool {
    matches_full(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,119}$", k)
}

pub fn safe_container_image(i: &str) -> bool {
    matches_full(r"^[A-Za-z0-9][A-Za-z0-9._:/@-]{0,511}$", i)
}

pub fn safe_pool_language(l: &str) -> bool {
    matches_full(r"^[A-Za-z0-9_-]{1,64}$", l)
}

pub fn safe_pool_slug(s: &str) -> bool {
    matches_full(r"^[A-Za-z0-9._:-]{1,119}$", s)
}

pub fn safe_nats_subject(s: &str) -> bool {
    matches_full(r"^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$", s)
}

/// UUID | slug | invalid — the identifier taxonomy from `identifier_kind/1`.
#[derive(Debug, PartialEq, Eq)]
pub enum IdentifierKind {
    Uuid,
    Slug,
    Invalid,
}

pub fn identifier_kind(id: &str) -> IdentifierKind {
    if matches_full(
        r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$",
        id,
    ) {
        IdentifierKind::Uuid
    } else if matches_full(r"^[a-z0-9][a-z0-9-]{1,118}[a-z0-9]$", id) {
        IdentifierKind::Slug
    } else {
        IdentifierKind::Invalid
    }
}

fn max_i(v: i64, min: i64) -> i64 {
    if v >= min {
        v
    } else {
        min
    }
}

pub fn idle_ms_from_definition(def: &str, fallback: u64) -> u64 {
    let seconds = json_int_field(def, "idleTimeoutSeconds", 0);
    if seconds > 0 {
        max_i(seconds * 1000, 1000) as u64
    } else {
        max_i(fallback as i64, 1000) as u64
    }
}

pub fn timeout_ms_from_definition(def: &str, fallback: u64) -> u64 {
    let t = json_int_field(def, "maxRunMs", 0);
    if t > 0 {
        max_i(t, 1000) as u64
    } else {
        max_i(fallback as i64, 1000) as u64
    }
}

pub fn runtime_from_definition(def: &str) -> String {
    canonical_runtime(&json_string_field(def, "runtime"))
}

/// Resolve the reuse key for a definition (`worker_key/4`).
pub fn worker_key(
    identifier: &str,
    def: &str,
    runtime: &str,
    containerized: bool,
) -> Result<String, String> {
    let reuse = json_string_field(def, "reuseKey");
    if reuse.is_empty() {
        Ok(if containerized {
            format!("pool:container:{runtime}")
        } else {
            format!("pool:host:{runtime}")
        })
    } else if safe_reuse_key(&reuse) {
        Ok(format!("function:{identifier}:{reuse}"))
    } else {
        Err("reuseKey contains unsupported characters".into())
    }
}

pub fn check_worker_key(runtime: &str, containerized: bool) -> String {
    if containerized {
        format!("check:container:{runtime}")
    } else {
        format!("check:host:{runtime}")
    }
}

/// Single-quote a shell word, escaping embedded quotes (`shell_word/1`).
fn shell_word(v: &str) -> String {
    format!("'{}'", v.replace('\'', "'\"'\"'"))
}

fn host_runtime_allowed(runtime: &str) -> bool {
    csv_env("LAMBDA_ALLOW_HOST_RUNTIMES", "nodejs")
        .iter()
        .any(|r| r == runtime)
}

pub fn host_command(runtime: &str) -> Result<String, String> {
    let (env, default): (&str, &str) = match runtime {
        "nodejs" => ("LAMBDA_NODEJS_HOST_COMMAND", crate::config::DEFAULT_NODEJS_HOST_COMMAND),
        "python3" => (
            "LAMBDA_PYTHON3_HOST_COMMAND",
            "env -i PATH=\"$PATH\" PYTHONUNBUFFERED=1 python3 child-runtimes/python-function-runner.py",
        ),
        "ruby" => (
            "LAMBDA_RUBY_HOST_COMMAND",
            "env -i PATH=\"$PATH\" ruby child-runtimes/ruby-function-runner.rb",
        ),
        "bash" => (
            "LAMBDA_BASH_HOST_COMMAND",
            "env -i PATH=\"$PATH\" NODE_NO_WARNINGS=1 node --permission --allow-net --allow-child-process child-runtimes/bash-function-runner.mjs",
        ),
        other => return Err(format!("unsupported lambda runtime: {other}")),
    };
    Ok(env_binary(env, default))
}

fn default_container_image(runtime: &str) -> String {
    let (env, default) = match runtime {
        "nodejs" => ("LAMBDA_NODEJS_CONTAINER_IMAGE", "docker.io/library/dd-lambda-nodejs-runtime:dev"),
        "python3" => ("LAMBDA_PYTHON3_CONTAINER_IMAGE", "docker.io/library/dd-lambda-python3-runtime:dev"),
        "ruby" => ("LAMBDA_RUBY_CONTAINER_IMAGE", "docker.io/library/dd-lambda-ruby-runtime:dev"),
        "bash" => ("LAMBDA_BASH_CONTAINER_IMAGE", "docker.io/library/dd-lambda-bash-runtime:dev"),
        "golang" => ("LAMBDA_GOLANG_CONTAINER_IMAGE", "docker.io/library/dd-lambda-golang-runtime:dev"),
        "dart" => ("LAMBDA_DART_CONTAINER_IMAGE", "docker.io/library/dd-lambda-dart-runtime:dev"),
        "erlang" => ("LAMBDA_ERLANG_CONTAINER_IMAGE", "docker.io/library/dd-lambda-erlang-runtime:dev"),
        "elixir" => ("LAMBDA_ELIXIR_CONTAINER_IMAGE", "docker.io/library/dd-lambda-elixir-runtime:dev"),
        "java" => ("LAMBDA_JAVA_CONTAINER_IMAGE", "docker.io/library/dd-lambda-java-runtime:dev"),
        _ => return String::new(),
    };
    env_binary(env, default)
}

fn safe_timeout_value(v: &str) -> Option<String> {
    if matches_full(r"^[0-9]{1,5}$", v) {
        Some(v.to_string())
    } else {
        None
    }
}

fn wrap_with_timeout(seconds: &str, command: String) -> String {
    match safe_timeout_value(seconds) {
        Some(s) => format!("timeout --kill-after=10 {s} {command}"),
        None => command,
    }
}

fn docker_compatible_run_args(network: &str, memory: &str, cpus: &str, image: &str) -> String {
    format!(
        " run --rm -i --pull=never --read-only --tmpfs /tmp:rw,noexec,nosuid,size=16m --network {} --user 10001:10001 --cap-drop ALL --security-opt no-new-privileges --pids-limit 64 --ulimit nofile=64:64 --memory {} --cpus {} {}",
        shell_word(network),
        shell_word(memory),
        shell_word(cpus),
        shell_word(image),
    )
}

/// Build the containerized invocation command (`container_command/2`). Only the
/// nerdctl / docker / podman runners are ported; `ctr` follows the same shape.
fn container_command(runtime: &str, def: &str) -> Result<String, String> {
    let build_status = json_string_field(def, "containerBuildStatus");
    let image0 = if build_status == "built" {
        json_string_field(def, "containerImage")
    } else {
        String::new()
    };
    let image = if image0.is_empty() {
        default_container_image(runtime)
    } else {
        image0
    };
    if !safe_container_image(&image) {
        return Err("containerImage contains unsupported characters".into());
    }
    let namespace = env_binary("LAMBDA_CONTAINER_NAMESPACE", "dd-lambda");
    let network = env_binary("LAMBDA_CONTAINER_NETWORK", "bridge");
    let memory = env_binary("LAMBDA_CONTAINER_MEMORY", "256m");
    let cpus = env_binary("LAMBDA_CONTAINER_CPUS", "0.50");
    let timeout_secs = env_binary("LAMBDA_CONTAINER_INVOKE_TIMEOUT_SECONDS", "120");
    let runner = env_binary("LAMBDA_CONTAINER_RUNNER", "nerdctl");
    let args = docker_compatible_run_args(&network, &memory, &cpus, &image);
    let cmd = match runner.as_str() {
        "docker" => {
            let bin = env_binary("LAMBDA_CONTAINER_DOCKER", "/usr/bin/docker");
            format!("{}{}", shell_word(&bin), args)
        }
        "podman" => {
            let bin = env_binary("LAMBDA_CONTAINER_PODMAN", "/usr/bin/podman");
            format!("{}{}", shell_word(&bin), args)
        }
        "nerdctl" => {
            let bin = env_binary("LAMBDA_CONTAINER_NERDCTL", "/usr/local/bin/nerdctl");
            format!("{} -n {}{}", shell_word(&bin), shell_word(&namespace), args)
        }
        other => {
            return Err(format!(
                "unsupported LAMBDA_CONTAINER_RUNNER (expected nerdctl|docker|podman): {other}"
            ))
        }
    };
    Ok(wrap_with_timeout(&timeout_secs, cmd))
}

/// Resolve the shell command for a definition (`command_for_definition/2`).
pub fn command_for_definition(fallback_command: &str, def: &str) -> Result<String, String> {
    let runtime = runtime_from_definition(def);
    if !supported_runtime(&runtime) {
        return Err(format!("unsupported lambda runtime: {runtime}"));
    }
    if json_bool_field(def, "containerized", false) {
        container_command(&runtime, def)
    } else if host_runtime_allowed(&runtime) {
        Ok(host_command(&runtime).unwrap_or_else(|_| fallback_command.to_string()))
    } else {
        Err(format!(
            "lambda runtime requires containerized=true for host execution: {runtime}"
        ))
    }
}

/// Build the JSON envelope handed to the child on stdin (`invocation_payload/3`).
pub fn invocation_payload(slug: &str, def: &str, request: &str) -> String {
    format!(
        "{{\"slug\":\"{}\",\"definition\":{},\"request\":{}}}",
        json_escape(slug),
        def,
        request
    )
}

pub fn check_payload(def: &str) -> String {
    let slug = {
        let s = json_string_field(def, "slug");
        if s.is_empty() {
            "lambda-check".to_string()
        } else {
            s
        }
    };
    format!(
        "{{\"slug\":\"{}\",\"definition\":{},\"request\":{{}},\"checkOnly\":true}}",
        json_escape(&slug),
        def
    )
}

/// Trim + default-to-`null` a request payload (`request_payload/1`).
pub fn normalize_request_payload(payload: &str) -> String {
    let t = payload.trim();
    if t.is_empty() {
        "null".to_string()
    } else {
        t.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn canonical_runtime_folds_aliases() {
        for a in ["javascript", "typescript", "node", "nodejs", ""] {
            assert_eq!(canonical_runtime(a), "nodejs", "alias {a}");
        }
        assert_eq!(canonical_runtime("python"), "python3");
        assert_eq!(canonical_runtime("go"), "golang");
        assert_eq!(canonical_runtime("rust"), "rust", "unknown passes through");
    }

    #[test]
    fn supported_runtime_gate() {
        assert!(supported_runtime("nodejs"));
        assert!(supported_runtime("java"));
        assert!(!supported_runtime("cobol"));
    }

    #[test]
    fn json_field_extractors_read_top_level_scalars() {
        let j = r#"{"runtime":"python3","containerized":true,"idleTimeoutSeconds":42,"reuseKey":"k-1","name":"a\"b"}"#;
        assert_eq!(json_string_field(j, "runtime"), "python3");
        assert!(json_bool_field(j, "containerized", false));
        assert!(!json_bool_field(j, "missing", false));
        assert_eq!(json_int_field(j, "idleTimeoutSeconds", 0), 42);
        assert_eq!(json_int_field(j, "missing", 7), 7);
        assert_eq!(json_string_field(j, "name"), "a\"b", "unescapes");
    }

    #[test]
    fn validators_reject_injection_shapes() {
        assert!(safe_reuse_key("fn:abc_1.2-3"));
        assert!(!safe_reuse_key("bad key")); // space
        assert!(!safe_nats_subject("a.b c")); // whitespace
        assert!(!safe_nats_subject("a.*.b")); // wildcard not publishable
        assert!(safe_nats_subject("dd.remote.container_pool.nodejs.requests"));
        assert!(!safe_container_image("img;rm -rf"));
        assert!(safe_container_image("docker.io/library/img:tag"));
    }

    #[test]
    fn identifier_kind_classifies() {
        assert_eq!(
            identifier_kind("11111111-1111-1111-1111-111111111111"),
            IdentifierKind::Uuid
        );
        assert_eq!(identifier_kind("my-func-1"), IdentifierKind::Slug);
        assert_eq!(identifier_kind("Bad Slug"), IdentifierKind::Invalid);
        assert_eq!(identifier_kind("' or 1=1"), IdentifierKind::Invalid);
    }

    #[test]
    fn worker_key_scopes_by_reuse_key_or_pool() {
        let def_pool = r#"{"runtime":"nodejs"}"#;
        assert_eq!(
            worker_key("id", def_pool, "nodejs", false).unwrap(),
            "pool:host:nodejs"
        );
        let def_reuse = r#"{"reuseKey":"warm-1"}"#;
        assert_eq!(
            worker_key("fn-a", def_reuse, "nodejs", false).unwrap(),
            "function:fn-a:warm-1"
        );
        let def_bad = r#"{"reuseKey":"bad key"}"#;
        assert!(worker_key("id", def_bad, "nodejs", false).is_err());
    }

    #[test]
    fn idle_and_timeout_prefer_definition_over_fallback() {
        let def = r#"{"idleTimeoutSeconds":5,"maxRunMs":9000}"#;
        assert_eq!(idle_ms_from_definition(def, 300_000), 5000);
        assert_eq!(timeout_ms_from_definition(def, 30_000), 9000);
        // Absent → clamp fallback to >= 1000.
        assert_eq!(idle_ms_from_definition("{}", 200), 1000);
        assert_eq!(timeout_ms_from_definition("{}", 30_000), 30_000);
    }

    #[test]
    fn command_for_definition_rejects_unsupported_and_non_host() {
        // Unknown runtime.
        assert!(command_for_definition("fallback", r#"{"runtime":"cobol"}"#).is_err());
        // Supported but host execution not allowed by default for python3.
        let err = command_for_definition("fallback", r#"{"runtime":"python3"}"#);
        assert!(err.is_err(), "python3 host needs allow-list");
        // nodejs host is allowed by default → falls back to the provided command.
        let ok = command_for_definition("FALLBACK", r#"{"runtime":"nodejs"}"#).unwrap();
        assert!(!ok.is_empty());
    }

    #[test]
    fn normalize_request_payload_defaults_to_null() {
        assert_eq!(normalize_request_payload("   "), "null");
        assert_eq!(normalize_request_payload(" {\"a\":1} "), "{\"a\":1}");
    }
}
