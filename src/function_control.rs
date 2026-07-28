//! Tenant-scoped function-definition control plane for cron jobs.
//!
//! Function source never enters fiducia-node's Raft log.  This module owns the
//! CRUD surface over the existing `lambda_functions` table and records the
//! customer organization in reserved metadata. All SQL text values are encoded
//! as hexadecimal before interpolation into the `psql` statement; the encoded
//! alphabet cannot terminate a SQL string literal.

use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use uuid::Uuid;

use crate::config::{Config, DEFAULT_BROWSER_HOST_COMMAND, DEFAULT_NODEJS_HOST_COMMAND};
use crate::definition::run_psql;
use crate::runtime;

pub const ORG_HEADER: &str = "x-fiducia-org-id";
pub const MAX_FUNCTION_BODY_BYTES: usize = 262_144;
const MAX_LIST_LIMIT: u16 = 10;
const ORG_META_KEY: &str = "fiduciaOrgId";
const LOGICAL_SLUG_META_KEY: &str = "fiduciaCustomerSlug";
const MAX_LABELS: usize = 32;
const MAX_METADATA_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FunctionDefinitionInput {
    pub slug: String,
    pub display_name: String,
    #[serde(default)]
    pub description: String,
    #[serde(default = "default_runtime")]
    pub runtime: String,
    pub function_body: String,
    pub reuse_key: Option<String>,
    pub idle_timeout_seconds: Option<u32>,
    pub max_run_ms: Option<u64>,
    pub containerized: Option<bool>,
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default)]
    pub meta_data: Map<String, Value>,
}

fn default_runtime() -> String {
    "nodejs".to_string()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FunctionDefinitionRecord {
    pub id: String,
    pub slug: String,
    pub display_name: String,
    pub description: String,
    pub runtime: String,
    pub entry_command: String,
    pub function_body: String,
    pub reuse_key: Option<String>,
    pub idle_timeout_seconds: u32,
    pub max_run_ms: u64,
    pub containerized: bool,
    pub container_image: Option<String>,
    pub container_build_status: String,
    pub status: String,
    pub labels: Vec<String>,
    pub meta_data: Map<String, Value>,
    pub created_at: Option<String>,
    pub updated_at: Option<String>,
}

#[derive(Debug, Clone)]
pub struct CleanFunctionDefinition {
    pub logical_slug: String,
    pub display_name: String,
    pub description: String,
    pub runtime: String,
    pub entry_command: String,
    pub function_body: String,
    pub reuse_key: Option<String>,
    pub idle_timeout_seconds: u32,
    pub max_run_ms: u64,
    pub containerized: bool,
    pub labels: Vec<String>,
    pub meta_data: Map<String, Value>,
}

#[derive(Debug, thiserror::Error)]
pub enum FunctionControlError {
    #[error("{0}")]
    Invalid(String),
    #[error("function not found")]
    NotFound,
    #[error("function definition changed while it was being checked")]
    Conflict,
    #[error("function definition store is unavailable")]
    Unavailable(String),
}

pub fn valid_org_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
        && !value.contains("..")
        && !value.contains("//")
        && value.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b':' | b'/' | b'-')
        })
}

pub fn validate_function_id(value: &str) -> Result<Uuid, FunctionControlError> {
    Uuid::parse_str(value)
        .map_err(|_| FunctionControlError::Invalid("valid function UUID is required".to_string()))
}

fn normalize_slug(input: &str) -> String {
    let mut slug = String::new();
    let mut previous_dash = false;
    for ch in input.trim().to_ascii_lowercase().chars() {
        if ch.is_ascii_alphanumeric() {
            slug.push(ch);
            previous_dash = false;
        } else if !previous_dash && !slug.is_empty() {
            slug.push('-');
            previous_dash = true;
        }
    }
    while slug.ends_with('-') {
        slug.pop();
    }
    slug
}

fn validate_text(
    field: &str,
    value: &str,
    min: usize,
    max: usize,
) -> Result<String, FunctionControlError> {
    let value = value.trim();
    let len = value.len();
    if !(min..=max).contains(&len) {
        return Err(FunctionControlError::Invalid(format!(
            "{field} must contain {min}-{max} bytes"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(FunctionControlError::Invalid(format!(
            "{field} must not contain control characters"
        )));
    }
    Ok(value.to_string())
}

fn validate_reuse_key(value: Option<&str>) -> Result<Option<String>, FunctionControlError> {
    let Some(value) = value.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    if !runtime::safe_reuse_key(value) {
        return Err(FunctionControlError::Invalid(
            "reuseKey contains unsupported characters".to_string(),
        ));
    }
    Ok(Some(value.to_string()))
}

fn managed_entry_command(runtime: &str) -> String {
    match runtime {
        "nodejs" => DEFAULT_NODEJS_HOST_COMMAND.to_string(),
        "playwright" | "puppeteer" => DEFAULT_BROWSER_HOST_COMMAND.to_string(),
        "python3" => {
            "env -i PATH=\"$PATH\" PYTHONUNBUFFERED=1 python3 child-runtimes/python-function-runner.py"
                .to_string()
        }
        "ruby" => "env -i PATH=\"$PATH\" ruby child-runtimes/ruby-function-runner.rb".to_string(),
        "bash" => "env -i PATH=\"$PATH\" NODE_NO_WARNINGS=1 node --permission --allow-net --allow-child-process child-runtimes/bash-function-runner.mjs".to_string(),
        other => format!("managed-container-runtime:{other}"),
    }
}

pub fn clean_input(
    input: FunctionDefinitionInput,
) -> Result<CleanFunctionDefinition, FunctionControlError> {
    let logical_slug = normalize_slug(&input.slug);
    if logical_slug.len() < 3 || logical_slug.len() > 80 {
        return Err(FunctionControlError::Invalid(
            "slug must normalize to 3-80 characters".to_string(),
        ));
    }
    let display_name = validate_text("displayName", &input.display_name, 1, 160)?;
    let description = if input.description.trim().is_empty() {
        String::new()
    } else {
        validate_text("description", &input.description, 1, 4_000)?
    };
    let function_body = input.function_body.trim().to_string();
    if function_body.is_empty() {
        return Err(FunctionControlError::Invalid(
            "functionBody is required".to_string(),
        ));
    }
    if function_body.len() > MAX_FUNCTION_BODY_BYTES {
        return Err(FunctionControlError::Invalid(
            "functionBody exceeds the 262144-byte limit".to_string(),
        ));
    }

    let runtime = runtime::canonical_runtime(input.runtime.trim());
    if runtime != "nodejs" {
        return Err(FunctionControlError::Invalid(
            "customer cron functions currently support the nodejs runtime".to_string(),
        ));
    }
    let containerized = input.containerized.unwrap_or(false);
    if containerized {
        return Err(FunctionControlError::Invalid(
            "customer cron functions must use the managed non-containerized runtime".to_string(),
        ));
    }
    let entry_command = managed_entry_command(&runtime);
    let reuse_key = validate_reuse_key(input.reuse_key.as_deref())?;
    let idle_timeout_seconds = input.idle_timeout_seconds.unwrap_or(300).clamp(1, 3_600);
    let max_run_ms = input.max_run_ms.unwrap_or(30_000).clamp(1_000, 300_000);

    if input.labels.len() > MAX_LABELS {
        return Err(FunctionControlError::Invalid(format!(
            "labels may contain at most {MAX_LABELS} entries"
        )));
    }
    let mut labels = Vec::with_capacity(input.labels.len());
    for label in input.labels {
        let label = validate_text("label", &label, 1, 64)?;
        if !labels.contains(&label) {
            labels.push(label);
        }
    }

    let mut meta_data = input.meta_data;
    for reserved in [ORG_META_KEY, LOGICAL_SLUG_META_KEY] {
        if meta_data.remove(reserved).is_some() {
            return Err(FunctionControlError::Invalid(format!(
                "metaData.{reserved} is reserved"
            )));
        }
    }
    let metadata_bytes = serde_json::to_vec(&meta_data)
        .map_err(|error| FunctionControlError::Invalid(error.to_string()))?;
    if metadata_bytes.len() > MAX_METADATA_BYTES {
        return Err(FunctionControlError::Invalid(
            "metaData exceeds the 16384-byte limit".to_string(),
        ));
    }

    // Validate the resolved execution policy before anything is persisted.
    let check_definition = json!({
        "runtime": runtime,
        "containerized": containerized,
        "functionBody": function_body,
        "reuseKey": reuse_key,
        "idleTimeoutSeconds": idle_timeout_seconds,
        "maxRunMs": max_run_ms,
    })
    .to_string();
    runtime::command_for_definition(DEFAULT_NODEJS_HOST_COMMAND, &check_definition)
        .map_err(FunctionControlError::Invalid)?;

    Ok(CleanFunctionDefinition {
        logical_slug,
        display_name,
        description,
        runtime,
        entry_command,
        function_body,
        reuse_key,
        idle_timeout_seconds,
        max_run_ms,
        containerized,
        labels,
        meta_data,
    })
}

pub(crate) fn sql_text(value: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut encoded = String::with_capacity(value.len().saturating_mul(2));
    for byte in value.bytes() {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    format!("convert_from(decode('{encoded}','hex'),'UTF8')")
}

fn sql_json(value: &Value) -> String {
    format!("({})::jsonb", sql_text(&value.to_string()))
}

fn sql_optional_text(value: Option<&str>) -> String {
    value.map(sql_text).unwrap_or_else(|| "null".to_string())
}

fn record_projection() -> &'static str {
    r#"jsonb_build_object(
        'id', id::text,
        'slug', coalesce(meta_data->>'fiduciaCustomerSlug', slug),
        'displayName', display_name,
        'description', description,
        'runtime', runtime,
        'entryCommand', entry_command,
        'functionBody', function_body,
        'reuseKey', reuse_key,
        'idleTimeoutSeconds', idle_timeout_seconds,
        'maxRunMs', max_run_ms,
        'containerized', containerized,
        'containerImage', container_image,
        'containerBuildStatus', container_build_status,
        'status', status,
        'labels', labels,
        'metaData', meta_data - 'fiduciaOrgId' - 'fiduciaCustomerSlug',
        'createdAt', to_char(created_at at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"'),
        'updatedAt', to_char(updated_at at time zone 'utc', 'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"')
    )"#
}

fn physical_slug(id: Uuid) -> String {
    format!("fiducia-{}", id.simple())
}

fn metadata_with_scope(clean: &CleanFunctionDefinition, org_id: &str) -> Value {
    let mut metadata = clean.meta_data.clone();
    metadata.insert(ORG_META_KEY.to_string(), Value::String(org_id.to_string()));
    metadata.insert(
        LOGICAL_SLUG_META_KEY.to_string(),
        Value::String(clean.logical_slug.clone()),
    );
    Value::Object(metadata)
}

fn database_url(config: &Config) -> Result<&str, FunctionControlError> {
    config
        .database_url
        .as_deref()
        .ok_or_else(|| FunctionControlError::Unavailable("LAMBDA_DATABASE_URL is unset".to_string()))
}

async fn parse_record(
    config: &Config,
    sql: String,
) -> Result<FunctionDefinitionRecord, FunctionControlError> {
    let output = run_psql(database_url(config)?, &sql, 2 * 1024 * 1024)
        .await
        .map_err(FunctionControlError::Unavailable)?;
    let output = output.trim();
    if output.is_empty() {
        return Err(FunctionControlError::NotFound);
    }
    serde_json::from_str(output).map_err(|error| {
        FunctionControlError::Unavailable(format!("invalid definition row: {error}"))
    })
}

pub async fn list(
    config: &Config,
    org_id: &str,
    limit: Option<u16>,
) -> Result<Vec<FunctionDefinitionRecord>, FunctionControlError> {
    let limit = limit.unwrap_or(MAX_LIST_LIMIT).clamp(1, MAX_LIST_LIMIT);
    let org = sql_text(org_id);
    let sql = format!(
        "select coalesce(jsonb_agg(item order by updated_at desc), '[]'::jsonb)::text \
         from (select {} as item, updated_at from lambda_functions \
         where is_soft_deleted = false and meta_data->>'{}' = {} \
         order by updated_at desc limit {}) scoped",
        record_projection(),
        ORG_META_KEY,
        org,
        limit
    );
    let output = run_psql(database_url(config)?, &sql, 4 * 1024 * 1024)
        .await
        .map_err(FunctionControlError::Unavailable)?;
    serde_json::from_str(output.trim()).map_err(|error| {
        FunctionControlError::Unavailable(format!("invalid function list response: {error}"))
    })
}

pub async fn get(
    config: &Config,
    org_id: &str,
    id: Uuid,
) -> Result<FunctionDefinitionRecord, FunctionControlError> {
    let sql = format!(
        "select {}::text from lambda_functions where id = '{}'::uuid \
         and is_soft_deleted = false and meta_data->>'{}' = {} limit 1",
        record_projection(),
        id,
        ORG_META_KEY,
        sql_text(org_id)
    );
    parse_record(config, sql).await
}

pub async fn create(
    config: &Config,
    org_id: &str,
    input: FunctionDefinitionInput,
) -> Result<FunctionDefinitionRecord, FunctionControlError> {
    let clean = clean_input(input)?;
    let id = Uuid::new_v4();
    let physical_slug = physical_slug(id);
    let metadata = metadata_with_scope(&clean, org_id);
    let labels = Value::Array(clean.labels.iter().cloned().map(Value::String).collect());
    let containerized = if clean.containerized { "true" } else { "false" };
    let build_status = if clean.containerized {
        "pending"
    } else {
        "not_requested"
    };
    let sql = format!(
        "with inserted as (insert into lambda_functions \
         (id, slug, display_name, description, runtime, entry_command, function_body, reuse_key, \
          idle_timeout_seconds, max_run_ms, containerized, container_build_status, status, labels, \
          meta_data, is_soft_deleted, created_at, updated_at) values \
         ('{}'::uuid, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, {}, 'draft', {}, {}, false, now(), now()) \
         returning *) select {}::text from inserted",
        id,
        sql_text(&physical_slug),
        sql_text(&clean.display_name),
        sql_text(&clean.description),
        sql_text(&clean.runtime),
        sql_text(&clean.entry_command),
        sql_text(&clean.function_body),
        sql_optional_text(clean.reuse_key.as_deref()),
        clean.idle_timeout_seconds,
        clean.max_run_ms,
        containerized,
        sql_text(build_status),
        sql_json(&labels),
        sql_json(&metadata),
        record_projection()
    );
    parse_record(config, sql).await
}

pub async fn update(
    config: &Config,
    org_id: &str,
    id: Uuid,
    input: FunctionDefinitionInput,
) -> Result<FunctionDefinitionRecord, FunctionControlError> {
    let clean = clean_input(input)?;
    let metadata = metadata_with_scope(&clean, org_id);
    let labels = Value::Array(clean.labels.iter().cloned().map(Value::String).collect());
    let containerized = if clean.containerized { "true" } else { "false" };
    let build_status = if clean.containerized {
        "pending"
    } else {
        "not_requested"
    };
    let sql = format!(
        "with updated as (update lambda_functions set display_name = {}, description = {}, \
         runtime = {}, entry_command = {}, function_body = {}, reuse_key = {}, \
         idle_timeout_seconds = {}, max_run_ms = {}, containerized = {}, \
         container_image = case when {} then container_image else null end, \
         container_build_status = {}, container_build_error = null, \
         status = 'draft', labels = {}, meta_data = {}, updated_at = now() \
         where id = '{}'::uuid and is_soft_deleted = false and meta_data->>'{}' = {} \
         returning *) select {}::text from updated",
        sql_text(&clean.display_name),
        sql_text(&clean.description),
        sql_text(&clean.runtime),
        sql_text(&clean.entry_command),
        sql_text(&clean.function_body),
        sql_optional_text(clean.reuse_key.as_deref()),
        clean.idle_timeout_seconds,
        clean.max_run_ms,
        containerized,
        containerized,
        sql_text(build_status),
        sql_json(&labels),
        sql_json(&metadata),
        id,
        ORG_META_KEY,
        sql_text(org_id),
        record_projection()
    );
    parse_record(config, sql).await
}

pub async fn delete(
    config: &Config,
    org_id: &str,
    id: Uuid,
) -> Result<(), FunctionControlError> {
    let sql = format!(
        "update lambda_functions set is_soft_deleted = true, status = 'archived', updated_at = now() \
         where id = '{}'::uuid and is_soft_deleted = false and meta_data->>'{}' = {} \
         returning id::text",
        id,
        ORG_META_KEY,
        sql_text(org_id)
    );
    let output = run_psql(database_url(config)?, &sql, 1024)
        .await
        .map_err(FunctionControlError::Unavailable)?;
    if output.trim().is_empty() {
        Err(FunctionControlError::NotFound)
    } else {
        Ok(())
    }
}

pub async fn pause(
    config: &Config,
    org_id: &str,
    id: Uuid,
) -> Result<FunctionDefinitionRecord, FunctionControlError> {
    let sql = format!(
        "with updated as (update lambda_functions set status = 'paused', updated_at = now() \
         where id = '{}'::uuid and is_soft_deleted = false and meta_data->>'{}' = {} \
         returning *) select {}::text from updated",
        id,
        ORG_META_KEY,
        sql_text(org_id),
        record_projection()
    );
    parse_record(config, sql).await
}

pub async fn activate_checked(
    config: &Config,
    org_id: &str,
    record: &FunctionDefinitionRecord,
) -> Result<FunctionDefinitionRecord, FunctionControlError> {
    let mut metadata = record.meta_data.clone();
    metadata.insert(ORG_META_KEY.to_string(), Value::String(org_id.to_string()));
    metadata.insert(
        LOGICAL_SLUG_META_KEY.to_string(),
        Value::String(record.slug.clone()),
    );
    let sql = format!(
        "with updated as (update lambda_functions set status = 'active', meta_data = {}, updated_at = now() \
         where id = '{}'::uuid and is_soft_deleted = false and meta_data->>'{}' = {} \
         and function_body = {} and runtime = {} and containerized = {} and max_run_ms = {} \
         returning *) select {}::text from updated",
        sql_json(&Value::Object(metadata)),
        record.id,
        ORG_META_KEY,
        sql_text(org_id),
        sql_text(&record.function_body),
        sql_text(&record.runtime),
        if record.containerized { "true" } else { "false" },
        record.max_run_ms,
        record_projection()
    );
    match parse_record(config, sql).await {
        Err(FunctionControlError::NotFound) => Err(FunctionControlError::Conflict),
        result => result,
    }
}

pub fn invocation_definition(record: &FunctionDefinitionRecord) -> Value {
    json!({
        "id": record.id,
        "slug": record.slug,
        "displayName": record.display_name,
        "description": record.description,
        "runtime": record.runtime,
        "entryCommand": record.entry_command,
        "functionBody": record.function_body,
        "reuseKey": record.reuse_key,
        "idleTimeoutSeconds": record.idle_timeout_seconds,
        "maxRunMs": record.max_run_ms,
        "containerized": record.containerized,
        "containerImage": record.container_image,
        "containerBuildStatus": record.container_build_status,
        "status": record.status,
        "labels": record.labels,
        "metaData": record.meta_data,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn valid_input() -> FunctionDefinitionInput {
        FunctionDefinitionInput {
            slug: "Hourly Billing".to_string(),
            display_name: "Hourly billing".to_string(),
            description: "Customer-owned cron function".to_string(),
            runtime: "javascript".to_string(),
            function_body: "export default async () => ({ ok: true });".to_string(),
            reuse_key: Some("billing-worker".to_string()),
            idle_timeout_seconds: Some(60),
            max_run_ms: Some(5_000),
            containerized: Some(false),
            labels: vec!["billing".to_string(), "cron".to_string()],
            meta_data: Map::new(),
        }
    }

    #[test]
    fn input_is_normalized_and_execution_policy_is_validated() {
        let clean = clean_input(valid_input()).unwrap();
        assert_eq!(clean.logical_slug, "hourly-billing");
        assert_eq!(clean.runtime, "nodejs");
        assert_eq!(clean.max_run_ms, 5_000);
        assert!(!clean.containerized);
    }

    #[test]
    fn unsupported_runtime_and_reserved_metadata_are_rejected() {
        let mut unsupported = valid_input();
        unsupported.runtime = "cobol".to_string();
        assert!(matches!(
            clean_input(unsupported),
            Err(FunctionControlError::Invalid(_))
        ));

        let mut reserved = valid_input();
        reserved
            .meta_data
            .insert(ORG_META_KEY.to_string(), json!("other-org"));
        assert!(matches!(
            clean_input(reserved),
            Err(FunctionControlError::Invalid(_))
        ));
    }

    #[test]
    fn sql_text_never_embeds_untrusted_plaintext() {
        let malicious = "x'); drop table lambda_functions; --";
        let expression = sql_text(malicious);
        assert!(!expression.contains(malicious));
        assert!(!expression.contains("drop table"));
        assert!(expression.starts_with("convert_from(decode('"));
    }

    #[test]
    fn tenant_and_function_identifiers_are_strict() {
        assert!(valid_org_id("00000000-0000-4000-8000-000000000001"));
        assert!(!valid_org_id("bad org"));
        assert!(!valid_org_id("../org"));
        assert!(validate_function_id("00000000-0000-4000-8000-000000000001").is_ok());
        assert!(validate_function_id("shared-slug").is_err());
    }

}
