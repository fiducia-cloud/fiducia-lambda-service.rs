//! Function-definition loading. Faithful to `load_function_definition/1` in
//! `lambda_child_runner.erl`: shell out to `psql` and read back a single
//! `jsonb_build_object(...)::text` row for the requested UUID or slug. Keeping
//! the psql path (rather than a compiled driver) means the service needs no
//! database features at build time and matches the operational contract of the
//! original runner exactly.

use tokio::process::Command;

use crate::config::Config;
use crate::runtime::{identifier_kind, IdentifierKind};

/// Load the JSON definition text for `identifier`, or a human-readable error.
pub async fn load_function_definition(config: &Config, identifier: &str) -> Result<String, String> {
    let kind = identifier_kind(identifier);
    if kind == IdentifierKind::Invalid {
        return Err("valid lambda function UUID or slug is required".into());
    }
    let database_url = config
        .database_url
        .as_deref()
        .ok_or("LAMBDA_DATABASE_URL is required")?;

    let sql = lambda_definition_sql(&kind, identifier);
    let out = run_psql(database_url, &sql).await?;
    let trimmed = out.trim();
    if trimmed.is_empty() {
        Err(format!("lambda function not found: {identifier}"))
    } else {
        Ok(trimmed.to_string())
    }
}

/// The wrapped `jsonb_build_object` projection over the shared
/// `lambda_functions` contract view (`lambda_definition_sql/2`). The inner
/// select mirrors `pg_defs.lambda_functions_select_sql`; the projection is the
/// exact field set the child runtimes expect.
fn lambda_definition_sql(kind: &IdentifierKind, identifier: &str) -> String {
    // Identifier is validated as a UUID or `[a-z0-9-]` slug before we get here,
    // so it cannot carry a quote; still, we only ever interpolate the validated
    // form.
    let where_clause = match kind {
        IdentifierKind::Uuid => format!("id = '{identifier}'"),
        IdentifierKind::Slug => format!("slug = '{identifier}'"),
        IdentifierKind::Invalid => unreachable!("filtered above"),
    };
    format!(
        "select jsonb_build_object(\
'id', id,\
'slug', slug,\
'functionBody', function_body,\
'runtime', runtime,\
'entryCommand', entry_command,\
'reuseKey', reuse_key,\
'idleTimeoutSeconds', idle_timeout_seconds,\
'maxRunMs', max_run_ms,\
'containerized', containerized,\
'containerImage', container_image,\
'containerBuildStatus', container_build_status,\
'containerBuildError', container_build_error,\
'containerBuiltAt', container_built_at,\
'status', status,\
'labels', labels_json::jsonb,\
'metaData', meta_data_json::jsonb\
)::text from lambda_functions as lambda_function_row where {where_clause} and is_soft_deleted = false limit 1"
    )
}

/// Run a read-only, single-statement psql query with a hard 5s cap and a 1 MiB
/// output ceiling (`run_psql/3` + `collect_port/4`).
async fn run_psql(database_url: &str, sql: &str) -> Result<String, String> {
    let child = Command::new("psql")
        .arg(database_url)
        .args(["-X", "-q", "-At", "-v", "ON_ERROR_STOP=1", "-c", sql])
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| format!("psql executable not found: {e}"))?;

    let out = match tokio::time::timeout(
        std::time::Duration::from_secs(5),
        child.wait_with_output(),
    )
    .await
    {
        Ok(Ok(o)) => o,
        Ok(Err(e)) => return Err(format!("psql failed: {e}")),
        Err(_) => return Err("lambda definition query timed out".into()),
    };

    if out.stdout.len() > 1_048_576 {
        return Err("lambda definition query exceeded byte limit".into());
    }
    if !out.status.success() {
        let combined = String::from_utf8_lossy(&out.stderr);
        return Err(format!(
            "psql exited with status {}: {}",
            out.status.code().unwrap_or(-1),
            combined.trim()
        ));
    }
    Ok(String::from_utf8_lossy(&out.stdout).to_string())
}
