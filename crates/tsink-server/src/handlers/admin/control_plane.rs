use super::*;
use crate::managed_control_plane::ManagedControlPlaneMutationError;

fn admin_control_plane_mutation_error_response(
    conflict_code: &'static str,
    err: ManagedControlPlaneMutationError,
) -> HttpResponse {
    match err {
        ManagedControlPlaneMutationError::Rejected(detail) => {
            admin_control_plane_error_response(409, conflict_code, detail)
        }
        ManagedControlPlaneMutationError::Persistence(source) => {
            admin_control_plane_persistence_error_response(source)
        }
    }
}

fn admin_control_plane_persistence_error_response(source: tsink::TsinkError) -> HttpResponse {
    let detail = format!("managed control-plane state persistence failed: {source}");
    if matches!(
        &source,
        tsink::TsinkError::InsufficientDiskSpace { .. }
            | tsink::TsinkError::DiskQuotaExceeded { .. }
            | tsink::TsinkError::InsufficientCompactionHeadroom { .. }
    ) {
        return admin_control_plane_error_response(413, "write_disk_quota_exceeded", detail)
            .with_header(WRITE_ERROR_CODE_HEADER, "write_disk_quota_exceeded");
    }

    let error_code = match source {
        tsink::TsinkError::IoWithPath { .. }
        | tsink::TsinkError::Io(_)
        | tsink::TsinkError::Json(_)
        | tsink::TsinkError::DataCorruption(_) => "write_internal_io",
        _ => "write_internal",
    };
    with_indeterminate_backend_headers(
        admin_control_plane_error_response(500, error_code, detail)
            .with_header(WRITE_ERROR_CODE_HEADER, error_code),
    )
}

pub(crate) fn handle_admin_control_plane_state(
    managed_control_plane: Option<&ManagedControlPlane>,
) -> HttpResponse {
    let Some(managed_control_plane) = managed_control_plane else {
        return admin_control_plane_error_response(
            503,
            "control_plane_unavailable",
            "managed control-plane store is unavailable",
        );
    };
    json_response(
        200,
        &json!({
            "status": "success",
            "data": managed_control_plane.state_snapshot(),
        }),
    )
}

pub(crate) fn handle_admin_control_plane_audit(
    request: &HttpRequest,
    managed_control_plane: Option<&ManagedControlPlane>,
) -> HttpResponse {
    let Some(managed_control_plane) = managed_control_plane else {
        return admin_control_plane_error_response(
            503,
            "control_plane_unavailable",
            "managed control-plane store is unavailable",
        );
    };
    let limit = match request.param("limit") {
        Some(value) => match value.parse::<usize>() {
            Ok(value) if value > 0 => value,
            _ => {
                return admin_control_plane_error_response(
                    400,
                    "invalid_request",
                    "limit must be a positive integer",
                );
            }
        },
        None => 100,
    };
    let entries = managed_control_plane.query_audit(ManagedControlPlaneAuditFilter {
        limit,
        target_kind: request
            .param("target_kind")
            .or_else(|| request.param("targetKind")),
        target_id: request
            .param("target_id")
            .or_else(|| request.param("targetId")),
        operation: request.param("operation"),
    });
    json_response(
        200,
        &json!({
            "status": "success",
            "data": {
                "count": entries.len(),
                "entries": entries,
            }
        }),
    )
}

pub(crate) fn handle_admin_control_plane_deployment_provision(
    request: &HttpRequest,
    managed_control_plane: Option<&ManagedControlPlane>,
) -> HttpResponse {
    let Some(managed_control_plane) = managed_control_plane else {
        return admin_control_plane_error_response(
            503,
            "control_plane_unavailable",
            "managed control-plane store is unavailable",
        );
    };
    let payload = match parse_optional_json_body::<ManagedDeploymentProvisionRequest>(request) {
        Ok(payload) => payload.unwrap_or_default(),
        Err(err) => {
            return admin_control_plane_error_response(400, "invalid_request", err);
        }
    };
    match managed_control_plane.provision_deployment(managed_control_plane_actor(request), payload)
    {
        Ok(deployment) => json_response(
            200,
            &json!({
                "status": "success",
                "data": {
                    "deployment": deployment,
                }
            }),
        ),
        Err(err) => admin_control_plane_mutation_error_response("deployment_update_failed", err),
    }
}

pub(crate) fn handle_admin_control_plane_backup_policy(
    request: &HttpRequest,
    managed_control_plane: Option<&ManagedControlPlane>,
) -> HttpResponse {
    let Some(managed_control_plane) = managed_control_plane else {
        return admin_control_plane_error_response(
            503,
            "control_plane_unavailable",
            "managed control-plane store is unavailable",
        );
    };
    let payload = match parse_optional_json_body::<ManagedBackupPolicyApplyRequest>(request) {
        Ok(payload) => payload.unwrap_or_default(),
        Err(err) => {
            return admin_control_plane_error_response(400, "invalid_request", err);
        }
    };
    match managed_control_plane.apply_backup_policy(managed_control_plane_actor(request), payload) {
        Ok(deployment) => json_response(
            200,
            &json!({
                "status": "success",
                "data": {
                    "deployment": deployment,
                }
            }),
        ),
        Err(err) => admin_control_plane_mutation_error_response("backup_policy_failed", err),
    }
}

pub(crate) fn handle_admin_control_plane_backup_run(
    request: &HttpRequest,
    managed_control_plane: Option<&ManagedControlPlane>,
) -> HttpResponse {
    let Some(managed_control_plane) = managed_control_plane else {
        return admin_control_plane_error_response(
            503,
            "control_plane_unavailable",
            "managed control-plane store is unavailable",
        );
    };
    let payload = match parse_optional_json_body::<ManagedBackupRunRecordRequest>(request) {
        Ok(Some(payload)) => payload,
        Ok(None) => {
            return admin_control_plane_error_response(
                400,
                "invalid_request",
                "request body is required",
            );
        }
        Err(err) => {
            return admin_control_plane_error_response(400, "invalid_request", err);
        }
    };
    match managed_control_plane.record_backup_run(managed_control_plane_actor(request), payload) {
        Ok(deployment) => json_response(
            200,
            &json!({
                "status": "success",
                "data": {
                    "deployment": deployment,
                }
            }),
        ),
        Err(err) => admin_control_plane_mutation_error_response("backup_run_failed", err),
    }
}

pub(crate) fn handle_admin_control_plane_maintenance(
    request: &HttpRequest,
    managed_control_plane: Option<&ManagedControlPlane>,
) -> HttpResponse {
    let Some(managed_control_plane) = managed_control_plane else {
        return admin_control_plane_error_response(
            503,
            "control_plane_unavailable",
            "managed control-plane store is unavailable",
        );
    };
    let payload = match parse_optional_json_body::<ManagedMaintenanceApplyRequest>(request) {
        Ok(payload) => payload.unwrap_or_default(),
        Err(err) => {
            return admin_control_plane_error_response(400, "invalid_request", err);
        }
    };
    match managed_control_plane.apply_maintenance(managed_control_plane_actor(request), payload) {
        Ok(deployment) => json_response(
            200,
            &json!({
                "status": "success",
                "data": {
                    "deployment": deployment,
                }
            }),
        ),
        Err(err) => admin_control_plane_mutation_error_response("maintenance_update_failed", err),
    }
}

pub(crate) fn handle_admin_control_plane_upgrade(
    request: &HttpRequest,
    managed_control_plane: Option<&ManagedControlPlane>,
) -> HttpResponse {
    let Some(managed_control_plane) = managed_control_plane else {
        return admin_control_plane_error_response(
            503,
            "control_plane_unavailable",
            "managed control-plane store is unavailable",
        );
    };
    let payload = match parse_optional_json_body::<ManagedUpgradeApplyRequest>(request) {
        Ok(payload) => payload.unwrap_or_default(),
        Err(err) => {
            return admin_control_plane_error_response(400, "invalid_request", err);
        }
    };
    match managed_control_plane.apply_upgrade(managed_control_plane_actor(request), payload) {
        Ok(deployment) => json_response(
            200,
            &json!({
                "status": "success",
                "data": {
                    "deployment": deployment,
                }
            }),
        ),
        Err(err) => admin_control_plane_mutation_error_response("upgrade_update_failed", err),
    }
}

pub(crate) fn handle_admin_control_plane_tenant_apply(
    request: &HttpRequest,
    managed_control_plane: Option<&ManagedControlPlane>,
) -> HttpResponse {
    let Some(managed_control_plane) = managed_control_plane else {
        return admin_control_plane_error_response(
            503,
            "control_plane_unavailable",
            "managed control-plane store is unavailable",
        );
    };
    let payload = match parse_optional_json_body::<ManagedTenantApplyRequest>(request) {
        Ok(payload) => payload.unwrap_or_default(),
        Err(err) => {
            return admin_control_plane_error_response(400, "invalid_request", err);
        }
    };
    match managed_control_plane.apply_tenant(managed_control_plane_actor(request), payload) {
        Ok(tenant) => json_response(
            200,
            &json!({
                "status": "success",
                "data": {
                    "tenant": tenant,
                }
            }),
        ),
        Err(err) => admin_control_plane_mutation_error_response("tenant_update_failed", err),
    }
}

pub(crate) fn handle_admin_control_plane_tenant_lifecycle(
    request: &HttpRequest,
    managed_control_plane: Option<&ManagedControlPlane>,
) -> HttpResponse {
    let Some(managed_control_plane) = managed_control_plane else {
        return admin_control_plane_error_response(
            503,
            "control_plane_unavailable",
            "managed control-plane store is unavailable",
        );
    };
    let payload = match parse_optional_json_body::<ManagedTenantLifecycleRequest>(request) {
        Ok(Some(payload)) => payload,
        Ok(None) => {
            return admin_control_plane_error_response(
                400,
                "invalid_request",
                "request body is required",
            );
        }
        Err(err) => {
            return admin_control_plane_error_response(400, "invalid_request", err);
        }
    };
    match managed_control_plane
        .apply_tenant_lifecycle(managed_control_plane_actor(request), payload)
    {
        Ok(tenant) => json_response(
            200,
            &json!({
                "status": "success",
                "data": {
                    "tenant": tenant,
                }
            }),
        ),
        Err(err) => admin_control_plane_mutation_error_response("tenant_lifecycle_failed", err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::fs;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn json_request(path: &str, body: JsonValue) -> HttpRequest {
        HttpRequest {
            method: "POST".to_string(),
            path: path.to_string(),
            headers: HashMap::from([("content-type".to_string(), "application/json".to_string())]),
            body: serde_json::to_vec(&body).expect("request body should encode"),
        }
    }

    fn response_header<'a>(response: &'a HttpResponse, name: &str) -> Option<&'a str> {
        response
            .headers
            .iter()
            .find(|(header_name, _)| header_name.eq_ignore_ascii_case(name))
            .map(|(_, value)| value.as_str())
    }

    fn deployment_request() -> HttpRequest {
        json_request(
            "/api/v1/admin/control-plane/deployments/provision",
            json!({
                "deploymentId": "prod-us-east",
                "displayName": "Production US East",
                "region": "us-east-1",
                "plan": "ha",
                "lifecycle": "ready"
            }),
        )
    }

    #[test]
    fn admin_control_plane_disk_quota_is_structured_413() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let initial_store = ManagedControlPlane::open(Some(temp_dir.path()))
            .expect("initial control-plane state should persist");
        drop(initial_store);
        let state_path = temp_dir
            .path()
            .join("managed-control-plane")
            .join("state.json");
        let initial_bytes = fs::metadata(&state_path)
            .expect("initial state should exist")
            .len();
        let budget = tsink::LocalDiskBudget::open(
            temp_dir.path(),
            tsink::LocalDiskLimits {
                max_bytes: Some(initial_bytes),
                ..tsink::LocalDiskLimits::default()
            },
        )
        .expect("disk budget should open");
        let store = ManagedControlPlane::open_with_disk_budget(
            Some(temp_dir.path()),
            Some(Arc::clone(&budget)),
        )
        .expect("control-plane state should reopen at the exact quota");

        let response =
            handle_admin_control_plane_deployment_provision(&deployment_request(), Some(&store));

        assert_eq!(response.status, 413);
        assert_eq!(
            response_header(&response, WRITE_ERROR_CODE_HEADER),
            Some("write_disk_quota_exceeded")
        );
        assert_eq!(response_header(&response, WRITE_PARTIAL_HEADER), None);
        assert_eq!(response_header(&response, WRITE_OUTCOME_HEADER), None);
        let body: JsonValue =
            serde_json::from_slice(&response.body).expect("response body should decode");
        assert_eq!(body["errorType"], "control_plane");
        assert_eq!(body["code"], "write_disk_quota_exceeded");
        assert!(store.state_snapshot().deployments.is_empty());
    }

    #[test]
    fn admin_control_plane_non_quota_persistence_failure_is_indeterminate_500() {
        let temp_dir = TempDir::new().expect("temp dir should create");
        let store = ManagedControlPlane::open(Some(temp_dir.path()))
            .expect("initial control-plane state should persist");
        let directory = temp_dir.path().join("managed-control-plane");
        fs::remove_file(directory.join("state.json")).expect("state file should be removable");
        fs::remove_dir(&directory).expect("state directory should be removable");
        fs::write(&directory, b"blocks state directory recreation")
            .expect("replacement file should create");

        let response =
            handle_admin_control_plane_deployment_provision(&deployment_request(), Some(&store));

        assert_eq!(response.status, 500);
        assert_eq!(
            response_header(&response, WRITE_ERROR_CODE_HEADER),
            Some("write_internal_io")
        );
        assert_eq!(
            response_header(&response, WRITE_PARTIAL_HEADER),
            Some("possible")
        );
        assert_eq!(
            response_header(&response, WRITE_OUTCOME_HEADER),
            Some("indeterminate_backend")
        );
        let body: JsonValue =
            serde_json::from_slice(&response.body).expect("response body should decode");
        assert_eq!(body["errorType"], "control_plane");
        assert_eq!(body["code"], "write_internal_io");
        assert!(store.state_snapshot().deployments.is_empty());
    }

    #[test]
    fn admin_control_plane_domain_conflict_retains_existing_409() {
        let store = ManagedControlPlane::open(None).expect("in-memory state should open");
        let request = json_request(
            "/api/v1/admin/control-plane/backups/policy",
            json!({
                "deploymentId": "missing",
                "enabled": true,
                "schedule": "0 * * * *",
                "retentionCopies": 7,
                "target": "s3://missing"
            }),
        );

        let response = handle_admin_control_plane_backup_policy(&request, Some(&store));

        assert_eq!(response.status, 409);
        assert_eq!(response_header(&response, WRITE_ERROR_CODE_HEADER), None);
        assert_eq!(response_header(&response, WRITE_OUTCOME_HEADER), None);
        let body: JsonValue =
            serde_json::from_slice(&response.body).expect("response body should decode");
        assert_eq!(body["code"], "backup_policy_failed");
    }
}
