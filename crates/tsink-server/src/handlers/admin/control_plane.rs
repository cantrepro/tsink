use super::*;

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
        Err(err) => admin_control_plane_error_response(409, "deployment_update_failed", err),
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
        Err(err) => admin_control_plane_error_response(409, "backup_policy_failed", err),
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
        Err(err) => admin_control_plane_error_response(409, "backup_run_failed", err),
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
        Err(err) => admin_control_plane_error_response(409, "maintenance_update_failed", err),
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
        Err(err) => admin_control_plane_error_response(409, "upgrade_update_failed", err),
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
        Err(err) => admin_control_plane_error_response(409, "tenant_update_failed", err),
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
        Err(err) => admin_control_plane_error_response(409, "tenant_lifecycle_failed", err),
    }
}
