use super::*;

pub(crate) fn handle_admin_rbac_state(rbac_registry: Option<&RbacRegistry>) -> HttpResponse {
    let state = match rbac_registry {
        Some(registry) => registry.state_snapshot(),
        None => rbac_disabled_state_snapshot(),
    };
    json_response(
        200,
        &json!({
            "status": "success",
            "data": state,
        }),
    )
}

pub(crate) fn handle_admin_secrets_state(
    rbac_registry: Option<&RbacRegistry>,
    security_manager: Option<&SecurityManager>,
) -> HttpResponse {
    json_response(
        200,
        &json!({
            "status": "success",
            "data": security_status_json(security_manager, rbac_registry),
        }),
    )
}

pub(crate) fn handle_admin_secrets_rotate(
    request: &HttpRequest,
    security_manager: Option<&SecurityManager>,
) -> HttpResponse {
    let Some(security_manager) = security_manager else {
        return text_response(503, "security runtime is not configured");
    };
    let payload = match parse_optional_json_body::<AdminSecretRotateRequest>(request) {
        Ok(Some(payload)) => payload,
        Ok(None) => return text_response(400, "missing JSON request body"),
        Err(err) => return text_response(400, &err),
    };
    let actor = derive_audit_actor(request);
    match security_manager.rotate(
        payload.target,
        payload.mode,
        payload.new_value,
        payload.overlap_seconds,
        Some(actor.id),
    ) {
        Ok(SecurityRotateResult {
            target,
            mode,
            issued_credential,
            state,
        }) => json_response(
            200,
            &json!({
                "status": "success",
                "data": {
                    "target": target,
                    "mode": mode,
                    "issuedCredential": issued_credential,
                    "state": state,
                }
            }),
        ),
        Err(err) => text_response(400, &err),
    }
}

pub(crate) fn handle_admin_rbac_audit(
    request: &HttpRequest,
    rbac_registry: Option<&RbacRegistry>,
) -> HttpResponse {
    let limit = match request
        .query_param("limit")
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    {
        Some(value) => match value.parse::<usize>() {
            Ok(limit) => limit,
            Err(_) => return text_response(400, "invalid limit parameter"),
        },
        None => 100,
    };
    let entries = rbac_registry
        .map(|registry| registry.audit_snapshot(limit))
        .unwrap_or_default();
    json_response(
        200,
        &json!({
            "status": "success",
            "data": {
                "entries": entries,
            }
        }),
    )
}

pub(crate) fn handle_admin_rbac_reload(rbac_registry: Option<&RbacRegistry>) -> HttpResponse {
    let Some(rbac_registry) = rbac_registry else {
        return text_response(503, "rbac registry is not configured");
    };
    match rbac_registry.reload() {
        Ok(state) => json_response(
            200,
            &json!({
                "status": "success",
                "data": state,
            }),
        ),
        Err(err) => text_response(500, &format!("rbac reload failed: {err}")),
    }
}

pub(crate) fn handle_admin_rbac_service_account_create(
    request: &HttpRequest,
    rbac_registry: Option<&RbacRegistry>,
) -> HttpResponse {
    let Some(rbac_registry) = rbac_registry else {
        return text_response(503, "rbac registry is not configured");
    };
    let spec = match parse_optional_json_body::<rbac::ServiceAccountSpec>(request) {
        Ok(Some(spec)) => spec,
        Ok(None) => return text_response(400, "missing JSON request body"),
        Err(err) => return text_response(400, &err),
    };
    match rbac_registry.create_service_account(spec) {
        Ok(result) => json_response(
            200,
            &json!({
                "status": "success",
                "data": {
                    "serviceAccount": result.service_account,
                    "token": result.token,
                }
            }),
        ),
        Err(err) => text_response(400, &err),
    }
}

pub(crate) fn handle_admin_rbac_service_account_update(
    request: &HttpRequest,
    rbac_registry: Option<&RbacRegistry>,
) -> HttpResponse {
    let Some(rbac_registry) = rbac_registry else {
        return text_response(503, "rbac registry is not configured");
    };
    let spec = match parse_optional_json_body::<rbac::ServiceAccountSpec>(request) {
        Ok(Some(spec)) => spec,
        Ok(None) => return text_response(400, "missing JSON request body"),
        Err(err) => return text_response(400, &err),
    };
    match rbac_registry.update_service_account(spec) {
        Ok(service_account) => json_response(
            200,
            &json!({
                "status": "success",
                "data": {
                    "serviceAccount": service_account,
                }
            }),
        ),
        Err(err) => text_response(400, &err),
    }
}

pub(crate) fn handle_admin_rbac_service_account_rotate(
    request: &HttpRequest,
    rbac_registry: Option<&RbacRegistry>,
) -> HttpResponse {
    let Some(rbac_registry) = rbac_registry else {
        return text_response(503, "rbac registry is not configured");
    };
    let id = match parse_admin_service_account_id(request) {
        Ok(id) => id,
        Err(response) => return response,
    };
    match rbac_registry.rotate_service_account(&id) {
        Ok(result) => json_response(
            200,
            &json!({
                "status": "success",
                "data": {
                    "serviceAccount": result.service_account,
                    "token": result.token,
                }
            }),
        ),
        Err(err) => text_response(400, &err),
    }
}

pub(crate) fn handle_admin_rbac_service_account_disable(
    request: &HttpRequest,
    rbac_registry: Option<&RbacRegistry>,
) -> HttpResponse {
    handle_admin_rbac_service_account_disablement(request, rbac_registry, true)
}

pub(crate) fn handle_admin_rbac_service_account_enable(
    request: &HttpRequest,
    rbac_registry: Option<&RbacRegistry>,
) -> HttpResponse {
    handle_admin_rbac_service_account_disablement(request, rbac_registry, false)
}
