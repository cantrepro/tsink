use crate::http::{text_response, HttpResponse};
use crate::rbac::{self, RbacAction, RbacPermission, RbacRegistry, RbacResource};
use crate::security::SecurityManager;
use crate::tenant;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestScope {
    Probe,
    Public,
    Admin,
    Internal,
}

pub(super) fn authorize_request_scope(
    request: &mut crate::http::HttpRequest,
    security_manager: Option<&SecurityManager>,
    public_auth_token: Option<&str>,
    admin_auth_token: Option<&str>,
    admin_api_enabled: bool,
    tenant_registry: Option<&tenant::TenantRegistry>,
    rbac_registry: Option<&RbacRegistry>,
) -> Result<(), HttpResponse> {
    if let Some(registry) = rbac_registry {
        match classify_request_scope(request) {
            RequestScope::Probe => return Ok(()),
            RequestScope::Internal => return Ok(()),
            _ => {}
        }
        let Some(permission) = request_permission_for(request, admin_api_enabled)? else {
            return Ok(());
        };
        match registry.authorize(extract_bearer_token(request), &permission) {
            Ok(authorized) => {
                request.headers.insert(
                    rbac::RBAC_AUTH_VERIFIED_HEADER.to_string(),
                    "true".to_string(),
                );
                request.headers.insert(
                    rbac::RBAC_AUTH_PRINCIPAL_ID_HEADER.to_string(),
                    authorized.principal_id,
                );
                request
                    .headers
                    .insert(rbac::RBAC_AUTH_ROLE_HEADER.to_string(), authorized.role);
                request.headers.insert(
                    rbac::RBAC_AUTH_METHOD_HEADER.to_string(),
                    authorized.auth_method,
                );
                if let Some(provider) = authorized.provider {
                    request
                        .headers
                        .insert(rbac::RBAC_AUTH_PROVIDER_HEADER.to_string(), provider);
                }
                if let Some(subject) = authorized.subject {
                    request
                        .headers
                        .insert(rbac::RBAC_AUTH_SUBJECT_HEADER.to_string(), subject);
                }
                return Ok(());
            }
            Err(err) => {
                return Err(match err.status() {
                    401 => unauthorized_auth_response(err.code()),
                    403 => forbidden_auth_response(err.code()),
                    _ => text_response(err.status(), err.code()),
                });
            }
        }
    }

    match classify_request_scope(request) {
        RequestScope::Probe => Ok(()),
        RequestScope::Internal => Ok(()),
        RequestScope::Public => {
            if tenant_registry.is_some() && tenant::public_request_access(request).is_some() {
                return Ok(());
            }
            if let Some(manager) = security_manager {
                if manager.public_auth_configured()
                    && !manager.public_auth_matches(extract_bearer_token(request))
                {
                    let provided = extract_bearer_token(request)
                        .ok_or_else(|| unauthorized_auth_response("auth_token_missing"))?;
                    if !manager.public_auth_matches(Some(provided)) {
                        return Err(unauthorized_auth_response("auth_token_invalid"));
                    }
                }
                return Ok(());
            }
            if let Some(token) = public_auth_token {
                let provided = extract_bearer_token(request)
                    .ok_or_else(|| unauthorized_auth_response("auth_token_missing"))?;
                if provided != token {
                    return Err(unauthorized_auth_response("auth_token_invalid"));
                }
            }
            Ok(())
        }
        RequestScope::Admin => {
            if !admin_api_enabled {
                return Ok(());
            }
            if let Some(manager) = security_manager {
                let provided = extract_bearer_token(request)
                    .ok_or_else(|| unauthorized_auth_response("admin_auth_token_missing"))?;
                if manager.admin_secret_configured() {
                    if manager.admin_auth_matches(Some(provided)) {
                        return Ok(());
                    }
                    if manager.public_auth_matches(Some(provided)) {
                        return Err(forbidden_auth_response("auth_scope_denied"));
                    }
                    return Err(unauthorized_auth_response("admin_auth_token_invalid"));
                }
                if manager.public_auth_configured() {
                    if manager.public_auth_matches(Some(provided)) {
                        return Ok(());
                    }
                    return Err(unauthorized_auth_response("admin_auth_token_invalid"));
                }
            }
            let expected_admin = admin_auth_token
                .or(public_auth_token)
                .ok_or_else(|| unauthorized_auth_response("admin_auth_token_missing"))?;
            let provided = extract_bearer_token(request)
                .ok_or_else(|| unauthorized_auth_response("admin_auth_token_missing"))?;
            if provided == expected_admin {
                return Ok(());
            }
            if admin_auth_token.is_some()
                && public_auth_token.is_some_and(|token| token == provided)
            {
                return Err(forbidden_auth_response("auth_scope_denied"));
            }
            Err(unauthorized_auth_response("admin_auth_token_invalid"))
        }
    }
}

pub(super) fn extract_bearer_token(request: &crate::http::HttpRequest) -> Option<&str> {
    let authorization = request.header("authorization")?;
    let (scheme, token) = authorization.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let token = token.trim();
    (!token.is_empty()).then_some(token)
}

fn classify_request_scope(request: &crate::http::HttpRequest) -> RequestScope {
    let path = request.path_without_query();
    if path == "/healthz" || path == "/ready" {
        return RequestScope::Probe;
    }
    if path.starts_with("/internal/v1/") {
        return RequestScope::Internal;
    }
    if path.starts_with("/api/v1/admin") {
        return RequestScope::Admin;
    }
    RequestScope::Public
}

fn unauthorized_auth_response(code: &str) -> HttpResponse {
    HttpResponse::new(401, "unauthorized")
        .with_header("Content-Type", "text/plain")
        .with_header("WWW-Authenticate", "Bearer")
        .with_header("X-Tsink-Auth-Error-Code", code)
}

fn forbidden_auth_response(code: &str) -> HttpResponse {
    HttpResponse::new(403, "forbidden")
        .with_header("Content-Type", "text/plain")
        .with_header("X-Tsink-Auth-Error-Code", code)
}

fn request_permission_for(
    request: &crate::http::HttpRequest,
    admin_api_enabled: bool,
) -> Result<Option<RbacPermission>, HttpResponse> {
    let path = request.path_without_query();
    let tenant_id = || {
        tenant::tenant_id_for_request(request)
            .map_err(|err| text_response(400, &err))
            .map(RbacResource::tenant)
    };
    let permission = match (request.method.as_str(), path) {
        ("GET", "/metrics") => Some(RbacPermission::new(
            RbacAction::Read,
            RbacResource::system("metrics"),
        )),
        ("GET" | "POST", "/api/v1/query")
        | ("GET" | "POST", "/api/v1/query_range")
        | ("GET", "/api/v1/series")
        | ("GET", "/api/v1/labels")
        | ("GET", "/api/v1/metadata")
        | ("GET" | "POST", "/api/v1/query_exemplars")
        | ("POST", "/api/v1/read")
        | ("GET", "/api/v1/status/tsdb") => {
            Some(RbacPermission::new(RbacAction::Read, tenant_id()?))
        }
        ("POST", "/api/v1/write")
        | ("POST", "/api/v1/import/prometheus")
        | ("POST", "/v1/metrics")
        | ("POST", "/write")
        | ("POST", "/api/v2/write") => Some(RbacPermission::new(RbacAction::Write, tenant_id()?)),
        ("GET", p) if p.starts_with("/api/v1/label/") && p.ends_with("/values") => {
            Some(RbacPermission::new(RbacAction::Read, tenant_id()?))
        }
        _ if !admin_api_enabled => None,
        ("GET", "/api/v1/admin/rbac/state") | ("GET", "/api/v1/admin/rbac/audit") => Some(
            RbacPermission::new(RbacAction::Read, RbacResource::admin("rbac")),
        ),
        ("GET", "/api/v1/admin/secrets/state") => Some(RbacPermission::new(
            RbacAction::Read,
            RbacResource::admin("security"),
        )),
        ("POST", "/api/v1/admin/rbac/reload") => Some(RbacPermission::new(
            RbacAction::Write,
            RbacResource::admin("rbac"),
        )),
        ("POST", "/api/v1/admin/secrets/rotate") => Some(RbacPermission::new(
            RbacAction::Write,
            RbacResource::admin("security"),
        )),
        ("POST", "/api/v1/admin/rbac/service_accounts/create")
        | ("POST", "/api/v1/admin/rbac/service_accounts/update")
        | ("POST", "/api/v1/admin/rbac/service_accounts/rotate")
        | ("POST", "/api/v1/admin/rbac/service_accounts/disable")
        | ("POST", "/api/v1/admin/rbac/service_accounts/enable") => Some(RbacPermission::new(
            RbacAction::Write,
            RbacResource::admin("rbac"),
        )),
        ("GET", "/api/v1/admin/rules/status") => Some(RbacPermission::new(
            RbacAction::Read,
            RbacResource::admin("rules"),
        )),
        ("GET", "/api/v1/admin/usage/report") | ("GET", "/api/v1/admin/usage/export") => Some(
            RbacPermission::new(RbacAction::Read, RbacResource::admin("usage")),
        ),
        ("GET", "/api/v1/admin/control-plane/state")
        | ("GET", "/api/v1/admin/control-plane/audit") => Some(RbacPermission::new(
            RbacAction::Read,
            RbacResource::admin("control_plane"),
        )),
        ("GET", "/api/v1/admin/support_bundle") => Some(RbacPermission::new(
            RbacAction::Read,
            RbacResource::admin("support"),
        )),
        ("POST", "/api/v1/admin/rules/apply") | ("POST", "/api/v1/admin/rules/run") => Some(
            RbacPermission::new(RbacAction::Write, RbacResource::admin("rules")),
        ),
        ("POST", "/api/v1/admin/usage/reconcile") => Some(RbacPermission::new(
            RbacAction::Write,
            RbacResource::admin("usage"),
        )),
        ("POST", "/api/v1/admin/control-plane/deployments/provision")
        | ("POST", "/api/v1/admin/control-plane/deployments/backup-policy")
        | ("POST", "/api/v1/admin/control-plane/deployments/backup-run")
        | ("POST", "/api/v1/admin/control-plane/deployments/maintenance")
        | ("POST", "/api/v1/admin/control-plane/deployments/upgrade")
        | ("POST", "/api/v1/admin/control-plane/tenants/apply")
        | ("POST", "/api/v1/admin/control-plane/tenants/lifecycle") => Some(RbacPermission::new(
            RbacAction::Write,
            RbacResource::admin("control_plane"),
        )),
        ("GET", "/api/v1/admin/rollups/status") => Some(RbacPermission::new(
            RbacAction::Read,
            RbacResource::admin("rollups"),
        )),
        ("POST", "/api/v1/admin/rollups/apply") | ("POST", "/api/v1/admin/rollups/run") => Some(
            RbacPermission::new(RbacAction::Write, RbacResource::admin("rollups")),
        ),
        ("POST", "/api/v1/admin/snapshot") | ("POST", "/api/v1/admin/restore") => {
            Some(RbacPermission::new(
                RbacAction::Write,
                RbacResource::admin("maintenance.snapshot"),
            ))
        }
        ("POST", "/api/v1/admin/delete_series") => Some(RbacPermission::new(
            RbacAction::Write,
            RbacResource::admin("maintenance.delete_series"),
        )),
        ("GET", "/api/v1/admin/cluster/audit") | ("GET", "/api/v1/admin/cluster/audit/export") => {
            Some(RbacPermission::new(
                RbacAction::Read,
                RbacResource::admin("cluster.audit"),
            ))
        }
        ("POST", "/api/v1/admin/cluster/join")
        | ("POST", "/api/v1/admin/cluster/leave")
        | ("POST", "/api/v1/admin/cluster/recommission") => Some(RbacPermission::new(
            RbacAction::Write,
            RbacResource::admin("cluster.membership"),
        )),
        ("GET", "/api/v1/admin/cluster/handoff/status") => Some(RbacPermission::new(
            RbacAction::Read,
            RbacResource::admin("cluster.handoff"),
        )),
        ("POST", "/api/v1/admin/cluster/handoff/begin")
        | ("POST", "/api/v1/admin/cluster/handoff/progress")
        | ("POST", "/api/v1/admin/cluster/handoff/complete") => Some(RbacPermission::new(
            RbacAction::Write,
            RbacResource::admin("cluster.handoff"),
        )),
        ("GET", "/api/v1/admin/cluster/repair/status") => Some(RbacPermission::new(
            RbacAction::Read,
            RbacResource::admin("cluster.repair"),
        )),
        ("POST", "/api/v1/admin/cluster/repair/pause")
        | ("POST", "/api/v1/admin/cluster/repair/resume")
        | ("POST", "/api/v1/admin/cluster/repair/cancel")
        | ("POST", "/api/v1/admin/cluster/repair/run") => Some(RbacPermission::new(
            RbacAction::Write,
            RbacResource::admin("cluster.repair"),
        )),
        ("GET", "/api/v1/admin/cluster/rebalance/status") => Some(RbacPermission::new(
            RbacAction::Read,
            RbacResource::admin("cluster.rebalance"),
        )),
        ("POST", "/api/v1/admin/cluster/rebalance/pause")
        | ("POST", "/api/v1/admin/cluster/rebalance/resume")
        | ("POST", "/api/v1/admin/cluster/rebalance/run") => Some(RbacPermission::new(
            RbacAction::Write,
            RbacResource::admin("cluster.rebalance"),
        )),
        ("POST", "/api/v1/admin/cluster/control/snapshot")
        | ("POST", "/api/v1/admin/cluster/control/restore") => Some(RbacPermission::new(
            RbacAction::Write,
            RbacResource::admin("cluster.control"),
        )),
        ("POST", "/api/v1/admin/cluster/snapshot") | ("POST", "/api/v1/admin/cluster/restore") => {
            Some(RbacPermission::new(
                RbacAction::Write,
                RbacResource::admin("cluster.snapshot"),
            ))
        }
        _ => None,
    };
    Ok(permission)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    fn request_with_auth(value: &str) -> crate::http::HttpRequest {
        crate::http::HttpRequest {
            method: "GET".to_string(),
            path: "/api/v1/labels".to_string(),
            headers: HashMap::from([("authorization".to_string(), value.to_string())]),
            body: Vec::new(),
        }
    }

    #[test]
    fn bearer_token_extraction_is_case_insensitive_and_trims_token() {
        let request = request_with_auth("bearer   secret-token   ");
        assert_eq!(extract_bearer_token(&request), Some("secret-token"));
    }
}
