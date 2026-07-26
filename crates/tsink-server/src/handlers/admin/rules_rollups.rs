use super::*;

pub(crate) async fn handle_admin_rules_apply(
    request: &HttpRequest,
    rules_runtime: Option<&RulesRuntime>,
) -> HttpResponse {
    let Some(rules_runtime) = rules_runtime else {
        return text_response(503, "rules runtime is not available");
    };
    let payload = match parse_optional_json_body::<RulesApplyRequest>(request) {
        Ok(Some(payload)) => payload,
        Ok(None) => RulesApplyRequest { groups: Vec::new() },
        Err(err) => return text_response(400, &err),
    };
    let groups = match payload.into_groups() {
        Ok(groups) => groups,
        Err(err) => return text_response(400, &err),
    };
    match rules_runtime.apply_groups(groups) {
        Ok(snapshot) => rules_snapshot_response(rules_runtime, snapshot),
        Err(err) => rules_apply_error_response(err),
    }
}

fn rules_apply_error_response(err: RulesApplyError) -> HttpResponse {
    match err {
        RulesApplyError::Rejected(detail) => text_response(400, &detail),
        RulesApplyError::Persistence(source) => {
            server_persistence_error_response("rules apply", &source)
        }
        RulesApplyError::Internal(detail) => indeterminate_backend_write_error_response(
            500,
            "write_internal",
            &format!("rules apply failed: {detail}"),
        ),
    }
}

pub(crate) async fn handle_admin_rules_run(rules_runtime: Option<&RulesRuntime>) -> HttpResponse {
    let Some(rules_runtime) = rules_runtime else {
        return text_response(503, "rules runtime is not available");
    };
    match rules_runtime.trigger_run().await {
        Ok(snapshot) => rules_snapshot_response(rules_runtime, snapshot),
        Err(RulesRunTriggerError::AlreadyRunning) => {
            text_response(409, "rules scheduler is already running")
        }
        Err(RulesRunTriggerError::Snapshot(err)) => {
            text_response(500, &format!("rules run failed: {err}"))
        }
    }
}

pub(crate) async fn handle_admin_rules_status(
    rules_runtime: Option<&RulesRuntime>,
) -> HttpResponse {
    let Some(rules_runtime) = rules_runtime else {
        return text_response(503, "rules runtime is not available");
    };
    match rules_runtime.snapshot() {
        Ok(snapshot) => rules_snapshot_response(rules_runtime, snapshot),
        Err(err) => text_response(500, &format!("rules status failed: {err}")),
    }
}

fn rules_snapshot_response(
    rules_runtime: &RulesRuntime,
    mut snapshot: rules::RulesStatusSnapshot,
) -> HttpResponse {
    match rules_runtime.encode_success_snapshot(&mut snapshot) {
        Ok(body) => HttpResponse::new(200, body).with_header("Content-Type", "application/json"),
        Err(err) => text_response(500, &format!("rules status serialization failed: {err}")),
    }
}

pub(crate) async fn handle_admin_rollups_apply(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
) -> HttpResponse {
    let payload = match parse_optional_json_body::<RollupPoliciesApplyRequest>(request) {
        Ok(Some(payload)) => payload,
        Ok(None) => RollupPoliciesApplyRequest {
            policies: Vec::new(),
        },
        Err(err) => return text_response(400, &err),
    };

    match storage.apply_rollup_policies(payload.policies) {
        Ok(snapshot) => json_response(
            200,
            &json!({
                "status": "success",
                "data": snapshot,
            }),
        ),
        Err(err) => rollup_error_response("rollup apply", &err),
    }
}

pub(crate) async fn handle_admin_rollups_run(storage: &Arc<dyn Storage>) -> HttpResponse {
    match storage.trigger_rollup_run() {
        Ok(snapshot) => json_response(
            200,
            &json!({
                "status": "success",
                "data": snapshot,
            }),
        ),
        Err(err) => rollup_error_response("rollup run", &err),
    }
}

pub(crate) async fn handle_admin_rollups_status(storage: &Arc<dyn Storage>) -> HttpResponse {
    json_response(
        200,
        &json!({
            "status": "success",
            "data": storage.observability_snapshot().rollups,
        }),
    )
}

fn rollup_error_response(action: &str, err: &tsink::TsinkError) -> HttpResponse {
    match err {
        tsink::TsinkError::InvalidConfiguration(_)
        | tsink::TsinkError::UnsupportedOperation { .. } => {
            text_response(400, &format!("{action} rejected: {err}"))
        }
        _ => server_persistence_error_response(action, err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rollup_quota_failure_is_a_structured_413() {
        let response = rollup_error_response(
            "rollup apply",
            &tsink::TsinkError::DiskQuotaExceeded {
                limit: 10,
                used: 9,
                reserved: 0,
                requested: 2,
            },
        );

        assert_eq!(response.status, 413);
        assert!(response.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case(WRITE_ERROR_CODE_HEADER)
                && value == "write_disk_quota_exceeded"
        }));
    }

    #[test]
    fn rollup_validation_failure_remains_a_400() {
        let response = rollup_error_response(
            "rollup apply",
            &tsink::TsinkError::InvalidConfiguration("bad policy".to_string()),
        );

        assert_eq!(response.status, 400);
        assert!(response
            .headers
            .iter()
            .all(|(name, _)| { !name.eq_ignore_ascii_case(WRITE_ERROR_CODE_HEADER) }));
    }

    #[test]
    fn rules_quota_failure_is_a_structured_413() {
        let response = rules_apply_error_response(RulesApplyError::Persistence(
            tsink::TsinkError::DiskQuotaExceeded {
                limit: 10,
                used: 9,
                reserved: 0,
                requested: 2,
            },
        ));

        assert_eq!(response.status, 413);
        assert!(response.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case(WRITE_ERROR_CODE_HEADER)
                && value == "write_disk_quota_exceeded"
        }));
    }

    #[test]
    fn rules_validation_failure_remains_a_400() {
        let response = rules_apply_error_response(RulesApplyError::Rejected(
            "invalid rules configuration".to_string(),
        ));

        assert_eq!(response.status, 400);
        assert!(response
            .headers
            .iter()
            .all(|(name, _)| { !name.eq_ignore_ascii_case(WRITE_ERROR_CODE_HEADER) }));
    }
}
