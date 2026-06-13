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
        Ok(snapshot) => json_response(
            200,
            &json!({
                "status": "success",
                "data": snapshot,
            }),
        ),
        Err(err) => text_response(400, &err),
    }
}

pub(crate) async fn handle_admin_rules_run(rules_runtime: Option<&RulesRuntime>) -> HttpResponse {
    let Some(rules_runtime) = rules_runtime else {
        return text_response(503, "rules runtime is not available");
    };
    match rules_runtime.trigger_run().await {
        Ok(snapshot) => json_response(
            200,
            &json!({
                "status": "success",
                "data": snapshot,
            }),
        ),
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
        Ok(snapshot) => json_response(
            200,
            &json!({
                "status": "success",
                "data": snapshot,
            }),
        ),
        Err(err) => text_response(500, &format!("rules status failed: {err}")),
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
        Err(err) => text_response(400, &format!("rollup apply failed: {err}")),
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
        Err(err) => text_response(400, &format!("rollup run failed: {err}")),
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
