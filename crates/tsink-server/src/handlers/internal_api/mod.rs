use super::*;

fn parse_internal_json_body<T: DeserializeOwned>(request: &HttpRequest) -> Result<T, HttpResponse> {
    if request.body.is_empty() {
        return Err(internal_error_response(
            400,
            "invalid_request",
            "missing JSON request body",
            false,
        ));
    }

    serde_json::from_slice(&request.body).map_err(|err| {
        internal_error_response(
            400,
            "invalid_request",
            format!("invalid JSON body: {err}"),
            false,
        )
    })
}

fn internal_storage_write_error_response(
    err: &tsink::TsinkError,
    fallback_status: u16,
    fallback_code: &str,
    message: String,
    fallback_retryable: bool,
) -> HttpResponse {
    if let Some((status, error_code)) = classify_storage_write_error(err) {
        return internal_error_response(status, error_code, message, false);
    }
    internal_error_response(fallback_status, fallback_code, message, fallback_retryable)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct InternalControlErrorContract<'a> {
    status: u16,
    code: &'a str,
    retryable: bool,
    write_error_header: bool,
}

fn internal_control_error_contract<'a>(
    resource_limited: bool,
    persistence_fenced: bool,
    indeterminate: bool,
    committed_checkpoint_pending: bool,
    fallback_code: &'a str,
) -> InternalControlErrorContract<'a> {
    if persistence_fenced || indeterminate {
        return InternalControlErrorContract {
            status: 503,
            code: CONTROL_PERSISTENCE_INDETERMINATE_ERROR_CODE,
            retryable: false,
            write_error_header: true,
        };
    }
    if resource_limited {
        return InternalControlErrorContract {
            status: 413,
            code: "write_disk_quota_exceeded",
            retryable: false,
            write_error_header: true,
        };
    }
    InternalControlErrorContract {
        status: 503,
        code: fallback_code,
        retryable: !committed_checkpoint_pending,
        write_error_header: false,
    }
}

fn internal_control_consensus_error_response(
    err: &ControlConsensusError,
    persistence_fenced: bool,
    fallback_code: &str,
    message: String,
) -> HttpResponse {
    let contract = internal_control_error_contract(
        err.resource_limit().is_some(),
        persistence_fenced,
        err.is_indeterminate(),
        err.is_committed_checkpoint_pending(),
        fallback_code,
    );
    internal_control_error_response_from_contract(contract, message)
}

fn internal_control_error_response_from_contract(
    contract: InternalControlErrorContract<'_>,
    message: String,
) -> HttpResponse {
    let response =
        internal_error_response(contract.status, contract.code, message, contract.retryable);
    if contract.write_error_header {
        response.with_header(WRITE_ERROR_CODE_HEADER, contract.code)
    } else {
        response
    }
}

#[derive(Debug, Clone)]
enum InternalAtomicWriteDisposition {
    Accepted(WriteAcknowledgement),
    Rejected(tsink::WriteRejection),
}

fn inspect_internal_atomic_write_result(
    expected_rows: usize,
    result: &BatchWriteResult,
) -> Result<InternalAtomicWriteDisposition, HttpResponse> {
    let invalid_result_response = || {
        internal_error_response(
            500,
            "write_invalid_outcome",
            "internal ingest failed: storage returned an invalid canonical write result",
            false,
        )
    };
    let indexed_outcomes_are_complete = result.submitted == expected_rows
        && result.outcomes.len() == expected_rows
        && result
            .outcomes
            .iter()
            .enumerate()
            .all(|(index, outcome)| outcome.index == index);
    let accepted_outcomes = result
        .outcomes
        .iter()
        .filter(|outcome| matches!(&outcome.status, RowWriteStatus::Accepted))
        .count();
    let rejected_outcomes = result
        .outcomes
        .iter()
        .filter(|outcome| matches!(&outcome.status, RowWriteStatus::Rejected(_)))
        .count();
    let counts_are_consistent = result.accepted == accepted_outcomes
        && result.rejected == rejected_outcomes
        && accepted_outcomes.saturating_add(rejected_outcomes) == expected_rows;
    if !indexed_outcomes_are_complete || !counts_are_consistent {
        return Err(invalid_result_response());
    }

    if result.rejected > 0 {
        if result.accepted != 0 || result.acknowledgement.is_some() {
            return Err(invalid_result_response());
        }
        record_canonical_write_rejections(result);
        let Some(rejection) = result
            .outcomes
            .iter()
            .find_map(|outcome| match &outcome.status {
                RowWriteStatus::Rejected(rejection) => Some(rejection.clone()),
                RowWriteStatus::Accepted => None,
                _ => None,
            })
        else {
            return Err(invalid_result_response());
        };
        return Ok(InternalAtomicWriteDisposition::Rejected(rejection));
    }

    if result.accepted != expected_rows {
        return Err(invalid_result_response());
    }
    let Some(acknowledgement) = result.acknowledgement else {
        return Err(invalid_result_response());
    };

    Ok(InternalAtomicWriteDisposition::Accepted(acknowledgement))
}

fn validate_internal_atomic_write_result(
    expected_rows: usize,
    result: &BatchWriteResult,
) -> Result<WriteAcknowledgement, HttpResponse> {
    match inspect_internal_atomic_write_result(expected_rows, result)? {
        InternalAtomicWriteDisposition::Accepted(acknowledgement) => Ok(acknowledgement),
        InternalAtomicWriteDisposition::Rejected(rejection) => {
            let (status, error_code, retry_after) =
                write_rejection_http_mapping(rejection.category);
            let retryable = retry_after.is_some()
                || matches!(
                    rejection.category,
                    WriteRejectionCategory::InternalIo | WriteRejectionCategory::Internal
                );
            let diagnostic = bounded_write_rejection_diagnostic(&rejection.message);
            let mut response = internal_error_response(
                status,
                error_code,
                format!("internal ingest rejected: {diagnostic}"),
                retryable,
            );
            if let Some(retry_after) = retry_after {
                response = response.with_header("Retry-After", retry_after);
            }
            Err(response)
        }
    }
}

pub(super) fn cluster_ring_version(cluster_context: Option<&ClusterRequestContext>) -> u64 {
    cluster_context
        .and_then(|context| context.control_consensus.as_ref())
        .map(|consensus| consensus.current_state().ring_version)
        .unwrap_or(DEFAULT_INTERNAL_RING_VERSION)
        .max(1)
}

pub(super) fn current_control_state(
    cluster_context: Option<&ClusterRequestContext>,
) -> Option<ControlState> {
    cluster_context
        .and_then(|context| context.control_consensus.as_ref())
        .map(|consensus| consensus.current_state())
}

fn authorize_internal_cluster_request(
    request: &HttpRequest,
    internal_api: Option<&InternalApiConfig>,
    cluster_context: Option<&ClusterRequestContext>,
    allow_unknown_mtls_node: bool,
    required_capabilities: &[&str],
) -> Result<(), HttpResponse> {
    let mut additional_allowed_node_ids = current_control_state(cluster_context)
        .map(|state| {
            state
                .nodes
                .into_iter()
                .filter(|node| node.status != ControlNodeStatus::Removed)
                .map(|node| node.id)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    additional_allowed_node_ids.sort();
    additional_allowed_node_ids.dedup();
    authorize_internal_request_with_policy(
        request,
        internal_api,
        &additional_allowed_node_ids,
        allow_unknown_mtls_node,
        required_capabilities,
    )
}

fn effective_internal_dedupe_store<'a>(
    cluster_context: Option<&'a ClusterRequestContext>,
    edge_sync_context: Option<&'a edge_sync::EdgeSyncRuntimeContext>,
) -> Option<&'a DedupeWindowStore> {
    cluster_context
        .and_then(|context| context.dedupe_store.as_deref())
        .or_else(|| edge_sync_context.and_then(|context| context.accept_dedupe_store.as_deref()))
}

fn unavailable_dedupe_replay_response(key: &str) -> HttpResponse {
    internal_error_response(
        409,
        "idempotency_result_unavailable",
        format!(
            "the completed result for idempotency_key '{key}' is unavailable or belongs to a different internal ingest endpoint"
        ),
        false,
    )
    .with_header("X-Tsink-Idempotency-Replayed", "true")
}

fn dedupe_persistence_failure_response(
    err: &crate::cluster::dedupe::DedupePersistenceError,
    accepted_rows: usize,
    acknowledgement: Option<WriteAcknowledgement>,
    accepted_metadata_updates: usize,
    applied_metadata_updates: usize,
    accepted_exemplars: usize,
) -> HttpResponse {
    let (status, code, retryable) = if err.resource_limit().is_some() {
        (413, "write_disk_quota_exceeded", false)
    } else {
        (503, "dedupe_persistence_failed", true)
    };
    partial_write_error_response(
        internal_error_response(status, code, err.to_string(), retryable)
            .with_header(WRITE_ERROR_CODE_HEADER, code),
        accepted_rows,
        acknowledgement,
        accepted_metadata_updates,
        applied_metadata_updates,
        accepted_exemplars,
    )
}

fn dedupe_begin_failure_response(
    err: &DedupeBeginError,
    fallback_code: &'static str,
    context: &'static str,
) -> HttpResponse {
    match err {
        DedupeBeginError::InvalidIdempotencyKey { .. } => {
            internal_error_response(400, "invalid_idempotency_key", err.to_string(), false)
        }
        DedupeBeginError::Persistence(persistence) if persistence.resource_limit().is_some() => {
            internal_error_response(
                413,
                "write_disk_quota_exceeded",
                persistence.to_string(),
                false,
            )
            .with_header(WRITE_ERROR_CODE_HEADER, "write_disk_quota_exceeded")
        }
        DedupeBeginError::Persistence(_) => {
            internal_error_response(503, fallback_code, format!("{context}: {err}"), true)
        }
    }
}

pub(super) fn membership_from_control_state(
    cluster_context: &ClusterRequestContext,
    state: &ControlState,
) -> Result<MembershipView, String> {
    let local_node_id = cluster_context.runtime.membership.local_node_id.clone();
    let mut nodes = state
        .nodes
        .iter()
        .filter(|node| node.status != ControlNodeStatus::Removed)
        .map(|node| ClusterNode {
            id: node.id.clone(),
            endpoint: node.endpoint.clone(),
        })
        .collect::<Vec<_>>();
    nodes.sort();
    if !nodes.iter().any(|node| node.id == local_node_id) {
        return Err(format!(
            "local node '{local_node_id}' missing from control-state membership"
        ));
    }
    Ok(MembershipView {
        local_node_id,
        nodes,
    })
}

pub(super) fn effective_write_router(
    cluster_context: &ClusterRequestContext,
) -> Result<WriteRouter, String> {
    let Some(state) = current_control_state(Some(cluster_context)) else {
        return Ok(cluster_context.write_router.clone());
    };
    let membership = membership_from_control_state(cluster_context, &state)?;
    let ring =
        ShardRing::from_snapshot(state.effective_ring_snapshot_at_ring_version(state.ring_version))
            .map_err(|err| {
                format!("failed to restore control-state ring for write routing: {err}")
            })?;
    cluster_context
        .write_router
        .reconfigured_for_topology(ring, &membership)
}

pub(super) fn effective_read_fanout(
    cluster_context: &ClusterRequestContext,
) -> Result<ReadFanoutExecutor, String> {
    let Some(state) = current_control_state(Some(cluster_context)) else {
        return Ok(cluster_context.read_fanout.clone());
    };
    let membership = membership_from_control_state(cluster_context, &state)?;
    let ring =
        ShardRing::from_snapshot(state.effective_ring_snapshot_at_ring_version(state.ring_version))
            .map_err(|err| {
                format!("failed to restore control-state ring for read fanout: {err}")
            })?;
    cluster_context
        .read_fanout
        .reconfigured_for_topology(ring, &membership)
}

pub(super) fn cluster_reads_use_local_storage(
    cluster_context: Option<&ClusterRequestContext>,
) -> bool {
    cluster_context.is_some_and(|context| context.runtime.local_reads_serve_global_queries)
}

#[derive(Debug, Clone)]
struct HandoffReadBridgeSource {
    source_node_id: String,
    endpoint: String,
    stale_ring_version: u64,
}

#[derive(Debug, Clone)]
struct HandoffMetadataBridgeSource {
    source_node_id: String,
    endpoint: String,
    stale_ring_version: u64,
    shards: BTreeSet<u32>,
}

#[derive(Debug, Clone)]
struct InternalSelectRingValidation {
    bridge_source: Option<HandoffReadBridgeSource>,
}

#[derive(Debug, Clone)]
struct InternalMetadataRingValidation {
    bridge_sources: Vec<HandoffMetadataBridgeSource>,
    shard_count: u32,
}

fn is_handoff_bridge_phase(phase: ShardHandoffPhase) -> bool {
    matches!(
        phase,
        ShardHandoffPhase::Cutover | ShardHandoffPhase::FinalSync | ShardHandoffPhase::Completed
    )
}

fn transition_stale_bridge_ring_version(activation_ring_version: u64) -> u64 {
    activation_ring_version.saturating_sub(1).max(1)
}

fn validate_internal_select_ring_version(
    received_ring_version: u64,
    metric: &str,
    labels: &[Label],
    cluster_context: Option<&ClusterRequestContext>,
) -> Result<InternalSelectRingValidation, HttpResponse> {
    if received_ring_version == 0 {
        return Err(internal_error_response(
            400,
            "invalid_ring_version",
            "ring_version must be greater than zero",
            false,
        ));
    }

    let expected_ring_version = cluster_ring_version(cluster_context);
    if received_ring_version == expected_ring_version {
        let mut bridge_source = None;
        if let Some(cluster_context) = cluster_context {
            if let Some(control_state) = current_control_state(Some(cluster_context)) {
                let local_node_id = cluster_context.runtime.membership.local_node_id.as_str();
                let series_hash = stable_series_identity_hash(metric, labels);
                let shard = control_state.shard_for_series_id(series_hash);
                if let Some(transition) = control_state.transitions.iter().find(|transition| {
                    transition.shard == shard
                        && transition.to_node_id == local_node_id
                        && expected_ring_version >= transition.activation_ring_version
                        && is_handoff_bridge_phase(transition.handoff.phase)
                }) {
                    let Some(source_node) = control_state.node_record(&transition.from_node_id)
                    else {
                        return Err(internal_error_response(
                            503,
                            "control_plane_unavailable",
                            format!(
                                "handoff transition for shard {shard} references unknown source node '{}'",
                                transition.from_node_id
                            ),
                            true,
                        ));
                    };
                    bridge_source = Some(HandoffReadBridgeSource {
                        source_node_id: source_node.id.clone(),
                        endpoint: source_node.endpoint.clone(),
                        stale_ring_version: transition_stale_bridge_ring_version(
                            transition.activation_ring_version,
                        ),
                    });
                }
            }
        }

        return Ok(InternalSelectRingValidation { bridge_source });
    }

    let Some(cluster_context) = cluster_context else {
        return Err(stale_ring_version_response(
            expected_ring_version,
            received_ring_version,
            None,
        ));
    };
    let Some(control_state) = current_control_state(Some(cluster_context)) else {
        return Err(stale_ring_version_response(
            expected_ring_version,
            received_ring_version,
            None,
        ));
    };
    if received_ring_version > expected_ring_version {
        return Err(stale_ring_version_response(
            expected_ring_version,
            received_ring_version,
            None,
        ));
    }

    let local_node_id = cluster_context.runtime.membership.local_node_id.as_str();
    let series_hash = stable_series_identity_hash(metric, labels);
    let shard = control_state.shard_for_series_id(series_hash);
    let Some(transition) = control_state
        .transitions
        .iter()
        .find(|item| item.shard == shard)
    else {
        return Err(stale_ring_version_response(
            expected_ring_version,
            received_ring_version,
            Some(format!(
                "stale select requests are only accepted during shard handoff transitions (missing transition for shard {shard})"
            )),
        ));
    };

    if transition.from_node_id != local_node_id {
        return Err(stale_ring_version_response(
            expected_ring_version,
            received_ring_version,
            Some(format!(
                "stale select requests for shard {shard} can only be processed on current transition source owner '{}'",
                transition.from_node_id
            )),
        ));
    }

    let stale_ring_version =
        transition_stale_bridge_ring_version(transition.activation_ring_version);
    if received_ring_version != stale_ring_version {
        return Err(stale_ring_version_response(
            expected_ring_version,
            received_ring_version,
            Some(format!(
                "stale select requests for shard {shard} must use ring_version {stale_ring_version} (activation_ring_version {})",
                transition.activation_ring_version
            )),
        ));
    }

    if !is_handoff_bridge_phase(transition.handoff.phase) {
        return Err(stale_ring_version_response(
            expected_ring_version,
            received_ring_version,
            Some(format!(
                "stale select requests for shard {shard} require transition phase cutover/final_sync/completed, found {}",
                transition.handoff.phase.as_str()
            )),
        ));
    }

    if !control_state.node_is_owner_for_shard_at_ring_version(
        shard,
        local_node_id,
        received_ring_version,
    ) {
        return Err(stale_ring_version_response(
            expected_ring_version,
            received_ring_version,
            Some(format!(
                "node '{local_node_id}' is not an owner for shard {shard} at stale ring_version {received_ring_version}"
            )),
        ));
    }

    Ok(InternalSelectRingValidation {
        bridge_source: None,
    })
}

fn validate_internal_metadata_ring_version(
    received_ring_version: u64,
    cluster_context: Option<&ClusterRequestContext>,
) -> Result<InternalMetadataRingValidation, HttpResponse> {
    if received_ring_version == 0 {
        return Err(internal_error_response(
            400,
            "invalid_ring_version",
            "ring_version must be greater than zero",
            false,
        ));
    }

    let expected_ring_version = cluster_ring_version(cluster_context);
    let shard_count = cluster_context
        .map(|context| context.runtime.ring.shard_count())
        .unwrap_or(1);
    if received_ring_version == expected_ring_version {
        let mut grouped_sources =
            BTreeMap::<(String, String, u64), HandoffMetadataBridgeSource>::new();
        if let Some(cluster_context) = cluster_context {
            if let Some(control_state) = current_control_state(Some(cluster_context)) {
                let local_node_id = cluster_context.runtime.membership.local_node_id.as_str();
                for transition in &control_state.transitions {
                    if transition.to_node_id != local_node_id
                        || expected_ring_version < transition.activation_ring_version
                        || !is_handoff_bridge_phase(transition.handoff.phase)
                    {
                        continue;
                    }
                    let Some(source_node) = control_state.node_record(&transition.from_node_id)
                    else {
                        return Err(internal_error_response(
                            503,
                            "control_plane_unavailable",
                            format!(
                                "handoff transition for shard {} references unknown source node '{}'",
                                transition.shard, transition.from_node_id
                            ),
                            true,
                        ));
                    };
                    let stale_ring_version =
                        transition_stale_bridge_ring_version(transition.activation_ring_version);
                    let key = (
                        source_node.id.clone(),
                        source_node.endpoint.clone(),
                        stale_ring_version,
                    );
                    grouped_sources
                        .entry(key.clone())
                        .or_insert_with(|| HandoffMetadataBridgeSource {
                            source_node_id: key.0.clone(),
                            endpoint: key.1.clone(),
                            stale_ring_version: key.2,
                            shards: BTreeSet::new(),
                        })
                        .shards
                        .insert(transition.shard);
                }
            }
        }

        return Ok(InternalMetadataRingValidation {
            bridge_sources: grouped_sources.into_values().collect(),
            shard_count,
        });
    }

    let Some(cluster_context) = cluster_context else {
        return Err(stale_ring_version_response(
            expected_ring_version,
            received_ring_version,
            None,
        ));
    };
    let Some(control_state) = current_control_state(Some(cluster_context)) else {
        return Err(stale_ring_version_response(
            expected_ring_version,
            received_ring_version,
            None,
        ));
    };
    if received_ring_version > expected_ring_version {
        return Err(stale_ring_version_response(
            expected_ring_version,
            received_ring_version,
            None,
        ));
    }

    let local_node_id = cluster_context.runtime.membership.local_node_id.as_str();
    let allow_stale = control_state.transitions.iter().any(|transition| {
        transition.from_node_id == local_node_id
            && is_handoff_bridge_phase(transition.handoff.phase)
            && received_ring_version
                == transition_stale_bridge_ring_version(transition.activation_ring_version)
            && control_state.node_is_owner_for_shard_at_ring_version(
                transition.shard,
                local_node_id,
                received_ring_version,
            )
    });
    if !allow_stale {
        return Err(stale_ring_version_response(
            expected_ring_version,
            received_ring_version,
            Some(
                "stale metadata requests are only accepted for transition source owners at handoff bridge ring versions"
                    .to_string(),
            ),
        ));
    }

    Ok(InternalMetadataRingValidation {
        bridge_sources: Vec::new(),
        shard_count,
    })
}

fn validate_internal_shard_ring_version(
    received_ring_version: u64,
    shard: u32,
    request_kind: &str,
    cluster_context: Option<&ClusterRequestContext>,
) -> Result<(), HttpResponse> {
    if received_ring_version == 0 {
        return Err(internal_error_response(
            400,
            "invalid_ring_version",
            "ring_version must be greater than zero",
            false,
        ));
    }

    let expected_ring_version = cluster_ring_version(cluster_context);
    if received_ring_version == expected_ring_version {
        return Ok(());
    }

    let Some(cluster_context) = cluster_context else {
        return Err(stale_ring_version_response(
            expected_ring_version,
            received_ring_version,
            None,
        ));
    };
    let Some(control_state) = current_control_state(Some(cluster_context)) else {
        return Err(stale_ring_version_response(
            expected_ring_version,
            received_ring_version,
            None,
        ));
    };
    if received_ring_version > expected_ring_version {
        return Err(stale_ring_version_response(
            expected_ring_version,
            received_ring_version,
            None,
        ));
    }

    let local_node_id = cluster_context.runtime.membership.local_node_id.as_str();
    let allow_stale = control_state.transitions.iter().any(|transition| {
        transition.shard == shard
            && transition.from_node_id == local_node_id
            && is_handoff_bridge_phase(transition.handoff.phase)
            && received_ring_version
                == transition_stale_bridge_ring_version(transition.activation_ring_version)
            && control_state.node_is_owner_for_shard_at_ring_version(
                transition.shard,
                local_node_id,
                received_ring_version,
            )
    });
    if !allow_stale {
        return Err(stale_ring_version_response(
            expected_ring_version,
            received_ring_version,
            Some(format!(
                "stale {request_kind} requests are only accepted for transition source owners at handoff bridge ring versions"
            )),
        ));
    }

    Ok(())
}

#[derive(Debug, Clone)]
struct HandoffMirrorBatch {
    target_node_id: String,
    endpoint: String,
    rows: Vec<InternalRow>,
}

#[derive(Debug, Clone)]
struct InternalIngestRingValidation {
    expected_ring_version: u64,
    mirror_batches: Vec<HandoffMirrorBatch>,
}

fn stale_ring_version_response(
    expected_ring_version: u64,
    received_ring_version: u64,
    detail: Option<String>,
) -> HttpResponse {
    let mut message = format!(
        "ring_version mismatch: expected {expected_ring_version}, received {received_ring_version}"
    );
    if let Some(detail) = detail {
        message.push_str(" (");
        message.push_str(detail.trim());
        message.push(')');
    }
    internal_error_response(409, "stale_ring_version", message, false)
}

fn validate_internal_ingest_ring_version(
    received_ring_version: u64,
    rows: &[InternalRow],
    cluster_context: Option<&ClusterRequestContext>,
) -> Result<InternalIngestRingValidation, HttpResponse> {
    if received_ring_version == 0 {
        return Err(internal_error_response(
            400,
            "invalid_ring_version",
            "ring_version must be greater than zero",
            false,
        ));
    }

    let expected_ring_version = cluster_ring_version(cluster_context);
    if received_ring_version == expected_ring_version {
        return Ok(InternalIngestRingValidation {
            expected_ring_version,
            mirror_batches: Vec::new(),
        });
    }

    let Some(cluster_context) = cluster_context else {
        return Err(stale_ring_version_response(
            expected_ring_version,
            received_ring_version,
            None,
        ));
    };
    let Some(control_state) = current_control_state(Some(cluster_context)) else {
        return Err(stale_ring_version_response(
            expected_ring_version,
            received_ring_version,
            None,
        ));
    };
    if received_ring_version > expected_ring_version {
        return Err(stale_ring_version_response(
            expected_ring_version,
            received_ring_version,
            None,
        ));
    }

    let local_node_id = cluster_context.runtime.membership.local_node_id.as_str();
    let mut mirror_batches = BTreeMap::<String, HandoffMirrorBatch>::new();
    for row in rows {
        let series_hash = stable_series_identity_hash(row.metric.as_str(), &row.labels);
        let shard = control_state.shard_for_series_id(series_hash);
        let Some(transition) = control_state
            .transitions
            .iter()
            .find(|item| item.shard == shard)
        else {
            return Err(stale_ring_version_response(
                expected_ring_version,
                received_ring_version,
                Some(format!(
                    "stale ingest requests are only accepted during shard handoff transitions (missing transition for shard {shard})"
                )),
            ));
        };

        if transition.from_node_id != local_node_id {
            return Err(stale_ring_version_response(
                expected_ring_version,
                received_ring_version,
                Some(format!(
                    "stale ingest requests for shard {shard} can only be processed on current transition source owner '{}'",
                    transition.from_node_id
                )),
            ));
        }

        if received_ring_version >= transition.activation_ring_version {
            return Err(stale_ring_version_response(
                expected_ring_version,
                received_ring_version,
                Some(format!(
                    "received ring_version {} is not older than activation_ring_version {} for shard {shard}",
                    received_ring_version, transition.activation_ring_version
                )),
            ));
        }

        if !matches!(
            transition.handoff.phase,
            ShardHandoffPhase::Cutover
                | ShardHandoffPhase::FinalSync
                | ShardHandoffPhase::Completed
        ) {
            return Err(stale_ring_version_response(
                expected_ring_version,
                received_ring_version,
                Some(format!(
                    "stale ingest requests for shard {shard} require transition phase cutover/final_sync/completed, found {}",
                    transition.handoff.phase.as_str()
                )),
            ));
        }

        if !control_state.node_is_owner_for_shard_at_ring_version(
            shard,
            local_node_id,
            received_ring_version,
        ) {
            return Err(stale_ring_version_response(
                expected_ring_version,
                received_ring_version,
                Some(format!(
                    "node '{local_node_id}' is not an owner for shard {shard} at stale ring_version {received_ring_version}"
                )),
            ));
        }

        let Some(target_record) = control_state.node_record(&transition.to_node_id) else {
            return Err(internal_error_response(
                503,
                "control_plane_unavailable",
                format!(
                    "handoff transition for shard {shard} references unknown target node '{}'",
                    transition.to_node_id
                ),
                true,
            ));
        };

        mirror_batches
            .entry(target_record.id.clone())
            .or_insert_with(|| HandoffMirrorBatch {
                target_node_id: target_record.id.clone(),
                endpoint: target_record.endpoint.clone(),
                rows: Vec::new(),
            })
            .rows
            .push(row.clone());
    }

    Ok(InternalIngestRingValidation {
        expected_ring_version,
        mirror_batches: mirror_batches.into_values().collect(),
    })
}

async fn mirror_handoff_ingest_rows(
    validation: &InternalIngestRingValidation,
    request_idempotency_key: Option<&str>,
    cluster_context: Option<&ClusterRequestContext>,
) -> Result<(), String> {
    if validation.mirror_batches.is_empty() {
        return Ok(());
    }

    let Some(cluster_context) = cluster_context else {
        return Err("handoff mirror requires cluster context".to_string());
    };

    for batch in &validation.mirror_batches {
        let idempotency_key = handoff_mirror_idempotency_key(
            request_idempotency_key,
            &batch.target_node_id,
            validation.expected_ring_version,
            &batch.rows,
        );
        let required_capabilities = required_capabilities_for_internal_rows(&batch.rows, &[]);
        let request = InternalIngestRowsRequest {
            ring_version: validation.expected_ring_version.max(1),
            idempotency_key: Some(idempotency_key.clone()),
            required_capabilities: required_capabilities.clone(),
            rows: batch.rows.clone(),
        };
        let expected_rows = request.rows.len();

        let forward_result = match cluster_context
            .rpc_client
            .ingest_rows(&batch.endpoint, &request)
            .await
        {
            Ok(response) if response.inserted_rows == expected_rows => Ok(()),
            Ok(response) => Err(format!(
                "handoff mirror to node '{}' inserted {} rows, expected {}",
                batch.target_node_id, response.inserted_rows, expected_rows
            )),
            Err(err) => Err(format!(
                "handoff mirror to node '{}' failed: {err}",
                batch.target_node_id
            )),
        };

        if let Err(err) = forward_result {
            let Some(outbox) = cluster_context.outbox.as_ref() else {
                return Err(format!(
                    "{err}; no outbox fallback is configured for handoff mirroring"
                ));
            };

            let rows = batch
                .rows
                .iter()
                .cloned()
                .map(InternalRow::into_row)
                .collect::<Vec<_>>();
            outbox
                .enqueue_replica_write_with_capabilities(
                    &batch.target_node_id,
                    &batch.endpoint,
                    &idempotency_key,
                    validation.expected_ring_version,
                    &required_capabilities,
                    &rows,
                )
                .map_err(|source| {
                    format!(
                        "failed to enqueue handoff mirror batch for node '{}': {source}",
                        batch.target_node_id
                    )
                })?;
            eprintln!(
                "handoff mirror forwarding failed for node '{}'; batch enqueued for replay: {err}",
                batch.target_node_id
            );
        }
    }

    Ok(())
}

fn handoff_mirror_idempotency_key(
    request_idempotency_key: Option<&str>,
    target_node_id: &str,
    ring_version: u64,
    rows: &[InternalRow],
) -> String {
    match request_idempotency_key {
        Some(base) => format!("{base}:handoff:{target_node_id}:r{ring_version}"),
        None => format!(
            "tsink:v1:handoff:{target_node_id}:r{ring_version}:{:016x}",
            stable_internal_rows_fingerprint(rows)
        ),
    }
}

fn stable_internal_rows_fingerprint(rows: &[InternalRow]) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;

    let mut row_hashes = Vec::with_capacity(rows.len());
    for row in rows {
        let mut row_hash = FNV_OFFSET_BASIS;
        fnv1a_update(&mut row_hash, row.metric.as_bytes());
        fnv1a_update(&mut row_hash, &row.data_point.timestamp.to_le_bytes());

        let mut labels = row
            .labels
            .iter()
            .map(|label| (label.name.clone(), label.value.clone()))
            .collect::<Vec<_>>();
        labels.sort();
        for (name, value) in labels {
            fnv1a_update(&mut row_hash, name.as_bytes());
            fnv1a_update(&mut row_hash, b"=");
            fnv1a_update(&mut row_hash, value.as_bytes());
            fnv1a_update(&mut row_hash, b",");
        }

        if let Ok(value_bytes) = serde_json::to_vec(&row.data_point.value) {
            fnv1a_update(&mut row_hash, &value_bytes);
        }
        row_hashes.push(row_hash);
    }

    row_hashes.sort_unstable();
    let mut hash = FNV_OFFSET_BASIS;
    for row_hash in row_hashes {
        fnv1a_update(&mut hash, &row_hash.to_le_bytes());
    }
    hash
}

fn fnv1a_update(hash: &mut u64, bytes: &[u8]) {
    const FNV_PRIME: u64 = 0x100000001b3;
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(FNV_PRIME);
    }
}

fn validate_internal_series_owner(
    metric: &str,
    labels: &[Label],
    ring_version: u64,
    cluster_context: Option<&ClusterRequestContext>,
) -> Option<HttpResponse> {
    let cluster_context = cluster_context?;
    let control_state = current_control_state(Some(cluster_context))?;
    let local_node_id = cluster_context.runtime.membership.local_node_id.as_str();
    let series_hash = stable_series_identity_hash(metric, labels);
    let shard = control_state.shard_for_series_id(series_hash);
    if control_state.node_is_owner_for_shard_at_ring_version(shard, local_node_id, ring_version) {
        return None;
    }

    let owners = control_state.owners_for_shard_at_ring_version(shard, ring_version);
    Some(internal_error_response(
        409,
        "stale_ring_owner",
        format!(
            "node '{local_node_id}' is not an owner for shard {shard} at ring_version {ring_version} (owners: {})",
            owners.join(", ")
        ),
        false,
    ))
}

fn validate_internal_rows_ownership(
    rows: &[InternalRow],
    ring_version: u64,
    cluster_context: Option<&ClusterRequestContext>,
) -> Option<HttpResponse> {
    for row in rows {
        if let Some(response) = validate_internal_series_owner(
            row.metric.as_str(),
            &row.labels,
            ring_version,
            cluster_context,
        ) {
            return Some(response);
        }
    }
    None
}

pub(super) fn owned_metadata_shard_scope_for_local_node(
    ring_version: u64,
    cluster_context: Option<&ClusterRequestContext>,
    shard_count: u32,
) -> Option<MetadataShardScope> {
    let cluster_context = cluster_context?;
    let control_state = current_control_state(Some(cluster_context))?;
    let local_node_id = cluster_context.runtime.membership.local_node_id.as_str();
    Some(MetadataShardScope::new(
        shard_count,
        (0..shard_count)
            .filter(|shard| {
                control_state.node_is_owner_for_shard_at_ring_version(
                    *shard,
                    local_node_id,
                    ring_version,
                )
            })
            .collect(),
    ))
}

fn invalid_metadata_shard_scope_response(message: impl Into<String>) -> HttpResponse {
    internal_error_response(400, "invalid_shard_scope", message, false)
}

fn resolve_internal_metadata_shard_scope(
    requested_scope: Option<&MetadataShardScope>,
    ring_version: u64,
    cluster_context: Option<&ClusterRequestContext>,
    shard_count: u32,
) -> Result<MetadataShardScope, HttpResponse> {
    let scope = match requested_scope {
        Some(scope) => match scope.normalized() {
            Ok(scope) => scope,
            Err(err) => {
                return Err(invalid_metadata_shard_scope_response(format!(
                    "invalid shard_scope: {err}"
                )))
            }
        },
        None => {
            owned_metadata_shard_scope_for_local_node(ring_version, cluster_context, shard_count)
                .unwrap_or_else(|| MetadataShardScope::new(shard_count, (0..shard_count).collect()))
        }
    };

    if scope.shard_count != shard_count {
        return Err(invalid_metadata_shard_scope_response(format!(
            "shard_scope shard_count {} does not match runtime shard_count {}",
            scope.shard_count, shard_count
        )));
    }

    let Some(cluster_context) = cluster_context else {
        return Ok(scope);
    };
    let Some(control_state) = current_control_state(Some(cluster_context)) else {
        return Ok(scope);
    };
    let local_node_id = cluster_context.runtime.membership.local_node_id.as_str();
    if let Some(shard) = scope.shards.iter().copied().find(|shard| {
        !control_state.node_is_owner_for_shard_at_ring_version(*shard, local_node_id, ring_version)
    }) {
        return Err(invalid_metadata_shard_scope_response(format!(
            "node '{local_node_id}' is not an owner for shard {shard} at ring_version {ring_version}"
        )));
    }

    Ok(scope)
}

pub(super) fn metric_series_identity_key(metric: &str, labels: &[Label]) -> String {
    tsink::label::canonical_series_identity_key(metric, labels)
}

fn merge_metric_series(
    primary: Vec<MetricSeries>,
    additional: Vec<MetricSeries>,
) -> Vec<MetricSeries> {
    let mut merged = BTreeMap::<String, MetricSeries>::new();
    for series in primary.into_iter().chain(additional) {
        let key = metric_series_identity_key(series.name.as_str(), &series.labels);
        merged.entry(key).or_insert(series);
    }
    merged.into_values().collect()
}

fn merge_handoff_points(
    local_points: Vec<DataPoint>,
    bridge_points: Vec<DataPoint>,
) -> Vec<DataPoint> {
    let mut merged = BTreeMap::<(i64, String), DataPoint>::new();
    for point in local_points.into_iter().chain(bridge_points) {
        let value_key =
            serde_json::to_string(&point.value).unwrap_or_else(|_| format!("{:?}", point.value));
        merged.entry((point.timestamp, value_key)).or_insert(point);
    }
    merged.into_values().collect()
}

fn merge_handoff_series_points(
    requested: &[MetricSeries],
    primary: Vec<SeriesPoints>,
    additional: Vec<SeriesPoints>,
) -> Vec<SeriesPoints> {
    let mut merged = BTreeMap::<String, SeriesPoints>::new();
    for item in primary.into_iter().chain(additional) {
        let key = metric_series_identity_key(item.series.name.as_str(), &item.series.labels);
        match merged.remove(&key) {
            Some(existing) => {
                merged.insert(
                    key,
                    SeriesPoints {
                        series: item.series,
                        points: merge_handoff_points(existing.points, item.points),
                    },
                );
            }
            None => {
                merged.insert(key, item);
            }
        }
    }

    requested
        .iter()
        .map(|series| {
            let key = metric_series_identity_key(series.name.as_str(), &series.labels);
            merged.remove(&key).unwrap_or_else(|| SeriesPoints {
                series: series.clone(),
                points: Vec::new(),
            })
        })
        .collect()
}

fn validate_internal_exemplars_ownership(
    exemplars: &[InternalWriteExemplar],
    ring_version: u64,
    cluster_context: Option<&ClusterRequestContext>,
) -> Option<HttpResponse> {
    for exemplar in exemplars {
        if let Some(response) = validate_internal_series_owner(
            exemplar.metric.as_str(),
            &exemplar.series_labels,
            ring_version,
            cluster_context,
        ) {
            return Some(response);
        }
    }
    None
}

pub(super) fn internal_write_exemplar_to_store_write(
    exemplar: InternalWriteExemplar,
) -> ExemplarWrite {
    ExemplarWrite {
        metric: exemplar.metric,
        series_labels: exemplar.series_labels,
        exemplar_labels: exemplar.exemplar_labels,
        timestamp: exemplar.timestamp,
        value: exemplar.value,
    }
}

pub(super) fn exemplar_series_to_internal(series: ExemplarSeries) -> InternalExemplarSeries {
    InternalExemplarSeries {
        metric: series.metric,
        labels: series.labels,
        exemplars: series
            .exemplars
            .into_iter()
            .map(|exemplar| InternalExemplar {
                labels: exemplar.labels,
                value: exemplar.value,
                timestamp: exemplar.timestamp,
            })
            .collect(),
    }
}

pub(super) async fn handle_internal_ingest_write(
    storage: &Arc<dyn Storage>,
    metadata_store: &Arc<MetricMetadataStore>,
    exemplar_store: &Arc<ExemplarStore>,
    request: &HttpRequest,
    internal_api: Option<&InternalApiConfig>,
    cluster_context: Option<&ClusterRequestContext>,
    edge_sync_context: Option<&edge_sync::EdgeSyncRuntimeContext>,
) -> HttpResponse {
    if let Err(response) =
        authorize_internal_cluster_request(request, internal_api, cluster_context, false, &[])
    {
        return response;
    }

    let payload: InternalIngestWriteRequest = match parse_internal_json_body(request) {
        Ok(payload) => payload,
        Err(response) => return response,
    };
    let required_capabilities = required_capabilities_for_internal_write(
        &payload.rows,
        &payload.exemplars,
        &payload.metadata_updates,
        &payload.required_capabilities,
    );
    let required_capability_refs = required_capabilities
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    if let Err(response) = authorize_internal_cluster_request(
        request,
        internal_api,
        cluster_context,
        false,
        &required_capability_refs,
    ) {
        return response;
    }

    let payload_config = prometheus_payload_config();
    let histogram_count = payload
        .rows
        .iter()
        .filter(|row| row.data_point.value_as_histogram().is_some())
        .count();
    let requested_metadata_count =
        payload
            .metadata_updates
            .len()
            .max(usize::from(required_capability_requested(
                &required_capabilities,
                CLUSTER_CAPABILITY_METADATA_INGEST_V1,
            )));
    let requested_exemplar_count =
        payload
            .exemplars
            .len()
            .max(usize::from(required_capability_requested(
                &required_capabilities,
                CLUSTER_CAPABILITY_EXEMPLAR_INGEST_V1,
            )));
    let requested_histogram_count =
        histogram_count.max(usize::from(required_capability_requested(
            &required_capabilities,
            CLUSTER_CAPABILITY_HISTOGRAM_INGEST_V1,
        )));
    if let Err((kind, message)) = validate_payload_feature_flags(
        payload_config,
        requested_metadata_count,
        requested_exemplar_count,
        requested_histogram_count,
    ) {
        return internal_payload_disabled_response(kind, message);
    }
    if payload.ring_version == 0 {
        return internal_error_response(
            400,
            "invalid_ring_version",
            "ring_version must be greater than zero",
            false,
        );
    }
    let expected_ring_version = cluster_ring_version(cluster_context);
    if payload.ring_version != expected_ring_version {
        return stale_ring_version_response(expected_ring_version, payload.ring_version, None);
    }

    if payload.rows.len() > MAX_INTERNAL_INGEST_ROWS {
        return internal_error_response(
            422,
            "payload_too_large",
            format!(
                "ingest_write payload exceeds row limit: {} > {MAX_INTERNAL_INGEST_ROWS}",
                payload.rows.len()
            ),
            false,
        );
    }
    let exemplar_limit = exemplar_store.config().max_exemplars_per_request;
    if let Err((kind, message)) = validate_payload_quotas(
        payload_config,
        payload.metadata_updates.len(),
        payload.exemplars.len(),
        exemplar_limit,
        histogram_bucket_entries_total_for_rows(&payload.rows),
    ) {
        return internal_payload_too_large_response(kind, message);
    }
    if payload.rows.iter().any(|row| row.metric.trim().is_empty()) {
        return internal_error_response(
            400,
            "invalid_request",
            "ingest_write payload contains an empty metric name",
            false,
        );
    }
    if payload
        .exemplars
        .iter()
        .any(|exemplar| exemplar.metric.trim().is_empty())
    {
        return internal_error_response(
            400,
            "invalid_request",
            "ingest_write payload contains an empty exemplar metric name",
            false,
        );
    }
    let tenant_id = payload
        .tenant_id
        .as_deref()
        .map(str::trim)
        .filter(|tenant_id| !tenant_id.is_empty())
        .map(ToString::to_string);
    if !payload.metadata_updates.is_empty() && tenant_id.is_none() {
        return internal_error_response(
            400,
            "invalid_request",
            "ingest_write payload with metadata updates is missing tenant_id",
            false,
        );
    }
    let metadata_updates = match payload
        .metadata_updates
        .clone()
        .into_iter()
        .map(internal_metadata_update_to_normalized)
        .collect::<Result<Vec<_>, _>>()
    {
        Ok(updates) => updates,
        Err(err) => return internal_error_response(400, "invalid_request", err, false),
    };
    if let Some(response) =
        validate_internal_rows_ownership(&payload.rows, payload.ring_version, cluster_context)
    {
        return response;
    }
    if let Some(response) = validate_internal_exemplars_ownership(
        &payload.exemplars,
        payload.ring_version,
        cluster_context,
    ) {
        return response;
    }

    let inserted_rows = payload.rows.len();
    let accepted_metadata_updates = payload.metadata_updates.len();
    let accepted_exemplars = payload.exemplars.len();
    if inserted_rows == 0 && accepted_metadata_updates == 0 && accepted_exemplars == 0 {
        return json_response(
            200,
            &InternalIngestWriteResponse {
                inserted_rows: 0,
                accepted_metadata_updates: 0,
                accepted_exemplars: 0,
                dropped_exemplars: 0,
            },
        );
    }

    let dedupe_store = effective_internal_dedupe_store(cluster_context, edge_sync_context);
    let request_idempotency_key = payload.idempotency_key.clone();
    let mut dedupe_reservation = None;
    if let Some(dedupe_store) = dedupe_store {
        let key = match request_idempotency_key.as_deref() {
            Some(key) => key,
            None => {
                return internal_error_response(
                    400,
                    "missing_idempotency_key",
                    "internal ingest request is missing idempotency_key",
                    false,
                );
            }
        };

        if let Err(err) = validate_idempotency_key(key) {
            return internal_error_response(400, "invalid_idempotency_key", err, false);
        }

        match dedupe_store.begin(key) {
            Ok(DedupeBeginOutcome::Duplicate {
                completion:
                    Some(crate::cluster::dedupe::DedupeCompletion::IngestWrite {
                        inserted_rows,
                        accepted_metadata_updates,
                        accepted_exemplars,
                        dropped_exemplars,
                        acknowledgement,
                    }),
            }) => {
                let mut response = json_response(
                    200,
                    &InternalIngestWriteResponse {
                        inserted_rows,
                        accepted_metadata_updates,
                        accepted_exemplars,
                        dropped_exemplars,
                    },
                )
                .with_header("X-Tsink-Idempotency-Replayed", "true");
                if let Some(acknowledgement) = acknowledgement {
                    response = response
                        .with_header(WRITE_ACKNOWLEDGEMENT_HEADER, acknowledgement.as_str());
                }
                return response;
            }
            Ok(DedupeBeginOutcome::Duplicate { .. }) => {
                return unavailable_dedupe_replay_response(key);
            }
            Ok(DedupeBeginOutcome::InFlight) => {
                return internal_error_response(
                    409,
                    "idempotency_in_flight",
                    format!("internal ingest request with idempotency_key '{key}' is in flight"),
                    true,
                );
            }
            Ok(DedupeBeginOutcome::Accepted(reservation)) => {
                dedupe_reservation = Some(reservation);
            }
            Err(err) => {
                return dedupe_begin_failure_response(
                    &err,
                    "dedupe_unavailable",
                    "internal dedupe is unavailable",
                );
            }
        }
    }

    let row_acknowledgement = if payload.rows.is_empty() {
        None
    } else {
        let storage = Arc::clone(storage);
        let rows = payload
            .rows
            .clone()
            .into_iter()
            .map(InternalRow::into_row)
            .collect::<Vec<_>>();
        let row_count = rows.len();
        let result =
            tokio::task::spawn_blocking(move || storage.write_batch(&rows, WriteMode::Atomic))
                .await;
        match result {
            Ok(Ok(result)) => match validate_internal_atomic_write_result(row_count, &result) {
                Ok(acknowledgement) => Some(acknowledgement),
                Err(response) => return response,
            },
            Ok(Err(err)) => {
                return internal_storage_write_error_response(
                    &err,
                    500,
                    "storage_insert_failed",
                    format!("internal ingest failed: {err}"),
                    true,
                );
            }
            Err(err) => {
                return internal_error_response(
                    500,
                    "storage_insert_task_failed",
                    format!("internal ingest task failed: {err}"),
                    true,
                );
            }
        }
    };

    let metadata_applied = if metadata_updates.is_empty() {
        0usize
    } else {
        match metadata_store
            .apply_updates(tenant_id.as_deref().unwrap_or_default(), &metadata_updates)
        {
            Ok(applied) => applied,
            Err(err) => {
                return partial_write_error_response(
                    internal_server_persistence_error_response("internal metadata ingest", &err),
                    inserted_rows,
                    row_acknowledgement,
                    0,
                    0,
                    0,
                );
            }
        }
    };

    let exemplar_outcome = match exemplar_store.apply_writes(
        &payload
            .exemplars
            .into_iter()
            .map(internal_write_exemplar_to_store_write)
            .collect::<Vec<_>>(),
    ) {
        Ok(outcome) => outcome,
        Err(err) => {
            let acknowledgement = if accepted_metadata_updates > 0 {
                Some(WriteAcknowledgement::Volatile)
            } else {
                row_acknowledgement
            };
            return partial_write_error_response(
                internal_server_persistence_error_response("internal exemplar ingest", &err),
                inserted_rows,
                acknowledgement,
                accepted_metadata_updates,
                metadata_applied,
                0,
            );
        }
    };

    let acknowledgement = if accepted_metadata_updates > 0 || accepted_exemplars > 0 {
        Some(WriteAcknowledgement::Volatile)
    } else {
        row_acknowledgement
    };
    if let Some(reservation) = dedupe_reservation.take() {
        if let Err(err) =
            reservation.commit(crate::cluster::dedupe::DedupeCompletion::IngestWrite {
                inserted_rows,
                accepted_metadata_updates,
                accepted_exemplars: exemplar_outcome.accepted,
                dropped_exemplars: exemplar_outcome.dropped,
                acknowledgement,
            })
        {
            return dedupe_persistence_failure_response(
                &err,
                inserted_rows,
                acknowledgement,
                accepted_metadata_updates,
                metadata_applied,
                exemplar_outcome.accepted,
            );
        }
    }

    let mut response = json_response(
        200,
        &InternalIngestWriteResponse {
            inserted_rows,
            accepted_metadata_updates,
            accepted_exemplars: exemplar_outcome.accepted,
            dropped_exemplars: exemplar_outcome.dropped,
        },
    );
    if let Some(acknowledgement) = acknowledgement {
        response = response.with_header(WRITE_ACKNOWLEDGEMENT_HEADER, acknowledgement.as_str());
    }
    response
}

struct InternalExemplarCancellationGuard {
    token: tsink::QueryCancellationToken,
}

const INTERNAL_ACCOUNTED_RESPONSE_HEADER_ENVELOPE_BYTES: u64 = 2 * 1024;

impl Drop for InternalExemplarCancellationGuard {
    fn drop(&mut self) {
        self.token.cancel();
    }
}

#[derive(Serialize)]
struct InternalAccountedQueryExemplarsResponse<'a> {
    series: &'a [ExemplarSeries],
    accounting: tsink::QueryExecutionSnapshot,
}

#[derive(Default)]
struct InternalAccountedJsonLengthWriter<'a> {
    bytes: usize,
    execution: Option<&'a tsink::QueryExecution>,
    control_error: Option<tsink::QueryBudgetError>,
}

impl std::io::Write for InternalAccountedJsonLengthWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if let Some(execution) = self.execution {
            if let Err(error) = execution.checkpoint() {
                self.control_error = Some(error);
                return Err(std::io::Error::new(
                    std::io::ErrorKind::Interrupted,
                    "internal accounted response was canceled",
                ));
            }
        }
        self.bytes = self.bytes.checked_add(bytes.len()).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "internal exemplar response length overflow",
            )
        })?;
        if self.bytes > MAX_BODY_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "internal accounted response exceeds its hard byte limit",
            ));
        }
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

struct InternalAccountedJsonWriter<'a, W> {
    inner: W,
    execution: &'a tsink::QueryExecution,
    control_error: Option<tsink::QueryBudgetError>,
}

impl<W: std::io::Write> std::io::Write for InternalAccountedJsonWriter<'_, W> {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if let Err(error) = self.execution.checkpoint() {
            self.control_error = Some(error);
            return Err(std::io::Error::new(
                std::io::ErrorKind::Interrupted,
                "internal accounted response was canceled",
            ));
        }
        self.inner.write(bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

fn internal_exemplar_query_error_response(error: ExemplarQueryError) -> HttpResponse {
    match error {
        ExemplarQueryError::Budget(error) => internal_select_batch_query_error_response(&error),
        ExemplarQueryError::InvalidSelection => internal_error_response(
            400,
            "invalid_request",
            "invalid exemplar query request",
            false,
        ),
        ExemplarQueryError::StoreUnavailable => internal_error_response(
            503,
            "exemplar_query_unavailable",
            "exemplar storage is unavailable",
            true,
        ),
        ExemplarQueryError::Allocation => internal_error_response(
            500,
            "exemplar_query_allocation_failed",
            "exemplar query allocation failed",
            false,
        ),
    }
}

fn encode_internal_accounted_json_response<T: Serialize + ?Sized>(
    payload: &T,
    execution: &tsink::QueryExecution,
) -> Result<(HttpResponse, tsink::QueryMemoryReservation), HttpResponse> {
    let mut length_writer = InternalAccountedJsonLengthWriter {
        bytes: 0,
        execution: Some(execution),
        control_error: None,
    };
    if serde_json::to_writer(&mut length_writer, payload).is_err() {
        if let Some(error) = length_writer.control_error {
            return Err(internal_select_batch_query_error_response(&error));
        }
        return Err(internal_error_response(
            if length_writer.bytes > MAX_BODY_BYTES {
                413
            } else {
                500
            },
            if length_writer.bytes > MAX_BODY_BYTES {
                "query_response_too_large"
            } else {
                "query_response_serialization_failed"
            },
            if length_writer.bytes > MAX_BODY_BYTES {
                "internal query response exceeds its hard byte limit"
            } else {
                "failed to measure internal query response"
            },
            false,
        ));
    }
    let reserved_bytes = u64::try_from(length_writer.bytes)
        .unwrap_or(u64::MAX)
        .saturating_add(INTERNAL_ACCOUNTED_RESPONSE_HEADER_ENVELOPE_BYTES);
    let mut reservation = execution
        .reserve_memory(reserved_bytes)
        .map_err(|error| internal_select_batch_query_error_response(&error))?;
    let mut body = Vec::new();
    body.try_reserve_exact(length_writer.bytes).map_err(|_| {
        internal_error_response(
            500,
            "query_response_allocation_failed",
            "failed to allocate internal query response",
            false,
        )
    })?;
    body.resize(length_writer.bytes, 0);
    let written = {
        let cursor = std::io::Cursor::new(body.as_mut_slice());
        let mut writer = InternalAccountedJsonWriter {
            inner: cursor,
            execution,
            control_error: None,
        };
        if serde_json::to_writer(&mut writer, payload).is_err() {
            if let Some(error) = writer.control_error {
                return Err(internal_select_batch_query_error_response(&error));
            }
            return Err(internal_error_response(
                500,
                "query_response_serialization_failed",
                "failed to serialize internal query response",
                false,
            ));
        }
        usize::try_from(writer.inner.position()).unwrap_or(usize::MAX)
    };
    if written != length_writer.bytes {
        return Err(internal_error_response(
            500,
            "query_response_length_changed",
            "internal query response length changed after preflight",
            false,
        ));
    }
    reservation
        .resize(
            u64::try_from(body.capacity())
                .unwrap_or(u64::MAX)
                .saturating_add(INTERNAL_ACCOUNTED_RESPONSE_HEADER_ENVELOPE_BYTES),
        )
        .map_err(|error| internal_select_batch_query_error_response(&error))?;
    Ok((
        HttpResponse::new(200, body).with_header("Content-Type", "application/json"),
        reservation,
    ))
}

fn encode_internal_accounted_exemplar_response(
    result: &AccountedExemplarQueryResult,
    execution: &tsink::QueryExecution,
) -> Result<(HttpResponse, tsink::QueryMemoryReservation), HttpResponse> {
    encode_internal_accounted_json_response(
        &InternalAccountedQueryExemplarsResponse {
            series: result.series(),
            accounting: execution.snapshot(),
        },
        execution,
    )
}

pub(super) async fn handle_internal_query_exemplars(
    storage: &Arc<dyn Storage>,
    exemplar_store: &Arc<ExemplarStore>,
    request: &HttpRequest,
    internal_api: Option<&InternalApiConfig>,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    if let Err(response) = authorize_internal_cluster_request(
        request,
        internal_api,
        cluster_context,
        false,
        &[CLUSTER_CAPABILITY_EXEMPLAR_QUERY_V1],
    ) {
        return response;
    }

    let payload: InternalQueryExemplarsRequest = match parse_internal_json_body(request) {
        Ok(payload) => payload,
        Err(response) => return response,
    };
    if payload.ring_version == 0 {
        return internal_error_response(
            400,
            "invalid_ring_version",
            "ring_version must be greater than zero",
            false,
        );
    }
    let expected_ring_version = cluster_ring_version(cluster_context);
    if payload.ring_version != expected_ring_version {
        return stale_ring_version_response(expected_ring_version, payload.ring_version, None);
    }
    if payload.end < payload.start {
        return internal_error_response(
            422,
            "invalid_request",
            "end timestamp must be greater than or equal to start timestamp",
            false,
        );
    }
    if payload.selectors.is_empty() {
        return internal_error_response(
            400,
            "invalid_request",
            "query_exemplars selectors must not be empty",
            false,
        );
    }

    let selector_limit = exemplar_store.config().max_query_selectors;
    if payload.selectors.len() > selector_limit {
        return internal_error_response(
            422,
            "selector_limit_exceeded",
            format!(
                "query_exemplars selector limit exceeded: {} > {selector_limit}",
                payload.selectors.len()
            ),
            false,
        );
    }
    if payload.limit == 0 || payload.limit > exemplar_store.config().max_query_results {
        return internal_error_response(
            422,
            "exemplar_limit_exceeded",
            "query_exemplars limit is outside the configured hard range",
            false,
        );
    }

    let Some(query_limits) = payload.query_limits else {
        return match exemplar_store.query(
            &payload.selectors,
            payload.start,
            payload.end,
            payload.limit,
        ) {
            Ok(series) => json_response(
                200,
                &InternalQueryExemplarsResponse {
                    series: series
                        .into_iter()
                        .map(exemplar_series_to_internal)
                        .collect(),
                    accounting: None,
                },
            ),
            Err(_) => internal_error_response(
                503,
                "exemplar_query_failed",
                "internal exemplar query failed",
                true,
            ),
        };
    };

    let cancellation = tsink::QueryCancellationToken::new();
    let _cancellation_guard = InternalExemplarCancellationGuard {
        token: cancellation.clone(),
    };
    let execution = match storage.begin_query_execution(query_limits, cancellation) {
        Ok(Some(execution)) => execution,
        Ok(None) => {
            return internal_error_response(
                409,
                "query_accounting_unavailable",
                "storage does not expose exemplar query execution admission",
                false,
            )
        }
        Err(tsink::TsinkError::QueryBudget(error)) => {
            return internal_select_batch_query_error_response(&error)
        }
        Err(_) => {
            return internal_error_response(
                503,
                "query_admission_failed",
                "internal exemplar query admission failed",
                true,
            )
        }
    };
    let store = Arc::clone(exemplar_store);
    let selectors = payload.selectors;
    let start = payload.start;
    let end = payload.end;
    let limit = payload.limit;
    let worker_execution = execution.clone();
    let result = match tokio::task::spawn_blocking(move || {
        store.query_with_execution_result(&selectors, start, end, limit, &worker_execution)
    })
    .await
    {
        Ok(Ok(result)) => result,
        Ok(Err(error)) => return internal_exemplar_query_error_response(error),
        Err(_) => {
            return internal_error_response(
                500,
                "exemplar_query_task_failed",
                "internal exemplar query task failed",
                false,
            )
        }
    };
    let (response, response_reservation) =
        match encode_internal_accounted_exemplar_response(&result, &execution) {
            Ok(encoded) => encoded,
            Err(response) => return response,
        };
    drop(result);
    drop(response_reservation);
    drop(execution);
    response
}

pub(super) async fn handle_internal_ingest_rows(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    internal_api: Option<&InternalApiConfig>,
    cluster_context: Option<&ClusterRequestContext>,
    edge_sync_context: Option<&edge_sync::EdgeSyncRuntimeContext>,
) -> HttpResponse {
    if let Err(response) =
        authorize_internal_cluster_request(request, internal_api, cluster_context, false, &[])
    {
        return response;
    }

    let payload: InternalIngestRowsRequest = match parse_internal_json_body(request) {
        Ok(payload) => payload,
        Err(response) => return response,
    };
    let required_capabilities =
        required_capabilities_for_internal_rows(&payload.rows, &payload.required_capabilities);
    let required_capability_refs = required_capabilities
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    if let Err(response) = authorize_internal_cluster_request(
        request,
        internal_api,
        cluster_context,
        false,
        &required_capability_refs,
    ) {
        return response;
    }
    let payload_config = prometheus_payload_config();
    let histogram_count = payload
        .rows
        .iter()
        .filter(|row| row.data_point.value_as_histogram().is_some())
        .count()
        .max(usize::from(required_capability_requested(
            &required_capabilities,
            CLUSTER_CAPABILITY_HISTOGRAM_INGEST_V1,
        )));
    if let Err((kind, message)) =
        validate_payload_feature_flags(payload_config, 0, 0, histogram_count)
    {
        return internal_payload_disabled_response(kind, message);
    }
    let ring_validation = match validate_internal_ingest_ring_version(
        payload.ring_version,
        &payload.rows,
        cluster_context,
    ) {
        Ok(validation) => validation,
        Err(response) => return response,
    };

    if payload.rows.len() > MAX_INTERNAL_INGEST_ROWS {
        return internal_error_response(
            422,
            "payload_too_large",
            format!(
                "ingest_rows payload exceeds row limit: {} > {MAX_INTERNAL_INGEST_ROWS}",
                payload.rows.len()
            ),
            false,
        );
    }
    if payload.rows.iter().any(|row| row.metric.trim().is_empty()) {
        return internal_error_response(
            400,
            "invalid_request",
            "ingest_rows payload contains an empty metric name",
            false,
        );
    }
    if let Err((kind, message)) = validate_payload_quotas(
        payload_config,
        0,
        0,
        1,
        histogram_bucket_entries_total_for_rows(&payload.rows),
    ) {
        return internal_payload_too_large_response(kind, message);
    }
    if let Some(response) =
        validate_internal_rows_ownership(&payload.rows, payload.ring_version, cluster_context)
    {
        return response;
    }

    let inserted_rows = payload.rows.len();
    if inserted_rows == 0 {
        return json_response(
            200,
            &InternalIngestRowsResponse {
                inserted_rows: 0,
                write_result: Some(BatchWriteResult::empty()),
            },
        );
    }

    let dedupe_store = effective_internal_dedupe_store(cluster_context, edge_sync_context);
    let request_idempotency_key = payload.idempotency_key.clone();
    let mut dedupe_reservation = None;
    if let Some(dedupe_store) = dedupe_store {
        let key = match request_idempotency_key.as_deref() {
            Some(key) => key,
            None => {
                return internal_error_response(
                    400,
                    "missing_idempotency_key",
                    "internal ingest request is missing idempotency_key",
                    false,
                );
            }
        };

        if let Err(err) = validate_idempotency_key(key) {
            return internal_error_response(400, "invalid_idempotency_key", err, false);
        }

        match dedupe_store.begin(key) {
            Ok(DedupeBeginOutcome::Duplicate {
                completion:
                    Some(crate::cluster::dedupe::DedupeCompletion::IngestRows {
                        inserted_rows,
                        write_result,
                    }),
            }) => {
                let mut response = json_response(
                    200,
                    &InternalIngestRowsResponse {
                        inserted_rows,
                        write_result: write_result.clone(),
                    },
                )
                .with_header("X-Tsink-Idempotency-Replayed", "true");
                if let Some(acknowledgement) = write_result
                    .as_ref()
                    .and_then(|result| result.acknowledgement)
                {
                    response = response
                        .with_header(WRITE_ACKNOWLEDGEMENT_HEADER, acknowledgement.as_str());
                }
                return response;
            }
            Ok(DedupeBeginOutcome::Duplicate { .. }) => {
                return unavailable_dedupe_replay_response(key);
            }
            Ok(DedupeBeginOutcome::InFlight) => {
                return internal_error_response(
                    409,
                    "idempotency_in_flight",
                    "idempotency key is currently being processed",
                    true,
                );
            }
            Ok(DedupeBeginOutcome::Accepted(reservation)) => {
                dedupe_reservation = Some(reservation);
            }
            Err(err) => {
                return dedupe_begin_failure_response(
                    &err,
                    "dedupe_lookup_failed",
                    "internal dedupe lookup failed",
                );
            }
        }
    } else if let Some(key) = request_idempotency_key.as_deref() {
        if let Err(err) = validate_idempotency_key(key) {
            return internal_error_response(400, "invalid_idempotency_key", err, false);
        }
    }

    let rows: Vec<Row> = payload
        .rows
        .into_iter()
        .map(InternalRow::into_row)
        .collect();
    let storage = Arc::clone(storage);
    let result =
        tokio::task::spawn_blocking(move || storage.write_batch(&rows, WriteMode::Atomic)).await;

    match result {
        Ok(Ok(write_result)) => {
            let acknowledgement =
                match inspect_internal_atomic_write_result(inserted_rows, &write_result) {
                    Ok(InternalAtomicWriteDisposition::Accepted(acknowledgement)) => {
                        acknowledgement
                    }
                    Ok(InternalAtomicWriteDisposition::Rejected(_)) => {
                        if let Some(reservation) = dedupe_reservation.take() {
                            if let Err(err) = reservation.commit(
                                crate::cluster::dedupe::DedupeCompletion::IngestRows {
                                    inserted_rows: 0,
                                    write_result: Some(write_result.clone()),
                                },
                            ) {
                                return dedupe_persistence_failure_response(&err, 0, None, 0, 0, 0);
                            }
                        }
                        return json_response(
                            200,
                            &InternalIngestRowsResponse {
                                inserted_rows: 0,
                                write_result: Some(write_result),
                            },
                        );
                    }
                    Err(response) => return response,
                };
            if let Err(err) = mirror_handoff_ingest_rows(
                &ring_validation,
                request_idempotency_key.as_deref(),
                cluster_context,
            )
            .await
            {
                if let Some(reservation) = dedupe_reservation.take() {
                    // Local storage insert already succeeded, so preserve idempotency for
                    // client retries even if post-write mirroring fails.
                    if let Err(commit_err) =
                        reservation.commit(crate::cluster::dedupe::DedupeCompletion::IngestRows {
                            inserted_rows,
                            write_result: Some(write_result.clone()),
                        })
                    {
                        return dedupe_persistence_failure_response(
                            &commit_err,
                            inserted_rows,
                            Some(acknowledgement),
                            0,
                            0,
                            0,
                        );
                    }
                }
                return partial_write_error_response(
                    internal_error_response(
                        503,
                        "handoff_mirror_failed",
                        format!("internal ingest handoff mirror failed: {err}"),
                        true,
                    ),
                    inserted_rows,
                    Some(acknowledgement),
                    0,
                    0,
                    0,
                );
            }
            if let Some(reservation) = dedupe_reservation.take() {
                if let Err(err) =
                    reservation.commit(crate::cluster::dedupe::DedupeCompletion::IngestRows {
                        inserted_rows,
                        write_result: Some(write_result.clone()),
                    })
                {
                    return dedupe_persistence_failure_response(
                        &err,
                        inserted_rows,
                        Some(acknowledgement),
                        0,
                        0,
                        0,
                    );
                }
            }
            json_response(
                200,
                &InternalIngestRowsResponse {
                    inserted_rows,
                    write_result: Some(write_result),
                },
            )
            .with_header(WRITE_ACKNOWLEDGEMENT_HEADER, acknowledgement.as_str())
        }
        Ok(Err(err)) => internal_storage_write_error_response(
            &err,
            503,
            "storage_insert_failed",
            format!("internal ingest failed: {err}"),
            true,
        ),
        Err(err) => internal_error_response(
            503,
            "storage_insert_task_failed",
            format!("internal ingest task failed: {err}"),
            true,
        ),
    }
}

pub(super) async fn handle_internal_select(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    internal_api: Option<&InternalApiConfig>,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    if let Err(response) =
        authorize_internal_cluster_request(request, internal_api, cluster_context, false, &[])
    {
        return response;
    }

    let payload: InternalSelectRequest = match parse_internal_json_body(request) {
        Ok(payload) => payload,
        Err(response) => return response,
    };
    let ring_validation = match validate_internal_select_ring_version(
        payload.ring_version,
        payload.metric.as_str(),
        &payload.labels,
        cluster_context,
    ) {
        Ok(validation) => validation,
        Err(response) => return response,
    };

    if payload.metric.trim().is_empty() {
        return internal_error_response(400, "invalid_request", "metric must not be empty", false);
    }
    if payload.end <= payload.start {
        return internal_error_response(
            422,
            "invalid_request",
            "end timestamp must be greater than start timestamp",
            false,
        );
    }
    if let Some(response) = validate_internal_series_owner(
        payload.metric.as_str(),
        &payload.labels,
        payload.ring_version,
        cluster_context,
    ) {
        return response;
    }

    let metric = payload.metric;
    let labels = payload.labels;
    let start = payload.start;
    let end = payload.end;
    if storage.select_many_execution_accounting() != tsink::QueryExecutionAccounting::Complete {
        return internal_select_batch_accounting_unavailable(
            "storage does not provide complete select query accounting",
        );
    }
    let cancellation = tsink::QueryCancellationToken::new();
    let execution = match storage.begin_query_execution(
        default_internal_read_query_limits(storage.as_ref()),
        cancellation.clone(),
    ) {
        Ok(Some(execution)) => execution,
        Ok(None) => {
            return internal_select_batch_accounting_unavailable(
                "storage does not expose query execution admission",
            )
        }
        Err(tsink::TsinkError::QueryBudget(error)) => {
            return internal_select_batch_query_error_response(&error)
        }
        Err(error) => {
            return internal_error_response(
                503,
                "query_admission_failed",
                format!("internal select query admission failed: {error}"),
                true,
            )
        }
    };
    let _cancellation_guard = InternalSelectCancellationGuard {
        token: cancellation,
    };
    let storage = Arc::clone(storage);
    let selectors = vec![MetricSeries {
        name: metric,
        labels,
    }];
    let worker_execution = execution.clone();
    let result = tokio::task::spawn_blocking(move || {
        execute_bounded_internal_select_batch(
            storage.as_ref(),
            &selectors,
            start,
            end,
            &worker_execution,
        )
    })
    .await;

    match result {
        Ok(Ok(mut selected)) => {
            let mut series = std::mem::take(&mut selected.series);
            let mut matched_selectors = selected.matched_selectors.take().unwrap_or_default();
            let Some(mut result_reservation) = selected.take_memory_reservation() else {
                return internal_error_response(
                    500,
                    "query_accounting_invalid",
                    "bounded select storage omitted its result reservation",
                    false,
                );
            };
            let Some(local) = series.pop() else {
                return internal_error_response(
                    500,
                    "query_accounting_invalid",
                    "bounded select storage omitted its ordered result",
                    false,
                );
            };
            let Some(mut matched) = matched_selectors.pop() else {
                return internal_error_response(
                    500,
                    "query_accounting_invalid",
                    "bounded select storage omitted selector-existence accounting",
                    false,
                );
            };
            if !series.is_empty() || !matched_selectors.is_empty() {
                return internal_error_response(
                    500,
                    "query_accounting_invalid",
                    "bounded select storage returned results outside its single selector",
                    false,
                );
            }
            let SeriesPoints {
                series: selector,
                mut points,
            } = local;
            drop(series);
            drop(matched_selectors);

            if let Some(bridge_source) = ring_validation.bridge_source.as_ref() {
                let Some(cluster_context) = cluster_context else {
                    return internal_error_response(
                        503,
                        "control_plane_unavailable",
                        "handoff read bridge requires cluster context",
                        true,
                    );
                };
                let bridge_query_limits = match remaining_internal_select_batch_limits(&execution) {
                    Ok(limits) => limits,
                    Err(error) => return internal_select_batch_query_error_response(&error),
                };
                let bridge_request = InternalSelectBatchRequest {
                    ring_version: bridge_source.stale_ring_version,
                    selectors: vec![selector],
                    start,
                    end,
                    query_limits: Some(bridge_query_limits),
                };
                let accounted = match cluster_context
                    .rpc_client
                    .select_batch_accounted(&bridge_source.endpoint, &bridge_request, &execution)
                    .await
                {
                    Ok(accounted) => accounted,
                    Err(crate::cluster::rpc::RpcError::HttpStatus { status: 404, .. }) => {
                        return internal_select_batch_accounting_unavailable(format!(
                            "handoff source node '{}' ({}) does not support bounded select accounting",
                            bridge_source.source_node_id, bridge_source.endpoint
                        ));
                    }
                    Err(err) => {
                        if let Some(response) = internal_select_batch_bridge_query_error(&err) {
                            return response;
                        }
                        return internal_error_response(
                            503,
                            "handoff_bridge_failed",
                            format!(
                                "handoff read bridge select failed for source node '{}' ({}): {err}",
                                bridge_source.source_node_id, bridge_source.endpoint
                            ),
                            true,
                        );
                    }
                };
                let mut bridge_response = accounted.response;
                let bridge_transport_reservation = accounted.reservation;
                let Some(accounting) = bridge_response.accounting.as_ref() else {
                    return internal_select_batch_accounting_unavailable(format!(
                        "handoff source node '{}' ({}) returned no bounded select accounting",
                        bridge_source.source_node_id, bridge_source.endpoint
                    ));
                };
                let Some(bridge_matched_selectors) = accounting.matched_selectors.as_deref() else {
                    return internal_select_batch_accounting_unavailable(format!(
                        "handoff source node '{}' ({}) returned aggregate-only select accounting",
                        bridge_source.source_node_id, bridge_source.endpoint
                    ));
                };
                if let Err(message) = validate_internal_select_batch_accounting(
                    &bridge_request.selectors,
                    &bridge_response.series,
                    accounting.execution,
                    bridge_matched_selectors,
                ) {
                    return internal_error_response(
                        502,
                        "query_accounting_invalid",
                        format!(
                            "handoff source node '{}' ({}) returned invalid select accounting: {message}",
                            bridge_source.source_node_id, bridge_source.endpoint
                        ),
                        false,
                    );
                }
                let bridge_matched = bridge_matched_selectors[0];
                if let Err(error) = aggregate_internal_select_batch_accounting(
                    &execution,
                    accounting.execution,
                    u64::from(bridge_matched && !matched),
                ) {
                    return internal_select_batch_query_error_response(&error);
                }
                matched |= bridge_matched;
                let Some(bridge) = bridge_response.series.pop() else {
                    return internal_error_response(
                        502,
                        "query_accounting_invalid",
                        "bounded handoff select response omitted its ordered result",
                        false,
                    );
                };
                if let Err(error) = result_reservation.resize(
                    result_reservation
                        .bytes()
                        .saturating_add(bridge_transport_reservation.bytes())
                        .saturating_mul(2),
                ) {
                    return internal_select_batch_query_error_response(&error);
                }
                points = merge_handoff_points(points, bridge.points);
                drop(bridge_response);
                drop(bridge_transport_reservation);
                drop(bridge_request);
            } else {
                drop(selector);
            }

            if execution.snapshot().series_matched != u64::from(matched) {
                return internal_error_response(
                    500,
                    "query_accounting_invalid",
                    "bounded select aggregate matched-series accounting is inconsistent",
                    false,
                );
            }
            let payload = InternalSelectResponse { points };
            let (response, response_reservation) =
                match encode_internal_accounted_json_response(&payload, &execution) {
                    Ok(prepared) => prepared,
                    Err(response) => return response,
                };
            drop(payload);
            drop(result_reservation);
            drop(response_reservation);
            response
        }
        Ok(Err(InternalSelectBatchExecutionError::Storage(tsink::TsinkError::QueryBudget(
            error,
        )))) => internal_select_batch_query_error_response(&error),
        Ok(Err(InternalSelectBatchExecutionError::Storage(err))) => internal_error_response(
            503,
            "storage_select_failed",
            format!("internal select failed: {err}"),
            true,
        ),
        Ok(Err(InternalSelectBatchExecutionError::InvalidAccounting(message))) => {
            internal_error_response(
                500,
                "query_accounting_invalid",
                format!("internal select storage accounting is invalid: {message}"),
                false,
            )
        }
        Err(err) => internal_error_response(
            503,
            "storage_select_task_failed",
            format!("internal select task failed: {err}"),
            true,
        ),
    }
}

fn internal_select_batch_query_error_response(error: &tsink::QueryBudgetError) -> HttpResponse {
    let (status, code, retryable) = match error {
        tsink::QueryBudgetError::InvalidLimits(_) => {
            (400, "invalid_query_limits".to_string(), false)
        }
        tsink::QueryBudgetError::LimitExceeded(exceeded) => {
            let retryable = matches!(
                exceeded.reason,
                tsink::QueryLimitReason::ConcurrentQueries
                    | tsink::QueryLimitReason::SharedMemoryBytes
            );
            (
                if retryable { 429 } else { 413 },
                format!("query_limit_{}", exceeded.reason.as_str()),
                retryable,
            )
        }
        tsink::QueryBudgetError::Cancelled => (503, "query_cancelled".to_string(), false),
        tsink::QueryBudgetError::DeadlineExceeded => {
            (503, "query_deadline_exceeded".to_string(), false)
        }
        _ => {
            return internal_error_response(
                500,
                "query_accounting_failed",
                format!("internal select_batch query accounting failed: {error}"),
                false,
            );
        }
    };
    internal_error_response(status, code, error.to_string(), retryable)
}

fn internal_select_batch_accounting_unavailable(message: impl Into<String>) -> HttpResponse {
    internal_error_response(409, "query_accounting_unavailable", message, false)
}

fn remaining_internal_select_batch_limit(
    limit: Option<u64>,
    current: u64,
    reason: tsink::QueryLimitReason,
) -> Result<Option<u64>, tsink::QueryBudgetError> {
    let Some(limit) = limit else {
        return Ok(None);
    };
    let remaining = limit.saturating_sub(current);
    if remaining == 0 {
        return Err(tsink::QueryLimitExceeded::new(reason, limit, current, 1).into());
    }
    Ok(Some(remaining))
}

fn remaining_internal_select_batch_limits(
    execution: &tsink::QueryExecution,
) -> Result<tsink::QueryWorkLimits, tsink::QueryBudgetError> {
    execution.checkpoint()?;
    let snapshot = execution.snapshot();
    let mut limits = execution.limits();
    // Handoff peers can contain the same logical selector as the current owner. Subtracting local
    // matches would reject a duplicate-only bridge before its response flags can establish the
    // exact union. The child retains the same finite cap; aggregation charges only newly matched
    // request indices.
    limits.max_samples_scanned = remaining_internal_select_batch_limit(
        limits.max_samples_scanned,
        snapshot.samples_scanned,
        tsink::QueryLimitReason::SamplesScanned,
    )?;
    limits.max_samples_returned = remaining_internal_select_batch_limit(
        limits.max_samples_returned,
        snapshot.samples_returned,
        tsink::QueryLimitReason::SamplesReturned,
    )?;
    limits.max_returned_bytes = remaining_internal_select_batch_limit(
        limits.max_returned_bytes,
        snapshot.returned_bytes,
        tsink::QueryLimitReason::ReturnedBytes,
    )?;
    limits.max_pattern_expansion = remaining_internal_select_batch_limit(
        limits.max_pattern_expansion,
        snapshot.pattern_expansion,
        tsink::QueryLimitReason::PatternExpansion,
    )?;
    limits.max_steps = remaining_internal_select_batch_limit(
        limits.max_steps,
        snapshot.steps,
        tsink::QueryLimitReason::Steps,
    )?;

    if let Some(deadline) = execution.cancellation_token().deadline() {
        let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
            return Err(tsink::QueryBudgetError::DeadlineExceeded);
        };
        if remaining.is_zero() {
            return Err(tsink::QueryBudgetError::DeadlineExceeded);
        }
        limits.max_wall_time = Some(remaining);
    }
    Ok(limits)
}

fn aggregate_internal_select_batch_accounting(
    execution: &tsink::QueryExecution,
    snapshot: tsink::QueryExecutionSnapshot,
    additional_series_matched: u64,
) -> Result<(), tsink::QueryBudgetError> {
    execution.checkpoint()?;
    execution.charge_series_matched(additional_series_matched)?;
    execution.charge_samples_scanned(snapshot.samples_scanned)?;
    execution.charge_samples_returned(snapshot.samples_returned)?;
    execution.charge_returned_bytes(snapshot.returned_bytes)?;
    execution.charge_pattern_expansion(snapshot.pattern_expansion)?;
    execution.charge_steps(snapshot.steps)?;
    execution.observe_intermediate_vector_size(snapshot.intermediate_vector_size)?;
    Ok(())
}

fn validate_internal_select_batch_accounting(
    selectors: &[MetricSeries],
    series: &[SeriesPoints],
    snapshot: tsink::QueryExecutionSnapshot,
    matched_selectors: &[bool],
) -> Result<(), &'static str> {
    if series.len() != selectors.len() {
        return Err("response series count does not match the requested selector count");
    }
    if series
        .iter()
        .zip(selectors)
        .any(|(item, selector)| item.series != *selector)
    {
        return Err("response series identities are not in requested selector order");
    }
    if matched_selectors.len() != selectors.len() {
        return Err("matched-selector count does not match the requested selector count");
    }
    let matched_count = matched_selectors.iter().filter(|matched| **matched).count();
    if snapshot.series_matched != u64::try_from(matched_count).unwrap_or(u64::MAX) {
        return Err("reported matched-series count does not match selector-existence bits");
    }
    if series
        .iter()
        .zip(matched_selectors)
        .any(|(item, matched)| !matched && !item.points.is_empty())
    {
        return Err("response returned points for a selector reported as missing");
    }
    if snapshot.series_matched > u64::try_from(selectors.len()).unwrap_or(u64::MAX) {
        return Err("reported matched-series count exceeds the requested selector count");
    }
    let returned_samples = series.iter().fold(0u64, |count, item| {
        count.saturating_add(u64::try_from(item.points.len()).unwrap_or(u64::MAX))
    });
    if snapshot.samples_returned < returned_samples {
        return Err("reported returned-sample count is smaller than the response");
    }
    if snapshot.samples_scanned < snapshot.samples_returned {
        return Err("reported scanned-sample count is smaller than returned samples");
    }
    let returned_bytes = crate::cluster::query::modeled_series_points_returned_bytes(series);
    if snapshot.returned_bytes < returned_bytes {
        return Err("reported returned-byte count is smaller than the response");
    }
    let minimum_vector_size = series.iter().fold(
        u64::try_from(selectors.len()).unwrap_or(u64::MAX),
        |size, item| size.max(u64::try_from(item.points.len()).unwrap_or(u64::MAX)),
    );
    if snapshot.intermediate_vector_size < minimum_vector_size {
        return Err("reported intermediate-vector high-water is smaller than the response");
    }
    Ok(())
}

#[derive(Debug)]
enum InternalSelectBatchExecutionError {
    Storage(tsink::TsinkError),
    InvalidAccounting(String),
}

fn execute_bounded_internal_select_batch(
    storage: &dyn Storage,
    selectors: &[MetricSeries],
    start: i64,
    end: i64,
    execution: &tsink::QueryExecution,
) -> Result<tsink::SelectManyExecutionResult, InternalSelectBatchExecutionError> {
    execution
        .observe_intermediate_vector_size(u64::try_from(selectors.len()).unwrap_or(u64::MAX))
        .map_err(tsink::TsinkError::from)
        .map_err(InternalSelectBatchExecutionError::Storage)?;
    let selected = storage
        .select_many_with_execution_result(selectors, start, end, execution)
        .map_err(InternalSelectBatchExecutionError::Storage)?;
    if selected.series.len() != selectors.len()
        || selected
            .series
            .iter()
            .zip(selectors)
            .any(|(item, selector)| item.series != *selector)
    {
        return Err(InternalSelectBatchExecutionError::InvalidAccounting(
            "storage returned identities or ordering outside the bounded request".to_string(),
        ));
    }
    if selected
        .matched_selectors
        .as_ref()
        .is_none_or(|matched| matched.len() != selectors.len())
    {
        return Err(InternalSelectBatchExecutionError::InvalidAccounting(
            "storage omitted exact selector-existence bits for the bounded request".to_string(),
        ));
    }
    let required_result_memory =
        crate::cluster::query::modeled_series_points_vec_retained_bytes(&selected.series)
            .saturating_add(selected.matched_selectors.as_ref().map_or(
                0,
                crate::cluster::query::modeled_matched_selectors_vec_retained_bytes,
            ));
    if selected.reserved_memory_bytes() < required_result_memory {
        return Err(InternalSelectBatchExecutionError::InvalidAccounting(
            format!(
                "storage retained {} result bytes but reserved only {}",
                required_result_memory,
                selected.reserved_memory_bytes()
            ),
        ));
    }
    let matched_selectors = selected
        .matched_selectors
        .as_deref()
        .expect("selector-existence length was validated above");
    validate_internal_select_batch_accounting(
        selectors,
        &selected.series,
        execution.snapshot(),
        matched_selectors,
    )
    .map_err(|message| InternalSelectBatchExecutionError::InvalidAccounting(message.to_string()))?;
    Ok(selected)
}

#[derive(Debug)]
enum InternalSelectSeriesExecutionError {
    Storage(tsink::TsinkError),
    InvalidAccounting(String),
}

fn validate_internal_select_series_accounting(
    series: &[MetricSeries],
    before: tsink::QueryExecutionSnapshot,
    after: tsink::QueryExecutionSnapshot,
) -> Result<(), &'static str> {
    let series_count = u64::try_from(series.len()).unwrap_or(u64::MAX);
    if after.series_matched.saturating_sub(before.series_matched) < series_count {
        return Err("reported matched-series count is smaller than the response");
    }
    let returned_bytes = crate::cluster::query::modeled_metric_series_slice_returned_bytes(series);
    if after.returned_bytes.saturating_sub(before.returned_bytes) < returned_bytes {
        return Err("reported returned-byte count is smaller than the response");
    }
    if after.intermediate_vector_size < series_count {
        return Err("reported intermediate-vector high-water is smaller than the response");
    }
    Ok(())
}

fn execute_bounded_internal_select_series(
    storage: &dyn Storage,
    selection: &SeriesSelection,
    scope: &MetadataShardScope,
    execution: &tsink::QueryExecution,
) -> Result<tsink::SelectSeriesExecutionResult, InternalSelectSeriesExecutionError> {
    let before = execution.snapshot();
    let selected = storage
        .select_series_in_shards_with_execution_result(selection, scope, execution)
        .map_err(InternalSelectSeriesExecutionError::Storage)?;
    let required_result_memory =
        crate::cluster::query::modeled_metric_series_vec_retained_bytes(&selected.series);
    if selected.reserved_memory_bytes() < required_result_memory {
        return Err(InternalSelectSeriesExecutionError::InvalidAccounting(
            format!(
                "storage retained {} metadata result bytes but reserved only {}",
                required_result_memory,
                selected.reserved_memory_bytes()
            ),
        ));
    }
    validate_internal_select_series_accounting(&selected.series, before, execution.snapshot())
        .map_err(|message| {
            InternalSelectSeriesExecutionError::InvalidAccounting(message.to_string())
        })?;
    Ok(selected)
}

/// Bounds legacy internal reads that predate an additive `query_limits` field.
///
/// The Server profile supplies a finite ceiling even for ExpertUnlimited storage, while the
/// instance budget remains authoritative whenever it is tighter.
fn default_internal_read_query_limits(storage: &dyn Storage) -> tsink::QueryWorkLimits {
    tsink::ResourceLimits::server()
        .query
        .per_query
        .tightened_by(storage.query_budget_snapshot().limits.per_query)
}

fn internal_select_batch_bridge_query_error(error: &RpcError) -> Option<HttpResponse> {
    if let RpcError::QueryBudget { error } = error {
        return Some(internal_select_batch_query_error_response(error));
    }
    let RpcError::HttpStatus {
        status,
        error_code: Some(code),
        message,
        retryable,
        ..
    } = error
    else {
        return None;
    };
    let is_query_error = code == "invalid_query_limits"
        || code == "query_accounting_unavailable"
        || code == "query_accounting_invalid"
        || code == "query_cancelled"
        || code == "query_deadline_exceeded"
        || code.starts_with("query_limit_");
    is_query_error.then(|| {
        internal_error_response(
            *status,
            code.clone(),
            format!("handoff bridge rejected bounded select_batch: {message}"),
            *retryable,
        )
    })
}

struct InternalSelectCancellationGuard {
    token: tsink::QueryCancellationToken,
}

impl Drop for InternalSelectCancellationGuard {
    fn drop(&mut self) {
        self.token.cancel();
    }
}

pub(super) async fn handle_internal_select_batch(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    internal_api: Option<&InternalApiConfig>,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    if let Err(response) =
        authorize_internal_cluster_request(request, internal_api, cluster_context, false, &[])
    {
        return response;
    }

    let payload: InternalSelectBatchRequest = match parse_internal_json_body(request) {
        Ok(payload) => payload,
        Err(response) => return response,
    };

    if payload.end <= payload.start {
        return internal_error_response(
            422,
            "invalid_request",
            "end timestamp must be greater than start timestamp",
            false,
        );
    }

    let mut bridge_batches = BTreeMap::<(String, String, u64), Vec<(usize, MetricSeries)>>::new();
    for (selector_index, selector) in payload.selectors.iter().enumerate() {
        if selector.name.trim().is_empty() {
            return internal_error_response(
                400,
                "invalid_request",
                "metric must not be empty",
                false,
            );
        }

        let ring_validation = match validate_internal_select_ring_version(
            payload.ring_version,
            selector.name.as_str(),
            &selector.labels,
            cluster_context,
        ) {
            Ok(validation) => validation,
            Err(response) => return response,
        };
        if let Some(bridge_source) = ring_validation.bridge_source {
            bridge_batches
                .entry((
                    bridge_source.source_node_id,
                    bridge_source.endpoint,
                    bridge_source.stale_ring_version,
                ))
                .or_default()
                .push((selector_index, selector.clone()));
        }

        if let Some(response) = validate_internal_series_owner(
            selector.name.as_str(),
            &selector.labels,
            payload.ring_version,
            cluster_context,
        ) {
            return response;
        }
    }

    let start = payload.start;
    let end = payload.end;
    let query_limits = payload.query_limits;
    let selectors = payload.selectors;
    let requested = selectors.clone();
    let (execution, _cancellation_guard) = if let Some(query_limits) = query_limits {
        if storage.select_many_execution_accounting() != tsink::QueryExecutionAccounting::Complete {
            return internal_select_batch_accounting_unavailable(
                "storage does not provide complete select_batch query accounting",
            );
        }
        let cancellation = tsink::QueryCancellationToken::new();
        match storage.begin_query_execution(query_limits, cancellation.clone()) {
            Ok(Some(execution)) => (
                Some(execution),
                Some(InternalSelectCancellationGuard {
                    token: cancellation,
                }),
            ),
            Ok(None) => {
                return internal_select_batch_accounting_unavailable(
                    "storage does not expose query execution admission",
                )
            }
            Err(tsink::TsinkError::QueryBudget(error)) => {
                return internal_select_batch_query_error_response(&error)
            }
            Err(error) => {
                return internal_error_response(
                    503,
                    "query_admission_failed",
                    format!("internal select_batch query admission failed: {error}"),
                    true,
                )
            }
        }
    } else {
        (None, None)
    };
    let storage = Arc::clone(storage);
    let worker_execution = execution.clone();
    let result = tokio::task::spawn_blocking(move || match worker_execution.as_ref() {
        Some(execution) => execute_bounded_internal_select_batch(
            storage.as_ref(),
            &selectors,
            start,
            end,
            execution,
        ),
        None => storage
            .select_many(&selectors, start, end)
            .map(tsink::SelectManyExecutionResult::unaccounted)
            .map_err(InternalSelectBatchExecutionError::Storage),
    })
    .await;

    match result {
        Ok(Ok(mut selected)) => {
            let mut series = std::mem::take(&mut selected.series);
            let mut matched_selectors = selected.matched_selectors.take();
            let mut result_reservation = selected.take_memory_reservation();
            if execution.is_some() && result_reservation.is_none() {
                return internal_error_response(
                    500,
                    "query_accounting_invalid",
                    "bounded select_batch storage omitted its result reservation",
                    false,
                );
            }
            if !bridge_batches.is_empty() {
                let Some(cluster_context) = cluster_context else {
                    return internal_error_response(
                        503,
                        "control_plane_unavailable",
                        "handoff read bridge requires cluster context",
                        true,
                    );
                };

                for ((source_node_id, endpoint, stale_ring_version), indexed_selectors) in
                    bridge_batches
                {
                    let selectors = indexed_selectors
                        .iter()
                        .map(|(_, selector)| selector.clone())
                        .collect::<Vec<_>>();
                    let bridge_query_limits = match execution.as_ref() {
                        Some(execution) => {
                            match remaining_internal_select_batch_limits(execution) {
                                Ok(limits) => Some(limits),
                                Err(error) => {
                                    return internal_select_batch_query_error_response(&error)
                                }
                            }
                        }
                        None => None,
                    };
                    let bridge_request = InternalSelectBatchRequest {
                        ring_version: stale_ring_version,
                        selectors: selectors.clone(),
                        start,
                        end,
                        query_limits: bridge_query_limits,
                    };
                    let bridge_rpc = match execution.as_ref() {
                        Some(execution) => cluster_context
                            .rpc_client
                            .select_batch_accounted(&endpoint, &bridge_request, execution)
                            .await
                            .map(|accounted| (accounted.response, Some(accounted.reservation))),
                        None => cluster_context
                            .rpc_client
                            .select_batch(&endpoint, &bridge_request)
                            .await
                            .map(|response| (response, None)),
                    };
                    let (bridge_response, bridge_transport_reservation) = match bridge_rpc {
                        Ok(response) => response,
                        Err(crate::cluster::rpc::RpcError::HttpStatus { status: 404, .. })
                            if execution.is_none() =>
                        {
                            let mut legacy = Vec::with_capacity(selectors.len());
                            for selector in &selectors {
                                let response = cluster_context
                                    .rpc_client
                                    .select(
                                        &endpoint,
                                        &InternalSelectRequest {
                                            ring_version: stale_ring_version,
                                            metric: selector.name.clone(),
                                            labels: selector.labels.clone(),
                                            start,
                                            end,
                                        },
                                    )
                                    .await;
                                match response {
                                    Ok(response) => legacy.push(SeriesPoints {
                                        series: selector.clone(),
                                        points: response.points,
                                    }),
                                    Err(err) => {
                                        return internal_error_response(
                                            503,
                                            "handoff_bridge_failed",
                                            format!(
                                                "handoff read bridge select_batch failed for source node '{}' ({}): {err}",
                                                source_node_id, endpoint
                                            ),
                                            true,
                                        );
                                    }
                                }
                            }
                            (
                                InternalSelectBatchResponse {
                                    series: legacy,
                                    accounting: None,
                                },
                                None,
                            )
                        }
                        Err(crate::cluster::rpc::RpcError::HttpStatus { status: 404, .. }) => {
                            return internal_select_batch_accounting_unavailable(format!(
                                "handoff source node '{}' ({}) does not support bounded select_batch accounting",
                                source_node_id, endpoint
                            ));
                        }
                        Err(err) => {
                            if let Some(response) = internal_select_batch_bridge_query_error(&err) {
                                return response;
                            }
                            return internal_error_response(
                                503,
                                "handoff_bridge_failed",
                                format!(
                                    "handoff read bridge select_batch failed for source node '{}' ({}): {err}",
                                    source_node_id, endpoint
                                ),
                                true,
                            );
                        }
                    };

                    if let Some(reservation) = result_reservation.as_mut() {
                        let current =
                            crate::cluster::query::modeled_series_points_vec_retained_bytes(
                                &series,
                            );
                        let additional =
                            crate::cluster::query::modeled_series_points_vec_retained_bytes(
                                &bridge_response.series,
                            );
                        if let Err(error) =
                            reservation.resize(current.saturating_add(additional).saturating_mul(2))
                        {
                            return internal_select_batch_query_error_response(&error);
                        }
                    }

                    if let Some(execution) = execution.as_ref() {
                        let Some(accounting) = bridge_response.accounting.as_ref() else {
                            return internal_select_batch_accounting_unavailable(format!(
                                "handoff source node '{}' ({}) returned no bounded select_batch accounting",
                                source_node_id, endpoint
                            ));
                        };
                        let Some(bridge_matched_selectors) =
                            accounting.matched_selectors.as_deref()
                        else {
                            return internal_select_batch_accounting_unavailable(format!(
                                "handoff source node '{}' ({}) returned aggregate-only select_batch accounting",
                                source_node_id, endpoint
                            ));
                        };
                        if let Err(message) = validate_internal_select_batch_accounting(
                            &selectors,
                            &bridge_response.series,
                            accounting.execution,
                            bridge_matched_selectors,
                        ) {
                            return internal_error_response(
                                502,
                                "query_accounting_invalid",
                                format!(
                                    "handoff source node '{}' ({}) returned invalid select_batch accounting: {message}",
                                    source_node_id, endpoint
                                ),
                                false,
                            );
                        }
                        let Some(global_matched_selectors) = matched_selectors.as_mut() else {
                            return internal_error_response(
                                500,
                                "query_accounting_invalid",
                                "bounded select_batch lost local selector-existence accounting",
                                false,
                            );
                        };
                        let mut additional_series_matched = 0u64;
                        for ((selector_index, _), bridge_matched) in indexed_selectors
                            .iter()
                            .zip(bridge_matched_selectors.iter().copied())
                        {
                            if bridge_matched && !global_matched_selectors[*selector_index] {
                                global_matched_selectors[*selector_index] = true;
                                additional_series_matched =
                                    additional_series_matched.saturating_add(1);
                            }
                        }
                        if let Err(error) = aggregate_internal_select_batch_accounting(
                            execution,
                            accounting.execution,
                            additional_series_matched,
                        ) {
                            return internal_select_batch_query_error_response(&error);
                        }
                    }
                    series =
                        merge_handoff_series_points(&requested, series, bridge_response.series);
                    drop(bridge_transport_reservation);
                }
            }

            if let Some(reservation) = result_reservation.as_mut() {
                if let Err(error) = reservation.resize(
                    crate::cluster::query::modeled_series_points_vec_retained_bytes(&series)
                        .saturating_add(matched_selectors.as_ref().map_or(0, |matched| {
                            u64::try_from(matched.capacity()).unwrap_or(u64::MAX)
                        })),
                ) {
                    return internal_select_batch_query_error_response(&error);
                }
            }

            let accounting = match (execution.as_ref(), matched_selectors) {
                (Some(execution), Some(matched_selectors)) => {
                    let snapshot = execution.snapshot();
                    let matched_count =
                        matched_selectors.iter().filter(|matched| **matched).count();
                    if snapshot.series_matched != u64::try_from(matched_count).unwrap_or(u64::MAX) {
                        return internal_error_response(
                            500,
                            "query_accounting_invalid",
                            "bounded select_batch aggregate matched-series accounting is inconsistent",
                            false,
                        );
                    }
                    Some(crate::cluster::rpc::InternalSelectBatchAccounting {
                        execution: snapshot,
                        matched_selectors: Some(matched_selectors),
                    })
                }
                (None, None) => None,
                _ => {
                    return internal_error_response(
                        500,
                        "query_accounting_invalid",
                        "bounded select_batch selector-existence accounting is inconsistent",
                        false,
                    )
                }
            };
            let payload = InternalSelectBatchResponse { series, accounting };
            let (response, response_reservation) = match execution.as_ref() {
                Some(execution) => {
                    match encode_internal_accounted_json_response(&payload, execution) {
                        Ok((response, reservation)) => (response, Some(reservation)),
                        Err(response) => return response,
                    }
                }
                None => (json_response(200, &payload), None),
            };
            drop(payload);
            drop(result_reservation);
            drop(response_reservation);
            response
        }
        Ok(Err(InternalSelectBatchExecutionError::Storage(tsink::TsinkError::QueryBudget(
            error,
        )))) => internal_select_batch_query_error_response(&error),
        Ok(Err(InternalSelectBatchExecutionError::Storage(err))) => internal_error_response(
            503,
            "storage_select_failed",
            format!("internal select_batch failed: {err}"),
            true,
        ),
        Ok(Err(InternalSelectBatchExecutionError::InvalidAccounting(message))) => {
            internal_error_response(
                500,
                "query_accounting_invalid",
                format!("internal select_batch storage accounting is invalid: {message}"),
                false,
            )
        }
        Err(err) => internal_error_response(
            503,
            "storage_select_task_failed",
            format!("internal select_batch task failed: {err}"),
            true,
        ),
    }
}

pub(super) async fn handle_internal_select_series(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    internal_api: Option<&InternalApiConfig>,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    if let Err(response) =
        authorize_internal_cluster_request(request, internal_api, cluster_context, false, &[])
    {
        return response;
    }

    let payload: InternalSelectSeriesRequest = match parse_internal_json_body(request) {
        Ok(payload) => payload,
        Err(response) => return response,
    };
    handle_internal_select_series_payload(storage, payload, cluster_context).await
}

async fn handle_internal_select_series_payload(
    storage: &Arc<dyn Storage>,
    payload: InternalSelectSeriesRequest,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    let ring_validation =
        match validate_internal_metadata_ring_version(payload.ring_version, cluster_context) {
            Ok(validation) => validation,
            Err(response) => return response,
        };
    let ring_version = payload.ring_version;
    let shard_scope = match resolve_internal_metadata_shard_scope(
        payload.shard_scope.as_ref(),
        ring_version,
        cluster_context,
        ring_validation.shard_count,
    ) {
        Ok(scope) => scope,
        Err(response) => return response,
    };
    let selection = payload.selection;
    let expose_accounting = payload.query_limits.is_some();
    let query_limits = payload
        .query_limits
        .unwrap_or_else(|| default_internal_read_query_limits(storage.as_ref()));
    let (execution, _cancellation_guard) = {
        if storage.select_series_in_shards_execution_accounting()
            != tsink::QueryExecutionAccounting::Complete
        {
            return internal_select_batch_accounting_unavailable(
                "storage does not provide complete select_series query accounting",
            );
        }
        let cancellation = tsink::QueryCancellationToken::new();
        match storage.begin_query_execution(query_limits, cancellation.clone()) {
            Ok(Some(execution)) => (
                execution,
                InternalSelectCancellationGuard {
                    token: cancellation,
                },
            ),
            Ok(None) => {
                return internal_select_batch_accounting_unavailable(
                    "storage does not expose query execution admission",
                )
            }
            Err(tsink::TsinkError::QueryBudget(error)) => {
                return internal_select_batch_query_error_response(&error)
            }
            Err(error) => {
                return internal_error_response(
                    503,
                    "query_admission_failed",
                    format!("internal select_series query admission failed: {error}"),
                    true,
                )
            }
        }
    };

    let storage = Arc::clone(storage);
    let selection_for_storage = selection.clone();
    let shard_scope_for_storage = shard_scope.clone();
    let worker_execution = execution.clone();
    let result = tokio::task::spawn_blocking(move || {
        execute_bounded_internal_select_series(
            storage.as_ref(),
            &selection_for_storage,
            &shard_scope_for_storage,
            &worker_execution,
        )
    })
    .await;

    match result {
        Ok(Ok(mut selected)) => {
            let mut series = std::mem::take(&mut selected.series);
            let Some(mut result_reservation) = selected.take_memory_reservation() else {
                return internal_error_response(
                    500,
                    "query_accounting_invalid",
                    "bounded select_series storage omitted its result reservation",
                    false,
                );
            };
            if !ring_validation.bridge_sources.is_empty() {
                let Some(cluster_context) = cluster_context else {
                    return internal_error_response(
                        503,
                        "control_plane_unavailable",
                        "handoff read bridge requires cluster context",
                        true,
                    );
                };
                for bridge_source in &ring_validation.bridge_sources {
                    let bridge_query_limits =
                        match remaining_internal_select_batch_limits(&execution) {
                            Ok(limits) => limits,
                            Err(error) => {
                                return internal_select_batch_query_error_response(&error)
                            }
                        };
                    let request = InternalSelectSeriesRequest {
                        ring_version: bridge_source.stale_ring_version,
                        shard_scope: Some(MetadataShardScope::new(
                            ring_validation.shard_count,
                            bridge_source.shards.iter().copied().collect(),
                        )),
                        selection: selection.clone(),
                        query_limits: Some(bridge_query_limits),
                    };
                    let bridge_rpc = cluster_context
                        .rpc_client
                        .select_series_accounted(&bridge_source.endpoint, &request, &execution)
                        .await;
                    match bridge_rpc {
                        Ok(accounted) => {
                            let response = accounted.response;
                            let bridge_transport_reservation = accounted.reservation;
                            let Some(accounting) = response.accounting.as_ref() else {
                                return internal_select_batch_accounting_unavailable(
                                    "bounded handoff select_series response omitted execution accounting",
                                );
                            };
                            if let Err(message) = validate_internal_select_series_accounting(
                                &response.series,
                                tsink::QueryExecutionSnapshot::default(),
                                accounting.execution,
                            ) {
                                return internal_error_response(
                                    502,
                                    "query_accounting_invalid",
                                    format!(
                                        "bounded handoff select_series returned invalid accounting: {message}"
                                    ),
                                    false,
                                );
                            }
                            let current =
                                crate::cluster::query::modeled_metric_series_vec_retained_bytes(
                                    &series,
                                );
                            let additional =
                                crate::cluster::query::modeled_metric_series_vec_retained_bytes(
                                    &response.series,
                                );
                            if let Err(error) = result_reservation
                                .resize(current.saturating_add(additional).saturating_mul(2))
                            {
                                return internal_select_batch_query_error_response(&error);
                            }
                            let previous_len = series.len();
                            series = merge_metric_series(series, response.series);
                            let additional_series_matched =
                                u64::try_from(series.len().saturating_sub(previous_len))
                                    .unwrap_or(u64::MAX);
                            if let Err(error) = aggregate_internal_select_batch_accounting(
                                &execution,
                                accounting.execution,
                                additional_series_matched,
                            ) {
                                return internal_select_batch_query_error_response(&error);
                            }
                            drop(bridge_transport_reservation);
                        }
                        Err(err) => {
                            if let Some(response) = internal_select_batch_bridge_query_error(&err) {
                                return response;
                            }
                            return internal_error_response(
                                503,
                                "handoff_bridge_failed",
                                format!(
                                    "handoff read bridge select_series failed for source node '{}' ({}): {err}",
                                    bridge_source.source_node_id, bridge_source.endpoint
                                ),
                                true,
                            );
                        }
                    }
                }
            }
            if let Err(error) = result_reservation
                .resize(crate::cluster::query::modeled_metric_series_vec_retained_bytes(&series))
            {
                return internal_select_batch_query_error_response(&error);
            }
            let accounting =
                expose_accounting.then(|| crate::cluster::rpc::InternalSelectSeriesAccounting {
                    execution: execution.snapshot(),
                });
            let payload = InternalSelectSeriesResponse { series, accounting };
            let (response, response_reservation) =
                match encode_internal_accounted_json_response(&payload, &execution) {
                    Ok(prepared) => prepared,
                    Err(response) => return response,
                };
            drop(payload);
            drop(result_reservation);
            drop(response_reservation);
            response
        }
        Ok(Err(InternalSelectSeriesExecutionError::Storage(tsink::TsinkError::QueryBudget(
            error,
        )))) => internal_select_batch_query_error_response(&error),
        Ok(Err(InternalSelectSeriesExecutionError::Storage(err))) => internal_error_response(
            503,
            "storage_select_series_failed",
            format!("internal select_series failed: {err}"),
            true,
        ),
        Ok(Err(InternalSelectSeriesExecutionError::InvalidAccounting(message))) => {
            internal_error_response(
                500,
                "query_accounting_invalid",
                format!("internal select_series storage accounting is invalid: {message}"),
                false,
            )
        }
        Err(err) => internal_error_response(
            503,
            "storage_select_series_task_failed",
            format!("internal select_series task failed: {err}"),
            true,
        ),
    }
}

pub(super) async fn handle_internal_list_metrics(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    internal_api: Option<&InternalApiConfig>,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    if let Err(response) =
        authorize_internal_cluster_request(request, internal_api, cluster_context, false, &[])
    {
        return response;
    }

    let payload = if request.body.is_empty() {
        InternalListMetricsRequest::default()
    } else {
        match parse_internal_json_body(request) {
            Ok(payload) => payload,
            Err(response) => return response,
        }
    };
    handle_internal_select_series_payload(
        storage,
        InternalSelectSeriesRequest {
            ring_version: payload.ring_version,
            shard_scope: payload.shard_scope,
            selection: SeriesSelection::new(),
            query_limits: payload.query_limits,
        },
        cluster_context,
    )
    .await
}

enum InternalDigestExecutionError {
    Storage(tsink::TsinkError),
    InvalidAccounting,
}

pub(super) async fn handle_internal_digest_window(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    internal_api: Option<&InternalApiConfig>,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    if let Err(response) =
        authorize_internal_cluster_request(request, internal_api, cluster_context, false, &[])
    {
        return response;
    }

    let payload: InternalDigestWindowRequest = match parse_internal_json_body(request) {
        Ok(payload) => payload,
        Err(response) => return response,
    };
    let Some(query_limits) = payload.query_limits else {
        return internal_error_response(
            400,
            "query_limits_required",
            "internal digest_window requires finite query_limits",
            false,
        );
    };
    if !crate::cluster::repair::internal_maintenance_query_limits_are_finite(query_limits) {
        return internal_error_response(
            400,
            "invalid_query_limits",
            "internal digest_window query_limits must make every work limit finite",
            false,
        );
    }
    if payload.window_end <= payload.window_start {
        return internal_error_response(
            422,
            "invalid_request",
            "window_end must be greater than window_start",
            false,
        );
    }

    let shard_count = cluster_context
        .map(|context| context.runtime.ring.shard_count())
        .unwrap_or(1)
        .max(1);
    if payload.shard >= shard_count {
        return internal_error_response(
            422,
            "invalid_shard",
            format!(
                "shard {} is out of range for shard_count {}",
                payload.shard, shard_count
            ),
            false,
        );
    }
    if let Err(response) = validate_internal_shard_ring_version(
        payload.ring_version,
        payload.shard,
        "digest_window",
        cluster_context,
    ) {
        return response;
    }

    if let Some(cluster_context) = cluster_context {
        if let Some(control_state) = current_control_state(Some(cluster_context)) {
            let local_node_id = cluster_context.runtime.membership.local_node_id.as_str();
            if !control_state.node_is_owner_for_shard_at_ring_version(
                payload.shard,
                local_node_id,
                payload.ring_version,
            ) {
                let owners = control_state
                    .owners_for_shard_at_ring_version(payload.shard, payload.ring_version);
                return internal_error_response(
                    409,
                    "stale_ring_owner",
                    format!(
                        "node '{local_node_id}' is not an owner for shard {} at ring_version {} (owners: {})",
                        payload.shard,
                        payload.ring_version,
                        owners.join(", ")
                    ),
                    false,
                );
            }
        }
    }

    if storage.compute_shard_window_digest_execution_accounting()
        != tsink::QueryExecutionAccounting::Complete
    {
        return internal_select_batch_accounting_unavailable(
            "storage does not provide complete digest_window query accounting",
        );
    }
    let cancellation = tsink::QueryCancellationToken::new();
    let _cancellation_guard = InternalSelectCancellationGuard {
        token: cancellation.clone(),
    };
    let execution = match storage.begin_query_execution(query_limits, cancellation) {
        Ok(Some(execution)) => execution,
        Ok(None) => {
            return internal_select_batch_accounting_unavailable(
                "storage does not expose digest_window query admission",
            )
        }
        Err(tsink::TsinkError::QueryBudget(error)) => {
            return internal_select_batch_query_error_response(&error)
        }
        Err(_) => {
            return internal_error_response(
                503,
                "query_admission_failed",
                "internal digest_window query admission failed",
                true,
            )
        }
    };

    let storage = Arc::clone(storage);
    let shard = payload.shard;
    let ring_version = payload.ring_version;
    let window_start = payload.window_start;
    let window_end = payload.window_end;
    let worker_execution = execution.clone();
    let digest_task = tokio::task::spawn_blocking(move || {
        let before = worker_execution.snapshot();
        let digest = storage
            .compute_shard_window_digest_with_execution(
                shard,
                shard_count,
                window_start,
                window_end,
                &worker_execution,
            )
            .map_err(InternalDigestExecutionError::Storage)?;
        crate::cluster::repair::validate_internal_digest_execution_accounting(
            digest.series_count,
            digest.point_count,
            before,
            worker_execution.snapshot(),
        )
        .map_err(|_| InternalDigestExecutionError::InvalidAccounting)?;
        Ok::<_, InternalDigestExecutionError>(digest)
    })
    .await;

    match digest_task {
        Ok(Ok(digest)) => {
            let payload = InternalAccountedDigestWindowResponse {
                digest: InternalDigestWindowResponse {
                    shard: digest.shard,
                    ring_version,
                    window_start: digest.window_start,
                    window_end: digest.window_end,
                    series_count: digest.series_count,
                    point_count: digest.point_count,
                    fingerprint: digest.fingerprint,
                },
                accounting: execution.snapshot(),
            };
            match encode_internal_accounted_json_response(&payload, &execution) {
                Ok((response, response_reservation)) => {
                    drop(response_reservation);
                    response
                }
                Err(response) => response,
            }
        }
        Ok(Err(InternalDigestExecutionError::Storage(tsink::TsinkError::QueryBudget(error)))) => {
            internal_select_batch_query_error_response(&error)
        }
        Ok(Err(InternalDigestExecutionError::InvalidAccounting)) => internal_error_response(
            500,
            "query_accounting_invalid",
            "bounded digest_window storage returned invalid accounting",
            false,
        ),
        Ok(Err(InternalDigestExecutionError::Storage(_))) => internal_error_response(
            503,
            "digest_compute_failed",
            "internal digest computation failed",
            true,
        ),
        Err(_) => internal_error_response(
            503,
            "digest_compute_task_failed",
            "internal digest compute task failed",
            true,
        ),
    }
}

#[allow(clippy::too_many_arguments)]
pub(super) async fn handle_internal_snapshot_data(
    storage: &Arc<dyn Storage>,
    metadata_store: &Arc<MetricMetadataStore>,
    exemplar_store: &Arc<ExemplarStore>,
    rules_runtime: Option<&RulesRuntime>,
    request: &HttpRequest,
    internal_api: Option<&InternalApiConfig>,
    cluster_context: Option<&ClusterRequestContext>,
    admin_path_prefix: Option<&Path>,
    offline_restore_disk_budget: Option<&Arc<tsink::LocalDiskBudget>>,
) -> HttpResponse {
    if let Err(response) =
        authorize_internal_cluster_request(request, internal_api, cluster_context, false, &[])
    {
        return response;
    }

    let payload: InternalDataSnapshotRequest = match parse_internal_json_body(request) {
        Ok(payload) => payload,
        Err(response) => return response,
    };
    let Some(requested_path) = non_empty_param(Some(payload.path)) else {
        return internal_error_response(
            422,
            "invalid_snapshot_request",
            "missing required field 'path'",
            false,
        );
    };
    let snapshot_path =
        match resolve_admin_path(Path::new(&requested_path), admin_path_prefix, false) {
            Ok(path) => path,
            Err(err) => {
                return internal_error_response(422, "invalid_snapshot_path", err, false);
            }
        };
    if let Err(err) = validate_snapshot_destination_outside_offline_root(
        &snapshot_path,
        offline_restore_disk_budget.map(Arc::as_ref),
    ) {
        return internal_error_response(422, "invalid_snapshot_path", err, false);
    }

    match perform_local_data_snapshot(
        storage,
        metadata_store,
        exemplar_store,
        rules_runtime,
        &snapshot_path,
        cluster_context,
    )
    .await
    {
        Ok(response) => json_response(200, &response),
        Err(err) => internal_error_response(503, "snapshot_failed", err, true),
    }
}

pub(super) async fn handle_internal_restore_data(
    request: &HttpRequest,
    internal_api: Option<&InternalApiConfig>,
    cluster_context: Option<&ClusterRequestContext>,
    admin_path_prefix: Option<&Path>,
    local_disk_budget: Option<&tsink::LocalDiskBudget>,
    offline_restore_disk_budget: Option<&Arc<tsink::LocalDiskBudget>>,
) -> HttpResponse {
    handle_internal_restore_data_impl(
        request,
        internal_api,
        cluster_context,
        admin_path_prefix,
        local_disk_budget,
        offline_restore_disk_budget,
        &[],
    )
    .await
}

pub(super) async fn handle_internal_restore_data_budgeted(
    request: &HttpRequest,
    internal_api: Option<&InternalApiConfig>,
    cluster_context: Option<&ClusterRequestContext>,
    admin_path_prefix: Option<&Path>,
    local_disk_budget: Option<&tsink::LocalDiskBudget>,
    offline_restore_disk_budget: Option<&Arc<tsink::LocalDiskBudget>>,
) -> HttpResponse {
    handle_internal_restore_data_impl(
        request,
        internal_api,
        cluster_context,
        admin_path_prefix,
        local_disk_budget,
        offline_restore_disk_budget,
        &[CLUSTER_CAPABILITY_BUDGETED_RESTORE_V1],
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn handle_internal_restore_data_impl(
    request: &HttpRequest,
    internal_api: Option<&InternalApiConfig>,
    cluster_context: Option<&ClusterRequestContext>,
    admin_path_prefix: Option<&Path>,
    local_disk_budget: Option<&tsink::LocalDiskBudget>,
    offline_restore_disk_budget: Option<&Arc<tsink::LocalDiskBudget>>,
    required_capabilities: &[&str],
) -> HttpResponse {
    if let Err(response) = authorize_internal_cluster_request(
        request,
        internal_api,
        cluster_context,
        false,
        required_capabilities,
    ) {
        return response;
    }
    let Some(offline_restore_disk_budget) = offline_restore_disk_budget else {
        return internal_error_response(
            503,
            "offline_restore_unconfigured",
            "offline restore is unavailable because no dedicated restore root and finite disk limit are configured",
            false,
        );
    };

    let payload: InternalDataRestoreRequest = match parse_internal_json_body(request) {
        Ok(payload) => payload,
        Err(response) => return response,
    };
    let Some(snapshot_path) = non_empty_param(Some(payload.snapshot_path)) else {
        return internal_error_response(
            422,
            "invalid_restore_request",
            "missing required field 'snapshotPath'",
            false,
        );
    };
    let Some(data_path) = non_empty_param(Some(payload.data_path)) else {
        return internal_error_response(
            422,
            "invalid_restore_request",
            "missing required field 'dataPath'",
            false,
        );
    };
    let snapshot_path = match resolve_admin_path(Path::new(&snapshot_path), admin_path_prefix, true)
    {
        Ok(path) => path,
        Err(err) => {
            return internal_error_response(422, "invalid_restore_path", err, false);
        }
    };
    let data_path = match resolve_admin_path(Path::new(&data_path), admin_path_prefix, false) {
        Ok(path) => path,
        Err(err) => {
            return internal_error_response(422, "invalid_restore_path", err, false);
        }
    };
    if let Err(err) = validate_restore_target_outside_live_root(&data_path, local_disk_budget) {
        return internal_error_response(422, "invalid_restore_path", err, false);
    }
    if let Err(err) =
        validate_restore_target_within_offline_root(&data_path, offline_restore_disk_budget)
    {
        return internal_error_response(422, "invalid_restore_path", err, false);
    }

    match perform_local_data_restore(
        &snapshot_path,
        &data_path,
        cluster_context,
        Arc::clone(offline_restore_disk_budget),
    )
    .await
    {
        Ok(response) => json_response(200, &response),
        Err(err) => admin_restore_error_response(&err),
    }
}

enum InternalRepairBackfillExecutionError {
    Storage(tsink::TsinkError),
    InvalidAccounting,
}

#[allow(clippy::too_many_arguments)]
fn collect_internal_repair_backfill_rows_with_execution(
    storage: &dyn Storage,
    ring_version: u64,
    shard: u32,
    shard_count: u32,
    window_start: i64,
    window_end: i64,
    max_series: Option<usize>,
    max_rows: Option<usize>,
    row_offset: Option<u64>,
    execution: &tsink::QueryExecution,
) -> Result<
    (
        InternalRepairBackfillResponse,
        tsink::QueryMemoryReservation,
    ),
    InternalRepairBackfillExecutionError,
> {
    let before = execution.snapshot();
    let result = storage
        .scan_shard_window_rows_with_execution_result(
            shard,
            shard_count,
            window_start,
            window_end,
            ShardWindowScanOptions {
                max_series,
                max_rows,
                row_offset,
            },
            execution,
        )
        .map_err(InternalRepairBackfillExecutionError::Storage)?;
    if result.page.shard != shard
        || result.page.shard_count != shard_count
        || result.page.window_start != window_start
        || result.page.window_end != window_end
        || result.reserved_memory_bytes()
            < tsink::modeled_query_rows_retained_bytes(&result.page.rows)
    {
        return Err(InternalRepairBackfillExecutionError::InvalidAccounting);
    }
    let returned_bytes = crate::cluster::rpc::modeled_repair_rows_returned_bytes(&result.page.rows);
    crate::cluster::repair::validate_internal_repair_backfill_execution_accounting(
        result.page.series_scanned,
        result.page.rows_scanned,
        result.page.rows.len(),
        returned_bytes,
        Some(result.reserved_memory_bytes()),
        before,
        execution.snapshot(),
    )
    .map_err(|_| InternalRepairBackfillExecutionError::InvalidAccounting)?;

    let response_clone_upper =
        crate::cluster::rpc::modeled_internal_repair_rows_clone_upper_bytes(&result.page.rows);
    let mut response_reservation = execution
        .reserve_memory(response_clone_upper)
        .map_err(|error| InternalRepairBackfillExecutionError::Storage(error.into()))?;
    let mut rows = Vec::new();
    rows.try_reserve_exact(result.page.rows.len())
        .map_err(|_| {
            InternalRepairBackfillExecutionError::Storage(tsink::TsinkError::Other(
                "failed to allocate bounded repair response rows".to_string(),
            ))
        })?;
    for row in &result.page.rows {
        execution
            .checkpoint()
            .map_err(|error| InternalRepairBackfillExecutionError::Storage(error.into()))?;
        rows.push(InternalRow::from(row));
    }
    response_reservation
        .resize(crate::cluster::rpc::modeled_internal_repair_rows_retained_bytes(&rows))
        .map_err(|error| InternalRepairBackfillExecutionError::Storage(error.into()))?;

    let response = InternalRepairBackfillResponse {
        shard: result.page.shard,
        ring_version,
        window_start: result.page.window_start,
        window_end: result.page.window_end,
        series_scanned: result.page.series_scanned,
        rows_scanned: result.page.rows_scanned,
        truncated: result.page.truncated,
        next_row_offset: result.page.next_row_offset,
        rows,
    };
    drop(result);
    Ok((response, response_reservation))
}

pub(super) async fn handle_internal_repair_backfill(
    storage: &Arc<dyn Storage>,
    request: &HttpRequest,
    internal_api: Option<&InternalApiConfig>,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    if let Err(response) =
        authorize_internal_cluster_request(request, internal_api, cluster_context, false, &[])
    {
        return response;
    }

    let payload: InternalRepairBackfillRequest = match parse_internal_json_body(request) {
        Ok(payload) => payload,
        Err(response) => return response,
    };
    let Some(query_limits) = payload.query_limits else {
        return internal_error_response(
            400,
            "query_limits_required",
            "internal repair_backfill requires finite query_limits",
            false,
        );
    };
    if !crate::cluster::repair::internal_maintenance_query_limits_are_finite(query_limits) {
        return internal_error_response(
            400,
            "invalid_query_limits",
            "internal repair_backfill query_limits must make every work limit finite",
            false,
        );
    }
    if payload.window_end <= payload.window_start {
        return internal_error_response(
            422,
            "invalid_request",
            "window_end must be greater than window_start",
            false,
        );
    }
    if payload.max_series.is_some_and(|value| value == 0) {
        return internal_error_response(
            422,
            "invalid_request",
            "max_series must be greater than zero when set",
            false,
        );
    }
    if payload.max_rows.is_some_and(|value| value == 0) {
        return internal_error_response(
            422,
            "invalid_request",
            "max_rows must be greater than zero when set",
            false,
        );
    }

    let shard_count = cluster_context
        .map(|context| context.runtime.ring.shard_count())
        .unwrap_or(1)
        .max(1);
    if payload.shard >= shard_count {
        return internal_error_response(
            422,
            "invalid_shard",
            format!(
                "shard {} is out of range for shard_count {}",
                payload.shard, shard_count
            ),
            false,
        );
    }
    if let Err(response) = validate_internal_shard_ring_version(
        payload.ring_version,
        payload.shard,
        "repair_backfill",
        cluster_context,
    ) {
        return response;
    }

    if let Some(cluster_context) = cluster_context {
        if let Some(control_state) = current_control_state(Some(cluster_context)) {
            let local_node_id = cluster_context.runtime.membership.local_node_id.as_str();
            if !control_state.node_is_owner_for_shard_at_ring_version(
                payload.shard,
                local_node_id,
                payload.ring_version,
            ) {
                let owners = control_state
                    .owners_for_shard_at_ring_version(payload.shard, payload.ring_version);
                return internal_error_response(
                    409,
                    "stale_ring_owner",
                    format!(
                        "node '{local_node_id}' is not an owner for shard {} at ring_version {} (owners: {})",
                        payload.shard,
                        payload.ring_version,
                        owners.join(", ")
                    ),
                    false,
                );
            }
        }
    }

    if storage.scan_shard_window_rows_execution_accounting()
        != tsink::QueryExecutionAccounting::Complete
    {
        return internal_select_batch_accounting_unavailable(
            "storage does not provide complete repair_backfill query accounting",
        );
    }
    let cancellation = tsink::QueryCancellationToken::new();
    let _cancellation_guard = InternalSelectCancellationGuard {
        token: cancellation.clone(),
    };
    let execution = match storage.begin_query_execution(query_limits, cancellation) {
        Ok(Some(execution)) => execution,
        Ok(None) => {
            return internal_select_batch_accounting_unavailable(
                "storage does not expose repair_backfill query admission",
            )
        }
        Err(tsink::TsinkError::QueryBudget(error)) => {
            return internal_select_batch_query_error_response(&error)
        }
        Err(_) => {
            return internal_error_response(
                503,
                "query_admission_failed",
                "internal repair_backfill query admission failed",
                true,
            )
        }
    };

    let storage = Arc::clone(storage);
    let shard = payload.shard;
    let ring_version = payload.ring_version;
    let window_start = payload.window_start;
    let window_end = payload.window_end;
    let max_series = payload.max_series;
    let max_rows = payload.max_rows;
    let row_offset = payload.row_offset;
    let worker_execution = execution.clone();
    let repair_task = tokio::task::spawn_blocking(move || {
        collect_internal_repair_backfill_rows_with_execution(
            storage.as_ref(),
            ring_version,
            shard,
            shard_count,
            window_start,
            window_end,
            max_series,
            max_rows,
            row_offset,
            &worker_execution,
        )
    })
    .await;

    match repair_task {
        Ok(Ok((backfill, result_reservation))) => {
            let payload = InternalAccountedRepairBackfillResponse {
                backfill,
                accounting: execution.snapshot(),
            };
            match encode_internal_accounted_json_response(&payload, &execution) {
                Ok((response, response_reservation)) => {
                    drop(payload);
                    drop(result_reservation);
                    drop(response_reservation);
                    response
                }
                Err(response) => response,
            }
        }
        Ok(Err(InternalRepairBackfillExecutionError::Storage(tsink::TsinkError::QueryBudget(
            error,
        )))) => internal_select_batch_query_error_response(&error),
        Ok(Err(InternalRepairBackfillExecutionError::InvalidAccounting)) => {
            internal_error_response(
                500,
                "query_accounting_invalid",
                "bounded repair_backfill storage returned invalid accounting",
                false,
            )
        }
        Ok(Err(InternalRepairBackfillExecutionError::Storage(_))) => internal_error_response(
            503,
            "repair_backfill_failed",
            "internal repair_backfill failed",
            true,
        ),
        Err(_) => internal_error_response(
            503,
            "repair_backfill_task_failed",
            "internal repair_backfill task failed",
            true,
        ),
    }
}

pub(super) async fn handle_internal_control_append(
    request: &HttpRequest,
    internal_api: Option<&InternalApiConfig>,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    if let Err(response) = authorize_internal_cluster_request(
        request,
        internal_api,
        cluster_context,
        false,
        &[CLUSTER_CAPABILITY_CONTROL_REPLICATION_V1],
    ) {
        return response;
    }

    let Some(consensus) = cluster_context.and_then(|context| context.control_consensus.as_ref())
    else {
        return internal_error_response(
            503,
            "control_plane_unavailable",
            "cluster control consensus runtime is not available",
            true,
        );
    };

    let payload: InternalControlAppendRequest = match parse_internal_json_body(request) {
        Ok(payload) => payload,
        Err(response) => return response,
    };

    match consensus.handle_append_request(payload) {
        Ok(response) => json_response(200, &response),
        Err(err) => internal_control_consensus_error_response(
            &err,
            consensus.persistence_status().fenced,
            "control_append_failed",
            format!("control append failed: {err}"),
        ),
    }
}

pub(super) async fn handle_internal_control_install_snapshot(
    request: &HttpRequest,
    internal_api: Option<&InternalApiConfig>,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    if let Err(response) = authorize_internal_cluster_request(
        request,
        internal_api,
        cluster_context,
        false,
        &[
            CLUSTER_CAPABILITY_CONTROL_REPLICATION_V1,
            CLUSTER_CAPABILITY_CONTROL_SNAPSHOT_RPC_V1,
        ],
    ) {
        return response;
    }

    let Some(consensus) = cluster_context.and_then(|context| context.control_consensus.as_ref())
    else {
        return internal_error_response(
            503,
            "control_plane_unavailable",
            "cluster control consensus runtime is not available",
            true,
        );
    };

    let payload: InternalControlInstallSnapshotRequest = match parse_internal_json_body(request) {
        Ok(payload) => payload,
        Err(response) => return response,
    };

    match consensus.handle_install_snapshot_request(payload) {
        Ok(response) => json_response(200, &response),
        Err(err) => internal_control_consensus_error_response(
            &err,
            consensus.persistence_status().fenced,
            "control_install_snapshot_failed",
            format!("control install_snapshot failed: {err}"),
        ),
    }
}

pub(super) async fn handle_internal_control_auto_join(
    request: &HttpRequest,
    internal_api: Option<&InternalApiConfig>,
    cluster_context: Option<&ClusterRequestContext>,
) -> HttpResponse {
    if let Err(response) = authorize_internal_cluster_request(
        request,
        internal_api,
        cluster_context,
        true,
        &[CLUSTER_CAPABILITY_CONTROL_REPLICATION_V1],
    ) {
        return response;
    }

    let Some(cluster_context) = cluster_context else {
        return internal_error_response(
            503,
            "control_plane_unavailable",
            "cluster runtime is not available",
            true,
        );
    };
    let Some(consensus) = cluster_context.control_consensus.as_ref() else {
        return internal_error_response(
            503,
            "control_plane_unavailable",
            "cluster control consensus runtime is not available",
            true,
        );
    };

    let payload: InternalControlAutoJoinRequest = match parse_internal_json_body(request) {
        Ok(payload) => payload,
        Err(response) => return response,
    };
    let command = InternalControlCommand::JoinNode {
        node_id: payload.node_id.clone(),
        endpoint: payload.endpoint.clone(),
    };

    if !consensus.is_local_control_leader() {
        if let Err(err) = consensus
            .ensure_leader_established(&cluster_context.rpc_client)
            .await
        {
            return internal_control_consensus_error_response(
                &err,
                consensus.persistence_status().fenced,
                "control_leader_establish_failed",
                format!("failed to establish control leader before auto_join: {err}"),
            );
        }
    }
    if !consensus.is_local_control_leader() {
        let persistence = consensus.persistence_status();
        if persistence.fenced {
            return internal_control_error_response_from_contract(
                internal_control_error_contract(
                    false,
                    true,
                    false,
                    false,
                    "control_leader_establish_failed",
                ),
                persistence.detail.unwrap_or_else(|| {
                    "control persistence remains fenced after leader establishment".to_string()
                }),
            );
        }
        return internal_error_response(
            409,
            "not_control_leader",
            format!(
                "node '{}' is not the active control leader",
                cluster_context.runtime.membership.local_node_id
            ),
            false,
        );
    }

    let current_state = consensus.current_state();
    let preview_outcome = match preview_membership_command(&current_state, &command) {
        Ok(outcome) => outcome,
        Err(err) => {
            return internal_error_response(409, "invalid_membership_mutation", err, false);
        }
    };
    if preview_outcome == ControlMembershipMutationOutcome::Noop {
        let node_status = current_state
            .node_record(&payload.node_id)
            .map(|node| node.status.as_str())
            .unwrap_or("unknown")
            .to_string();
        return json_response(
            200,
            &InternalControlAutoJoinResponse {
                result: "noop".to_string(),
                membership_epoch: current_state.membership_epoch,
                node_status,
                leader_node_id: current_state.leader_node_id,
            },
        );
    }

    match consensus
        .propose_command(&cluster_context.rpc_client, command)
        .await
    {
        Ok(ProposeOutcome::Committed { .. }) => {
            let state = consensus.current_state();
            let node_status = state
                .node_record(&payload.node_id)
                .map(|node| node.status.as_str())
                .unwrap_or("unknown")
                .to_string();
            json_response(
                200,
                &InternalControlAutoJoinResponse {
                    result: "accepted".to_string(),
                    membership_epoch: state.membership_epoch,
                    node_status,
                    leader_node_id: state.leader_node_id,
                },
            )
        }
        Ok(ProposeOutcome::CommittedCheckpointPending { .. }) => {
            let state = consensus.current_state();
            let node_status = state
                .node_record(&payload.node_id)
                .map(|node| node.status.as_str())
                .unwrap_or("unknown")
                .to_string();
            json_response(
                200,
                &InternalControlAutoJoinResponse {
                    result: "accepted_checkpoint_pending".to_string(),
                    membership_epoch: state.membership_epoch,
                    node_status,
                    leader_node_id: state.leader_node_id,
                },
            )
        }
        Ok(ProposeOutcome::CommittedCleanupPending { .. }) => {
            let state = consensus.current_state();
            let node_status = state
                .node_record(&payload.node_id)
                .map(|node| node.status.as_str())
                .unwrap_or("unknown")
                .to_string();
            json_response(
                200,
                &InternalControlAutoJoinResponse {
                    result: "accepted_cleanup_pending".to_string(),
                    membership_epoch: state.membership_epoch,
                    node_status,
                    leader_node_id: state.leader_node_id,
                },
            )
        }
        Ok(ProposeOutcome::CommittedPersistencePending { .. }) => {
            let state = consensus.current_state();
            let node_status = state
                .node_record(&payload.node_id)
                .map(|node| node.status.as_str())
                .unwrap_or("unknown")
                .to_string();
            json_response(
                200,
                &InternalControlAutoJoinResponse {
                    result: "accepted_persistence_pending".to_string(),
                    membership_epoch: state.membership_epoch,
                    node_status,
                    leader_node_id: state.leader_node_id,
                },
            )
        }
        Ok(ProposeOutcome::Pending { .. }) => {
            let state = consensus.current_state();
            let node_status = state
                .node_record(&payload.node_id)
                .map(|node| node.status.as_str())
                .unwrap_or("unknown")
                .to_string();
            json_response(
                202,
                &InternalControlAutoJoinResponse {
                    result: "pending".to_string(),
                    membership_epoch: state.membership_epoch,
                    node_status,
                    leader_node_id: state.leader_node_id,
                },
            )
        }
        Err(err) => internal_control_consensus_error_response(
            &err,
            consensus.persistence_status().fenced,
            "control_mutation_failed",
            format!("control auto_join failed: {err}"),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::rpc::InternalErrorResponse;
    use std::collections::HashMap;
    use std::sync::Barrier;

    fn make_storage() -> Arc<dyn Storage> {
        StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(crate::cluster::config::DEFAULT_CLUSTER_SHARDS)
            .build()
            .expect("storage should build")
    }

    #[test]
    fn legacy_internal_read_limits_are_finite_and_tightened_by_storage() {
        let mut configured = tsink::ResourceLimits::test().query;
        configured.per_query.max_series_matched = Some(7);
        let storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_resource_profile(tsink::ResourceProfile::Test)
            .with_query_budget_limits(configured)
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(crate::cluster::config::DEFAULT_CLUSTER_SHARDS)
            .build()
            .expect("bounded storage should build");

        let limits = default_internal_read_query_limits(storage.as_ref());

        assert_eq!(
            limits,
            tsink::ResourceLimits::server()
                .query
                .per_query
                .tightened_by(configured.per_query)
        );
        assert_eq!(limits.max_series_matched, Some(7));
        assert!(limits.max_samples_scanned.is_some());
        assert!(limits.max_samples_returned.is_some());
        assert!(limits.max_returned_bytes.is_some());
        assert!(limits.max_pattern_expansion.is_some());
        assert!(limits.max_steps.is_some());
        assert!(limits.max_intermediate_vector_size.is_some());
        assert!(limits.max_memory_bytes.is_some());
        assert!(limits.max_wall_time.is_some());
    }

    fn internal_api() -> InternalApiConfig {
        InternalApiConfig::new(
            "cluster-test-token".to_string(),
            INTERNAL_RPC_PROTOCOL_VERSION.to_string(),
            false,
            Vec::new(),
        )
    }

    fn internal_headers(
        token: Option<&str>,
        version: Option<&str>,
        extra: &[(&str, &str)],
    ) -> HashMap<String, String> {
        let mut headers = HashMap::new();
        if let Some(token) = token {
            headers.insert(INTERNAL_RPC_AUTH_HEADER.to_string(), token.to_string());
        }
        if let Some(version) = version {
            headers.insert(INTERNAL_RPC_VERSION_HEADER.to_string(), version.to_string());
            headers.insert(
                INTERNAL_RPC_CAPABILITIES_HEADER.to_string(),
                crate::cluster::rpc::CompatibilityProfile::default()
                    .capabilities
                    .join(","),
            );
        }
        for (name, value) in extra {
            headers.insert((*name).to_string(), (*value).to_string());
        }
        headers
    }

    fn internal_restore_request(
        internal_api: &InternalApiConfig,
        path: &str,
        snapshot_path: &Path,
        data_path: &Path,
    ) -> HttpRequest {
        HttpRequest {
            method: "POST".to_string(),
            path: path.to_string(),
            headers: internal_headers(
                Some(&internal_api.auth_token),
                Some(INTERNAL_RPC_PROTOCOL_VERSION),
                &[("content-type", "application/json")],
            ),
            body: serde_json::to_vec(&InternalDataRestoreRequest {
                snapshot_path: snapshot_path.display().to_string(),
                data_path: data_path.display().to_string(),
            })
            .expect("restore request should encode"),
        }
    }

    #[tokio::test]
    async fn internal_restore_alias_and_budgeted_endpoint_fail_closed_with_stable_contracts() {
        let temp_dir = tempfile::tempdir().expect("tempdir should build");
        let source_path = temp_dir.path().join("source");
        let snapshot_path = temp_dir.path().join("source.snapshot");
        let source: Arc<dyn Storage> = StorageBuilder::new()
            .with_data_path(&source_path)
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(crate::cluster::config::DEFAULT_CLUSTER_SHARDS)
            .build()
            .expect("source storage should build");
        source
            .insert_rows(&[Row::new(
                "restore_budget_metric",
                DataPoint::new(1_700_000_000_000, 1.0),
            )])
            .expect("source row should insert");
        source
            .snapshot(&snapshot_path)
            .expect("source snapshot should build");

        let internal_api = internal_api();
        let offline_root = temp_dir.path().join("offline-restores");
        let quota_budget = tsink::LocalDiskBudget::open(
            &offline_root,
            tsink::LocalDiskLimits {
                max_bytes: Some(1),
                ..tsink::LocalDiskLimits::default()
            },
        )
        .expect("quota budget should open");
        for (path, budgeted) in [
            ("/internal/v1/restore_data", false),
            ("/internal/v1/restore_data_budgeted", true),
        ] {
            let destination = offline_root.join(if budgeted { "new" } else { "legacy" });
            let mut request =
                internal_restore_request(&internal_api, path, &snapshot_path, &destination);
            if !budgeted {
                request.headers.insert(
                    INTERNAL_RPC_CAPABILITIES_HEADER.to_string(),
                    CLUSTER_CAPABILITY_RPC_V1.to_string(),
                );
            }
            let response = if budgeted {
                handle_internal_restore_data_budgeted(
                    &request,
                    Some(&internal_api),
                    None,
                    None,
                    None,
                    Some(&quota_budget),
                )
                .await
            } else {
                handle_internal_restore_data(
                    &request,
                    Some(&internal_api),
                    None,
                    None,
                    None,
                    Some(&quota_budget),
                )
                .await
            };
            assert_eq!(response.status, 413, "endpoint {path}");
            let body: InternalErrorResponse =
                serde_json::from_slice(&response.body).expect("quota response should decode");
            assert_eq!(body.code, "write_disk_quota_exceeded");
            assert!(!body.retryable);
            assert_eq!(
                response_header_value(&response, WRITE_ERROR_CODE_HEADER),
                Some("write_disk_quota_exceeded")
            );
        }

        let unconfigured_request = internal_restore_request(
            &internal_api,
            "/internal/v1/restore_data",
            &snapshot_path,
            &offline_root.join("unconfigured"),
        );
        let unconfigured = handle_internal_restore_data(
            &unconfigured_request,
            Some(&internal_api),
            None,
            None,
            None,
            None,
        )
        .await;
        assert_eq!(unconfigured.status, 503);
        let body: InternalErrorResponse = serde_json::from_slice(&unconfigured.body)
            .expect("unconfigured response should decode");
        assert_eq!(body.code, "offline_restore_unconfigured");
        assert!(!body.retryable);

        let roomy_budget = tsink::LocalDiskBudget::open(
            temp_dir.path().join("roomy-offline-restores"),
            tsink::LocalDiskLimits {
                max_bytes: Some(64 * 1024 * 1024),
                ..tsink::LocalDiskLimits::default()
            },
        )
        .expect("roomy budget should open");
        let outside_request = internal_restore_request(
            &internal_api,
            "/internal/v1/restore_data_budgeted",
            &snapshot_path,
            &temp_dir.path().join("outside-offline-root"),
        );
        let outside = handle_internal_restore_data_budgeted(
            &outside_request,
            Some(&internal_api),
            None,
            None,
            None,
            Some(&roomy_budget),
        )
        .await;
        assert_eq!(outside.status, 422);
        let body: InternalErrorResponse =
            serde_json::from_slice(&outside.body).expect("path response should decode");
        assert_eq!(body.code, "invalid_restore_path");
        assert!(!body.retryable);

        let mut missing_capability_request = internal_restore_request(
            &internal_api,
            "/internal/v1/restore_data_budgeted",
            &snapshot_path,
            &roomy_budget.root().join("missing-capability"),
        );
        missing_capability_request.headers.insert(
            INTERNAL_RPC_CAPABILITIES_HEADER.to_string(),
            CLUSTER_CAPABILITY_RPC_V1.to_string(),
        );
        let missing_capability = handle_internal_restore_data_budgeted(
            &missing_capability_request,
            Some(&internal_api),
            None,
            None,
            None,
            Some(&roomy_budget),
        )
        .await;
        assert_eq!(missing_capability.status, 409);
        let body: InternalErrorResponse = serde_json::from_slice(&missing_capability.body)
            .expect("capability response should decode");
        assert_eq!(body.code, "peer_capability_missing");
        assert_eq!(
            body.missing_capabilities,
            vec![CLUSTER_CAPABILITY_BUDGETED_RESTORE_V1.to_string()]
        );

        source.close().expect("source storage should close");
    }

    #[tokio::test]
    async fn internal_snapshot_rejects_destinations_inside_offline_restore_root() {
        let temp_dir = tempfile::tempdir().expect("tempdir should build");
        let offline_restore_disk_budget = tsink::LocalDiskBudget::open(
            temp_dir.path().join("offline-restores"),
            tsink::LocalDiskLimits {
                max_bytes: Some(64 * 1024 * 1024),
                ..tsink::LocalDiskLimits::default()
            },
        )
        .expect("offline restore budget should open");
        let destination = offline_restore_disk_budget
            .root()
            .join("forbidden-snapshot");
        let internal_api = internal_api();
        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/snapshot_data".to_string(),
            headers: internal_headers(
                Some(&internal_api.auth_token),
                Some(INTERNAL_RPC_PROTOCOL_VERSION),
                &[("content-type", "application/json")],
            ),
            body: serde_json::to_vec(&InternalDataSnapshotRequest {
                path: destination.display().to_string(),
            })
            .expect("snapshot request should encode"),
        };
        let response = handle_internal_snapshot_data(
            &make_storage(),
            &Arc::new(MetricMetadataStore::in_memory()),
            &Arc::new(ExemplarStore::in_memory()),
            None,
            &request,
            Some(&internal_api),
            None,
            None,
            Some(&offline_restore_disk_budget),
        )
        .await;
        assert_eq!(response.status, 422);
        let body: InternalErrorResponse =
            serde_json::from_slice(&response.body).expect("snapshot response should decode");
        assert_eq!(body.code, "invalid_snapshot_path");
        assert!(!body.retryable);
        assert!(!destination.exists());
    }

    #[test]
    fn control_persistence_resource_and_fence_contracts_are_nonretryable() {
        let quota = internal_control_error_response_from_contract(
            internal_control_error_contract(true, false, false, false, "fallback"),
            "cluster quota rejected the control publication".to_string(),
        );
        assert_eq!(quota.status, 413);
        let quota_body: InternalErrorResponse =
            serde_json::from_slice(&quota.body).expect("quota body should decode");
        assert_eq!(quota_body.code, "write_disk_quota_exceeded");
        assert!(!quota_body.retryable);
        assert_eq!(
            response_header_value(&quota, WRITE_ERROR_CODE_HEADER),
            Some("write_disk_quota_exceeded")
        );

        for contract in [
            internal_control_error_contract(false, true, false, false, "fallback"),
            internal_control_error_contract(false, false, true, false, "fallback"),
            internal_control_error_contract(true, true, false, false, "fallback"),
        ] {
            let response = internal_control_error_response_from_contract(
                contract,
                "control persistence requires authoritative repair".to_string(),
            );
            assert_eq!(response.status, 503);
            let body: InternalErrorResponse =
                serde_json::from_slice(&response.body).expect("fenced body should decode");
            assert_eq!(body.code, CONTROL_PERSISTENCE_INDETERMINATE_ERROR_CODE);
            assert!(!body.retryable);
            assert_eq!(
                response_header_value(&response, WRITE_ERROR_CODE_HEADER),
                Some(CONTROL_PERSISTENCE_INDETERMINATE_ERROR_CODE)
            );
        }
    }

    fn metric_series(metric: &str, labels: &[(&str, &str)]) -> MetricSeries {
        MetricSeries {
            name: metric.to_string(),
            labels: labels
                .iter()
                .map(|(name, value)| Label::new(*name, *value))
                .collect(),
        }
    }

    fn series_points(metric: &str, labels: &[(&str, &str)], points: &[(i64, f64)]) -> SeriesPoints {
        SeriesPoints {
            series: metric_series(metric, labels),
            points: points
                .iter()
                .map(|(timestamp, value)| DataPoint::new(*timestamp, *value))
                .collect(),
        }
    }

    #[test]
    fn merge_metric_series_distinguishes_delimiter_collision_series() {
        let left = metric_series("cpu", &[("job", "api,zone=west|prod\u{1f}blue")]);
        let right = metric_series("cpu", &[("job", "api"), ("zone", "west|prod\u{1f}blue")]);

        let merged = merge_metric_series(vec![left.clone()], vec![right.clone()]);

        assert_eq!(merged.len(), 2);
        assert!(merged.iter().any(|series| series == &left));
        assert!(merged.iter().any(|series| series == &right));
    }

    #[test]
    fn merge_handoff_series_points_distinguishes_delimiter_collision_series() {
        let left_series = metric_series("cpu", &[("job", "api,zone=west|prod\u{1f}blue")]);
        let right_series = metric_series("cpu", &[("job", "api"), ("zone", "west|prod\u{1f}blue")]);

        let merged = merge_handoff_series_points(
            &[left_series.clone(), right_series.clone()],
            vec![series_points(
                "cpu",
                &[("job", "api,zone=west|prod\u{1f}blue")],
                &[(10, 1.0)],
            )],
            vec![series_points(
                "cpu",
                &[("job", "api"), ("zone", "west|prod\u{1f}blue")],
                &[(20, 2.0)],
            )],
        );

        assert_eq!(merged.len(), 2);
        assert_eq!(merged[0].series, left_series);
        assert_eq!(merged[0].points, vec![DataPoint::new(10, 1.0)]);
        assert_eq!(merged[1].series, right_series);
        assert_eq!(merged[1].points, vec![DataPoint::new(20, 2.0)]);
    }

    #[tokio::test]
    async fn internal_handlers_round_trip_rows_without_router_indirection() {
        let storage = make_storage();
        let internal_api = internal_api();

        let ingest_request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/ingest_rows".to_string(),
            headers: internal_headers(
                Some(&internal_api.auth_token),
                Some(INTERNAL_RPC_PROTOCOL_VERSION),
                &[("content-type", "application/json")],
            ),
            body: serde_json::to_vec(&InternalIngestRowsRequest {
                ring_version: DEFAULT_INTERNAL_RING_VERSION,
                idempotency_key: Some("tsink:test:internal-module-roundtrip".to_string()),
                required_capabilities: Vec::new(),
                rows: vec![InternalRow {
                    metric: "internal_metric".to_string(),
                    labels: vec![Label::new("node", "a")],
                    data_point: DataPoint::new(1_700_000_000_000, 12.5),
                }],
            })
            .expect("payload should serialize"),
        };

        let ingest_response =
            handle_internal_ingest_rows(&storage, &ingest_request, Some(&internal_api), None, None)
                .await;
        assert_eq!(ingest_response.status, 200);
        let ingest_body: InternalIngestRowsResponse =
            serde_json::from_slice(&ingest_response.body).expect("response JSON should decode");
        assert_eq!(ingest_body.inserted_rows, 1);
        let write_result = ingest_body
            .write_result
            .expect("canonical write result should be present");
        assert_eq!(write_result.submitted, 1);
        assert_eq!(write_result.accepted, 1);
        assert_eq!(write_result.rejected, 0);
        assert_eq!(
            write_result.acknowledgement,
            Some(WriteAcknowledgement::Volatile)
        );

        let select_request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/select".to_string(),
            headers: internal_headers(
                Some(&internal_api.auth_token),
                Some(INTERNAL_RPC_PROTOCOL_VERSION),
                &[("content-type", "application/json")],
            ),
            body: serde_json::to_vec(&InternalSelectRequest {
                ring_version: DEFAULT_INTERNAL_RING_VERSION,
                metric: "internal_metric".to_string(),
                labels: vec![Label::new("node", "a")],
                start: 1_700_000_000_000,
                end: 1_700_000_000_100,
            })
            .expect("payload should serialize"),
        };

        let select_response =
            handle_internal_select(&storage, &select_request, Some(&internal_api), None).await;
        assert_eq!(select_response.status, 200);
        let select_body: InternalSelectResponse =
            serde_json::from_slice(&select_response.body).expect("response JSON should decode");
        assert_eq!(select_body.points.len(), 1);
        assert_eq!(select_body.points[0].value.as_f64(), Some(12.5));
    }

    #[tokio::test]
    async fn internal_ingest_rows_returns_canonical_atomic_rejection() {
        let storage = make_storage();
        let internal_api = internal_api();
        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/ingest_rows".to_string(),
            headers: internal_headers(
                Some(&internal_api.auth_token),
                Some(INTERNAL_RPC_PROTOCOL_VERSION),
                &[("content-type", "application/json")],
            ),
            body: serde_json::to_vec(&InternalIngestRowsRequest {
                ring_version: DEFAULT_INTERNAL_RING_VERSION,
                idempotency_key: Some("tsink:test:internal-row-rejection".to_string()),
                required_capabilities: Vec::new(),
                rows: vec![InternalRow {
                    metric: "internal_rejected_metric".to_string(),
                    labels: vec![Label::new("duplicate", "a"), Label::new("duplicate", "b")],
                    data_point: DataPoint::new(1_700_000_000_000, 1.0),
                }],
            })
            .expect("payload should serialize"),
        };

        let response =
            handle_internal_ingest_rows(&storage, &request, Some(&internal_api), None, None).await;
        assert_eq!(response.status, 200);
        let body: InternalIngestRowsResponse =
            serde_json::from_slice(&response.body).expect("response should decode");
        assert_eq!(body.inserted_rows, 0);
        let result = body
            .write_result
            .expect("canonical rejection result should be present");
        assert_eq!(result.submitted, 1);
        assert_eq!(result.accepted, 0);
        assert_eq!(result.rejected, 1);
        assert_eq!(result.acknowledgement, None);
        assert!(matches!(
            &result.outcomes[0].status,
            RowWriteStatus::Rejected(rejection)
                if rejection.category == WriteRejectionCategory::InvalidLabels
        ));
    }

    #[tokio::test]
    async fn internal_ingest_rows_replays_exact_canonical_result_after_restart() {
        let storage = make_storage();
        let internal_api = internal_api();
        let temp_dir = tempfile::tempdir().expect("tempdir should build");
        let dedupe_path = temp_dir.path().join("row-dedupe.log");
        let dedupe_config = crate::cluster::dedupe::DedupeConfig {
            window_secs: 60,
            max_entries: 32,
            max_log_bytes: 8 * 1024,
            cleanup_interval_secs: 1,
        };
        let dedupe_store = Arc::new(
            DedupeWindowStore::open(dedupe_path.clone(), dedupe_config)
                .expect("dedupe store should open"),
        );
        let edge_sync_context = edge_sync::EdgeSyncRuntimeContext {
            source: None,
            accept_dedupe_store: Some(Arc::clone(&dedupe_store)),
            accept_dedupe_config: Some(dedupe_config),
        };
        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/ingest_rows".to_string(),
            headers: internal_headers(
                Some(&internal_api.auth_token),
                Some(INTERNAL_RPC_PROTOCOL_VERSION),
                &[("content-type", "application/json")],
            ),
            body: serde_json::to_vec(&InternalIngestRowsRequest {
                ring_version: DEFAULT_INTERNAL_RING_VERSION,
                idempotency_key: Some("tsink:test:internal-row-replay".to_string()),
                required_capabilities: Vec::new(),
                rows: vec![InternalRow {
                    metric: "internal_replay_metric".to_string(),
                    labels: vec![Label::new("node", "a")],
                    data_point: DataPoint::new(1_700_000_000_000, 3.0),
                }],
            })
            .expect("payload should serialize"),
        };

        let first = handle_internal_ingest_rows(
            &storage,
            &request,
            Some(&internal_api),
            None,
            Some(&edge_sync_context),
        )
        .await;
        assert_eq!(first.status, 200);
        let first_body: InternalIngestRowsResponse =
            serde_json::from_slice(&first.body).expect("first response should decode");
        assert_eq!(first_body.inserted_rows, 1);
        assert!(first_body.write_result.is_some());
        let first_acknowledgement = first
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(WRITE_ACKNOWLEDGEMENT_HEADER))
            .map(|(_, value)| value.clone());

        drop(edge_sync_context);
        drop(dedupe_store);
        let reopened = Arc::new(
            DedupeWindowStore::open(dedupe_path, dedupe_config)
                .expect("dedupe store should reopen"),
        );
        let reopened_edge_sync_context = edge_sync::EdgeSyncRuntimeContext {
            source: None,
            accept_dedupe_store: Some(reopened),
            accept_dedupe_config: Some(dedupe_config),
        };
        let replay = handle_internal_ingest_rows(
            &storage,
            &request,
            Some(&internal_api),
            None,
            Some(&reopened_edge_sync_context),
        )
        .await;

        assert_eq!(replay.status, 200);
        assert_eq!(replay.body, first.body);
        assert!(replay.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("X-Tsink-Idempotency-Replayed") && value == "true"
        }));
        let replay_acknowledgement = replay
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(WRITE_ACKNOWLEDGEMENT_HEADER))
            .map(|(_, value)| value.clone());
        assert_eq!(replay_acknowledgement, first_acknowledgement);
        let points = storage
            .select(
                "internal_replay_metric",
                &[Label::new("node", "a")],
                1_700_000_000_000,
                1_700_000_000_001,
            )
            .expect("stored point should be readable");
        assert_eq!(points.len(), 1);
    }

    #[tokio::test]
    async fn internal_ingest_rows_surfaces_dedupe_persistence_failure_after_row_commit() {
        let storage = make_storage();
        let internal_api = internal_api();
        let temp_dir = tempfile::tempdir().expect("tempdir should build");
        let dedupe_config = crate::cluster::dedupe::DedupeConfig {
            window_secs: 60,
            max_entries: 32,
            max_log_bytes: 8 * 1024,
            cleanup_interval_secs: 30,
        };
        let dedupe_store = Arc::new(
            DedupeWindowStore::open(temp_dir.path().join("dedupe.log"), dedupe_config)
                .expect("dedupe store should open"),
        );
        let edge_sync_context = edge_sync::EdgeSyncRuntimeContext {
            source: None,
            accept_dedupe_store: Some(Arc::clone(&dedupe_store)),
            accept_dedupe_config: Some(dedupe_config),
        };
        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/ingest_rows".to_string(),
            headers: internal_headers(
                Some(&internal_api.auth_token),
                Some(INTERNAL_RPC_PROTOCOL_VERSION),
                &[("content-type", "application/json")],
            ),
            body: serde_json::to_vec(&InternalIngestRowsRequest {
                ring_version: DEFAULT_INTERNAL_RING_VERSION,
                idempotency_key: Some("tsink:test:dedupe-persist-failure".to_string()),
                required_capabilities: Vec::new(),
                rows: vec![InternalRow {
                    metric: "dedupe_persist_failure_metric".to_string(),
                    labels: vec![Label::new("node", "a")],
                    data_point: DataPoint::new(1_700_000_000_000, 9.0),
                }],
            })
            .expect("payload should serialize"),
        };

        dedupe_store.fail_next_append_at(crate::cluster::dedupe::DedupePersistenceStage::Append);
        let first = handle_internal_ingest_rows(
            &storage,
            &request,
            Some(&internal_api),
            None,
            Some(&edge_sync_context),
        )
        .await;

        assert_eq!(first.status, 503);
        let error: InternalErrorResponse =
            serde_json::from_slice(&first.body).expect("error response should decode");
        assert_eq!(error.code, "dedupe_persistence_failed");
        assert_eq!(
            error.error,
            "cluster dedupe completion marker persistence failed during record append"
        );
        assert!(error.retryable);
        assert!(first.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case(WRITE_PARTIAL_HEADER) && value == "true"
        }));
        assert!(first.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case(WRITE_ROWS_ACCEPTED_HEADER) && value == "1"
        }));
        assert!(first.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case(WRITE_ACKNOWLEDGEMENT_HEADER)
                && value == WriteAcknowledgement::Volatile.as_str()
        }));

        let replay = handle_internal_ingest_rows(
            &storage,
            &request,
            Some(&internal_api),
            None,
            Some(&edge_sync_context),
        )
        .await;
        assert_eq!(replay.status, 200);
        assert!(replay.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("X-Tsink-Idempotency-Replayed") && value == "true"
        }));
        let points = storage
            .select(
                "dedupe_persist_failure_metric",
                &[Label::new("node", "a")],
                1_700_000_000_000,
                1_700_000_000_001,
            )
            .expect("stored point should be readable");
        assert_eq!(points.len(), 1);
    }

    #[tokio::test]
    async fn internal_ingest_rows_maps_dedupe_disk_quota_after_row_commit() {
        let storage = make_storage();
        let internal_api = internal_api();
        let temp_dir = tempfile::tempdir().expect("tempdir should build");
        let local_disk_budget = tsink::LocalDiskBudget::open(
            temp_dir.path(),
            tsink::LocalDiskLimits {
                max_bytes: Some(1),
                ..tsink::LocalDiskLimits::default()
            },
        )
        .expect("disk budget should open");
        let dedupe_config = crate::cluster::dedupe::DedupeConfig {
            window_secs: 60,
            max_entries: 32,
            max_log_bytes: 8 * 1024,
            cleanup_interval_secs: 30,
        };
        let dedupe_store = Arc::new(
            DedupeWindowStore::open_with_disk_budget(
                temp_dir.path().join("edge_sync/dedupe.log"),
                dedupe_config,
                Some(Arc::clone(&local_disk_budget)),
                tsink::DiskCategory::EdgeSync,
            )
            .expect("dedupe store should open"),
        );
        let edge_sync_context = edge_sync::EdgeSyncRuntimeContext {
            source: None,
            accept_dedupe_store: Some(Arc::clone(&dedupe_store)),
            accept_dedupe_config: Some(dedupe_config),
        };
        let make_request = |key: &str, metric: &str| HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/ingest_rows".to_string(),
            headers: internal_headers(
                Some(&internal_api.auth_token),
                Some(INTERNAL_RPC_PROTOCOL_VERSION),
                &[("content-type", "application/json")],
            ),
            body: serde_json::to_vec(&InternalIngestRowsRequest {
                ring_version: DEFAULT_INTERNAL_RING_VERSION,
                idempotency_key: Some(key.to_string()),
                required_capabilities: Vec::new(),
                rows: vec![InternalRow {
                    metric: metric.to_string(),
                    labels: vec![Label::new("node", "a")],
                    data_point: DataPoint::new(1_700_000_000_000, 9.0),
                }],
            })
            .expect("payload should serialize"),
        };
        let request = make_request("tsink:test:dedupe-disk-quota", "dedupe_disk_quota_metric");

        let first = handle_internal_ingest_rows(
            &storage,
            &request,
            Some(&internal_api),
            None,
            Some(&edge_sync_context),
        )
        .await;

        assert_eq!(first.status, 413);
        let error: InternalErrorResponse =
            serde_json::from_slice(&first.body).expect("error response should decode");
        assert_eq!(error.code, "write_disk_quota_exceeded");
        assert!(!error.retryable);
        assert_eq!(
            response_header_value(&first, WRITE_ERROR_CODE_HEADER),
            Some("write_disk_quota_exceeded")
        );
        assert_eq!(
            response_header_value(&first, WRITE_PARTIAL_HEADER),
            Some("true")
        );
        assert_eq!(
            response_header_value(&first, WRITE_ROWS_ACCEPTED_HEADER),
            Some("1")
        );
        assert_eq!(
            response_header_value(&first, WRITE_ACKNOWLEDGEMENT_HEADER),
            Some(WriteAcknowledgement::Volatile.as_str())
        );
        assert_eq!(
            storage
                .select(
                    "dedupe_disk_quota_metric",
                    &[Label::new("node", "a")],
                    1_700_000_000_000,
                    1_700_000_000_001,
                )
                .expect("committed point should be readable")
                .len(),
            1
        );

        let replay = handle_internal_ingest_rows(
            &storage,
            &request,
            Some(&internal_api),
            None,
            Some(&edge_sync_context),
        )
        .await;
        assert_eq!(replay.status, 200);
        assert!(replay.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("X-Tsink-Idempotency-Replayed") && value == "true"
        }));

        let fenced = handle_internal_ingest_rows(
            &storage,
            &make_request(
                "tsink:test:dedupe-disk-quota:new",
                "dedupe_disk_quota_fenced_metric",
            ),
            Some(&internal_api),
            None,
            Some(&edge_sync_context),
        )
        .await;
        assert_eq!(fenced.status, 413);
        let fenced_error: InternalErrorResponse =
            serde_json::from_slice(&fenced.body).expect("fenced error should decode");
        assert_eq!(fenced_error.code, "write_disk_quota_exceeded");
        assert!(!fenced_error.retryable);
        assert!(storage
            .select(
                "dedupe_disk_quota_fenced_metric",
                &[Label::new("node", "a")],
                1_700_000_000_000,
                1_700_000_000_001,
            )
            .expect("fenced metric query should succeed")
            .is_empty());

        let snapshot = local_disk_budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
    }

    #[tokio::test]
    async fn internal_handlers_reject_malformed_json_bodies_directly() {
        let storage = make_storage();
        let internal_api = internal_api();
        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/ingest_rows".to_string(),
            headers: internal_headers(
                Some(&internal_api.auth_token),
                Some(INTERNAL_RPC_PROTOCOL_VERSION),
                &[("content-type", "application/json")],
            ),
            body: br#"{"rows":"not-valid""#.to_vec(),
        };

        let response =
            handle_internal_ingest_rows(&storage, &request, Some(&internal_api), None, None).await;
        assert_eq!(response.status, 400);

        let body: JsonValue =
            serde_json::from_slice(&response.body).expect("response body should be valid JSON");
        assert_eq!(body["code"], "invalid_request");
    }

    #[tokio::test]
    async fn internal_ingest_authenticates_before_parsing_json_body() {
        let storage = make_storage();
        let internal_api = internal_api();
        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/ingest_rows".to_string(),
            headers: internal_headers(None, Some(INTERNAL_RPC_PROTOCOL_VERSION), &[]),
            body: br#"{"rows":"not-valid""#.to_vec(),
        };

        let response =
            handle_internal_ingest_rows(&storage, &request, Some(&internal_api), None, None).await;

        assert_eq!(response.status, 401);
    }

    #[tokio::test]
    async fn internal_handlers_return_canonical_out_of_retention_rejection() {
        let now = crate::handlers::unix_timestamp_millis() as i64;
        let old_ts = now.saturating_sub(120_000);
        let storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_retention(std::time::Duration::from_secs(60))
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(crate::cluster::config::DEFAULT_CLUSTER_SHARDS)
            .build()
            .expect("storage should build");
        let internal_api = internal_api();

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/ingest_rows".to_string(),
            headers: internal_headers(
                Some(&internal_api.auth_token),
                Some(INTERNAL_RPC_PROTOCOL_VERSION),
                &[("content-type", "application/json")],
            ),
            body: serde_json::to_vec(&InternalIngestRowsRequest {
                ring_version: DEFAULT_INTERNAL_RING_VERSION,
                idempotency_key: Some("tsink:test:internal-out-of-retention".to_string()),
                required_capabilities: Vec::new(),
                rows: vec![InternalRow {
                    metric: "internal_metric".to_string(),
                    labels: vec![Label::new("node", "a")],
                    data_point: DataPoint::new(old_ts, 12.5),
                }],
            })
            .expect("payload should serialize"),
        };

        let response =
            handle_internal_ingest_rows(&storage, &request, Some(&internal_api), None, None).await;
        assert_eq!(response.status, 200);

        let body: InternalIngestRowsResponse =
            serde_json::from_slice(&response.body).expect("response JSON should decode");
        assert_eq!(body.inserted_rows, 0);
        let result = body
            .write_result
            .expect("canonical result should be present");
        assert_eq!(result.accepted, 0);
        assert_eq!(result.rejected, 1);
        assert!(matches!(
            &result.outcomes[0].status,
            RowWriteStatus::Rejected(rejection)
                if rejection.category == WriteRejectionCategory::BelowRetentionFloor
                    && rejection.message.contains("outside the retention window")
        ));
    }

    #[tokio::test]
    async fn internal_ingest_write_failure_releases_dedupe_reservation() {
        let storage = make_storage();
        storage.close().expect("storage should close");
        let metadata_store = Arc::new(MetricMetadataStore::in_memory());
        let exemplar_store = Arc::new(ExemplarStore::in_memory());
        let internal_api = internal_api();
        let temp_dir = tempfile::tempdir().expect("tempdir should build");
        let dedupe_config = crate::cluster::dedupe::DedupeConfig {
            window_secs: 60,
            max_entries: 32,
            max_log_bytes: 8 * 1024,
            cleanup_interval_secs: 1,
        };
        let dedupe_store = Arc::new(
            DedupeWindowStore::open(temp_dir.path().join("dedupe.log"), dedupe_config)
                .expect("dedupe store should open"),
        );
        let edge_sync_context = edge_sync::EdgeSyncRuntimeContext {
            source: None,
            accept_dedupe_store: Some(Arc::clone(&dedupe_store)),
            accept_dedupe_config: Some(dedupe_config),
        };
        let key = "tsink:test:internal-write-storage-failure";
        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/ingest_write".to_string(),
            headers: internal_headers(
                Some(&internal_api.auth_token),
                Some(INTERNAL_RPC_PROTOCOL_VERSION),
                &[("content-type", "application/json")],
            ),
            body: serde_json::to_vec(&InternalIngestWriteRequest {
                ring_version: DEFAULT_INTERNAL_RING_VERSION,
                idempotency_key: Some(key.to_string()),
                tenant_id: None,
                required_capabilities: Vec::new(),
                rows: vec![InternalRow {
                    metric: "closed_storage_metric".to_string(),
                    labels: Vec::new(),
                    data_point: DataPoint::new(1_700_000_000_000, 1.0),
                }],
                metadata_updates: Vec::new(),
                exemplars: Vec::new(),
            })
            .expect("payload should serialize"),
        };

        let first = handle_internal_ingest_write(
            &storage,
            &metadata_store,
            &exemplar_store,
            &request,
            Some(&internal_api),
            None,
            Some(&edge_sync_context),
        )
        .await;
        assert_eq!(first.status, 503);

        let second = handle_internal_ingest_write(
            &storage,
            &metadata_store,
            &exemplar_store,
            &request,
            Some(&internal_api),
            None,
            Some(&edge_sync_context),
        )
        .await;
        assert_eq!(second.status, 503);
        let body: InternalErrorResponse =
            serde_json::from_slice(&second.body).expect("error response should decode");
        assert_ne!(body.code, "idempotency_in_flight");

        match dedupe_store
            .begin(key)
            .expect("reservation should be reusable")
        {
            DedupeBeginOutcome::Accepted(reservation) => drop(reservation),
            other => panic!("failed write left an unexpected dedupe state: {other:?}"),
        };
    }

    #[tokio::test]
    async fn internal_ingest_write_preserves_sidecar_disk_quota_errors_and_partial_progress() {
        let storage = make_storage();
        let internal_api = internal_api();
        let temp_dir = tempfile::tempdir().expect("tempdir should build");
        let local_disk_budget = tsink::LocalDiskBudget::open(
            temp_dir.path(),
            tsink::LocalDiskLimits {
                max_bytes: Some(64),
                ..tsink::LocalDiskLimits::default()
            },
        )
        .expect("disk budget should open");
        let metadata_store = Arc::new(
            MetricMetadataStore::open_with_disk_budget(
                Some(temp_dir.path()),
                Some(Arc::clone(&local_disk_budget)),
            )
            .expect("metadata store should open"),
        );
        let exemplar_store = Arc::new(
            ExemplarStore::open_with_disk_budget(
                Some(temp_dir.path()),
                Some(Arc::clone(&local_disk_budget)),
            )
            .expect("exemplar store should open"),
        );

        let make_request = |payload: InternalIngestWriteRequest| HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/ingest_write".to_string(),
            headers: internal_headers(
                Some(&internal_api.auth_token),
                Some(INTERNAL_RPC_PROTOCOL_VERSION),
                &[("content-type", "application/json")],
            ),
            body: serde_json::to_vec(&payload).expect("payload should serialize"),
        };
        let metadata_response = handle_internal_ingest_write(
            &storage,
            &metadata_store,
            &exemplar_store,
            &make_request(InternalIngestWriteRequest {
                ring_version: DEFAULT_INTERNAL_RING_VERSION,
                idempotency_key: None,
                tenant_id: Some("team-a".to_string()),
                required_capabilities: Vec::new(),
                rows: Vec::new(),
                metadata_updates: vec![InternalMetricMetadataUpdate {
                    metric_family_name: "internal_metadata".to_string(),
                    metric_type: MetricType::Gauge as i32,
                    help: "must exceed the tiny shared disk quota".to_string(),
                    unit: String::new(),
                }],
                exemplars: Vec::new(),
            }),
            Some(&internal_api),
            None,
            None,
        )
        .await;
        assert_eq!(metadata_response.status, 413);
        let metadata_error: InternalErrorResponse =
            serde_json::from_slice(&metadata_response.body).expect("error should decode");
        assert_eq!(metadata_error.code, "write_disk_quota_exceeded");
        assert!(!metadata_error.retryable);
        assert_eq!(
            response_header_value(&metadata_response, WRITE_PARTIAL_HEADER),
            None
        );

        let exemplar_response = handle_internal_ingest_write(
            &storage,
            &metadata_store,
            &exemplar_store,
            &make_request(InternalIngestWriteRequest {
                ring_version: DEFAULT_INTERNAL_RING_VERSION,
                idempotency_key: None,
                tenant_id: None,
                required_capabilities: Vec::new(),
                rows: vec![InternalRow {
                    metric: "internal_exemplar".to_string(),
                    labels: Vec::new(),
                    data_point: DataPoint::new(1_700_000_000_000, 2.0),
                }],
                metadata_updates: Vec::new(),
                exemplars: vec![InternalWriteExemplar {
                    metric: "internal_exemplar".to_string(),
                    series_labels: Vec::new(),
                    exemplar_labels: vec![Label::new(
                        "trace_id",
                        "must-exceed-the-tiny-shared-disk-quota",
                    )],
                    timestamp: 1_700_000_000_000,
                    value: 2.0,
                }],
            }),
            Some(&internal_api),
            None,
            None,
        )
        .await;
        assert_eq!(exemplar_response.status, 413);
        let exemplar_error: InternalErrorResponse =
            serde_json::from_slice(&exemplar_response.body).expect("error should decode");
        assert_eq!(exemplar_error.code, "write_disk_quota_exceeded");
        assert!(!exemplar_error.retryable);
        assert_eq!(
            response_header_value(&exemplar_response, WRITE_PARTIAL_HEADER),
            Some("true")
        );
        assert_eq!(
            response_header_value(&exemplar_response, WRITE_ROWS_ACCEPTED_HEADER),
            Some("1")
        );

        let snapshot = local_disk_budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.rejections_total, 2);
    }

    #[tokio::test]
    async fn internal_ingest_write_replays_original_result_after_restart() {
        let storage = make_storage();
        let metadata_store = Arc::new(MetricMetadataStore::in_memory());
        let exemplar_store = Arc::new(ExemplarStore::in_memory_with_config(ExemplarStoreConfig {
            max_total_exemplars: 8,
            max_exemplars_per_series: 1,
            max_exemplars_per_request: 8,
            max_query_results: 8,
            max_query_selectors: 8,
        }));
        let internal_api = internal_api();
        let temp_dir = tempfile::tempdir().expect("tempdir should build");
        let dedupe_path = temp_dir.path().join("dedupe.log");
        let dedupe_config = crate::cluster::dedupe::DedupeConfig {
            window_secs: 60,
            max_entries: 32,
            max_log_bytes: 8 * 1024,
            cleanup_interval_secs: 1,
        };
        let dedupe_store = Arc::new(
            DedupeWindowStore::open(dedupe_path.clone(), dedupe_config)
                .expect("dedupe store should open"),
        );
        let edge_sync_context = edge_sync::EdgeSyncRuntimeContext {
            source: None,
            accept_dedupe_store: Some(dedupe_store),
            accept_dedupe_config: Some(dedupe_config),
        };
        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/ingest_write".to_string(),
            headers: internal_headers(
                Some(&internal_api.auth_token),
                Some(INTERNAL_RPC_PROTOCOL_VERSION),
                &[("content-type", "application/json")],
            ),
            body: serde_json::to_vec(&InternalIngestWriteRequest {
                ring_version: DEFAULT_INTERNAL_RING_VERSION,
                idempotency_key: Some("tsink:test:internal-write-replay".to_string()),
                tenant_id: Some("tenant-a".to_string()),
                required_capabilities: Vec::new(),
                rows: Vec::new(),
                metadata_updates: vec![InternalMetricMetadataUpdate {
                    metric_family_name: "request_duration_seconds".to_string(),
                    metric_type: MetricType::Histogram as i32,
                    help: "request duration".to_string(),
                    unit: "seconds".to_string(),
                }],
                exemplars: vec![
                    InternalWriteExemplar {
                        metric: "request_duration_seconds".to_string(),
                        series_labels: vec![Label::new("service", "api")],
                        exemplar_labels: vec![Label::new("trace_id", "one")],
                        timestamp: 100,
                        value: 1.0,
                    },
                    InternalWriteExemplar {
                        metric: "request_duration_seconds".to_string(),
                        series_labels: vec![Label::new("service", "api")],
                        exemplar_labels: vec![Label::new("trace_id", "two")],
                        timestamp: 200,
                        value: 2.0,
                    },
                ],
            })
            .expect("payload should serialize"),
        };

        let first = handle_internal_ingest_write(
            &storage,
            &metadata_store,
            &exemplar_store,
            &request,
            Some(&internal_api),
            None,
            Some(&edge_sync_context),
        )
        .await;
        assert_eq!(first.status, 200);
        let first_body: InternalIngestWriteResponse =
            serde_json::from_slice(&first.body).expect("first response should decode");
        assert_eq!(first_body.accepted_metadata_updates, 1);
        assert_eq!(first_body.accepted_exemplars, 2);
        assert_eq!(first_body.dropped_exemplars, 1);
        let first_acknowledgement = first
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(WRITE_ACKNOWLEDGEMENT_HEADER))
            .map(|(_, value)| value.as_str());
        assert_eq!(
            first_acknowledgement,
            Some(WriteAcknowledgement::Volatile.as_str())
        );

        drop(edge_sync_context);
        let reopened = Arc::new(
            DedupeWindowStore::open(dedupe_path, dedupe_config)
                .expect("dedupe store should reopen"),
        );
        let reopened_edge_sync_context = edge_sync::EdgeSyncRuntimeContext {
            source: None,
            accept_dedupe_store: Some(reopened),
            accept_dedupe_config: Some(dedupe_config),
        };
        let replay = handle_internal_ingest_write(
            &storage,
            &metadata_store,
            &exemplar_store,
            &request,
            Some(&internal_api),
            None,
            Some(&reopened_edge_sync_context),
        )
        .await;
        assert_eq!(replay.status, 200);
        assert!(replay.headers.iter().any(|(name, value)| {
            name.eq_ignore_ascii_case("X-Tsink-Idempotency-Replayed") && value == "true"
        }));
        let replay_acknowledgement = replay
            .headers
            .iter()
            .find(|(name, _)| name.eq_ignore_ascii_case(WRITE_ACKNOWLEDGEMENT_HEADER))
            .map(|(_, value)| value.as_str());
        assert_eq!(replay_acknowledgement, first_acknowledgement);
        let replay_body: InternalIngestWriteResponse =
            serde_json::from_slice(&replay.body).expect("replay response should decode");
        assert_eq!(replay.body, first.body);
        assert_eq!(replay_body.inserted_rows, first_body.inserted_rows);
        assert_eq!(
            replay_body.accepted_metadata_updates,
            first_body.accepted_metadata_updates
        );
        assert_eq!(
            replay_body.accepted_exemplars,
            first_body.accepted_exemplars
        );
        assert_eq!(replay_body.dropped_exemplars, first_body.dropped_exemplars);
        assert_eq!(
            exemplar_store
                .metrics_snapshot()
                .expect("exemplar metrics should be readable")
                .accepted_total,
            2
        );
    }

    fn query_exemplar_storage(memory_bytes: u64) -> Arc<dyn Storage> {
        StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(crate::cluster::config::DEFAULT_CLUSTER_SHARDS)
            .with_query_budget_limits(tsink::QueryBudgetLimits {
                max_concurrent_queries: Some(2),
                max_shared_memory_bytes: Some(memory_bytes),
                per_query: tsink::QueryWorkLimits {
                    max_memory_bytes: Some(memory_bytes),
                    ..tsink::QueryWorkLimits::default()
                },
            })
            .build()
            .expect("query storage should build")
    }

    fn seeded_query_exemplar_store() -> Arc<ExemplarStore> {
        let store = Arc::new(ExemplarStore::in_memory_with_config(ExemplarStoreConfig {
            max_total_exemplars: 8,
            max_exemplars_per_series: 8,
            max_exemplars_per_request: 8,
            max_query_results: 8,
            max_query_selectors: 4,
        }));
        store
            .apply_writes(&[ExemplarWrite {
                metric: "latency_seconds".to_string(),
                series_labels: vec![
                    Label::new("job", "api"),
                    Label::new(tenant::TENANT_LABEL, tenant::DEFAULT_TENANT_ID),
                ],
                exemplar_labels: vec![Label::new("trace_id", "abc")],
                timestamp: 10,
                value: 1.5,
            }])
            .expect("seed internal exemplar");
        store
    }

    fn internal_query_exemplar_request(internal_api: &InternalApiConfig) -> HttpRequest {
        HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/query_exemplars".to_string(),
            headers: internal_headers(
                Some(&internal_api.auth_token),
                Some(INTERNAL_RPC_PROTOCOL_VERSION),
                &[("content-type", "application/json")],
            ),
            body: serde_json::to_vec(&InternalQueryExemplarsRequest {
                ring_version: DEFAULT_INTERNAL_RING_VERSION,
                selectors: vec![SeriesSelection {
                    metric: Some("latency_seconds".to_string()),
                    matchers: vec![SeriesMatcher::equal(
                        tenant::TENANT_LABEL,
                        tenant::DEFAULT_TENANT_ID,
                    )],
                    start: None,
                    end: None,
                }],
                start: 0,
                end: 20,
                limit: 1,
                query_limits: Some(tsink::QueryWorkLimits::default()),
            })
            .expect("internal exemplar request should encode"),
        }
    }

    #[tokio::test]
    async fn internal_query_exemplars_has_exact_memory_boundary_and_zero_residual() {
        const CALIBRATION_MEMORY_BYTES: u64 = 128 * 1024 * 1024;
        let internal_api = internal_api();
        let calibration_storage = query_exemplar_storage(CALIBRATION_MEMORY_BYTES);
        let calibration_store = seeded_query_exemplar_store();
        let request = internal_query_exemplar_request(&internal_api);
        let calibration = handle_internal_query_exemplars(
            &calibration_storage,
            &calibration_store,
            &request,
            Some(&internal_api),
            None,
        )
        .await;
        assert_eq!(calibration.status, 200);
        let calibration_body: InternalQueryExemplarsResponse =
            serde_json::from_slice(&calibration.body).expect("calibration response should decode");
        assert_eq!(calibration_body.series.len(), 1);
        assert!(calibration_body.accounting.is_some());
        let calibration_snapshot = calibration_storage.query_budget_snapshot();
        let exact_memory = calibration_snapshot.peak_shared_reserved_memory_bytes;
        assert!(exact_memory > 1);
        assert_eq!(calibration_snapshot.active_queries, 0);
        assert_eq!(calibration_snapshot.shared_reserved_memory_bytes, 0);

        let exact_storage = query_exemplar_storage(exact_memory);
        let exact_store = seeded_query_exemplar_store();
        let exact = handle_internal_query_exemplars(
            &exact_storage,
            &exact_store,
            &request,
            Some(&internal_api),
            None,
        )
        .await;
        assert_eq!(exact.status, 200);
        assert_eq!(exact.body, calibration.body);
        let exact_snapshot = exact_storage.query_budget_snapshot();
        assert_eq!(exact_snapshot.active_queries, 0);
        assert_eq!(exact_snapshot.shared_reserved_memory_bytes, 0);

        let under_storage = query_exemplar_storage(exact_memory - 1);
        let under_store = seeded_query_exemplar_store();
        let under = handle_internal_query_exemplars(
            &under_storage,
            &under_store,
            &request,
            Some(&internal_api),
            None,
        )
        .await;
        assert_eq!(under.status, 413);
        let error: InternalErrorResponse =
            serde_json::from_slice(&under.body).expect("memory rejection should decode");
        assert_eq!(error.code, "query_limit_per_query_memory_bytes");
        let under_snapshot = under_storage.query_budget_snapshot();
        assert_eq!(under_snapshot.active_queries, 0);
        assert_eq!(under_snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(under_snapshot.per_query_memory_rejections_total, 1);
        assert_eq!(under_snapshot.accounting_invariant_violations_total, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn dropping_internal_query_exemplars_cancels_worker_and_releases_budget() {
        const TEST_MEMORY_BYTES: u64 = 128 * 1024 * 1024;
        let storage = query_exemplar_storage(TEST_MEMORY_BYTES);
        let store = seeded_query_exemplar_store();
        let entered = Arc::new(Barrier::new(2));
        let release = Arc::new(Barrier::new(2));
        store.set_query_test_gate(Arc::clone(&entered), Arc::clone(&release));
        let internal_api = internal_api();
        let request = internal_query_exemplar_request(&internal_api);
        let storage_for_handler = Arc::clone(&storage);
        let store_for_handler = Arc::clone(&store);
        let task = tokio::spawn(async move {
            handle_internal_query_exemplars(
                &storage_for_handler,
                &store_for_handler,
                &request,
                Some(&internal_api),
                None,
            )
            .await
        });

        tokio::task::spawn_blocking(move || entered.wait())
            .await
            .expect("query-entry waiter should join");
        assert_eq!(storage.query_budget_snapshot().active_queries, 1);
        task.abort();
        assert!(task.await.expect_err("handler should abort").is_cancelled());
        tokio::task::spawn_blocking(move || release.wait())
            .await
            .expect("query-release waiter should join");
        for _ in 0..1_000 {
            if storage.query_budget_snapshot().active_queries == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert!(snapshot.cancellations_total >= 1);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn parse_internal_json_body_rejects_missing_payloads() {
        let response = parse_internal_json_body::<JsonValue>(&HttpRequest {
            method: "POST".to_string(),
            path: "/internal/v1/select".to_string(),
            headers: HashMap::new(),
            body: Vec::new(),
        })
        .expect_err("empty payload should be rejected");

        assert_eq!(response.status, 400);
        let body: JsonValue =
            serde_json::from_slice(&response.body).expect("response body should be valid JSON");
        assert_eq!(body["code"], "invalid_request");
    }
}
