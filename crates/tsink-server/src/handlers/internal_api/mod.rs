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

pub(super) async fn handle_internal_query_exemplars(
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

    match exemplar_store.query(
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
            },
        ),
        Err(err) => internal_error_response(
            503,
            "exemplar_query_failed",
            format!("internal exemplar query failed: {err}"),
            true,
        ),
    }
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
    let storage = Arc::clone(storage);
    let storage_metric = metric.clone();
    let storage_labels = labels.clone();
    let result = tokio::task::spawn_blocking(move || {
        match storage.select(&storage_metric, &storage_labels, start, end) {
            Ok(points) => Ok(points),
            Err(tsink::TsinkError::NoDataPoints { .. }) => Ok(Vec::new()),
            Err(err) => Err(err.to_string()),
        }
    })
    .await;

    match result {
        Ok(Ok(points)) => {
            let mut points = points;
            if let Some(bridge_source) = ring_validation.bridge_source.as_ref() {
                let Some(cluster_context) = cluster_context else {
                    return internal_error_response(
                        503,
                        "control_plane_unavailable",
                        "handoff read bridge requires cluster context",
                        true,
                    );
                };
                let bridge_request = InternalSelectRequest {
                    ring_version: bridge_source.stale_ring_version,
                    metric: metric.clone(),
                    labels: labels.clone(),
                    start,
                    end,
                };
                match cluster_context
                    .rpc_client
                    .select(&bridge_source.endpoint, &bridge_request)
                    .await
                {
                    Ok(response) => {
                        points = merge_handoff_points(points, response.points);
                    }
                    Err(err) => {
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
                }
            }
            json_response(200, &InternalSelectResponse { points })
        }
        Ok(Err(err)) => internal_error_response(
            503,
            "storage_select_failed",
            format!("internal select failed: {err}"),
            true,
        ),
        Err(err) => internal_error_response(
            503,
            "storage_select_task_failed",
            format!("internal select task failed: {err}"),
            true,
        ),
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

    let mut bridge_batches = BTreeMap::<(String, String, u64), Vec<MetricSeries>>::new();
    for selector in &payload.selectors {
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
                .push(selector.clone());
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
    let selectors = payload.selectors;
    let requested = selectors.clone();
    let storage = Arc::clone(storage);
    let result =
        tokio::task::spawn_blocking(move || storage.select_many(&selectors, start, end)).await;

    match result {
        Ok(Ok(series)) => {
            let mut series = series;
            if !bridge_batches.is_empty() {
                let Some(cluster_context) = cluster_context else {
                    return internal_error_response(
                        503,
                        "control_plane_unavailable",
                        "handoff read bridge requires cluster context",
                        true,
                    );
                };

                for ((source_node_id, endpoint, stale_ring_version), selectors) in bridge_batches {
                    let bridge_response = match cluster_context
                        .rpc_client
                        .select_batch(
                            &endpoint,
                            &InternalSelectBatchRequest {
                                ring_version: stale_ring_version,
                                selectors: selectors.clone(),
                                start,
                                end,
                            },
                        )
                        .await
                    {
                        Ok(response) => Ok(response.series),
                        Err(crate::cluster::rpc::RpcError::HttpStatus { status: 404, .. }) => {
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
                            Ok(legacy)
                        }
                        Err(err) => Err(err),
                    };

                    match bridge_response {
                        Ok(bridge_series) => {
                            series = merge_handoff_series_points(&requested, series, bridge_series);
                        }
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
            }

            json_response(200, &InternalSelectBatchResponse { series })
        }
        Ok(Err(err)) => internal_error_response(
            503,
            "storage_select_failed",
            format!("internal select_batch failed: {err}"),
            true,
        ),
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

    let storage = Arc::clone(storage);
    let selection_for_storage = selection.clone();
    let shard_scope_for_storage = shard_scope.clone();
    let result = tokio::task::spawn_blocking(move || {
        storage.select_series_in_shards(&selection_for_storage, &shard_scope_for_storage)
    })
    .await;

    match result {
        Ok(Ok(series)) => {
            let mut series = series;
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
                    let request = InternalSelectSeriesRequest {
                        ring_version: bridge_source.stale_ring_version,
                        shard_scope: Some(MetadataShardScope::new(
                            ring_validation.shard_count,
                            bridge_source.shards.iter().copied().collect(),
                        )),
                        selection: selection.clone(),
                    };
                    match cluster_context
                        .rpc_client
                        .select_series(&bridge_source.endpoint, &request)
                        .await
                    {
                        Ok(response) => {
                            series = merge_metric_series(series, response.series);
                        }
                        Err(err) => {
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
            json_response(200, &InternalSelectSeriesResponse { series })
        }
        Ok(Err(err)) => internal_error_response(
            503,
            "storage_select_series_failed",
            format!("internal select_series failed: {err}"),
            true,
        ),
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
    let ring_validation =
        match validate_internal_metadata_ring_version(payload.ring_version, cluster_context) {
            Ok(validation) => validation,
            Err(response) => return response,
        };
    let shard_scope = match resolve_internal_metadata_shard_scope(
        payload.shard_scope.as_ref(),
        payload.ring_version,
        cluster_context,
        ring_validation.shard_count,
    ) {
        Ok(scope) => scope,
        Err(response) => return response,
    };

    let storage = Arc::clone(storage);
    let result =
        tokio::task::spawn_blocking(move || storage.list_metrics_in_shards(&shard_scope)).await;

    match result {
        Ok(Ok(series)) => {
            let mut series = series;
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
                    let request = InternalListMetricsRequest {
                        ring_version: bridge_source.stale_ring_version,
                        shard_scope: Some(MetadataShardScope::new(
                            ring_validation.shard_count,
                            bridge_source.shards.iter().copied().collect(),
                        )),
                    };
                    match cluster_context
                        .rpc_client
                        .list_metrics_with_request(&bridge_source.endpoint, &request)
                        .await
                    {
                        Ok(response) => {
                            series = merge_metric_series(series, response.series);
                        }
                        Err(err) => {
                            return internal_error_response(
                                503,
                                "handoff_bridge_failed",
                                format!(
                                    "handoff read bridge list_metrics failed for source node '{}' ({}): {err}",
                                    bridge_source.source_node_id, bridge_source.endpoint
                                ),
                                true,
                            );
                        }
                    }
                }
            }
            json_response(200, &InternalListMetricsResponse { series })
        }
        Ok(Err(err)) => internal_error_response(
            503,
            "storage_list_metrics_failed",
            format!("internal list_metrics failed: {err}"),
            true,
        ),
        Err(err) => internal_error_response(
            503,
            "storage_list_metrics_task_failed",
            format!("internal list_metrics task failed: {err}"),
            true,
        ),
    }
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

    let storage = Arc::clone(storage);
    let shard = payload.shard;
    let ring_version = payload.ring_version;
    let window_start = payload.window_start;
    let window_end = payload.window_end;
    let digest_task = tokio::task::spawn_blocking(move || {
        compute_shard_window_digest(
            storage.as_ref(),
            shard,
            shard_count,
            ring_version,
            window_start,
            window_end,
        )
    })
    .await;

    match digest_task {
        Ok(Ok(digest)) => json_response(200, &digest),
        Ok(Err(err)) => internal_error_response(
            503,
            "digest_compute_failed",
            format!("internal digest computation failed: {err}"),
            true,
        ),
        Err(err) => internal_error_response(
            503,
            "digest_compute_task_failed",
            format!("internal digest compute task failed: {err}"),
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

#[allow(clippy::too_many_arguments)]
pub(super) fn collect_internal_repair_backfill_rows(
    storage: &dyn Storage,
    ring_version: u64,
    shard: u32,
    shard_count: u32,
    window_start: i64,
    window_end: i64,
    max_series: Option<usize>,
    max_rows: Option<usize>,
    row_offset: Option<u64>,
) -> Result<InternalRepairBackfillResponse, String> {
    let page = storage
        .scan_shard_window_rows(
            shard,
            shard_count,
            window_start,
            window_end,
            ShardWindowScanOptions {
                max_series,
                max_rows,
                row_offset,
            },
        )
        .map_err(|err| format!("storage shard-window row scan failed: {err}"))?;

    Ok(InternalRepairBackfillResponse {
        shard: page.shard,
        ring_version,
        window_start: page.window_start,
        window_end: page.window_end,
        series_scanned: page.series_scanned,
        rows_scanned: page.rows_scanned,
        truncated: page.truncated,
        next_row_offset: page.next_row_offset,
        rows: page
            .rows
            .into_iter()
            .map(|row| InternalRow {
                metric: row.metric().to_string(),
                labels: row.labels().to_vec(),
                data_point: row.data_point().clone(),
            })
            .collect(),
    })
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

    let storage = Arc::clone(storage);
    let shard = payload.shard;
    let ring_version = payload.ring_version;
    let window_start = payload.window_start;
    let window_end = payload.window_end;
    let max_series = payload.max_series;
    let max_rows = payload.max_rows;
    let row_offset = payload.row_offset;
    let repair_task = tokio::task::spawn_blocking(move || {
        collect_internal_repair_backfill_rows(
            storage.as_ref(),
            ring_version,
            shard,
            shard_count,
            window_start,
            window_end,
            max_series,
            max_rows,
            row_offset,
        )
    })
    .await;

    match repair_task {
        Ok(Ok(response)) => json_response(200, &response),
        Ok(Err(err)) => internal_error_response(
            503,
            "repair_backfill_failed",
            format!("internal repair_backfill failed: {err}"),
            true,
        ),
        Err(err) => internal_error_response(
            503,
            "repair_backfill_task_failed",
            format!("internal repair_backfill task failed: {err}"),
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

    fn make_storage() -> Arc<dyn Storage> {
        StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(crate::cluster::config::DEFAULT_CLUSTER_SHARDS)
            .build()
            .expect("storage should build")
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
