use super::*;

struct PendingPoint {
    series_id: SeriesId,
    lane: ValueLane,
    ts: i64,
    value: Value,
}

struct PendingNewSeriesPlan {
    metric: String,
    labels: Vec<Label>,
}

struct PreparedWalWrite {
    encoded_series_definition_payloads: Vec<Vec<u8>>,
    encoded_samples_payload: Option<Vec<u8>>,
    encoded_bytes: u64,
    sample_batch_count: usize,
    sample_point_count: usize,
}

struct ResolvedWrite {
    pending_points: Vec<PendingPoint>,
    new_series_defs: Vec<SeriesDefinitionFrame>,
    created_series: Vec<SeriesResolution>,
}

struct PreparedWrite {
    resolved: ResolvedWrite,
    prepared_wal: Option<PreparedWalWrite>,
    reserved_series: Vec<SeriesId>,
}

struct CommittedWrite;
type PrepareResolvedWriteError = Box<(ResolvedWrite, TsinkError)>;
type CommitPreparedWriteError = Box<(PreparedWrite, TsinkError)>;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RawSeriesKey {
    metric: String,
    labels: Vec<Label>,
}

impl RawSeriesKey {
    fn new(metric: &str, labels: &[Label]) -> Self {
        let mut normalized_labels = labels.to_vec();
        normalized_labels.sort();
        normalized_labels.dedup();
        Self {
            metric: metric.to_string(),
            labels: normalized_labels,
        }
    }
}

fn lane_name(lane: ValueLane) -> &'static str {
    match lane {
        ValueLane::Numeric => "numeric",
        ValueLane::Blob => "blob",
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingValueFamily {
    F64,
    I64,
    U64,
    Bool,
    Blob,
}

fn value_family_for_lane(value: &Value, lane: ValueLane) -> Result<PendingValueFamily> {
    match (value, lane) {
        (Value::F64(_), ValueLane::Numeric) => Ok(PendingValueFamily::F64),
        (Value::I64(_), ValueLane::Numeric) => Ok(PendingValueFamily::I64),
        (Value::U64(_), ValueLane::Numeric) => Ok(PendingValueFamily::U64),
        (Value::Bool(_), ValueLane::Numeric) => Ok(PendingValueFamily::Bool),
        (Value::Bytes(_) | Value::String(_), ValueLane::Blob) => Ok(PendingValueFamily::Blob),
        (_, ValueLane::Numeric) => Err(TsinkError::ValueTypeMismatch {
            expected: "numeric lane value".to_string(),
            actual: value.kind().to_string(),
        }),
        (_, ValueLane::Blob) => Err(TsinkError::ValueTypeMismatch {
            expected: "blob lane value".to_string(),
            actual: value.kind().to_string(),
        }),
    }
}

fn value_family_name(family: PendingValueFamily) -> &'static str {
    match family {
        PendingValueFamily::F64 => "f64",
        PendingValueFamily::I64 => "i64",
        PendingValueFamily::U64 => "u64",
        PendingValueFamily::Bool => "bool",
        PendingValueFamily::Blob => "bytes/string",
    }
}

impl ChunkStorage {
    pub(super) fn insert_rows_impl(&self, rows: &[Row]) -> Result<()> {
        self.ensure_open()?;
        let write_permit = self.write_limiter.try_acquire_for(self.write_timeout)?;
        // A write may pass the first lifecycle check and then block on permits while close starts.
        // Re-check after acquiring a permit so shutdown cannot race new writes through.
        self.ensure_open()?;
        // Serialize writers that touch the same metric shard while allowing unrelated metrics
        // to progress concurrently through WAL and ingestion work.
        let _registry_write_txn_shards = self.lock_registry_write_shards_for_rows(rows);

        let resolved = self.resolve_write_rows(rows)?;
        let prepared = match self.prepare_resolved_write(resolved) {
            Ok(prepared) => prepared,
            Err(err) => {
                let (resolved, err) = *err;
                self.rollback_resolved_write(resolved);
                return Err(err);
            }
        };
        if let Err(err) = self.commit_prepared_write(prepared) {
            let (prepared, err) = *err;
            self.rollback_prepared_write(prepared);
            return Err(err);
        }

        if self.memory_budget_value() != usize::MAX {
            drop(write_permit);
            self.enforce_memory_budget_if_needed()?;
        }

        self.notify_flush_thread();
        Ok(())
    }

    fn resolve_write_rows(&self, rows: &[Row]) -> Result<ResolvedWrite> {
        let mut pending_points = Vec::with_capacity(rows.len());
        let mut new_series_defs = Vec::new();
        let mut created_series = Vec::<SeriesResolution>::new();
        {
            let mut registry = self.registry.write();

            if let Err(err) = (|| -> Result<()> {
                let cardinality_limit = self.cardinality_limit_value();
                let current = registry.series_count();
                let mut pending_new_series = HashMap::<RawSeriesKey, usize>::new();
                let mut pending_new_series_plans = Vec::<PendingNewSeriesPlan>::new();
                let mut pending_new_point_refs = Vec::<(usize, usize)>::new();

                for row in rows {
                    validate_metric(row.metric())?;
                    validate_labels(row.labels())?;

                    let data_point = row.data_point();
                    let lane = lane_for_value(&data_point.value);

                    if let Some(resolution) = registry.resolve_existing(row.metric(), row.labels())
                    {
                        pending_points.push(PendingPoint {
                            series_id: resolution.series_id,
                            lane,
                            ts: data_point.timestamp,
                            value: data_point.value.clone(),
                        });
                        continue;
                    }

                    let key = RawSeriesKey::new(row.metric(), row.labels());
                    let plan_idx = if let Some(existing) = pending_new_series.get(&key) {
                        *existing
                    } else {
                        let next = pending_new_series_plans.len();
                        pending_new_series.insert(key, next);
                        pending_new_series_plans.push(PendingNewSeriesPlan {
                            metric: row.metric().to_string(),
                            labels: row.labels().to_vec(),
                        });
                        next
                    };

                    let point_idx = pending_points.len();
                    pending_points.push(PendingPoint {
                        series_id: 0,
                        lane,
                        ts: data_point.timestamp,
                        value: data_point.value.clone(),
                    });
                    pending_new_point_refs.push((point_idx, plan_idx));
                }

                let requested = pending_new_series_plans.len();
                if cardinality_limit != usize::MAX
                    && requested > 0
                    && current.saturating_add(requested) > cardinality_limit
                {
                    return Err(TsinkError::CardinalityLimitExceeded {
                        limit: cardinality_limit,
                        current,
                        requested,
                    });
                }

                if requested > 0 {
                    let mut pending_plan_series_ids = vec![0; requested];
                    for (plan_idx, plan) in pending_new_series_plans.into_iter().enumerate() {
                        let resolution = registry.resolve_or_insert(&plan.metric, &plan.labels)?;
                        pending_plan_series_ids[plan_idx] = resolution.series_id;

                        if resolution.created {
                            created_series.push(resolution.clone());
                            new_series_defs.push(SeriesDefinitionFrame {
                                series_id: resolution.series_id,
                                metric: plan.metric,
                                labels: plan.labels,
                            });
                        }
                    }

                    for (point_idx, plan_idx) in pending_new_point_refs {
                        pending_points[point_idx].series_id = pending_plan_series_ids[plan_idx];
                    }
                }

                Ok(())
            })() {
                registry.rollback_created_series(&created_series);
                return Err(err);
            }
        }

        Ok(ResolvedWrite {
            pending_points,
            new_series_defs,
            created_series,
        })
    }

    fn prepare_resolved_write(
        &self,
        resolved: ResolvedWrite,
    ) -> std::result::Result<PreparedWrite, PrepareResolvedWriteError> {
        for point in &resolved.pending_points {
            if let Err(err) = self.validate_series_lane_compatible(point.series_id, point.lane) {
                return Err(Box::new((resolved, err)));
            }
        }

        let grouped_points =
            match Self::group_pending_point_indexes_by_series(&resolved.pending_points) {
                Ok(grouped_points) => grouped_points,
                Err(err) => return Err(Box::new((resolved, err))),
            };
        if let Err(err) =
            self.validate_pending_point_families(&resolved.pending_points, &grouped_points)
        {
            return Err(Box::new((resolved, err)));
        }
        if let Err(err) = self.validate_points_against_retention(&resolved.pending_points) {
            return Err(Box::new((resolved, err)));
        }
        let prepared_wal = match self.prepare_wal_write(
            &resolved.new_series_defs,
            &resolved.pending_points,
            &grouped_points,
        ) {
            Ok(prepared_wal) => prepared_wal,
            Err(err) => return Err(Box::new((resolved, err))),
        };
        let estimated_memory_growth =
            self.estimate_write_memory_growth_bytes(&resolved.pending_points, &grouped_points);
        let estimated_wal_growth = prepared_wal
            .as_ref()
            .map(|prepared| prepared.encoded_bytes)
            .unwrap_or(0);
        if let Err(err) =
            self.enforce_admission_controls(estimated_memory_growth, estimated_wal_growth)
        {
            return Err(Box::new((resolved, err)));
        }
        let reserved_series = match self.reserve_series_lanes(&resolved.pending_points) {
            Ok(reserved_series) => reserved_series,
            Err(err) => return Err(Box::new((resolved, err))),
        };

        Ok(PreparedWrite {
            resolved,
            prepared_wal,
            reserved_series,
        })
    }

    fn commit_prepared_write(
        &self,
        mut prepared: PreparedWrite,
    ) -> std::result::Result<CommittedWrite, CommitPreparedWriteError> {
        if let (Some(wal), Some(prepared_wal)) = (&self.wal, prepared.prepared_wal.as_ref()) {
            for payload in &prepared_wal.encoded_series_definition_payloads {
                if let Err(err) = wal.append_series_definition_payload(payload) {
                    self.observability
                        .wal
                        .append_errors_total
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(Box::new((prepared, err)));
                }
            }

            if let Some(samples_payload) = prepared_wal.encoded_samples_payload.as_deref() {
                if let Err(err) = wal.append_samples_payload(samples_payload) {
                    self.observability
                        .wal
                        .append_errors_total
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(Box::new((prepared, err)));
                }
            }

            self.observability
                .wal
                .append_series_definitions_total
                .fetch_add(
                    saturating_u64_from_usize(
                        prepared_wal.encoded_series_definition_payloads.len(),
                    ),
                    Ordering::Relaxed,
                );
            self.observability
                .wal
                .append_sample_batches_total
                .fetch_add(
                    saturating_u64_from_usize(prepared_wal.sample_batch_count),
                    Ordering::Relaxed,
                );
            self.observability.wal.append_points_total.fetch_add(
                saturating_u64_from_usize(prepared_wal.sample_point_count),
                Ordering::Relaxed,
            );
            self.observability
                .wal
                .append_bytes_total
                .fetch_add(prepared_wal.encoded_bytes, Ordering::Relaxed);
        }

        if let Err(err) =
            self.ingest_pending_points(std::mem::take(&mut prepared.resolved.pending_points))
        {
            return Err(Box::new((prepared, err)));
        }

        Ok(CommittedWrite)
    }

    fn rollback_resolved_write(&self, resolved: ResolvedWrite) {
        if resolved.created_series.is_empty() {
            return;
        }
        let mut registry = self.registry.write();
        registry.rollback_created_series(&resolved.created_series);
    }

    fn rollback_prepared_write(&self, prepared: PreparedWrite) {
        self.rollback_empty_series_lane_reservations(&prepared.reserved_series);
        self.rollback_resolved_write(prepared.resolved);
    }

    fn validate_points_against_retention(&self, points: &[PendingPoint]) -> Result<()> {
        if !self.retention_enforced {
            return Ok(());
        }
        if points.is_empty() {
            return Ok(());
        }

        let existing_max = self.max_observed_timestamp.load(Ordering::Acquire);
        let incoming_max = points
            .iter()
            .map(|point| point.ts)
            .max()
            .unwrap_or(i64::MIN);
        let effective_max = existing_max.max(incoming_max);
        if effective_max == i64::MIN {
            return Ok(());
        }

        let cutoff = effective_max.saturating_sub(self.retention_window);
        for point in points {
            if point.ts < cutoff {
                return Err(TsinkError::OutOfRetention {
                    timestamp: point.ts,
                });
            }
        }
        Ok(())
    }

    fn estimate_write_memory_growth_bytes(
        &self,
        points: &[PendingPoint],
        grouped: &BTreeMap<SeriesId, (ValueLane, Vec<usize>)>,
    ) -> usize {
        let per_point_bytes = std::mem::size_of::<ChunkPoint>();
        let point_storage_bytes = points.len().saturating_mul(per_point_bytes);
        let heap_bytes = points.iter().fold(0usize, |acc, point| {
            acc.saturating_add(value_heap_bytes(&point.value))
        });

        // New active states preallocate a chunk-sized point buffer.
        let per_new_active_series = std::mem::size_of::<ActiveSeriesState>().saturating_add(
            self.chunk_point_cap
                .saturating_mul(std::mem::size_of::<ChunkPoint>()),
        );
        let new_active_series = grouped
            .keys()
            .filter(|series_id| {
                !self
                    .active_shard(**series_id)
                    .read()
                    .contains_key(series_id)
            })
            .count();
        let active_state_bytes = new_active_series.saturating_mul(per_new_active_series);

        point_storage_bytes
            .saturating_add(heap_bytes)
            .saturating_add(active_state_bytes)
    }

    fn prepare_wal_write(
        &self,
        new_series_defs: &[SeriesDefinitionFrame],
        points: &[PendingPoint],
        grouped: &BTreeMap<SeriesId, (ValueLane, Vec<usize>)>,
    ) -> Result<Option<PreparedWalWrite>> {
        let Some(_wal) = &self.wal else {
            return Ok(None);
        };

        let mut encoded_series_definition_payloads = Vec::with_capacity(new_series_defs.len());
        let mut encoded_bytes = 0u64;
        for definition in new_series_defs {
            let payload = FramedWal::encode_series_definition_frame_payload(definition)?;
            encoded_bytes = encoded_bytes
                .saturating_add(FramedWal::frame_size_bytes_for_payload_len(payload.len()));
            encoded_series_definition_payloads.push(payload);
        }

        let mut encoded_samples_payload = None;
        let mut sample_batch_count = 0usize;
        let mut sample_point_count = 0usize;
        if !grouped.is_empty() {
            let batches = Self::encode_wal_batches(points, grouped)?;
            sample_batch_count = batches.len();
            sample_point_count = batches.iter().map(|batch| batch.point_count as usize).sum();
            let payload = FramedWal::encode_samples_frame_payload(&batches)?;
            encoded_bytes = encoded_bytes
                .saturating_add(FramedWal::frame_size_bytes_for_payload_len(payload.len()));
            encoded_samples_payload = Some(payload);
        }

        Ok(Some(PreparedWalWrite {
            encoded_series_definition_payloads,
            encoded_samples_payload,
            encoded_bytes,
            sample_batch_count,
            sample_point_count,
        }))
    }

    fn memory_budget_shortfall(&self, estimated_growth_bytes: usize) -> Option<(usize, usize)> {
        let budget = self.memory_budget_value();
        if budget == usize::MAX {
            return None;
        }

        let used = self.memory_used_value();
        let required = used.saturating_add(estimated_growth_bytes);
        (required > budget).then_some((budget, required))
    }

    fn wal_size_shortfall(&self, estimated_growth_bytes: u64) -> Result<Option<(u64, u64)>> {
        let limit = self.wal_size_limit_value();
        if limit == u64::MAX || estimated_growth_bytes == 0 {
            return Ok(None);
        }

        let Some(wal) = &self.wal else {
            return Ok(None);
        };

        if estimated_growth_bytes > limit {
            return Ok(Some((limit, estimated_growth_bytes)));
        }

        let current = wal.total_size_bytes()?;
        let required = current.saturating_add(estimated_growth_bytes);
        Ok((required > limit).then_some((limit, required)))
    }

    fn acquire_write_permits_excluding_current_writer(&self) -> Result<Vec<SemaphoreGuard<'_>>> {
        let permits_needed = self.write_limiter.capacity().saturating_sub(1);
        if permits_needed == 0 {
            return Ok(Vec::new());
        }

        let deadline = Instant::now() + self.write_timeout;
        let mut guards = Vec::with_capacity(permits_needed);
        for _ in 0..permits_needed {
            let now = Instant::now();
            let remaining = deadline.saturating_duration_since(now);
            guards.push(self.write_limiter.try_acquire_for(remaining)?);
        }

        Ok(guards)
    }

    fn relieve_pressure_once(&self) -> Result<()> {
        let _backpressure_guard = self.admission_backpressure_lock.lock();
        // `relieve_pressure_once` is entered while the caller already holds one writer permit.
        // Drain the remaining permits so WAL reset/truncate cannot race in-flight writers.
        let _drained_writers = self.acquire_write_permits_excluding_current_writer()?;
        self.flush_all_active()?;
        self.prune_empty_active_series();
        if self.persist_segment(true)? {
            self.refresh_persisted_indexes_and_evict_flushed_sealed_chunks()?;
        }
        self.enforce_memory_budget_if_needed_with_writers_already_drained()?;
        Ok(())
    }

    fn enforce_admission_controls(
        &self,
        estimated_memory_growth_bytes: usize,
        estimated_wal_growth_bytes: u64,
    ) -> Result<()> {
        let deadline = Instant::now() + self.write_timeout;

        loop {
            if let Some((_budget, _required)) =
                self.memory_budget_shortfall(estimated_memory_growth_bytes)
            {
                self.relieve_pressure_once()?;

                if let Some((post_budget, post_required)) =
                    self.memory_budget_shortfall(estimated_memory_growth_bytes)
                {
                    if Instant::now() >= deadline {
                        return Err(TsinkError::MemoryBudgetExceeded {
                            budget: post_budget,
                            required: post_required,
                        });
                    }
                    self.notify_flush_thread();
                    std::thread::sleep(
                        self.admission_poll_interval
                            .min(deadline.saturating_duration_since(Instant::now())),
                    );
                    continue;
                }
            }

            if let Some((_limit, _required)) =
                self.wal_size_shortfall(estimated_wal_growth_bytes)?
            {
                self.relieve_pressure_once()?;

                if let Some((post_limit, post_required)) =
                    self.wal_size_shortfall(estimated_wal_growth_bytes)?
                {
                    if Instant::now() >= deadline {
                        return Err(TsinkError::WalSizeLimitExceeded {
                            limit: post_limit,
                            required: post_required,
                        });
                    }
                    self.notify_flush_thread();
                    std::thread::sleep(
                        self.admission_poll_interval
                            .min(deadline.saturating_duration_since(Instant::now())),
                    );
                    continue;
                }
            }

            return Ok(());
        }
    }

    fn validate_series_lane_compatible(&self, series_id: SeriesId, lane: ValueLane) -> Result<()> {
        if let Some(active_lane) = self
            .active_shard(series_id)
            .read()
            .get(&series_id)
            .map(|state| state.lane)
        {
            if active_lane != lane {
                return Err(TsinkError::ValueTypeMismatch {
                    expected: lane_name(active_lane).to_string(),
                    actual: lane_name(lane).to_string(),
                });
            }
        }

        if let Some(sealed_lane) = self
            .sealed_shard(series_id)
            .read()
            .get(&series_id)
            .and_then(|chunks| chunks.last_key_value().map(|(_, chunk)| chunk))
            .map(|chunk| chunk.header.lane)
        {
            if sealed_lane != lane {
                return Err(TsinkError::ValueTypeMismatch {
                    expected: lane_name(sealed_lane).to_string(),
                    actual: lane_name(lane).to_string(),
                });
            }
        }

        if let Some(persisted_lane) = self
            .persisted_index
            .read()
            .chunk_refs
            .get(&series_id)
            .and_then(|chunks| chunks.last())
            .map(|chunk_ref| chunk_ref.lane)
        {
            if persisted_lane != lane {
                return Err(TsinkError::ValueTypeMismatch {
                    expected: lane_name(persisted_lane).to_string(),
                    actual: lane_name(lane).to_string(),
                });
            }
        }

        Ok(())
    }

    fn collect_pending_series_lanes(
        points: &[PendingPoint],
    ) -> Result<BTreeMap<SeriesId, ValueLane>> {
        let mut series_lanes = BTreeMap::new();

        for point in points {
            if let Some(existing_lane) = series_lanes.get(&point.series_id) {
                if *existing_lane != point.lane {
                    return Err(TsinkError::ValueTypeMismatch {
                        expected: lane_name(*existing_lane).to_string(),
                        actual: lane_name(point.lane).to_string(),
                    });
                }
            } else {
                series_lanes.insert(point.series_id, point.lane);
            }
        }

        Ok(series_lanes)
    }

    fn reserve_series_lanes(&self, points: &[PendingPoint]) -> Result<Vec<SeriesId>> {
        let series_lanes = Self::collect_pending_series_lanes(points)?;
        let mut lanes_by_shard = BTreeMap::<usize, Vec<(SeriesId, ValueLane)>>::new();
        for (series_id, lane) in series_lanes {
            lanes_by_shard
                .entry(Self::series_shard_idx(series_id))
                .or_default()
                .push((series_id, lane));
        }

        let mut shard_guards = Vec::with_capacity(lanes_by_shard.len());
        for (shard_idx, entries) in lanes_by_shard {
            shard_guards.push((shard_idx, entries, self.active_builders[shard_idx].write()));
        }

        for (_, entries, active) in &shard_guards {
            for (series_id, lane) in entries {
                if let Some(state) = active.get(series_id) {
                    if state.lane != *lane {
                        return Err(TsinkError::ValueTypeMismatch {
                            expected: lane_name(state.lane).to_string(),
                            actual: lane_name(*lane).to_string(),
                        });
                    }
                }
            }
        }

        let mut reserved = Vec::new();
        let account_memory = self.memory_accounting_enabled;
        for (shard_idx, entries, active) in &mut shard_guards {
            for (series_id, lane) in entries {
                if active.contains_key(series_id) {
                    continue;
                }
                let state = ActiveSeriesState::new(*series_id, *lane, self.chunk_point_cap);
                if account_memory {
                    self.add_memory_usage_bytes(
                        *shard_idx,
                        Self::active_state_memory_usage_bytes(&state),
                    );
                }
                active.insert(*series_id, state);
                reserved.push(*series_id);
            }
        }

        Ok(reserved)
    }

    fn rollback_empty_series_lane_reservations(&self, series_ids: &[SeriesId]) {
        if series_ids.is_empty() {
            return;
        }

        let mut ids_by_shard = BTreeMap::<usize, Vec<SeriesId>>::new();
        for &series_id in series_ids {
            ids_by_shard
                .entry(Self::series_shard_idx(series_id))
                .or_default()
                .push(series_id);
        }

        for (shard_idx, shard_series_ids) in ids_by_shard {
            let mut active = self.active_builders[shard_idx].write();
            for series_id in shard_series_ids {
                let should_remove = active
                    .get(&series_id)
                    .is_some_and(|state| state.builder.is_empty());
                if should_remove {
                    if let Some(removed) = active.remove(&series_id) {
                        if self.memory_accounting_enabled {
                            self.sub_memory_usage_bytes(
                                shard_idx,
                                Self::active_state_memory_usage_bytes(&removed),
                            );
                        }
                    }
                }
            }
        }
    }

    pub(super) fn append_point_to_series(
        &self,
        series_id: SeriesId,
        lane: ValueLane,
        ts: i64,
        value: Value,
    ) -> Result<()> {
        let shard_idx = Self::series_shard_idx(series_id);
        let account_memory = self.memory_accounting_enabled;
        let finalized = {
            let mut active = self.active_shard(series_id).write();
            let before = if account_memory {
                active
                    .get(&series_id)
                    .map(Self::active_state_memory_usage_bytes)
                    .unwrap_or(0)
            } else {
                0
            };
            let state = active
                .entry(series_id)
                .or_insert_with(|| ActiveSeriesState::new(series_id, lane, self.chunk_point_cap));

            if state.lane != lane {
                return Err(TsinkError::ValueTypeMismatch {
                    expected: lane_name(state.lane).to_string(),
                    actual: lane_name(lane).to_string(),
                });
            }

            let mut finalized = Vec::new();
            if let Some(chunk) = state.rotate_partition_if_needed(ts, self.partition_window)? {
                finalized.push(chunk);
            }
            state.append_point(ts, value);
            if let Some(chunk) = state.rotate_full_if_needed()? {
                finalized.push(chunk);
            }
            if account_memory {
                let after = Self::active_state_memory_usage_bytes(state);
                self.account_memory_delta_bytes(shard_idx, before, after);
            }
            finalized
        };

        for chunk in finalized {
            self.append_sealed_chunk(series_id, chunk);
        }
        self.update_max_observed_timestamp(ts);
        self.mark_materialized_series_ids(std::iter::once(series_id));

        Ok(())
    }

    fn ingest_pending_points(&self, points: Vec<PendingPoint>) -> Result<()> {
        if points.is_empty() {
            return Ok(());
        }

        let mut points_by_shard: [Vec<PendingPoint>; IN_MEMORY_SHARD_COUNT] =
            std::array::from_fn(|_| Vec::new());
        for point in points {
            let shard_idx = Self::series_shard_idx(point.series_id);
            points_by_shard[shard_idx].push(point);
        }

        for (shard_idx, shard_points) in points_by_shard.into_iter().enumerate() {
            if shard_points.is_empty() {
                continue;
            }
            self.ingest_pending_points_for_shard(shard_idx, shard_points)?;
        }

        Ok(())
    }

    fn ingest_pending_points_for_shard(
        &self,
        shard_idx: usize,
        shard_points: Vec<PendingPoint>,
    ) -> Result<()> {
        if shard_points.is_empty() {
            return Ok(());
        }

        let account_memory = self.memory_accounting_enabled;
        let mut finalized = Vec::<(SeriesId, Chunk)>::new();
        let mut materialized_series_ids = BTreeSet::new();
        let mut active_added_bytes = 0usize;
        let mut active_removed_bytes = 0usize;
        {
            let mut active = self.active_builders[shard_idx].write();

            for point in shard_points {
                let state_bytes_before = if account_memory {
                    active
                        .get(&point.series_id)
                        .map(Self::active_state_memory_usage_bytes)
                        .unwrap_or(0)
                } else {
                    0
                };
                let state = active.entry(point.series_id).or_insert_with(|| {
                    ActiveSeriesState::new(point.series_id, point.lane, self.chunk_point_cap)
                });

                if state.lane != point.lane {
                    return Err(TsinkError::ValueTypeMismatch {
                        expected: lane_name(state.lane).to_string(),
                        actual: lane_name(point.lane).to_string(),
                    });
                }
                materialized_series_ids.insert(point.series_id);

                if let Some(chunk) =
                    state.rotate_partition_if_needed(point.ts, self.partition_window)?
                {
                    finalized.push((point.series_id, chunk));
                }

                state.append_point(point.ts, point.value);
                self.update_max_observed_timestamp(point.ts);

                if let Some(chunk) = state.rotate_full_if_needed()? {
                    finalized.push((point.series_id, chunk));
                }

                if account_memory {
                    let state_bytes_after = Self::active_state_memory_usage_bytes(state);
                    if state_bytes_after >= state_bytes_before {
                        active_added_bytes = active_added_bytes
                            .saturating_add(state_bytes_after.saturating_sub(state_bytes_before));
                    } else {
                        active_removed_bytes = active_removed_bytes
                            .saturating_add(state_bytes_before.saturating_sub(state_bytes_after));
                    }
                }
            }
        }
        if account_memory {
            self.account_memory_delta_from_totals(
                shard_idx,
                active_added_bytes,
                active_removed_bytes,
            );
        }

        self.mark_materialized_series_ids(materialized_series_ids);

        if finalized.is_empty() {
            return Ok(());
        }

        let mut sealed = self.sealed_chunks[shard_idx].write();
        let mut sealed_added_bytes = 0usize;
        let mut sealed_removed_bytes = 0usize;
        for (series_id, chunk) in finalized {
            let chunk_bytes = if account_memory {
                Self::chunk_memory_usage_bytes(&chunk)
            } else {
                0
            };
            let sequence = self.next_chunk_sequence.fetch_add(1, Ordering::SeqCst);
            let key = SealedChunkKey::from_chunk(&chunk, sequence);
            let replaced = sealed.entry(series_id).or_default().insert(key, chunk);
            if account_memory {
                sealed_added_bytes = sealed_added_bytes.saturating_add(chunk_bytes);
                if let Some(previous_chunk) = replaced.as_ref() {
                    sealed_removed_bytes = sealed_removed_bytes
                        .saturating_add(Self::chunk_memory_usage_bytes(previous_chunk));
                }
            }
        }
        if account_memory {
            self.account_memory_delta_from_totals(
                shard_idx,
                sealed_added_bytes,
                sealed_removed_bytes,
            );
        }

        Ok(())
    }

    fn group_pending_point_indexes_by_series(
        points: &[PendingPoint],
    ) -> Result<BTreeMap<SeriesId, (ValueLane, Vec<usize>)>> {
        let mut grouped: BTreeMap<SeriesId, (ValueLane, Vec<usize>)> = BTreeMap::new();

        for (idx, point) in points.iter().enumerate() {
            let entry = grouped
                .entry(point.series_id)
                .or_insert_with(|| (point.lane, Vec::new()));

            if entry.0 != point.lane {
                return Err(TsinkError::ValueTypeMismatch {
                    expected: lane_name(entry.0).to_string(),
                    actual: lane_name(point.lane).to_string(),
                });
            }

            entry.1.push(idx);
        }

        Ok(grouped)
    }

    fn validate_pending_point_families(
        &self,
        points: &[PendingPoint],
        grouped: &BTreeMap<SeriesId, (ValueLane, Vec<usize>)>,
    ) -> Result<()> {
        for (series_id, (lane, indexes)) in grouped {
            let Some((&first_idx, remaining)) = indexes.split_first() else {
                continue;
            };

            let first_point = &points[first_idx];
            let first_family = value_family_for_lane(&first_point.value, *lane)?;

            for idx in remaining {
                let point = &points[*idx];
                let family = value_family_for_lane(&point.value, *lane)?;
                if family != first_family {
                    return Err(TsinkError::ValueTypeMismatch {
                        expected: value_family_name(first_family).to_string(),
                        actual: point.value.kind().to_string(),
                    });
                }
            }

            if let Some(state) = self.active_shard(*series_id).read().get(series_id) {
                for existing_point in state.builder.points() {
                    let existing_family = value_family_for_lane(&existing_point.value, *lane)?;
                    if existing_family != first_family {
                        return Err(TsinkError::ValueTypeMismatch {
                            expected: value_family_name(existing_family).to_string(),
                            actual: first_point.value.kind().to_string(),
                        });
                    }
                }
            }
        }
        Ok(())
    }

    fn encode_wal_batches(
        points: &[PendingPoint],
        grouped: &BTreeMap<SeriesId, (ValueLane, Vec<usize>)>,
    ) -> Result<Vec<SamplesBatchFrame>> {
        let mut batches = Vec::with_capacity(grouped.len());
        for (series_id, (lane, indexes)) in grouped {
            let mut point_refs = Vec::with_capacity(indexes.len());
            for idx in indexes {
                let point = &points[*idx];
                point_refs.push((point.ts, &point.value));
            }

            batches.push(SamplesBatchFrame::from_timestamp_value_refs(
                *series_id,
                *lane,
                &point_refs,
            )?);
        }

        Ok(batches)
    }
}
