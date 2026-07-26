use std::collections::HashMap;

use super::super::super::{
    lane_for_value, Label, Result, Row, SeriesCreationRateReservation, SeriesDefinitionFrame,
    SeriesRegistry, SeriesResolution, TsinkError, Value, WalHighWatermark, WriteResolveContext,
    WriteTransientMemoryReservation,
};
use super::phases::{PendingPoint, ResolvedWrite};
use crate::engine::series::SeriesKey;
use crate::validation::validate_series_identity;

fn checked_add(lhs: usize, rhs: usize) -> Result<usize> {
    lhs.checked_add(rhs)
        .ok_or(TsinkError::WriteBatchSizeOverflow)
}

fn checked_mul(lhs: usize, rhs: usize) -> Result<usize> {
    lhs.checked_mul(rhs)
        .ok_or(TsinkError::WriteBatchSizeOverflow)
}

pub(super) fn modeled_write_rejection_result_bytes(rows_len: usize) -> Result<usize> {
    let per_row = std::mem::size_of::<crate::RowWriteOutcome>()
        .checked_add(crate::MAX_WRITE_REJECTION_MESSAGE_BYTES)
        .ok_or(TsinkError::WriteBatchSizeOverflow)?;
    checked_mul(rows_len, per_row)
}

/// Conservative peak for every tsink-owned allocation that can coexist between write
/// preflight and WAL publication. Caller-owned rows are deliberately excluded.
fn modeled_write_preparation_peak_bytes(rows: &[Row], wal_enabled: bool) -> Result<usize> {
    let input = crate::modeled_write_batch_input_bytes(rows)?;
    let rows_len = rows.len();

    // Resolution can simultaneously own a pending value clone plus normalized raw key, new-series
    // plan, WAL definition, and registry-estimation identity clones.
    let clone_envelope = checked_mul(input, 4)?;
    let per_row_control = std::mem::size_of::<PendingPoint>()
        .checked_add(std::mem::size_of::<RawSeriesKey>())
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<PendingNewSeriesPlan>()))
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<SeriesDefinitionFrame>()))
        .and_then(|bytes| bytes.checked_add(std::mem::size_of::<SeriesResolution>()))
        .and_then(|bytes| {
            std::mem::size_of::<(usize, usize)>()
                .checked_mul(4)
                .and_then(|refs| bytes.checked_add(refs))
        })
        .ok_or(TsinkError::WriteBatchSizeOverflow)?;
    let control_envelope = checked_mul(rows_len, per_row_control)?;

    // WAL encoding can hold codec candidates, split batch payloads, the combined frame payload,
    // and per-series reference/index vectors at once. Five logical-input copies conservatively
    // cover the candidate and copy peaks for bytes, strings, and serialized histograms.
    let wal_envelope = if wal_enabled {
        checked_add(
            checked_mul(input, 5)?,
            checked_mul(
                rows_len,
                std::mem::size_of::<(i64, &Value)>()
                    .checked_add(
                        std::mem::size_of::<usize>()
                            .checked_mul(4)
                            .ok_or(TsinkError::WriteBatchSizeOverflow)?,
                    )
                    .ok_or(TsinkError::WriteBatchSizeOverflow)?,
            )?,
        )?
    } else {
        0
    };

    let canonical_result_envelope = modeled_write_rejection_result_bytes(rows_len)?;
    checked_add(
        checked_add(checked_add(clone_envelope, control_envelope)?, wal_envelope)?,
        canonical_result_envelope,
    )
}

struct PendingNewSeriesPlan {
    metric: String,
    labels: Vec<Label>,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct RawSeriesKey {
    metric: String,
    labels: Vec<Label>,
}

impl RawSeriesKey {
    fn new(metric: &str, labels: &[Label]) -> Self {
        let mut normalized_labels = labels.to_vec();
        normalized_labels.sort();
        Self {
            metric: metric.to_string(),
            labels: normalized_labels,
        }
    }
}

impl<'a> WriteResolveContext<'a> {
    fn with_registry<R>(self, f: impl FnOnce(&SeriesRegistry) -> R) -> R {
        let registry = self.catalog.registry.read();
        f(&registry)
    }

    fn sync_registry_memory_usage(self) {
        self.support.registry_memory.sync_registry_memory_usage();
    }

    fn rollback_created_series(self, created: &[SeriesResolution]) {
        if created.is_empty() {
            return;
        }

        self.with_registry(|registry| {
            registry.rollback_created_series(created);
        });
        self.sync_registry_memory_usage();
    }

    fn reserve_new_series_capacity(
        self,
        registry: &SeriesRegistry,
        requested: usize,
    ) -> Result<usize> {
        if self.cardinality_limit == usize::MAX || requested == 0 {
            return Ok(0);
        }

        registry.reserve_new_series_capacity(self.cardinality_limit, requested)?;
        Ok(requested)
    }

    fn release_new_series_capacity(self, released: usize) {
        if released == 0 {
            return;
        }

        self.with_registry(|registry| {
            registry.release_new_series_capacity(released);
        });
    }

    fn reserve_new_series_overlap(
        self,
        reservation: &WriteTransientMemoryReservation,
        additional_bytes: usize,
    ) -> Result<()> {
        let required = reservation
            .base_reserved_bytes()
            .checked_add(additional_bytes)
            .ok_or(TsinkError::WriteBatchSizeOverflow)?;
        reservation.ensure(required, self.memory_reservation_admission)
    }

    fn publish_registry_memory_and_release_overlap(
        self,
        reservation: &WriteTransientMemoryReservation,
    ) {
        // Serialize the transient-to-retained transfer with every other storage-memory
        // reservation. No concurrent writer can observe both the old retained counter and the
        // released overlap.
        let _admission_guard = self
            .memory_reservation_admission
            .reservation_admission_lock
            .lock();
        self.sync_registry_memory_usage();
        reservation.reset_to_base();
    }

    fn reserve_new_series_rate(
        self,
        requested: usize,
    ) -> Result<Option<SeriesCreationRateReservation>> {
        self.series_creation_rate_limiter
            .reserve(self.clock.current_timestamp_units(), requested)
    }
}

pub(super) struct WriteResolver<'a> {
    engine: WriteResolveContext<'a>,
}

impl<'a> WriteResolver<'a> {
    pub(super) fn new(engine: WriteResolveContext<'a>) -> Self {
        Self { engine }
    }

    #[cfg(test)]
    pub(super) fn preflight_and_reserve_write_rows(
        &self,
        rows: &[Row],
    ) -> Result<WriteTransientMemoryReservation> {
        let scratch = self.preflight_write_rows_scratch_bytes(rows)?;
        self.reserve_write_scratch(scratch)
    }

    pub(super) fn preflight_write_rows_scratch_bytes(&self, rows: &[Row]) -> Result<usize> {
        if let Some(limit) = self.engine.write_batch_limits.max_rows {
            if rows.len() > limit {
                return Err(TsinkError::WriteBatchRowLimitExceeded {
                    limit,
                    submitted: rows.len(),
                });
            }
        }
        let modeled_input = crate::modeled_write_batch_input_bytes(rows)?;
        if let Some(limit) = self.engine.write_batch_limits.max_modeled_input_bytes {
            if modeled_input > limit {
                return Err(TsinkError::WriteBatchInputLimitExceeded {
                    limit,
                    submitted: modeled_input,
                });
            }
        }
        modeled_write_preparation_peak_bytes(rows, self.engine.wal_enabled)
    }

    pub(super) fn reserve_write_scratch(
        &self,
        scratch: usize,
    ) -> Result<WriteTransientMemoryReservation> {
        self.engine
            .write_transient
            .new_reservation(scratch, self.engine.memory_reservation_admission)
    }

    pub(super) fn reserve_write_rejection_result(
        &self,
        rows_len: usize,
    ) -> Result<WriteTransientMemoryReservation> {
        self.reserve_write_scratch(modeled_write_rejection_result_bytes(rows_len)?)
    }

    #[cfg(test)]
    pub(super) fn resolve_write_rows(&self, rows: &[Row]) -> Result<ResolvedWrite> {
        let transient_memory = self.preflight_and_reserve_write_rows(rows)?;
        self.resolve_write_rows_with_reservation(rows, transient_memory)
    }

    pub(super) fn resolve_write_rows_with_reservation(
        &self,
        rows: &[Row],
        transient_memory: WriteTransientMemoryReservation,
    ) -> Result<ResolvedWrite> {
        let mut pending_points = Vec::with_capacity(rows.len());
        let mut new_series_defs = Vec::new();
        let mut created_series = Vec::<SeriesResolution>::new();
        let mut pending_new_series = HashMap::<RawSeriesKey, usize>::new();
        let mut pending_new_series_plans = Vec::<PendingNewSeriesPlan>::new();
        let mut pending_new_point_refs = Vec::<(usize, usize)>::new();
        let mut series_creation_rate_reservation = None;
        let max_future_timestamp = self.engine.max_future_skew_window.map(|window| {
            self.engine
                .clock
                .current_timestamp_units()
                .saturating_add(window)
        });

        self.engine.with_registry(|registry| {
            for row in rows {
                validate_series_identity(
                    row.metric(),
                    row.labels(),
                    self.engine.max_labels_per_series,
                    self.engine.max_series_identity_bytes,
                )?;

                let data_point = row.data_point();
                if let Some(cutoff) = max_future_timestamp {
                    if data_point.timestamp > cutoff {
                        return Err(TsinkError::FutureSkewExceeded {
                            timestamp: data_point.timestamp,
                            cutoff,
                        });
                    }
                }
                let lane = lane_for_value(&data_point.value);

                if let Some(resolution) = registry.resolve_existing(row.metric(), row.labels()) {
                    pending_points.push(PendingPoint {
                        series_id: resolution.series_id,
                        lane,
                        ts: data_point.timestamp,
                        value: data_point.value.clone(),
                        wal_highwater: WalHighWatermark::default(),
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
                    wal_highwater: WalHighWatermark::default(),
                });
                pending_new_point_refs.push((point_idx, plan_idx));
            }
            Ok::<(), TsinkError>(())
        })?;

        if !pending_new_series_plans.is_empty() {
            let planned_series = pending_new_series_plans
                .iter()
                .map(|plan| SeriesKey {
                    metric: plan.metric.clone(),
                    labels: plan.labels.clone(),
                })
                .collect::<Vec<_>>();
            let estimated_registry_growth = self.engine.with_registry(|registry| {
                registry.estimate_new_series_memory_growth_bytes_with_transient_admission(
                    &planned_series,
                    |required| {
                        self.engine
                            .reserve_new_series_overlap(&transient_memory, required)
                    },
                )
            })?;
            // Estimation clones are gone. Replace their temporary peak with an atomic retained
            // growth overlap before dictionaries or postings can be mutated.
            transient_memory.reset_to_base();
            self.engine
                .reserve_new_series_overlap(&transient_memory, estimated_registry_growth)?;
        }

        if !pending_new_series_plans.is_empty() {
            let mut pending_plan_series_ids = vec![0; pending_new_series_plans.len()];
            let mut newly_created_series = Vec::<SeriesResolution>::new();
            let mut created_plan_indexes = Vec::<usize>::new();
            let mut reserved_capacity = 0usize;

            if let Err(err) = self.engine.with_registry(|registry| -> Result<()> {
                let mut missing_plan_indexes = Vec::new();
                for (plan_idx, plan) in pending_new_series_plans.iter().enumerate() {
                    if let Some(existing) = registry.resolve_existing(&plan.metric, &plan.labels) {
                        pending_plan_series_ids[plan_idx] = existing.series_id;
                    } else {
                        missing_plan_indexes.push(plan_idx);
                    }
                }

                let requested = missing_plan_indexes.len();
                reserved_capacity = self
                    .engine
                    .reserve_new_series_capacity(registry, requested)?;
                series_creation_rate_reservation =
                    self.engine.reserve_new_series_rate(requested)?;

                if !missing_plan_indexes.is_empty() {
                    let planned_series = missing_plan_indexes
                        .iter()
                        .map(|plan_idx| {
                            let plan = &pending_new_series_plans[*plan_idx];
                            SeriesKey {
                                metric: plan.metric.clone(),
                                labels: plan.labels.clone(),
                            }
                        })
                        .collect::<Vec<_>>();
                    registry.prime_dictionaries_for_series(&planned_series)?;
                }

                for plan_idx in missing_plan_indexes {
                    let plan = &pending_new_series_plans[plan_idx];
                    let resolution = registry.resolve_or_insert(&plan.metric, &plan.labels)?;
                    pending_plan_series_ids[plan_idx] = resolution.series_id;

                    if resolution.created {
                        created_plan_indexes.push(plan_idx);
                        newly_created_series.push(resolution);
                    }
                }

                Ok(())
            }) {
                self.engine.release_new_series_capacity(reserved_capacity);
                self.engine.rollback_created_series(&newly_created_series);
                self.engine
                    .publish_registry_memory_and_release_overlap(&transient_memory);
                return Err(err);
            }

            self.engine.release_new_series_capacity(reserved_capacity);
            if let Some(reservation) = series_creation_rate_reservation.as_mut() {
                reservation.retain(newly_created_series.len());
            }
            self.engine
                .publish_registry_memory_and_release_overlap(&transient_memory);
            created_series.extend(newly_created_series.iter().cloned());
            for plan_idx in created_plan_indexes {
                let plan = &pending_new_series_plans[plan_idx];
                new_series_defs.push(SeriesDefinitionFrame {
                    series_id: pending_plan_series_ids[plan_idx],
                    metric: plan.metric.clone(),
                    labels: plan.labels.clone(),
                });
            }

            for (point_idx, plan_idx) in pending_new_point_refs {
                pending_points[point_idx].series_id = pending_plan_series_ids[plan_idx];
            }
        }

        Ok(ResolvedWrite {
            pending_points,
            new_series_defs,
            created_series,
            series_creation_rate_reservation,
            transient_memory,
        })
    }

    pub(super) fn rollback_resolved_write(&self, resolved: ResolvedWrite) {
        self.engine
            .rollback_created_series(&resolved.created_series);
    }
}
