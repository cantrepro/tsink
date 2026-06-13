use std::collections::HashMap;
use std::sync::atomic::Ordering;

use super::super::super::{
    lane_for_value, validate_labels, validate_metric, Label, Result, Row, SeriesDefinitionFrame,
    SeriesRegistry, SeriesResolution, TsinkError, WalHighWatermark, WriteResolveContext,
};
use super::phases::{PendingPoint, ResolvedWrite};
use crate::engine::series::SeriesKey;

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

    fn enforce_new_series_admission(self, estimated_registry_growth: usize) -> Result<()> {
        let budget = self
            .budget_bytes
            .load(Ordering::Acquire)
            .min(usize::MAX as u64) as usize;
        if budget == usize::MAX {
            return Ok(());
        }

        let used = self
            .used_bytes
            .load(Ordering::Acquire)
            .min(usize::MAX as u64) as usize;
        let required = used.saturating_add(estimated_registry_growth);
        if required > budget {
            return Err(TsinkError::MemoryBudgetExceeded { budget, required });
        }

        Ok(())
    }
}

pub(super) struct WriteResolver<'a> {
    engine: WriteResolveContext<'a>,
}

impl<'a> WriteResolver<'a> {
    pub(super) fn new(engine: WriteResolveContext<'a>) -> Self {
        Self { engine }
    }

    pub(super) fn resolve_write_rows(&self, rows: &[Row]) -> Result<ResolvedWrite> {
        let mut pending_points = Vec::with_capacity(rows.len());
        let mut new_series_defs = Vec::new();
        let mut created_series = Vec::<SeriesResolution>::new();
        let mut pending_new_series = HashMap::<RawSeriesKey, usize>::new();
        let mut pending_new_series_plans = Vec::<PendingNewSeriesPlan>::new();
        let mut pending_new_point_refs = Vec::<(usize, usize)>::new();

        self.engine.with_registry(|registry| {
            for row in rows {
                validate_metric(row.metric())?;
                validate_labels(row.labels())?;

                let data_point = row.data_point();
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
                registry.estimate_new_series_memory_growth_bytes(&planned_series)
            })?;
            self.engine
                .enforce_new_series_admission(estimated_registry_growth)?;
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
                return Err(err);
            }

            self.engine.release_new_series_capacity(reserved_capacity);
            if !newly_created_series.is_empty() {
                self.engine.sync_registry_memory_usage();
            }
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
        })
    }

    pub(super) fn rollback_resolved_write(&self, resolved: ResolvedWrite) {
        self.engine
            .rollback_created_series(&resolved.created_series);
    }
}
