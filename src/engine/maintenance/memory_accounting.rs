#[path = "memory_accounting/context.rs"]
mod context;

use roaring::RoaringTreemap;

use super::super::MemoryDeltaBytes;
use super::super::*;

const MEMORY_APPROACHING_LIMIT_BASIS_POINTS: u16 = 9_000;
const KNOWN_EXCLUDED_MEMORY_CATEGORIES: &[&str] = &[
    "query_working_sets",
    "decompression_buffers",
    "caller_owned_write_inputs",
    "public_wal_helper_result_collections",
    "rollup_working_state",
    "catalog_input_inventory_materialization",
    "thread_stacks",
    "allocator_and_runtime_overhead",
    "adapter_and_server_state",
];

/// Shared accounting for conservative foreground-write and startup-WAL scratch reservations.
#[derive(Debug, Default)]
pub(in super::super) struct WriteTransientMemoryAccounting {
    current_bytes: AtomicU64,
    peak_bytes: AtomicU64,
    reservations_total: AtomicU64,
    rejections_total: AtomicU64,
}

#[derive(Debug)]
struct WriteTransientMemoryLease {
    accounting: Arc<WriteTransientMemoryAccounting>,
    /// Immutable scratch envelope admitted before the write owns any retained engine state.
    /// Retried and best-effort sub-writes size their temporary retained-growth overlap from this
    /// baseline instead of cumulatively adding the same conversion allowance to the lease.
    base_reserved_bytes: u64,
    reserved_bytes: AtomicU64,
}

/// Cloneable handle to one reservation. Clones share one lease, and the final drop releases it.
#[derive(Debug, Clone)]
pub(in super::super) struct WriteTransientMemoryReservation {
    lease: Arc<WriteTransientMemoryLease>,
}

impl WriteTransientMemoryAccounting {
    fn increment(counter: &AtomicU64) {
        let _ = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
            Some(value.saturating_add(1))
        });
    }

    fn update_peak(&self, current: u64) {
        let _ = self
            .peak_bytes
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |peak| {
                (current > peak).then_some(current)
            });
    }

    fn reserve_additional(
        &self,
        additional: u64,
        admission: MemoryReservationAdmissionContext<'_>,
    ) -> Result<()> {
        if additional == 0 {
            return Ok(());
        }

        let _admission_guard = admission.reservation_admission_lock.lock();
        loop {
            let current = self.current_bytes.load(Ordering::Acquire);
            let used = admission.used_bytes.load(Ordering::Acquire);
            let tombstone_staged = admission.tombstone_staged_bytes.load(Ordering::Acquire);
            let remote_catalog_staged = admission.remote_catalog_staging.current_bytes_u64();
            let budget = admission.budget_bytes.load(Ordering::Acquire);
            let required = used
                .checked_add(tombstone_staged)
                .and_then(|bytes| bytes.checked_add(remote_catalog_staged))
                .and_then(|bytes| bytes.checked_add(current))
                .and_then(|bytes| bytes.checked_add(additional))
                .ok_or(TsinkError::WriteBatchSizeOverflow)?;
            if budget != u64::MAX && required > budget {
                Self::increment(&self.rejections_total);
                Self::increment(admission.memory_rejections_total);
                return Err(TsinkError::MemoryBudgetExceeded {
                    budget: budget.min(usize::MAX as u64) as usize,
                    required: required.min(usize::MAX as u64) as usize,
                });
            }
            let next = current
                .checked_add(additional)
                .ok_or(TsinkError::WriteBatchSizeOverflow)?;
            match self.current_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.update_peak(next);
                    return Ok(());
                }
                Err(_) => continue,
            }
        }
    }

    pub(in crate::engine::storage_engine) fn new_reservation(
        self: &Arc<Self>,
        requested_bytes: usize,
        admission: MemoryReservationAdmissionContext<'_>,
    ) -> Result<WriteTransientMemoryReservation> {
        let requested =
            u64::try_from(requested_bytes).map_err(|_| TsinkError::WriteBatchSizeOverflow)?;
        self.reserve_additional(requested, admission)?;
        Self::increment(&self.reservations_total);
        Ok(WriteTransientMemoryReservation {
            lease: Arc::new(WriteTransientMemoryLease {
                accounting: Arc::clone(self),
                base_reserved_bytes: requested,
                reserved_bytes: AtomicU64::new(requested),
            }),
        })
    }

    pub(in crate::engine::storage_engine) fn current_bytes(&self) -> usize {
        self.current_bytes_u64().min(usize::MAX as u64) as usize
    }

    fn current_bytes_u64(&self) -> u64 {
        self.current_bytes.load(Ordering::Acquire)
    }
}

impl WriteTransientMemoryReservation {
    pub(in super::super) fn base_reserved_bytes(&self) -> usize {
        self.lease.base_reserved_bytes.min(usize::MAX as u64) as usize
    }

    pub(in super::super) fn reserved_bytes(&self) -> usize {
        self.lease
            .reserved_bytes
            .load(Ordering::Acquire)
            .min(usize::MAX as u64) as usize
    }

    /// Releases the temporary retained-growth overlap after publication while preserving the
    /// original top-level scratch envelope for outcomes, best-effort rows, or a caller-held clone.
    pub(in super::super) fn reset_to_base(&self) {
        let base = self.lease.base_reserved_bytes;
        let previous = self.lease.reserved_bytes.swap(base, Ordering::AcqRel);
        if previous > base {
            self.lease
                .accounting
                .current_bytes
                .fetch_sub(previous - base, Ordering::AcqRel);
        }
    }

    pub(in super::super) fn ensure(
        &self,
        requested_bytes: usize,
        admission: MemoryReservationAdmissionContext<'_>,
    ) -> Result<()> {
        let requested =
            u64::try_from(requested_bytes).map_err(|_| TsinkError::WriteBatchSizeOverflow)?;
        loop {
            let current = self.lease.reserved_bytes.load(Ordering::Acquire);
            if requested <= current {
                return Ok(());
            }
            let additional = requested - current;
            self.lease
                .accounting
                .reserve_additional(additional, admission)?;
            match self.lease.reserved_bytes.compare_exchange(
                current,
                requested,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(observed) => {
                    self.lease
                        .accounting
                        .current_bytes
                        .fetch_sub(additional, Ordering::AcqRel);
                    if observed >= requested {
                        return Ok(());
                    }
                }
            }
        }
    }
}

impl Drop for WriteTransientMemoryLease {
    fn drop(&mut self) {
        let reserved = self.reserved_bytes.swap(0, Ordering::AcqRel);
        if reserved > 0 {
            self.accounting
                .current_bytes
                .fetch_sub(reserved, Ordering::AcqRel);
        }
    }
}

/// Shared accounting for finite remote catalog reads and read-write catalog publication staging.
#[derive(Debug, Default)]
pub(in super::super) struct RemoteCatalogMemoryAccounting {
    current_bytes: AtomicU64,
}

/// Shared inputs for globally serialized storage-memory reservation admission.
#[derive(Clone, Copy)]
pub(in super::super) struct MemoryReservationAdmissionContext<'a> {
    pub(in super::super) reservation_admission_lock: &'a Mutex<()>,
    pub(in super::super) used_bytes: &'a AtomicU64,
    pub(in super::super) tombstone_staged_bytes: &'a AtomicU64,
    pub(in super::super) remote_catalog_staging: &'a RemoteCatalogMemoryAccounting,
    pub(in super::super) write_transient: &'a WriteTransientMemoryAccounting,
    pub(in super::super) budget_bytes: &'a AtomicU64,
    pub(in super::super) memory_rejections_total: &'a AtomicU64,
}

/// One retained catalog staging lease. Resizing admits growth before allocation, while shrinking
/// and final drop immediately return bytes to the global storage-memory envelope.
pub(in super::super) struct RemoteCatalogMemoryReservation {
    accounting: Arc<RemoteCatalogMemoryAccounting>,
    reserved_bytes: u64,
}

impl RemoteCatalogMemoryAccounting {
    fn current_bytes_u64(&self) -> u64 {
        self.current_bytes.load(Ordering::Acquire)
    }

    pub(in super::super) fn current_bytes(&self) -> usize {
        self.current_bytes_u64().min(usize::MAX as u64) as usize
    }

    fn reserve_additional(
        &self,
        additional: u64,
        admission: MemoryReservationAdmissionContext<'_>,
    ) -> Result<()> {
        if additional == 0 {
            return Ok(());
        }

        let _admission_guard = admission.reservation_admission_lock.lock();
        loop {
            let current = self.current_bytes.load(Ordering::Acquire);
            let used = admission.used_bytes.load(Ordering::Acquire);
            let tombstone_staged = admission.tombstone_staged_bytes.load(Ordering::Acquire);
            let transient = admission.write_transient.current_bytes_u64();
            let budget = admission.budget_bytes.load(Ordering::Acquire);
            let required = used
                .checked_add(tombstone_staged)
                .and_then(|bytes| bytes.checked_add(transient))
                .and_then(|bytes| bytes.checked_add(current))
                .and_then(|bytes| bytes.checked_add(additional))
                .ok_or(TsinkError::WriteBatchSizeOverflow)?;
            if budget != u64::MAX && required > budget {
                WriteTransientMemoryAccounting::increment(admission.memory_rejections_total);
                return Err(TsinkError::MemoryBudgetExceeded {
                    budget: budget.min(usize::MAX as u64) as usize,
                    required: required.min(usize::MAX as u64) as usize,
                });
            }
            let next = current
                .checked_add(additional)
                .ok_or(TsinkError::WriteBatchSizeOverflow)?;
            match self.current_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => return Ok(()),
                Err(_) => continue,
            }
        }
    }

    pub(in super::super) fn new_reservation(
        self: &Arc<Self>,
        requested_bytes: usize,
        admission: MemoryReservationAdmissionContext<'_>,
    ) -> Result<RemoteCatalogMemoryReservation> {
        let requested =
            u64::try_from(requested_bytes).map_err(|_| TsinkError::WriteBatchSizeOverflow)?;
        self.reserve_additional(requested, admission)?;
        Ok(RemoteCatalogMemoryReservation {
            accounting: Arc::clone(self),
            reserved_bytes: requested,
        })
    }
}

impl RemoteCatalogMemoryReservation {
    pub(in super::super) fn reserved_bytes(&self) -> usize {
        self.reserved_bytes.min(usize::MAX as u64) as usize
    }

    pub(in super::super) fn resize(
        &mut self,
        requested_bytes: usize,
        admission: MemoryReservationAdmissionContext<'_>,
    ) -> Result<()> {
        let requested =
            u64::try_from(requested_bytes).map_err(|_| TsinkError::WriteBatchSizeOverflow)?;
        if requested <= self.reserved_bytes {
            let released = self.reserved_bytes - requested;
            if released > 0 {
                self.accounting
                    .current_bytes
                    .fetch_sub(released, Ordering::AcqRel);
                self.reserved_bytes = requested;
            }
            return Ok(());
        }

        self.accounting
            .reserve_additional(requested - self.reserved_bytes, admission)?;
        self.reserved_bytes = requested;
        Ok(())
    }
}

impl Drop for RemoteCatalogMemoryReservation {
    fn drop(&mut self) {
        if self.reserved_bytes > 0 {
            self.accounting
                .current_bytes
                .fetch_sub(self.reserved_bytes, Ordering::AcqRel);
        }
    }
}

/// A conservative admission charge for tombstone decode/RMW staging. The durable delete and
/// snapshot/recovery callers also drain writer permits, while ordinary write admission includes
/// this counter so future call sites cannot silently ignore an active staging envelope.
pub(in super::super) struct TombstoneMemoryReservation<'a> {
    used_bytes: &'a AtomicU64,
    staged_bytes: &'a AtomicU64,
    remote_catalog_staging: &'a RemoteCatalogMemoryAccounting,
    write_transient: &'a WriteTransientMemoryAccounting,
    reservation_admission_lock: &'a Mutex<()>,
    budget_bytes: &'a AtomicU64,
    rejections_total: &'a AtomicU64,
    reserved_bytes: u64,
}

impl TombstoneMemoryReservation<'_> {
    pub(in super::super) fn ensure(&mut self, requested_bytes: usize) -> Result<()> {
        let requested = saturating_u64_from_usize(requested_bytes);
        let current = self.reserved_bytes.min(usize::MAX as u64) as usize;
        if requested <= self.reserved_bytes {
            return Ok(());
        }
        self.resize(current.max(requested_bytes))
    }

    pub(in super::super) fn resize(&mut self, requested_bytes: usize) -> Result<()> {
        let requested = saturating_u64_from_usize(requested_bytes);
        if requested <= self.reserved_bytes {
            let released = self.reserved_bytes - requested;
            if released > 0 {
                self.staged_bytes.fetch_sub(released, Ordering::AcqRel);
                self.reserved_bytes = requested;
            }
            return Ok(());
        }

        let additional = requested - self.reserved_bytes;
        let _admission_guard = self.reservation_admission_lock.lock();
        loop {
            let budget = self.budget_bytes.load(Ordering::Acquire);
            let used = self.used_bytes.load(Ordering::Acquire);
            let staged = self.staged_bytes.load(Ordering::Acquire);
            let remote_catalog_staged = self.remote_catalog_staging.current_bytes_u64();
            let write_transient = self.write_transient.current_bytes_u64();
            let required = used
                .saturating_add(staged)
                .saturating_add(remote_catalog_staged)
                .saturating_add(write_transient)
                .saturating_add(additional);
            if budget != u64::MAX && required > budget {
                let _ = self.rejections_total.fetch_update(
                    Ordering::AcqRel,
                    Ordering::Acquire,
                    |value| Some(value.saturating_add(1)),
                );
                return Err(TsinkError::MemoryBudgetExceeded {
                    budget: budget.min(usize::MAX as u64) as usize,
                    required: required.min(usize::MAX as u64) as usize,
                });
            }
            match self.staged_bytes.compare_exchange_weak(
                staged,
                staged.saturating_add(additional),
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.reserved_bytes = requested;
                    return Ok(());
                }
                Err(_) => continue,
            }
        }
    }
}

impl Drop for TombstoneMemoryReservation<'_> {
    fn drop(&mut self) {
        if self.reserved_bytes > 0 {
            self.staged_bytes
                .fetch_sub(self.reserved_bytes, Ordering::AcqRel);
        }
    }
}

impl ChunkStorage {
    pub(in super::super) fn memory_reservation_admission_context(
        &self,
    ) -> MemoryReservationAdmissionContext<'_> {
        MemoryReservationAdmissionContext {
            reservation_admission_lock: &self.memory.reservation_admission_lock,
            used_bytes: &self.memory.used_bytes,
            tombstone_staged_bytes: &self.memory.tombstone_staged_bytes,
            remote_catalog_staging: self.memory.remote_catalog_staging.as_ref(),
            write_transient: self.memory.write_transient.as_ref(),
            budget_bytes: &self.memory.budget_bytes,
            memory_rejections_total: &self.memory.rejections_total,
        }
    }

    pub(in super::super) fn remote_catalog_memory_reservation(
        &self,
        requested_bytes: usize,
    ) -> Result<RemoteCatalogMemoryReservation> {
        self.memory
            .remote_catalog_staging
            .new_reservation(requested_bytes, self.memory_reservation_admission_context())
    }

    pub(in super::super) fn resize_remote_catalog_memory_reservation(
        &self,
        reservation: &mut RemoteCatalogMemoryReservation,
        requested_bytes: usize,
    ) -> Result<()> {
        reservation.resize(requested_bytes, self.memory_reservation_admission_context())
    }

    pub(in super::super) fn reserve_write_transient_memory(
        &self,
        requested_bytes: usize,
    ) -> Result<WriteTransientMemoryReservation> {
        self.memory
            .write_transient
            .new_reservation(requested_bytes, self.memory_reservation_admission_context())
    }

    pub(in super::super) fn ensure_write_transient_memory(
        &self,
        reservation: &WriteTransientMemoryReservation,
        requested_bytes: usize,
    ) -> Result<()> {
        reservation.ensure(requested_bytes, self.memory_reservation_admission_context())
    }

    pub(in super::super) fn memory_budget_value(&self) -> usize {
        self.memory_accounting_context().budget_value()
    }

    pub(in super::super) fn memory_used_value(&self) -> usize {
        self.memory_accounting_context().used_value()
    }

    pub(in super::super) fn tombstone_memory_reservation(&self) -> TombstoneMemoryReservation<'_> {
        TombstoneMemoryReservation {
            used_bytes: &self.memory.used_bytes,
            staged_bytes: &self.memory.tombstone_staged_bytes,
            remote_catalog_staging: self.memory.remote_catalog_staging.as_ref(),
            write_transient: self.memory.write_transient.as_ref(),
            reservation_admission_lock: &self.memory.reservation_admission_lock,
            budget_bytes: &self.memory.budget_bytes,
            rejections_total: &self.memory.rejections_total,
            reserved_bytes: 0,
        }
    }

    pub(in super::super) fn add_included_memory_component_bytes(
        &self,
        component: &AtomicU64,
        bytes: usize,
    ) {
        if !self.memory.accounting_enabled || bytes == 0 {
            return;
        }

        let increment = saturating_u64_from_usize(bytes);
        component.fetch_add(increment, Ordering::AcqRel);
        self.memory
            .shared_used_bytes
            .fetch_add(increment, Ordering::AcqRel);
        self.memory
            .used_bytes
            .fetch_add(increment, Ordering::AcqRel);
    }

    pub(in super::super) fn sub_included_memory_component_bytes(
        &self,
        component: &AtomicU64,
        bytes: usize,
    ) {
        if !self.memory.accounting_enabled || bytes == 0 {
            return;
        }

        let decrement = saturating_u64_from_usize(bytes);
        component.fetch_sub(decrement, Ordering::AcqRel);
        self.memory
            .shared_used_bytes
            .fetch_sub(decrement, Ordering::AcqRel);
        self.memory
            .used_bytes
            .fetch_sub(decrement, Ordering::AcqRel);
    }

    pub(in super::super) fn account_included_memory_component_delta_bytes(
        &self,
        component: &AtomicU64,
        before: usize,
        after: usize,
    ) {
        self.account_included_memory_component_delta(
            component,
            MemoryDeltaBytes::between(before, after),
        );
    }

    pub(in super::super) fn account_included_memory_component_delta(
        &self,
        component: &AtomicU64,
        delta: MemoryDeltaBytes,
    ) {
        if !self.memory.accounting_enabled {
            return;
        }

        if delta.added_bytes >= delta.removed_bytes {
            self.add_included_memory_component_bytes(
                component,
                delta.added_bytes.saturating_sub(delta.removed_bytes),
            );
        } else {
            self.sub_included_memory_component_bytes(
                component,
                delta.removed_bytes.saturating_sub(delta.added_bytes),
            );
        }
    }

    pub(in super::super) fn with_included_memory_delta<T, R, Measure, Mutate>(
        &self,
        component: &AtomicU64,
        state: &mut T,
        measure: Measure,
        mutate: Mutate,
    ) -> R
    where
        Measure: Fn(&T) -> usize,
        Mutate: FnOnce(&mut T) -> R,
    {
        if !self.memory.accounting_enabled {
            return mutate(state);
        }

        let before = measure(state);
        let result = mutate(state);
        let after = measure(state);
        self.account_included_memory_component_delta_bytes(component, before, after);
        result
    }

    pub(in super::super) fn add_memory_usage_bytes(&self, shard_idx: usize, bytes: usize) {
        if !self.memory.accounting_enabled || bytes == 0 {
            return;
        }

        let increment = saturating_u64_from_usize(bytes);
        self.memory.used_bytes_by_shard[shard_idx].fetch_add(increment, Ordering::AcqRel);
        self.memory
            .used_bytes
            .fetch_add(increment, Ordering::AcqRel);
    }

    pub(in super::super) fn sub_memory_usage_bytes(&self, shard_idx: usize, bytes: usize) {
        if !self.memory.accounting_enabled || bytes == 0 {
            return;
        }

        let decrement = saturating_u64_from_usize(bytes);
        self.memory.used_bytes_by_shard[shard_idx].fetch_sub(decrement, Ordering::AcqRel);
        self.memory
            .used_bytes
            .fetch_sub(decrement, Ordering::AcqRel);
    }

    pub(in super::super) fn account_memory_delta(&self, shard_idx: usize, delta: MemoryDeltaBytes) {
        if !self.memory.accounting_enabled {
            return;
        }
        if delta.added_bytes >= delta.removed_bytes {
            self.add_memory_usage_bytes(
                shard_idx,
                delta.added_bytes.saturating_sub(delta.removed_bytes),
            );
        } else {
            self.sub_memory_usage_bytes(
                shard_idx,
                delta.removed_bytes.saturating_sub(delta.added_bytes),
            );
        }
    }

    #[allow(dead_code)]
    pub(in super::super) fn account_memory_delta_bytes(
        &self,
        shard_idx: usize,
        before: usize,
        after: usize,
    ) {
        self.account_memory_delta(shard_idx, MemoryDeltaBytes::between(before, after));
    }

    pub(in super::super) fn refresh_memory_usage(&self) -> usize {
        self.memory_accounting_context().refresh_memory_usage()
    }

    pub(in super::super) fn memory_observability_snapshot(
        &self,
    ) -> crate::MemoryObservabilitySnapshot {
        let memory_accounting = self.memory_accounting_context();
        if !self.memory.accounting_enabled {
            memory_accounting.refresh_memory_usage();
        }

        let accounted_bytes = memory_accounting.used_value();
        let persisted_mmap_bytes = context::MemoryAccountingContext::component_value(
            &self.memory.persisted_mmap_used_bytes,
        );
        let budget = memory_accounting.budget_value();
        let finite_budget = (budget != usize::MAX).then_some(budget);
        let approaching_limit_bytes = finite_budget.map(|budget| {
            let numerator = (budget as u128)
                .saturating_mul(u128::from(MEMORY_APPROACHING_LIMIT_BASIS_POINTS))
                .saturating_add(9_999);
            u64::try_from((numerator / 10_000).min(u64::MAX.into())).unwrap_or(u64::MAX)
        });
        let active_backpressured_writers = self
            .memory
            .active_backpressured_writers
            .load(Ordering::Acquire);
        let pressure_level = if self.storage_health_degraded() {
            crate::MemoryPressureLevel::Degraded
        } else if active_backpressured_writers > 0 {
            crate::MemoryPressureLevel::Backpressured
        } else if finite_budget.is_some_and(|budget| accounted_bytes >= budget) {
            crate::MemoryPressureLevel::Rejecting
        } else if approaching_limit_bytes
            .is_some_and(|threshold| (accounted_bytes as u128) >= u128::from(threshold))
        {
            crate::MemoryPressureLevel::ApproachingLimit
        } else {
            crate::MemoryPressureLevel::Normal
        };

        crate::MemoryObservabilitySnapshot {
            accounted_bytes,
            estimated_accounted_bytes: accounted_bytes.saturating_sub(persisted_mmap_bytes),
            budgeted_bytes: accounted_bytes,
            excluded_bytes: 0,
            excluded_bytes_known: false,
            excluded_categories: KNOWN_EXCLUDED_MEMORY_CATEGORIES
                .iter()
                .map(|category| (*category).to_string())
                .collect(),
            active_and_sealed_bytes: memory_accounting.active_and_sealed_used_value(),
            registry_bytes: context::MemoryAccountingContext::component_value(
                &self.memory.registry_used_bytes,
            ),
            metadata_cache_bytes: context::MemoryAccountingContext::component_value(
                &self.memory.metadata_used_bytes,
            ),
            persisted_index_bytes: context::MemoryAccountingContext::component_value(
                &self.memory.persisted_index_used_bytes,
            ),
            persisted_mmap_bytes,
            // Tombstone memory includes the live map plus an active decode/RMW staging envelope.
            // The envelope converts to live accounting before it is released at publication.
            tombstone_bytes: context::MemoryAccountingContext::component_value(
                &self.memory.tombstone_used_bytes,
            )
            .saturating_add(context::MemoryAccountingContext::component_value(
                &self.memory.tombstone_staged_bytes,
            )),
            remote_catalog_staging_bytes: self.memory.remote_catalog_staging.current_bytes(),
            wal_writer_buffer_bytes: memory_accounting.wal_writer_buffer_used_value(),
            wal_series_definition_cache_bytes: context::MemoryAccountingContext::component_value(
                &self.memory.wal_series_definition_cache_used_bytes,
            ),
            write_transient_bytes: self.memory.write_transient.current_bytes(),
            peak_write_transient_bytes: self
                .memory
                .write_transient
                .peak_bytes
                .load(Ordering::Acquire)
                .min(usize::MAX as u64) as usize,
            write_transient_reservations_total: self
                .memory
                .write_transient
                .reservations_total
                .load(Ordering::Acquire),
            write_transient_rejections_total: self
                .memory
                .write_transient
                .rejections_total
                .load(Ordering::Acquire),
            write_transient_bytes_estimated: true,
            excluded_persisted_mmap_bytes: 0,
            pressure: crate::MemoryPressureSnapshot {
                level: Some(pressure_level),
                approaching_limit_basis_points: finite_budget
                    .map(|_| MEMORY_APPROACHING_LIMIT_BASIS_POINTS),
                approaching_limit_bytes,
                active_backpressured_writers,
                backpressure_events_total: self
                    .memory
                    .backpressure_events_total
                    .load(Ordering::Acquire),
                rejections_total: self.memory.rejections_total.load(Ordering::Acquire),
            },
        }
    }

    pub(in super::super) fn active_state_memory_usage_bytes(state: &ActiveSeriesState) -> usize {
        std::mem::size_of::<ActiveSeriesState>()
            .saturating_add(
                state
                    .partition_head_count()
                    .saturating_mul(std::mem::size_of::<super::super::state::ActivePartitionHead>())
                    .saturating_add(
                        state
                            .partition_head_count()
                            .saturating_mul(std::mem::size_of::<(WalHighWatermark, usize)>()),
                    ),
            )
            .saturating_add(state.partition_heads.values().fold(0usize, |acc, head| {
                acc.saturating_add(
                    head.builder
                        .capacity()
                        .saturating_mul(std::mem::size_of::<ChunkPoint>()),
                )
                .saturating_add(
                    head.builder
                        .point_block_capacity()
                        .saturating_mul(std::mem::size_of::<Arc<Vec<ChunkPoint>>>()),
                )
                .saturating_add(
                    head.builder
                        .frozen_point_block_count()
                        .saturating_mul(std::mem::size_of::<Vec<ChunkPoint>>()),
                )
                .saturating_add(head.builder_value_heap_bytes)
            }))
    }

    fn active_state_memory_usage_bytes_reconciled(state: &ActiveSeriesState) -> usize {
        let mut bytes = std::mem::size_of::<ActiveSeriesState>().saturating_add(
            state
                .partition_head_count()
                .saturating_mul(std::mem::size_of::<super::super::state::ActivePartitionHead>())
                .saturating_add(
                    state
                        .partition_head_count()
                        .saturating_mul(std::mem::size_of::<(WalHighWatermark, usize)>()),
                ),
        );
        for head in state.partition_heads.values() {
            bytes = bytes.saturating_add(
                head.builder
                    .capacity()
                    .saturating_mul(std::mem::size_of::<ChunkPoint>()),
            );
            bytes = bytes.saturating_add(
                head.builder
                    .point_block_capacity()
                    .saturating_mul(std::mem::size_of::<Arc<Vec<ChunkPoint>>>()),
            );
            bytes = bytes.saturating_add(
                head.builder
                    .frozen_point_block_count()
                    .saturating_mul(std::mem::size_of::<Vec<ChunkPoint>>()),
            );
            for point in head.builder.iter_points() {
                bytes = bytes.saturating_add(value_heap_bytes(&point.value));
            }
        }
        bytes
    }

    pub(in super::super) fn chunk_memory_usage_bytes(chunk: &Chunk) -> usize {
        let mut bytes = std::mem::size_of::<Chunk>()
            // Every sealed chunk also owns one fixed-size entry in each view of the pending
            // persistence index. B-tree allocator overhead remains part of the documented
            // allocator/runtime estimate, consistent with the other ordered indexes.
            .saturating_add(std::mem::size_of::<PendingSealedChunkIndexKey>())
            .saturating_add(std::mem::size_of::<PendingSealedChunkLocation>())
            .saturating_add(
                chunk
                    .points
                    .capacity()
                    .saturating_mul(std::mem::size_of::<ChunkPoint>()),
            )
            .saturating_add(chunk.encoded_payload.capacity());

        for point in &chunk.points {
            bytes = bytes.saturating_add(value_heap_bytes(&point.value));
        }

        bytes
    }

    pub(in super::super) fn hash_map_memory_usage_bytes<K, V>(map: &HashMap<K, V>) -> usize {
        map.capacity().saturating_mul(std::mem::size_of::<(K, V)>())
    }

    pub(in super::super) fn btree_set_series_id_memory_usage_bytes(
        set: &BTreeSet<SeriesId>,
    ) -> usize {
        set.len().saturating_mul(std::mem::size_of::<SeriesId>())
    }

    pub(in super::super) fn bitmap_memory_usage_bytes(bitmap: &RoaringTreemap) -> usize {
        if bitmap.is_empty() {
            0
        } else {
            bitmap.serialized_size()
        }
    }

    pub(in super::super) fn tombstone_map_memory_usage_bytes(
        tombstones: &crate::engine::tombstone::TombstoneMap,
    ) -> usize {
        let mut bytes = tombstones
            .len()
            .saturating_mul(crate::engine::tombstone::TOMBSTONE_BTREE_ENTRY_ALLOCATION_BYTES);
        for ranges in tombstones.values() {
            bytes =
                bytes.saturating_add(ranges.capacity().saturating_mul(std::mem::size_of::<
                    crate::engine::tombstone::TombstoneRange,
                >()));
        }
        bytes
    }

    fn metadata_shard_index_memory_usage_bytes(index: &MetadataShardIndex) -> usize {
        index
            .series_ids_by_shard
            .read()
            .iter()
            .fold(0usize, |acc, bucket| {
                acc.saturating_add(Self::btree_set_series_id_memory_usage_bytes(bucket))
            })
    }

    pub(in super::super) fn persisted_segment_state_memory_usage_bytes(
        root: &std::path::Path,
        state: &super::super::state::PersistedSegmentState,
    ) -> usize {
        let mut bytes = root.as_os_str().len().saturating_add(std::mem::size_of::<
            super::super::state::PersistedSegmentState,
        >());
        if let Some(time_bucket_postings) = &state.time_bucket_postings {
            bytes = bytes.saturating_add(
                std::mem::size_of::<super::super::state::PersistedSegmentTimeBucketIndex>()
                    .saturating_add(
                        time_bucket_postings
                            .buckets
                            .capacity()
                            .saturating_mul(std::mem::size_of::<RoaringTreemap>()),
                    ),
            );
            for bucket in &time_bucket_postings.buckets {
                if !bucket.is_empty() {
                    bytes = bytes.saturating_add(bucket.serialized_size());
                }
            }
        }
        bytes = bytes.saturating_add(Self::hash_map_memory_usage_bytes::<
            SeriesId,
            super::super::state::PersistedSeriesTimeRangeSummary,
        >(&state.series_time_summaries));
        bytes = bytes.saturating_add(Self::hash_map_memory_usage_bytes::<
            SeriesId,
            Vec<PersistedChunkRef>,
        >(&state.chunk_refs_by_series));
        for refs in state.chunk_refs_by_series.values() {
            bytes = bytes.saturating_add(
                refs.capacity()
                    .saturating_mul(std::mem::size_of::<PersistedChunkRef>()),
            );
        }
        bytes
    }

    pub(in super::super) fn persisted_index_included_memory_usage_bytes(
        index: &PersistedIndexState,
    ) -> usize {
        let mut bytes = Self::hash_map_memory_usage_bytes::<SeriesId, Vec<PersistedChunkRef>>(
            &index.chunk_refs,
        )
        .saturating_add(Self::hash_map_memory_usage_bytes::<
            u64,
            std::sync::OnceLock<crate::engine::encoder::TimestampSearchIndex>,
        >(&index.chunk_timestamp_indexes))
        .saturating_add(
            Self::hash_map_memory_usage_bytes::<usize, Arc<PlatformMmap>>(&index.segment_maps),
        )
        .saturating_add(Self::hash_map_memory_usage_bytes::<
            usize,
            super::super::tiering::PersistedSegmentTier,
        >(&index.segment_tiers))
        .saturating_add(index.merged_postings.memory_usage_bytes())
        .saturating_add(Self::bitmap_memory_usage_bytes(
            &index.runtime_metadata_delta_series_ids,
        ));

        for refs in index.chunk_refs.values() {
            bytes = bytes.saturating_add(
                refs.capacity()
                    .saturating_mul(std::mem::size_of::<PersistedChunkRef>()),
            );
        }
        for search_index in index.chunk_timestamp_indexes.values() {
            if let Some(search_index) = search_index.get() {
                bytes = bytes.saturating_add(search_index.memory_usage_bytes());
            }
        }
        for (root, state) in &index.segments_by_root {
            bytes = bytes.saturating_add(Self::persisted_segment_state_memory_usage_bytes(
                root, state,
            ));
        }
        bytes
    }

    pub(in super::super) fn persisted_index_persisted_mmap_bytes(
        index: &PersistedIndexState,
    ) -> usize {
        index
            .segment_maps
            .values()
            .fold(0usize, |acc, mapped| acc.saturating_add(mapped.len()))
    }

    pub(in super::super) fn prune_empty_active_series(&self) {
        if !self.memory.accounting_enabled {
            for shard in &self.chunks.active_builders {
                shard.write().retain(|_, state| !state.is_empty());
            }
            return;
        }

        for (shard_idx, shard) in self.chunks.active_builders.iter().enumerate() {
            let mut active = shard.write();
            let mut removed_bytes = 0usize;
            active.retain(|_, state| {
                let keep = !state.is_empty();
                if !keep {
                    removed_bytes =
                        removed_bytes.saturating_add(Self::active_state_memory_usage_bytes(state));
                }
                keep
            });
            self.account_memory_delta(shard_idx, MemoryDeltaBytes::from_totals(0, removed_bytes));
        }
    }

    pub(in super::super) fn mark_persisted_chunk_watermarks(
        &self,
        watermarks: &HashMap<SeriesId, u64>,
    ) {
        if watermarks.is_empty() {
            return;
        }

        let mut persisted = self.chunks.persisted_chunk_watermarks.write();
        self.with_included_memory_delta(
            &self.memory.metadata_used_bytes,
            &mut persisted,
            |persisted| Self::hash_map_memory_usage_bytes::<SeriesId, u64>(persisted),
            |persisted| {
                for (series_id, watermark) in watermarks {
                    let entry = persisted.entry(*series_id).or_insert(0);
                    *entry = (*entry).max(*watermark);
                }
            },
        );
        drop(persisted);
    }
}
