use super::*;
use std::borrow::Cow;
use std::time::Instant;

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::engine::storage_engine) struct MemoryDeltaBytes {
    pub(in crate::engine::storage_engine) added_bytes: usize,
    pub(in crate::engine::storage_engine) removed_bytes: usize,
}

impl MemoryDeltaBytes {
    pub(in crate::engine::storage_engine) fn from_totals(
        added_bytes: usize,
        removed_bytes: usize,
    ) -> Self {
        Self {
            added_bytes,
            removed_bytes,
        }
    }

    pub(in crate::engine::storage_engine) fn between(before: usize, after: usize) -> Self {
        let mut delta = Self::default();
        delta.record_change(before, after);
        delta
    }

    pub(in crate::engine::storage_engine) fn record_change(&mut self, before: usize, after: usize) {
        if after >= before {
            self.record_addition(after.saturating_sub(before));
        } else {
            self.record_removal(before.saturating_sub(after));
        }
    }

    pub(in crate::engine::storage_engine) fn record_addition(&mut self, bytes: usize) {
        self.added_bytes = self.added_bytes.saturating_add(bytes);
    }

    pub(in crate::engine::storage_engine) fn record_removal(&mut self, bytes: usize) {
        self.removed_bytes = self.removed_bytes.saturating_add(bytes);
    }

    pub(in crate::engine::storage_engine) fn record_replacement<T>(
        &mut self,
        added_bytes: usize,
        replaced: Option<&T>,
        measure: impl Fn(&T) -> usize,
    ) {
        self.record_addition(added_bytes);
        if let Some(replaced) = replaced {
            self.record_removal(measure(replaced));
        }
    }
}

pub(in crate::engine::storage_engine) fn saturating_u64_from_usize(value: usize) -> u64 {
    value.min(u64::MAX as usize) as u64
}

pub(in crate::engine::storage_engine) fn elapsed_nanos_u64(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u64::MAX as u128) as u64
}

pub(in crate::engine::storage_engine) fn current_unix_millis_u64() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u64::MAX as u128) as u64)
        .unwrap_or(0)
}

pub(in crate::engine::storage_engine) fn duration_to_timestamp_units(
    duration: Duration,
    precision: TimestampPrecision,
) -> i64 {
    match precision {
        TimestampPrecision::Seconds => i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        TimestampPrecision::Milliseconds => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
        TimestampPrecision::Microseconds => i64::try_from(duration.as_micros()).unwrap_or(i64::MAX),
        TimestampPrecision::Nanoseconds => i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX),
    }
}

pub(in crate::engine::storage_engine) fn current_unix_timestamp_units(
    precision: TimestampPrecision,
) -> i64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    duration_to_timestamp_units(now, precision)
}

pub(in crate::engine::storage_engine) fn partition_id_for_timestamp(
    timestamp: i64,
    partition_window: i64,
) -> i64 {
    timestamp.div_euclid(partition_window.max(1))
}

pub(in crate::engine::storage_engine) fn value_heap_bytes(value: &Value) -> usize {
    match value {
        Value::Bytes(bytes) => bytes.capacity(),
        Value::String(text) => text.capacity(),
        Value::Histogram(histogram) => {
            std::mem::size_of::<crate::NativeHistogram>()
                + histogram.negative_spans.capacity()
                    * std::mem::size_of::<crate::HistogramBucketSpan>()
                + histogram.negative_deltas.capacity() * std::mem::size_of::<i64>()
                + histogram.negative_counts.capacity() * std::mem::size_of::<f64>()
                + histogram.positive_spans.capacity()
                    * std::mem::size_of::<crate::HistogramBucketSpan>()
                + histogram.positive_deltas.capacity() * std::mem::size_of::<i64>()
                + histogram.positive_counts.capacity() * std::mem::size_of::<f64>()
                + histogram.custom_values.capacity() * std::mem::size_of::<f64>()
        }
        _ => 0,
    }
}

pub(in crate::engine::storage_engine) fn lane_for_value(value: &Value) -> ValueLane {
    match value {
        Value::Bytes(_) | Value::String(_) | Value::Histogram(_) => ValueLane::Blob,
        _ => ValueLane::Numeric,
    }
}

pub(in crate::engine::storage_engine) fn persisted_chunk_payload<'a>(
    persisted_segment_maps: &'a HashMap<usize, Arc<PlatformMmap>>,
    chunk_ref: &PersistedChunkRef,
) -> Result<Cow<'a, [u8]>> {
    let Some(mapped_segment) = persisted_segment_maps.get(&chunk_ref.segment_slot) else {
        return Err(TsinkError::DataCorruption(format!(
            "missing mapped segment slot {}",
            chunk_ref.segment_slot
        )));
    };
    crate::engine::segment::chunk_payload_from_record(
        mapped_segment.as_slice(),
        chunk_ref.chunk_offset,
        chunk_ref.chunk_len,
    )
}

impl ChunkStorage {
    pub(in crate::engine::storage_engine) fn ensure_open(&self) -> Result<()> {
        if self
            .observability
            .health
            .fail_fast_triggered
            .load(Ordering::SeqCst)
        {
            return Err(TsinkError::StorageShuttingDown);
        }
        if self.coordination.lifecycle.load(Ordering::SeqCst) != STORAGE_OPEN {
            return Err(TsinkError::StorageClosed);
        }
        Ok(())
    }

    pub(in crate::engine::storage_engine) fn install_data_path_process_lock(
        &self,
        data_path_process_lock: DataPathProcessLock,
    ) {
        *self.coordination.data_path_process_lock.lock() = Some(data_path_process_lock);
    }

    pub(in crate::engine::storage_engine) fn release_data_path_process_lock(&self) {
        self.coordination.data_path_process_lock.lock().take();
    }

    pub(in crate::engine::storage_engine) fn background_maintenance_gate(
        &self,
    ) -> MutexGuard<'_, ()> {
        self.coordination.background_maintenance_lock.lock()
    }

    pub(in crate::engine::storage_engine) fn compaction_gate(&self) -> MutexGuard<'_, ()> {
        self.coordination.compaction_lock.lock()
    }

    pub(in crate::engine::storage_engine) fn lock_compaction_gate(
        compaction_lock: &Mutex<()>,
    ) -> MutexGuard<'_, ()> {
        compaction_lock.lock()
    }

    pub(in crate::engine::storage_engine) fn current_timestamp_units(&self) -> i64 {
        #[cfg(test)]
        {
            let override_timestamp = self.current_time_override.load(Ordering::Acquire);
            if override_timestamp != i64::MIN {
                return override_timestamp;
            }
        }

        current_unix_timestamp_units(self.runtime.timestamp_precision)
    }

    pub(in crate::engine::storage_engine) fn current_future_skew_cutoff(&self) -> i64 {
        self.current_timestamp_units()
            .saturating_add(self.runtime.future_skew_window)
    }

    #[cfg(test)]
    pub(in crate::engine::storage_engine) fn set_current_time_override(&self, timestamp: i64) {
        self.current_time_override
            .store(timestamp, Ordering::Release);
        self.bump_live_series_pruning_generation();
    }
}
