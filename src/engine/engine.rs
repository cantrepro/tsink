use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU8, AtomicU64, Ordering};
use std::time::Duration;

use parking_lot::RwLock;

use crate::concurrency::Semaphore;
use crate::engine::chunk::{Chunk, ChunkBuilder, ChunkPoint, ValueLane};
use crate::engine::compactor::Compactor;
use crate::engine::encoder::Encoder;
use crate::engine::query::{ChunkSeriesCursor, decode_chunk_points_in_range_into};
use crate::engine::segment::{SegmentWriter, load_segments};
use crate::engine::series_registry::{SeriesId, SeriesRegistry, validate_labels, validate_metric};
use crate::engine::wal::{FramedWal, ReplayFrame, SamplesBatchFrame, SeriesDefinitionFrame};
use crate::storage::{
    TimestampPrecision, aggregate_series, downsample_points, downsample_points_with_custom,
};
use crate::{
    Aggregation, DataPoint, Label, MetricSeries, QueryOptions, Result, Row, Storage,
    StorageBuilder, TsinkError, Value,
};

const STORAGE_OPEN: u8 = 0;
const STORAGE_CLOSING: u8 = 1;
const STORAGE_CLOSED: u8 = 2;
const DEFAULT_RETENTION: Duration = Duration::from_secs(14 * 24 * 3600);
const DEFAULT_WRITE_TIMEOUT: Duration = Duration::from_secs(30);
const DEFAULT_PARTITION_DURATION: Duration = Duration::from_secs(3600);

#[derive(Debug, Clone, Copy)]
struct ChunkStorageOptions {
    retention_window: i64,
    retention_enforced: bool,
    partition_window: i64,
    max_writers: usize,
    write_timeout: Duration,
}

impl Default for ChunkStorageOptions {
    fn default() -> Self {
        Self {
            retention_window: duration_to_timestamp_units(
                DEFAULT_RETENTION,
                TimestampPrecision::Nanoseconds,
            ),
            retention_enforced: false,
            partition_window: duration_to_timestamp_units(
                DEFAULT_PARTITION_DURATION,
                TimestampPrecision::Nanoseconds,
            )
            .max(1),
            max_writers: crate::cgroup::default_workers_limit().max(1),
            write_timeout: DEFAULT_WRITE_TIMEOUT,
        }
    }
}

struct ActiveSeriesState {
    series_id: SeriesId,
    lane: ValueLane,
    point_cap: usize,
    builder: ChunkBuilder,
    partition_id: Option<i64>,
}

impl ActiveSeriesState {
    fn new(series_id: SeriesId, lane: ValueLane, point_cap: usize) -> Self {
        Self {
            series_id,
            lane,
            point_cap,
            builder: ChunkBuilder::new(series_id, lane, point_cap),
            partition_id: None,
        }
    }

    fn rotate_partition_if_needed(
        &mut self,
        ts: i64,
        partition_window: i64,
    ) -> Result<Option<Chunk>> {
        let partition_window = partition_window.max(1);
        let next_partition = partition_id_for_timestamp(ts, partition_window);

        if self.builder.is_empty() {
            self.partition_id = Some(next_partition);
            return Ok(None);
        }

        if self.partition_id.is_none() {
            self.partition_id = self
                .builder
                .first_point()
                .map(|first| partition_id_for_timestamp(first.ts, partition_window));
        }

        if self.partition_id == Some(next_partition) {
            return Ok(None);
        }

        let chunk = self.finalize_current()?;
        self.partition_id = Some(next_partition);
        Ok(chunk)
    }

    fn rotate_full_if_needed(&mut self) -> Result<Option<Chunk>> {
        if !self.builder.is_full() {
            return Ok(None);
        }
        let chunk = self.finalize_current()?;
        self.partition_id = None;
        Ok(chunk)
    }

    fn flush_partial(&mut self) -> Result<Option<Chunk>> {
        if self.builder.is_empty() {
            return Ok(None);
        }
        let chunk = self.finalize_current()?;
        self.partition_id = None;
        Ok(chunk)
    }

    fn finalize_current(&mut self) -> Result<Option<Chunk>> {
        let old_builder = std::mem::replace(
            &mut self.builder,
            ChunkBuilder::new(self.series_id, self.lane, self.point_cap),
        );
        let mut chunk = old_builder
            .finalize(
                super::chunk::TimestampCodecId::DeltaVarint,
                super::chunk::ValueCodecId::ConstantRle,
            )
            .ok_or_else(|| {
                TsinkError::InvalidConfiguration("failed to finalize chunk".to_string())
            })?;

        // Preserve a monotonic timestamp stream per chunk for better timestamp codec density.
        chunk.points.sort_by_key(|point| point.ts);

        let encoded = Encoder::encode_chunk_points(&chunk.points, self.lane)?;
        chunk.header.ts_codec = encoded.ts_codec;
        chunk.header.value_codec = encoded.value_codec;
        chunk.encoded_payload = encoded.payload;

        Ok(Some(chunk))
    }
}

struct PendingPoint {
    series_id: SeriesId,
    lane: ValueLane,
    ts: i64,
    value: Value,
}

const NUMERIC_LANE_ROOT: &str = "lane_numeric";
const BLOB_LANE_ROOT: &str = "lane_blob";
const WAL_DIR_NAME: &str = "wal";

pub struct ChunkStorage {
    registry: RwLock<SeriesRegistry>,
    active_builders: RwLock<HashMap<SeriesId, ActiveSeriesState>>,
    sealed_chunks: RwLock<HashMap<SeriesId, Vec<Chunk>>>,
    persisted_chunk_counts: RwLock<HashMap<SeriesId, usize>>,
    chunk_point_cap: usize,
    numeric_lane_path: Option<PathBuf>,
    blob_lane_path: Option<PathBuf>,
    next_segment_id: AtomicU64,
    numeric_compactor: Option<Compactor>,
    blob_compactor: Option<Compactor>,
    wal: Option<FramedWal>,
    retention_window: i64,
    retention_enforced: bool,
    partition_window: i64,
    write_limiter: Semaphore,
    write_timeout: Duration,
    max_observed_timestamp: AtomicI64,
    lifecycle: AtomicU8,
}

impl ChunkStorage {
    pub fn new(chunk_point_cap: usize, wal: Option<FramedWal>) -> Self {
        Self::new_with_data_path_and_options(
            chunk_point_cap,
            wal,
            None,
            None,
            1,
            ChunkStorageOptions::default(),
        )
    }

    pub fn new_with_data_path(
        chunk_point_cap: usize,
        wal: Option<FramedWal>,
        numeric_lane_path: Option<PathBuf>,
        blob_lane_path: Option<PathBuf>,
        next_segment_id: u64,
    ) -> Self {
        Self::new_with_data_path_and_options(
            chunk_point_cap,
            wal,
            numeric_lane_path,
            blob_lane_path,
            next_segment_id,
            ChunkStorageOptions::default(),
        )
    }

    fn new_with_data_path_and_options(
        chunk_point_cap: usize,
        wal: Option<FramedWal>,
        numeric_lane_path: Option<PathBuf>,
        blob_lane_path: Option<PathBuf>,
        next_segment_id: u64,
        options: ChunkStorageOptions,
    ) -> Self {
        Self {
            registry: RwLock::new(SeriesRegistry::new()),
            active_builders: RwLock::new(HashMap::new()),
            sealed_chunks: RwLock::new(HashMap::new()),
            persisted_chunk_counts: RwLock::new(HashMap::new()),
            chunk_point_cap: chunk_point_cap.clamp(1, u16::MAX as usize),
            numeric_compactor: numeric_lane_path
                .as_ref()
                .map(|path| Compactor::new(path, chunk_point_cap)),
            blob_compactor: blob_lane_path
                .as_ref()
                .map(|path| Compactor::new(path, chunk_point_cap)),
            numeric_lane_path,
            blob_lane_path,
            next_segment_id: AtomicU64::new(next_segment_id.max(1)),
            wal,
            retention_window: options.retention_window.max(0),
            retention_enforced: options.retention_enforced,
            partition_window: options.partition_window.max(1),
            write_limiter: Semaphore::new(options.max_writers.max(1)),
            write_timeout: options.write_timeout,
            max_observed_timestamp: AtomicI64::new(i64::MIN),
            lifecycle: AtomicU8::new(STORAGE_OPEN),
        }
    }

    fn ensure_open(&self) -> Result<()> {
        if self.lifecycle.load(Ordering::SeqCst) != STORAGE_OPEN {
            return Err(TsinkError::StorageClosed);
        }
        Ok(())
    }

    fn update_max_observed_timestamp(&self, ts: i64) {
        let mut current = self.max_observed_timestamp.load(Ordering::Acquire);
        while ts > current {
            match self.max_observed_timestamp.compare_exchange_weak(
                current,
                ts,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(actual) => current = actual,
            }
        }
    }

    fn active_retention_cutoff(&self) -> Option<i64> {
        if !self.retention_enforced {
            return None;
        }
        let max_observed = self.max_observed_timestamp.load(Ordering::Acquire);
        if max_observed == i64::MIN {
            return None;
        }
        Some(max_observed.saturating_sub(self.retention_window))
    }

    fn apply_retention_filter(&self, points: &mut Vec<DataPoint>) {
        let Some(cutoff) = self.active_retention_cutoff() else {
            return;
        };
        points.retain(|point| point.timestamp >= cutoff);
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

    fn append_sealed_chunk(&self, series_id: SeriesId, chunk: Chunk) {
        let mut sealed = self.sealed_chunks.write();
        let entry = sealed.entry(series_id).or_default();
        let key = (
            chunk.header.min_ts,
            chunk.header.max_ts,
            chunk.header.point_count,
        );
        // Maintain sorted chunk order with binary insertion.
        let insert_at = entry.partition_point(|existing| {
            (
                existing.header.min_ts,
                existing.header.max_ts,
                existing.header.point_count,
            ) <= key
        });
        entry.insert(insert_at, chunk);
    }

    fn flush_all_active(&self) -> Result<()> {
        let mut finalized = Vec::new();

        {
            let mut active = self.active_builders.write();
            for (series_id, state) in active.iter_mut() {
                if let Some(chunk) = state.flush_partial()? {
                    finalized.push((*series_id, chunk));
                }
            }
        }

        if finalized.is_empty() {
            return Ok(());
        }

        for (series_id, chunk) in finalized {
            self.append_sealed_chunk(series_id, chunk);
        }

        Ok(())
    }

    fn collect_points_for_series_into(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        out: &mut Vec<DataPoint>,
    ) -> Result<()> {
        out.clear();
        let mut has_overlap = false;
        let mut has_previous_chunk = false;
        let mut previous_max_ts = i64::MIN;
        let mut requires_output_validation = false;

        {
            let sealed = self.sealed_chunks.read();
            if let Some(chunks) = sealed.get(&series_id) {
                for chunk in ChunkSeriesCursor::new(chunks, start, end) {
                    if has_previous_chunk && chunk.header.min_ts <= previous_max_ts {
                        has_overlap = true;
                    }
                    has_previous_chunk = true;
                    previous_max_ts = previous_max_ts.max(chunk.header.max_ts);

                    // Chunks without encoded payload may be ad-hoc/manual and not guaranteed sorted.
                    if chunk.points.len() > 1 && chunk.encoded_payload.is_empty() {
                        requires_output_validation = true;
                    }

                    decode_chunk_points_in_range_into(chunk, start, end, out)?;
                }
            }
        }

        {
            let active = self.active_builders.read();
            if let Some(state) = active.get(&series_id) {
                let mut previous_active_ts = i64::MIN;
                let mut has_previous_active = false;

                for point in state.builder.points() {
                    if point.ts < start || point.ts >= end {
                        continue;
                    }

                    if has_previous_chunk && point.ts <= previous_max_ts {
                        has_overlap = true;
                    }
                    if has_previous_active && point.ts < previous_active_ts {
                        requires_output_validation = true;
                    }

                    has_previous_active = true;
                    previous_active_ts = point.ts;
                    out.push(DataPoint::new(point.ts, point.value.clone()));
                }
            }
        }

        self.apply_retention_filter(out);

        if has_overlap || requires_output_validation {
            if !points_are_sorted_by_timestamp(out) {
                out.sort_by_key(|point| point.timestamp);
            }
            dedupe_last_value_per_timestamp(out);
        }
        Ok(())
    }

    fn collect_points_for_series(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
    ) -> Result<Vec<DataPoint>> {
        let mut out = Vec::new();
        self.collect_points_for_series_into(series_id, start, end, &mut out)?;
        Ok(out)
    }

    fn validate_select_request(metric: &str, labels: &[Label], start: i64, end: i64) -> Result<()> {
        validate_metric(metric)?;
        validate_labels(labels)?;
        if start >= end {
            return Err(TsinkError::InvalidTimeRange { start, end });
        }
        Ok(())
    }

    fn select_into_impl(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        out: &mut Vec<DataPoint>,
    ) -> Result<()> {
        if !labels.is_empty() {
            // Exact series identity query path.
            let Some(series_id) = self
                .registry
                .read()
                .resolve_existing(metric, labels)
                .map(|resolution| resolution.series_id)
            else {
                out.clear();
                return Ok(());
            };
            return self.collect_points_for_series_into(series_id, start, end, out);
        }

        let (single_series, candidate_series) = {
            let registry = self.registry.read();
            let Some(series_ids) = registry.series_id_postings_for_metric(metric) else {
                out.clear();
                return Ok(());
            };

            if series_ids.len() == 1 {
                (series_ids.iter().next().copied(), Vec::new())
            } else {
                (None, series_ids.iter().copied().collect::<Vec<_>>())
            }
        };

        if let Some(series_id) = single_series {
            return self.collect_points_for_series_into(series_id, start, end, out);
        }

        out.clear();
        let mut scratch = Vec::new();
        for series_id in candidate_series {
            self.collect_points_for_series_into(series_id, start, end, &mut scratch)?;
            out.append(&mut scratch);
        }
        if !points_are_sorted_by_timestamp(out) {
            out.sort_by_key(|point| point.timestamp);
        }
        Ok(())
    }

    fn validate_series_lane_compatible(&self, series_id: SeriesId, lane: ValueLane) -> Result<()> {
        if let Some(active_lane) = self
            .active_builders
            .read()
            .get(&series_id)
            .map(|state| state.lane)
            && active_lane != lane
        {
            return Err(TsinkError::ValueTypeMismatch {
                expected: lane_name(active_lane).to_string(),
                actual: lane_name(lane).to_string(),
            });
        }

        if let Some(sealed_lane) = self
            .sealed_chunks
            .read()
            .get(&series_id)
            .and_then(|chunks| chunks.last())
            .map(|chunk| chunk.header.lane)
            && sealed_lane != lane
        {
            return Err(TsinkError::ValueTypeMismatch {
                expected: lane_name(sealed_lane).to_string(),
                actual: lane_name(lane).to_string(),
            });
        }

        Ok(())
    }

    fn append_point_to_series(
        &self,
        series_id: SeriesId,
        lane: ValueLane,
        ts: i64,
        value: Value,
    ) -> Result<()> {
        let finalized = {
            let mut active = self.active_builders.write();
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
            state.builder.append(ts, value);
            if let Some(chunk) = state.rotate_full_if_needed()? {
                finalized.push(chunk);
            }
            finalized
        };

        for chunk in finalized {
            self.append_sealed_chunk(series_id, chunk);
        }
        self.update_max_observed_timestamp(ts);

        Ok(())
    }

    fn ingest_pending_points(&self, points: Vec<PendingPoint>) -> Result<()> {
        for point in points {
            self.append_point_to_series(point.series_id, point.lane, point.ts, point.value)?;
        }
        Ok(())
    }

    fn group_pending_points_by_series(
        points: &[PendingPoint],
    ) -> Result<BTreeMap<SeriesId, (ValueLane, Vec<ChunkPoint>)>> {
        let mut grouped: BTreeMap<SeriesId, (ValueLane, Vec<ChunkPoint>)> = BTreeMap::new();

        for point in points {
            let entry = grouped
                .entry(point.series_id)
                .or_insert_with(|| (point.lane, Vec::new()));

            if entry.0 != point.lane {
                return Err(TsinkError::ValueTypeMismatch {
                    expected: lane_name(entry.0).to_string(),
                    actual: lane_name(point.lane).to_string(),
                });
            }

            entry.1.push(ChunkPoint {
                ts: point.ts,
                value: point.value.clone(),
            });
        }

        Ok(grouped)
    }

    fn validate_pending_point_families(&self, points: &[PendingPoint]) -> Result<()> {
        let mut grouped = Self::group_pending_points_by_series(points)?;
        let active = self.active_builders.read();

        for (series_id, (lane, chunk_points)) in grouped.iter_mut() {
            if let Some(existing_point) = active
                .get(series_id)
                .and_then(|state| state.builder.first_point())
            {
                chunk_points.insert(0, existing_point.clone());
            }

            Encoder::validate_chunk_points(chunk_points, *lane)?;
        }
        Ok(())
    }

    fn encode_wal_batches(points: &[PendingPoint]) -> Result<Vec<SamplesBatchFrame>> {
        let grouped = Self::group_pending_points_by_series(points)?;

        let mut batches = Vec::with_capacity(grouped.len());
        for (series_id, (lane, chunk_points)) in grouped {
            batches.push(SamplesBatchFrame::from_points(
                series_id,
                lane,
                &chunk_points,
            )?);
        }

        Ok(batches)
    }

    fn replay_from_wal(&self) -> Result<()> {
        let Some(wal) = &self.wal else {
            return Ok(());
        };

        let frames = wal.replay_frames()?;
        for frame in frames {
            match frame {
                ReplayFrame::SeriesDefinition(definition) => {
                    self.registry.write().register_series_with_id(
                        definition.series_id,
                        &definition.metric,
                        &definition.labels,
                    )?;
                }
                ReplayFrame::Samples(batches) => {
                    for batch in batches {
                        let points = batch.decode_points()?;
                        for point in points {
                            self.append_point_to_series(
                                batch.series_id,
                                batch.lane,
                                point.ts,
                                point.value,
                            )?;
                        }
                    }
                }
            }
        }

        Ok(())
    }

    fn apply_loaded_segments(&self, loaded: crate::engine::segment::LoadedSegments) -> Result<()> {
        {
            let mut registry = self.registry.write();
            for series in loaded.series {
                registry.register_series_with_id(
                    series.series_id,
                    &series.metric,
                    &series.labels,
                )?;
            }
        }

        {
            let mut sealed = self.sealed_chunks.write();
            let mut loaded_max_timestamp = i64::MIN;
            for (series_id, mut chunks) in loaded.chunks_by_series {
                for chunk in &chunks {
                    loaded_max_timestamp = loaded_max_timestamp.max(chunk.header.max_ts);
                }
                sealed.entry(series_id).or_default().append(&mut chunks);
            }

            for chunks in sealed.values_mut() {
                chunks.sort_by(|a, b| {
                    (a.header.min_ts, a.header.max_ts, a.header.point_count).cmp(&(
                        b.header.min_ts,
                        b.header.max_ts,
                        b.header.point_count,
                    ))
                });
            }

            if loaded_max_timestamp != i64::MIN {
                self.update_max_observed_timestamp(loaded_max_timestamp);
            }
        }

        {
            let sealed = self.sealed_chunks.read();
            let mut persisted = self.persisted_chunk_counts.write();
            persisted.clear();
            for (series_id, chunks) in sealed.iter() {
                persisted.insert(*series_id, chunks.len());
            }
        }

        self.next_segment_id
            .store(loaded.next_segment_id.max(1), Ordering::SeqCst);
        Ok(())
    }

    fn persist_segment(&self) -> Result<()> {
        if self.numeric_lane_path.is_none() && self.blob_lane_path.is_none() {
            return Ok(());
        }

        let delta_chunks = {
            let sealed = self.sealed_chunks.read();
            let persisted = self.persisted_chunk_counts.read();

            let mut delta = HashMap::new();
            for (series_id, chunks) in sealed.iter() {
                let already_persisted = persisted.get(series_id).copied().unwrap_or(0);
                if already_persisted >= chunks.len() {
                    continue;
                }

                delta.insert(*series_id, chunks[already_persisted..].to_vec());
            }
            delta
        };

        if delta_chunks.is_empty() {
            if let Some(wal) = &self.wal {
                wal.reset()?;
            }
            return Ok(());
        }

        let mut numeric_chunks = HashMap::new();
        let mut blob_chunks = HashMap::new();
        for (series_id, chunks) in &delta_chunks {
            let Some(first) = chunks.first() else {
                continue;
            };

            match first.header.lane {
                ValueLane::Numeric => {
                    numeric_chunks.insert(*series_id, chunks.clone());
                }
                ValueLane::Blob => {
                    blob_chunks.insert(*series_id, chunks.clone());
                }
            }
        }

        {
            let registry = self.registry.read();

            if let (Some(path), false) = (&self.numeric_lane_path, numeric_chunks.is_empty()) {
                let segment_id = self.next_segment_id.fetch_add(1, Ordering::SeqCst);
                let writer = SegmentWriter::new(path, 0, segment_id)?;
                writer.write_segment(&registry, &numeric_chunks)?;
            }

            if let (Some(path), false) = (&self.blob_lane_path, blob_chunks.is_empty()) {
                let segment_id = self.next_segment_id.fetch_add(1, Ordering::SeqCst);
                let writer = SegmentWriter::new(path, 0, segment_id)?;
                writer.write_segment(&registry, &blob_chunks)?;
            }
        }

        {
            let sealed = self.sealed_chunks.read();
            let mut persisted = self.persisted_chunk_counts.write();
            for (series_id, chunks) in sealed.iter() {
                persisted.insert(*series_id, chunks.len());
            }
        }

        if let Some(wal) = &self.wal {
            wal.reset()?;
        }

        Ok(())
    }

    fn compact_once_if_needed(&self) -> Result<()> {
        if let Some(compactor) = &self.numeric_compactor {
            compactor.compact_once()?;
        }
        if let Some(compactor) = &self.blob_compactor {
            compactor.compact_once()?;
        }
        Ok(())
    }
}

impl Storage for ChunkStorage {
    fn insert_rows(&self, rows: &[Row]) -> Result<()> {
        self.ensure_open()?;
        let _write_permit = self.write_limiter.try_acquire_for(self.write_timeout)?;
        // A write may pass the first lifecycle check and then block on permits while close starts.
        // Re-check after acquiring a permit so shutdown cannot race new writes through.
        self.ensure_open()?;

        let mut pending_points = Vec::with_capacity(rows.len());
        let mut new_series_defs = Vec::new();

        {
            let mut registry = self.registry.write();

            for row in rows {
                let data_point = row.data_point();
                let lane = lane_for_value(&data_point.value);
                let resolution = registry.resolve_or_insert(row.metric(), row.labels())?;

                if resolution.created {
                    new_series_defs.push(SeriesDefinitionFrame {
                        series_id: resolution.series_id,
                        metric: row.metric().to_string(),
                        labels: row.labels().to_vec(),
                    });
                }

                pending_points.push(PendingPoint {
                    series_id: resolution.series_id,
                    lane,
                    ts: data_point.timestamp,
                    value: data_point.value.clone(),
                });
            }
        }

        for point in &pending_points {
            self.validate_series_lane_compatible(point.series_id, point.lane)?;
        }

        self.validate_pending_point_families(&pending_points)?;
        self.validate_points_against_retention(&pending_points)?;

        if let Some(wal) = &self.wal {
            let batches = Self::encode_wal_batches(&pending_points)?;

            for definition in &new_series_defs {
                wal.append_series_definition(definition)?;
            }

            wal.append_samples(&batches)?;
        }

        self.ingest_pending_points(pending_points)
    }

    fn select(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
    ) -> Result<Vec<DataPoint>> {
        self.ensure_open()?;
        Self::validate_select_request(metric, labels, start, end)?;

        let mut out = Vec::new();
        self.select_into_impl(metric, labels, start, end, &mut out)?;
        Ok(out)
    }

    fn select_into(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        out: &mut Vec<DataPoint>,
    ) -> Result<()> {
        self.ensure_open()?;
        Self::validate_select_request(metric, labels, start, end)?;
        self.select_into_impl(metric, labels, start, end, out)
    }

    fn select_with_options(&self, metric: &str, opts: QueryOptions) -> Result<Vec<DataPoint>> {
        self.ensure_open()?;
        validate_metric(metric)?;
        validate_labels(&opts.labels)?;

        if opts.start >= opts.end {
            return Err(TsinkError::InvalidTimeRange {
                start: opts.start,
                end: opts.end,
            });
        }

        if let Some(downsample) = opts.downsample
            && downsample.interval <= 0
        {
            return Err(TsinkError::InvalidConfiguration(
                "downsample interval must be positive".to_string(),
            ));
        }

        let mut points = Vec::new();
        self.select_into_impl(metric, &opts.labels, opts.start, opts.end, &mut points)?;

        let aggregation = match (opts.downsample.is_some(), opts.aggregation) {
            (true, Aggregation::None) => Aggregation::Last,
            _ => opts.aggregation,
        };

        let mut processed = if let Some(custom) = opts.custom_aggregation {
            if let Some(downsample) = opts.downsample {
                downsample_points_with_custom(
                    &points,
                    downsample.interval,
                    custom.as_ref(),
                    opts.start,
                    opts.end,
                )?
            } else {
                custom
                    .aggregate_series(&points)?
                    .into_iter()
                    .collect::<Vec<DataPoint>>()
            }
        } else if let Some(downsample) = opts.downsample {
            downsample_points(
                &points,
                downsample.interval,
                aggregation,
                opts.start,
                opts.end,
            )?
        } else if aggregation != Aggregation::None {
            aggregate_series(&points, aggregation)?
                .into_iter()
                .collect::<Vec<DataPoint>>()
        } else {
            points
        };

        if opts.offset > 0 && opts.offset < processed.len() {
            processed.drain(0..opts.offset);
        } else if opts.offset >= processed.len() {
            processed.clear();
        }

        if let Some(limit) = opts.limit {
            processed.truncate(limit);
        }

        Ok(processed)
    }

    fn select_all(
        &self,
        metric: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
        self.ensure_open()?;
        validate_metric(metric)?;

        if start >= end {
            return Err(TsinkError::InvalidTimeRange { start, end });
        }

        let series_ids = self.registry.read().series_ids_for_metric(metric);
        if series_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for series_id in series_ids {
            let points = self.collect_points_for_series(series_id, start, end)?;
            if points.is_empty() {
                continue;
            }

            let labels = self
                .registry
                .read()
                .decode_series_key(series_id)
                .map(|key| key.labels)
                .unwrap_or_default();
            out.push((labels, points));
        }

        out.sort_by(|a, b| a.0.cmp(&b.0));
        Ok(out)
    }

    fn list_metrics(&self) -> Result<Vec<MetricSeries>> {
        self.ensure_open()?;

        let registry = self.registry.read();
        let mut metrics = Vec::new();
        for series_id in registry.all_series_ids() {
            if let Some(series_key) = registry.decode_series_key(series_id) {
                metrics.push(MetricSeries {
                    name: series_key.metric,
                    labels: series_key.labels,
                });
            }
        }

        Ok(metrics)
    }

    fn close(&self) -> Result<()> {
        if self
            .lifecycle
            .compare_exchange(
                STORAGE_OPEN,
                STORAGE_CLOSING,
                Ordering::SeqCst,
                Ordering::SeqCst,
            )
            .is_err()
        {
            return Err(TsinkError::StorageClosed);
        }

        let close_result = (|| {
            let _write_permits = self.write_limiter.acquire_all(self.write_timeout)?;
            self.flush_all_active()?;
            self.persist_segment()?;
            self.compact_once_if_needed()
        })();

        if close_result.is_ok() {
            self.lifecycle.store(STORAGE_CLOSED, Ordering::SeqCst);
        } else {
            self.lifecycle.store(STORAGE_OPEN, Ordering::SeqCst);
        }

        close_result
    }
}

impl Drop for ChunkStorage {
    fn drop(&mut self) {
        if self.lifecycle.load(Ordering::SeqCst) != STORAGE_OPEN {
            return;
        }

        // Best-effort shutdown to avoid losing in-memory active chunks on last Arc drop.
        let _ = <Self as Storage>::close(self);
    }
}

pub fn build_storage(builder: StorageBuilder) -> Result<Arc<dyn Storage>> {
    let timestamp_precision = builder.timestamp_precision();
    let retention = builder.retention();
    let storage_options = ChunkStorageOptions {
        retention_window: duration_to_timestamp_units(retention, timestamp_precision),
        retention_enforced: builder.retention_enforced(),
        partition_window: duration_to_timestamp_units(
            builder.partition_duration(),
            timestamp_precision,
        )
        .max(1),
        max_writers: builder.max_writers(),
        write_timeout: builder.write_timeout(),
    };

    let base_data_path = builder.data_path().map(|path| path.to_path_buf());
    let (numeric_lane_path, blob_lane_path) = if let Some(base_path) = &base_data_path {
        (
            Some(base_path.join(NUMERIC_LANE_ROOT)),
            Some(base_path.join(BLOB_LANE_ROOT)),
        )
    } else {
        (None, None)
    };

    let loaded_numeric = if let Some(path) = &numeric_lane_path {
        load_segments(path)?
    } else {
        crate::engine::segment::LoadedSegments::default()
    };
    let loaded_blob = if let Some(path) = &blob_lane_path {
        load_segments(path)?
    } else {
        crate::engine::segment::LoadedSegments::default()
    };
    let loaded_segments = merge_loaded_segments(loaded_numeric, loaded_blob)?;

    let wal = if let Some(data_path) = builder.data_path() {
        let wal_path = data_path.join(WAL_DIR_NAME);
        if builder.wal_enabled() {
            Some(FramedWal::open_with_buffer_size(
                wal_path,
                builder.wal_sync_mode(),
                builder.wal_buffer_size(),
            )?)
        } else {
            clear_wal_dir_if_present(&wal_path)?;
            None
        }
    } else {
        None
    };

    let storage = Arc::new(ChunkStorage::new_with_data_path_and_options(
        builder.chunk_points(),
        wal,
        numeric_lane_path,
        blob_lane_path,
        loaded_segments.next_segment_id,
        storage_options,
    ));
    storage.apply_loaded_segments(loaded_segments)?;
    storage.replay_from_wal()?;

    Ok(storage as Arc<dyn Storage>)
}

fn clear_wal_dir_if_present(wal_path: &Path) -> Result<()> {
    if !wal_path.exists() {
        return Ok(());
    }

    if wal_path.is_dir() {
        std::fs::remove_dir_all(wal_path)?;
    } else {
        std::fs::remove_file(wal_path)?;
    }

    Ok(())
}

fn merge_loaded_segments(
    mut numeric: crate::engine::segment::LoadedSegments,
    mut blob: crate::engine::segment::LoadedSegments,
) -> Result<crate::engine::segment::LoadedSegments> {
    let mut series_by_id = BTreeMap::new();
    for series in numeric.series.drain(..) {
        series_by_id.insert(series.series_id, series);
    }

    for series in blob.series.drain(..) {
        match series_by_id.get(&series.series_id) {
            Some(existing)
                if existing.metric == series.metric && existing.labels == series.labels => {}
            Some(_) => {
                return Err(TsinkError::DataCorruption(format!(
                    "series id {} conflicts across lane segment families",
                    series.series_id
                )));
            }
            None => {
                series_by_id.insert(series.series_id, series);
            }
        }
    }

    let mut chunks_by_series = numeric.chunks_by_series;
    for (series_id, mut chunks) in blob.chunks_by_series.drain() {
        chunks_by_series
            .entry(series_id)
            .or_default()
            .append(&mut chunks);
    }

    for chunks in chunks_by_series.values_mut() {
        chunks.sort_by(|a, b| {
            (a.header.min_ts, a.header.max_ts, a.header.point_count).cmp(&(
                b.header.min_ts,
                b.header.max_ts,
                b.header.point_count,
            ))
        });
    }

    Ok(crate::engine::segment::LoadedSegments {
        next_segment_id: numeric.next_segment_id.max(blob.next_segment_id).max(1),
        series: series_by_id.into_values().collect(),
        chunks_by_series,
    })
}

fn duration_to_timestamp_units(duration: Duration, precision: TimestampPrecision) -> i64 {
    match precision {
        TimestampPrecision::Seconds => i64::try_from(duration.as_secs()).unwrap_or(i64::MAX),
        TimestampPrecision::Milliseconds => i64::try_from(duration.as_millis()).unwrap_or(i64::MAX),
        TimestampPrecision::Microseconds => i64::try_from(duration.as_micros()).unwrap_or(i64::MAX),
        TimestampPrecision::Nanoseconds => i64::try_from(duration.as_nanos()).unwrap_or(i64::MAX),
    }
}

fn partition_id_for_timestamp(timestamp: i64, partition_window: i64) -> i64 {
    timestamp.div_euclid(partition_window.max(1))
}

fn points_are_sorted_by_timestamp(points: &[DataPoint]) -> bool {
    points
        .windows(2)
        .all(|window| window[0].timestamp <= window[1].timestamp)
}

fn dedupe_last_value_per_timestamp(points: &mut Vec<DataPoint>) {
    if points.len() < 2 {
        return;
    }

    let mut deduped: Vec<DataPoint> = Vec::with_capacity(points.len());

    for point in points.drain(..) {
        if let Some(last) = deduped.last_mut()
            && last.timestamp == point.timestamp
        {
            *last = point;
            continue;
        }
        deduped.push(point);
    }

    *points = deduped;
}

fn lane_for_value(value: &Value) -> ValueLane {
    match value {
        Value::Bytes(_) | Value::String(_) => ValueLane::Blob,
        _ => ValueLane::Numeric,
    }
}

fn lane_name(lane: ValueLane) -> &'static str {
    match lane {
        ValueLane::Numeric => "numeric",
        ValueLane::Blob => "blob",
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::time::Duration;

    use tempfile::TempDir;

    use super::{
        BLOB_LANE_ROOT, ChunkStorage, ChunkStorageOptions, NUMERIC_LANE_ROOT, WAL_DIR_NAME,
    };
    use crate::engine::chunk::{
        Chunk, ChunkHeader, ChunkPoint, TimestampCodecId, ValueCodecId, ValueLane,
    };
    use crate::engine::encoder::Encoder;
    use crate::engine::segment::SegmentWriter;
    use crate::engine::series_registry::SeriesRegistry;
    use crate::engine::wal::{FramedWal, ReplayFrame, SamplesBatchFrame, SeriesDefinitionFrame};
    use crate::wal::WalSyncMode;
    use crate::{
        DataPoint, Label, Row, Storage, StorageBuilder, TimestampPrecision, TsinkError, Value,
    };

    #[test]
    fn rotates_chunks_at_configured_cap() {
        let storage = ChunkStorage::new(2, None);
        let labels = vec![Label::new("host", "a")];

        let rows = vec![
            Row::with_labels("cpu", labels.clone(), DataPoint::new(1, 1.0)),
            Row::with_labels("cpu", labels.clone(), DataPoint::new(2, 2.0)),
            Row::with_labels("cpu", labels.clone(), DataPoint::new(3, 3.0)),
            Row::with_labels("cpu", labels.clone(), DataPoint::new(4, 4.0)),
            Row::with_labels("cpu", labels.clone(), DataPoint::new(5, 5.0)),
        ];

        storage.insert_rows(&rows).unwrap();

        let series_id = storage
            .registry
            .read()
            .resolve_existing("cpu", &labels)
            .unwrap()
            .series_id;

        let sealed = storage.sealed_chunks.read();
        let chunks = sealed.get(&series_id).unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].header.point_count, 2);
        assert_eq!(chunks[1].header.point_count, 2);

        let active = storage.active_builders.read();
        let state = active.get(&series_id).unwrap();
        assert_eq!(state.builder.len(), 1);
    }

    #[test]
    fn select_reads_active_points_without_flushing_and_sorts_points() {
        let storage = ChunkStorage::new(2, None);
        let labels = vec![Label::new("host", "a")];

        storage
            .insert_rows(&[
                Row::with_labels("latency", labels.clone(), DataPoint::new(3, 3.0)),
                Row::with_labels("latency", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("latency", labels.clone(), DataPoint::new(2, 2.0)),
            ])
            .unwrap();

        let points = storage.select("latency", &labels, 0, 10).unwrap();
        let timestamps = points
            .iter()
            .map(|point| point.timestamp)
            .collect::<Vec<_>>();
        assert_eq!(timestamps, vec![1, 2, 3]);
        assert_eq!(storage.select("latency", &labels, 0, 10).unwrap(), points);

        let series_id = storage
            .registry
            .read()
            .resolve_existing("latency", &labels)
            .unwrap()
            .series_id;

        let active = storage.active_builders.read();
        assert_eq!(active.get(&series_id).unwrap().builder.len(), 1);
        drop(active);

        let sealed = storage.sealed_chunks.read();
        assert_eq!(sealed.get(&series_id).unwrap().len(), 1);
    }

    #[test]
    fn select_sorts_unsorted_active_points_without_flushing() {
        let storage = ChunkStorage::new(8, None);
        let labels = vec![Label::new("host", "a")];

        storage
            .insert_rows(&[
                Row::with_labels("latency", labels.clone(), DataPoint::new(3, 3.0)),
                Row::with_labels("latency", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("latency", labels.clone(), DataPoint::new(2, 2.0)),
            ])
            .unwrap();

        let points = storage.select("latency", &labels, 0, 10).unwrap();
        let timestamps = points
            .iter()
            .map(|point| point.timestamp)
            .collect::<Vec<_>>();
        assert_eq!(timestamps, vec![1, 2, 3]);

        let series_id = storage
            .registry
            .read()
            .resolve_existing("latency", &labels)
            .unwrap()
            .series_id;

        let active = storage.active_builders.read();
        assert_eq!(active.get(&series_id).unwrap().builder.len(), 3);
        drop(active);

        let sealed = storage.sealed_chunks.read();
        assert!(sealed.get(&series_id).is_none());
    }

    #[test]
    fn select_sorts_overlapping_sealed_chunks() {
        let storage = ChunkStorage::new(2, None);
        let labels = vec![Label::new("host", "a")];

        storage
            .insert_rows(&[
                Row::with_labels("latency", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("latency", labels.clone(), DataPoint::new(3, 3.0)),
                Row::with_labels("latency", labels.clone(), DataPoint::new(2, 2.0)),
                Row::with_labels("latency", labels.clone(), DataPoint::new(4, 4.0)),
            ])
            .unwrap();

        let points = storage.select("latency", &labels, 0, 10).unwrap();
        let timestamps = points
            .iter()
            .map(|point| point.timestamp)
            .collect::<Vec<_>>();
        assert_eq!(timestamps, vec![1, 2, 3, 4]);
    }

    #[test]
    fn select_sorts_manual_unsorted_chunk_without_payload() {
        let storage = ChunkStorage::new(4, None);
        let labels = vec![Label::new("host", "a")];

        let series_id = storage
            .registry
            .write()
            .resolve_or_insert("manual", &labels)
            .unwrap()
            .series_id;

        storage.sealed_chunks.write().insert(
            series_id,
            vec![Chunk {
                header: ChunkHeader {
                    series_id,
                    lane: ValueLane::Numeric,
                    point_count: 3,
                    min_ts: 1,
                    max_ts: 3,
                    ts_codec: TimestampCodecId::DeltaVarint,
                    value_codec: ValueCodecId::ConstantRle,
                },
                points: vec![
                    ChunkPoint {
                        ts: 3,
                        value: Value::F64(3.0),
                    },
                    ChunkPoint {
                        ts: 1,
                        value: Value::F64(1.0),
                    },
                    ChunkPoint {
                        ts: 2,
                        value: Value::F64(2.0),
                    },
                ],
                encoded_payload: Vec::new(),
            }],
        );

        let points = storage.select("manual", &labels, 0, 10).unwrap();
        let timestamps = points
            .iter()
            .map(|point| point.timestamp)
            .collect::<Vec<_>>();
        assert_eq!(timestamps, vec![1, 2, 3]);
    }

    #[test]
    fn select_into_reuses_output_buffer() {
        let storage = ChunkStorage::new(4, None);
        let labels = vec![Label::new("host", "a")];

        storage
            .insert_rows(&[
                Row::with_labels("cpu", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("cpu", labels.clone(), DataPoint::new(2, 2.0)),
                Row::with_labels("cpu", labels.clone(), DataPoint::new(3, 3.0)),
            ])
            .unwrap();

        let mut out = vec![DataPoint::new(-1, -1.0)];
        storage
            .select_into("cpu", &labels, 0, 10, &mut out)
            .unwrap();
        assert_eq!(out.len(), 3);
        assert_eq!(out[0].timestamp, 1);
        assert_eq!(out[1].timestamp, 2);
        assert_eq!(out[2].timestamp, 3);

        let reused_capacity = out.capacity();
        storage
            .select_into("cpu", &labels, 100, 200, &mut out)
            .unwrap();
        assert!(out.is_empty());
        assert!(out.capacity() >= reused_capacity);
    }

    #[test]
    fn rejects_lane_mismatch_for_same_series() {
        let storage = ChunkStorage::new(4, None);
        let labels = vec![Label::new("host", "a")];

        storage
            .insert_rows(&[Row::with_labels(
                "events",
                labels.clone(),
                DataPoint::new(1, 1.0),
            )])
            .unwrap();

        let err = storage
            .insert_rows(&[Row::with_labels(
                "events",
                labels,
                DataPoint::new(2, "oops"),
            )])
            .unwrap_err();

        assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));
    }

    #[test]
    fn rejects_mixed_numeric_insert_when_wal_is_disabled() {
        let storage = StorageBuilder::new()
            .with_wal_enabled(false)
            .with_chunk_points(8)
            .build()
            .unwrap();
        let labels = vec![Label::new("host", "a")];

        let err = storage
            .insert_rows(&[
                Row::with_labels("mixed_no_wal", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("mixed_no_wal", labels.clone(), DataPoint::new(2, 2_i64)),
            ])
            .unwrap_err();

        assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));

        let points = storage.select("mixed_no_wal", &labels, 0, 10).unwrap();
        assert!(points.is_empty());
    }

    #[test]
    fn rejects_mixed_numeric_insert_across_calls_when_wal_is_disabled() {
        let storage = StorageBuilder::new()
            .with_wal_enabled(false)
            .with_chunk_points(8)
            .build()
            .unwrap();
        let labels = vec![Label::new("host", "a")];

        storage
            .insert_rows(&[Row::with_labels(
                "mixed_no_wal_across_calls",
                labels.clone(),
                DataPoint::new(1, 1.0),
            )])
            .unwrap();

        let err = storage
            .insert_rows(&[Row::with_labels(
                "mixed_no_wal_across_calls",
                labels.clone(),
                DataPoint::new(2, 2_i64),
            )])
            .unwrap_err();

        assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));

        let points = storage
            .select("mixed_no_wal_across_calls", &labels, 0, 10)
            .unwrap();
        assert_eq!(points, vec![DataPoint::new(1, 1.0)]);
    }

    #[test]
    fn failed_mixed_numeric_insert_does_not_resurrect_series_after_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let labels = vec![Label::new("host", "a")];

        {
            let storage = StorageBuilder::new()
                .with_data_path(temp_dir.path())
                .with_timestamp_precision(TimestampPrecision::Seconds)
                .with_chunk_points(2)
                .build()
                .unwrap();

            let err = storage
                .insert_rows(&[
                    Row::with_labels("mixed_metric", labels.clone(), DataPoint::new(1, 1.0)),
                    Row::with_labels("mixed_metric", labels.clone(), DataPoint::new(2, 2_i64)),
                ])
                .unwrap_err();
            assert!(matches!(err, TsinkError::ValueTypeMismatch { .. }));

            storage.close().unwrap();
        }

        let reopened = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        let has_mixed_metric = reopened
            .list_metrics()
            .unwrap()
            .into_iter()
            .any(|series| series.name == "mixed_metric" && series.labels == labels);
        assert!(!has_mixed_metric);

        reopened.close().unwrap();
    }

    #[test]
    fn close_clears_metadata_only_wal_when_no_chunks_are_sealed() {
        let temp_dir = TempDir::new().unwrap();
        let labels = vec![Label::new("host", "a")];
        let metric = "phantom_metric";

        {
            let wal = FramedWal::open(temp_dir.path().join("wal"), WalSyncMode::PerAppend).unwrap();
            wal.append_series_definition(&SeriesDefinitionFrame {
                series_id: 42,
                metric: metric.to_string(),
                labels: labels.clone(),
            })
            .unwrap();
        }

        {
            let storage = StorageBuilder::new()
                .with_data_path(temp_dir.path())
                .with_timestamp_precision(TimestampPrecision::Seconds)
                .with_chunk_points(2)
                .build()
                .unwrap();

            let has_phantom = storage
                .list_metrics()
                .unwrap()
                .into_iter()
                .any(|series| series.name == metric && series.labels == labels);
            assert!(has_phantom);

            storage.close().unwrap();
        }

        let reopened = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        let has_phantom = reopened
            .list_metrics()
            .unwrap()
            .into_iter()
            .any(|series| series.name == metric && series.labels == labels);
        assert!(!has_phantom);

        reopened.close().unwrap();
    }

    #[test]
    fn select_uses_exact_label_match_with_postings_candidates() {
        let storage = ChunkStorage::new(2, None);
        let short = vec![Label::new("host", "a")];
        let long = vec![Label::new("host", "a"), Label::new("region", "us")];

        storage
            .insert_rows(&[
                Row::with_labels("cpu", short.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("cpu", long.clone(), DataPoint::new(1, 10.0)),
            ])
            .unwrap();

        let points_short = storage.select("cpu", &short, 0, 10).unwrap();
        let points_long = storage.select("cpu", &long, 0, 10).unwrap();

        assert_eq!(points_short, vec![DataPoint::new(1, 1.0)]);
        assert_eq!(points_long, vec![DataPoint::new(1, 10.0)]);
    }

    #[test]
    fn recovers_rows_from_wal_after_reopen() {
        let temp_dir = TempDir::new().unwrap();
        let labels = vec![Label::new("host", "a")];

        {
            let storage = StorageBuilder::new()
                .with_data_path(temp_dir.path())
                .with_timestamp_precision(TimestampPrecision::Seconds)
                .with_chunk_points(2)
                .build()
                .unwrap();

            storage
                .insert_rows(&[
                    Row::with_labels("recover", labels.clone(), DataPoint::new(1, 1.0)),
                    Row::with_labels("recover", labels.clone(), DataPoint::new(2, 2.0)),
                    Row::with_labels("recover", labels.clone(), DataPoint::new(3, 3.0)),
                ])
                .unwrap();
        }

        let reopened = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        let points = reopened.select("recover", &labels, 0, 10).unwrap();
        assert_eq!(points.len(), 3);
        assert_eq!(points[0], DataPoint::new(1, 1.0));
        assert_eq!(points[2], DataPoint::new(3, 3.0));

        reopened.close().unwrap();
    }

    #[test]
    fn stale_wal_with_already_persisted_points_does_not_duplicate_query_results() {
        let temp_dir = TempDir::new().unwrap();
        let labels = vec![Label::new("host", "a")];
        let wal_dir = temp_dir.path().join(WAL_DIR_NAME);

        let stale_frames = {
            let storage = StorageBuilder::new()
                .with_data_path(temp_dir.path())
                .with_timestamp_precision(TimestampPrecision::Seconds)
                .with_chunk_points(2)
                .build()
                .unwrap();

            storage
                .insert_rows(&[
                    Row::with_labels("dupe_recovery", labels.clone(), DataPoint::new(1, 1.0)),
                    Row::with_labels("dupe_recovery", labels.clone(), DataPoint::new(2, 2.0)),
                    Row::with_labels("dupe_recovery", labels.clone(), DataPoint::new(3, 3.0)),
                ])
                .unwrap();

            let wal = FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap();
            let frames = wal.replay_frames().unwrap();
            assert!(
                !frames.is_empty(),
                "expected WAL frames before close so we can simulate crash window replay"
            );

            storage.close().unwrap();
            frames
        };

        {
            let wal = FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap();
            for frame in stale_frames {
                match frame {
                    ReplayFrame::SeriesDefinition(definition) => {
                        wal.append_series_definition(&definition).unwrap();
                    }
                    ReplayFrame::Samples(batches) => {
                        wal.append_samples(&batches).unwrap();
                    }
                }
            }
        }

        let reopened = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        let points = reopened.select("dupe_recovery", &labels, 0, 10).unwrap();
        assert_eq!(
            points,
            vec![
                DataPoint::new(1, 1.0),
                DataPoint::new(2, 2.0),
                DataPoint::new(3, 3.0)
            ]
        );

        reopened.close().unwrap();
    }

    #[test]
    fn stale_wal_with_already_persisted_nan_points_does_not_duplicate_query_results() {
        let temp_dir = TempDir::new().unwrap();
        let labels = vec![Label::new("host", "a")];
        let wal_dir = temp_dir.path().join(WAL_DIR_NAME);

        let stale_frames = {
            let storage = StorageBuilder::new()
                .with_data_path(temp_dir.path())
                .with_timestamp_precision(TimestampPrecision::Seconds)
                .with_chunk_points(2)
                .build()
                .unwrap();

            storage
                .insert_rows(&[
                    Row::with_labels(
                        "dupe_recovery_nan",
                        labels.clone(),
                        DataPoint::new(1, f64::NAN),
                    ),
                    Row::with_labels("dupe_recovery_nan", labels.clone(), DataPoint::new(2, 2.0)),
                ])
                .unwrap();

            let wal = FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap();
            let frames = wal.replay_frames().unwrap();
            assert!(
                !frames.is_empty(),
                "expected WAL frames before close so we can simulate crash window replay"
            );

            storage.close().unwrap();
            frames
        };

        {
            let wal = FramedWal::open(&wal_dir, WalSyncMode::PerAppend).unwrap();
            for frame in stale_frames {
                match frame {
                    ReplayFrame::SeriesDefinition(definition) => {
                        wal.append_series_definition(&definition).unwrap();
                    }
                    ReplayFrame::Samples(batches) => {
                        wal.append_samples(&batches).unwrap();
                    }
                }
            }
        }

        let reopened = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        let points = reopened
            .select("dupe_recovery_nan", &labels, 0, 10)
            .unwrap();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].timestamp, 1);
        assert!(points[0].value_as_f64().is_some_and(f64::is_nan));
        assert_eq!(points[1], DataPoint::new(2, 2.0));

        reopened.close().unwrap();
    }

    #[test]
    fn query_prefers_compacted_generation_when_compaction_crash_leaves_both_generations() {
        let temp_dir = TempDir::new().unwrap();
        let lane_path = temp_dir.path().join(NUMERIC_LANE_ROOT);
        let labels = vec![Label::new("host", "a")];
        let metric = "compaction_crash_dupe";

        let mut registry = SeriesRegistry::new();
        let series_id = registry
            .resolve_or_insert(metric, &labels)
            .unwrap()
            .series_id;

        let mut stale_l0 = HashMap::new();
        stale_l0.insert(
            series_id,
            vec![make_persisted_numeric_chunk(
                series_id,
                &[(1, 1.0), (2, 2.0)],
            )],
        );
        SegmentWriter::new(&lane_path, 0, 1)
            .unwrap()
            .write_segment(&registry, &stale_l0)
            .unwrap();

        let mut compacted_l1 = HashMap::new();
        compacted_l1.insert(
            series_id,
            vec![make_persisted_numeric_chunk(
                series_id,
                &[(1, 10.0), (2, 2.0)],
            )],
        );
        SegmentWriter::new(&lane_path, 1, 2)
            .unwrap()
            .write_segment(&registry, &compacted_l1)
            .unwrap();

        let reopened = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        let points = reopened.select(metric, &labels, 0, 10).unwrap();
        assert_eq!(
            points,
            vec![DataPoint::new(1, 10.0), DataPoint::new(2, 2.0)]
        );

        reopened.close().unwrap();
    }

    #[test]
    fn reenable_wal_after_wal_disabled_run_ignores_stale_wal_generation() {
        let temp_dir = TempDir::new().unwrap();
        let stale_labels = vec![Label::new("host", "stale")];
        let fresh_labels = vec![Label::new("host", "fresh")];

        // Seed a stale WAL generation directly so no segment files are created.
        let stale_series_id = 1;
        let wal =
            FramedWal::open(temp_dir.path().join(WAL_DIR_NAME), WalSyncMode::PerAppend).unwrap();
        wal.append_series_definition(&SeriesDefinitionFrame {
            series_id: stale_series_id,
            metric: "metric".to_string(),
            labels: stale_labels.clone(),
        })
        .unwrap();
        let stale_batch = SamplesBatchFrame::from_points(
            stale_series_id,
            ValueLane::Numeric,
            &[
                ChunkPoint {
                    ts: 1,
                    value: Value::F64(1.0),
                },
                ChunkPoint {
                    ts: 2,
                    value: Value::F64(2.0),
                },
            ],
        )
        .unwrap();
        wal.append_samples(&[stale_batch]).unwrap();
        drop(wal);

        // Run with WAL disabled: no replay should occur, and existing WAL must be cleared.
        {
            let storage = StorageBuilder::new()
                .with_data_path(temp_dir.path())
                .with_wal_enabled(false)
                .with_timestamp_precision(TimestampPrecision::Seconds)
                .with_chunk_points(2)
                .build()
                .unwrap();

            storage
                .insert_rows(&[Row::with_labels(
                    "metric",
                    fresh_labels.clone(),
                    DataPoint::new(10, 10.0),
                )])
                .unwrap();
            storage.close().unwrap();
        }

        // Re-enabling WAL should no longer replay stale frames into the new id space.
        let reopened = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        let fresh = reopened.select("metric", &fresh_labels, 0, 20).unwrap();
        assert_eq!(fresh, vec![DataPoint::new(10, 10.0)]);
        let stale = reopened.select("metric", &stale_labels, 0, 20).unwrap();
        assert!(stale.is_empty());

        reopened.close().unwrap();
    }

    #[test]
    fn reopens_from_segment_files_without_wal() {
        let temp_dir = TempDir::new().unwrap();
        let labels = vec![Label::new("host", "a")];

        {
            let storage = StorageBuilder::new()
                .with_data_path(temp_dir.path())
                .with_wal_enabled(false)
                .with_timestamp_precision(TimestampPrecision::Seconds)
                .with_chunk_points(2)
                .build()
                .unwrap();

            storage
                .insert_rows(&[
                    Row::with_labels("seg", labels.clone(), DataPoint::new(1, 1.0)),
                    Row::with_labels("seg", labels.clone(), DataPoint::new(2, 2.0)),
                    Row::with_labels("seg", labels.clone(), DataPoint::new(3, 3.0)),
                ])
                .unwrap();
            storage.close().unwrap();
        }

        let reopened = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_wal_enabled(false)
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        let points = reopened.select("seg", &labels, 0, 10).unwrap();
        assert_eq!(points.len(), 3);
        assert_eq!(points[0], DataPoint::new(1, 1.0));
        assert_eq!(points[2], DataPoint::new(3, 3.0));

        let segments_root = temp_dir
            .path()
            .join(NUMERIC_LANE_ROOT)
            .join("segments")
            .join("L0");
        assert!(
            segments_root.exists(),
            "numeric lane segments should exist at {:?}",
            segments_root
        );
        let mut found_segment = false;
        for entry in std::fs::read_dir(segments_root).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                found_segment = true;
                assert!(path.join("manifest.bin").exists());
                assert!(path.join("chunks.bin").exists());
                assert!(path.join("chunk_index.bin").exists());
                assert!(path.join("series.bin").exists());
                assert!(path.join("postings.bin").exists());
            }
        }
        assert!(found_segment);

        reopened.close().unwrap();
    }

    #[test]
    fn isolates_numeric_and_blob_segments_and_merges_in_queries() {
        let temp_dir = TempDir::new().unwrap();
        let numeric_labels = vec![Label::new("kind", "numeric")];
        let blob_labels = vec![Label::new("kind", "blob")];

        {
            let storage = StorageBuilder::new()
                .with_data_path(temp_dir.path())
                .with_wal_enabled(false)
                .with_timestamp_precision(TimestampPrecision::Seconds)
                .with_chunk_points(2)
                .build()
                .unwrap();

            storage
                .insert_rows(&[
                    Row::with_labels("mix", numeric_labels.clone(), DataPoint::new(1, 1.0)),
                    Row::with_labels("mix", numeric_labels.clone(), DataPoint::new(2, 2.0)),
                    Row::with_labels("mix", blob_labels.clone(), DataPoint::new(1, "a")),
                    Row::with_labels("mix", blob_labels.clone(), DataPoint::new(2, "b")),
                ])
                .unwrap();
            storage.close().unwrap();
        }

        let reopened = StorageBuilder::new()
            .with_data_path(temp_dir.path())
            .with_wal_enabled(false)
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .with_chunk_points(2)
            .build()
            .unwrap();

        let numeric = reopened.select("mix", &numeric_labels, 0, 10).unwrap();
        let blob = reopened.select("mix", &blob_labels, 0, 10).unwrap();
        assert_eq!(numeric.len(), 2);
        assert_eq!(blob.len(), 2);
        assert_eq!(blob[0], DataPoint::new(1, "a"));
        assert_eq!(blob[1], DataPoint::new(2, "b"));

        let mut all = reopened.select_all("mix", 0, 10).unwrap();
        all.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].1.len(), 2);
        assert_eq!(all[1].1.len(), 2);

        let numeric_root = temp_dir
            .path()
            .join(NUMERIC_LANE_ROOT)
            .join("segments")
            .join("L0");
        let blob_root = temp_dir
            .path()
            .join(BLOB_LANE_ROOT)
            .join("segments")
            .join("L0");
        assert!(numeric_root.exists());
        assert!(blob_root.exists());

        reopened.close().unwrap();
    }

    #[test]
    fn partition_window_rotates_chunks_before_reaching_chunk_cap() {
        let storage = ChunkStorage::new_with_data_path_and_options(
            8,
            None,
            None,
            None,
            1,
            ChunkStorageOptions {
                retention_window: i64::MAX,
                retention_enforced: false,
                partition_window: 1,
                max_writers: 2,
                write_timeout: Duration::from_secs(1),
            },
        );

        let labels = vec![Label::new("host", "a")];
        storage
            .insert_rows(&[
                Row::with_labels("partitioned", labels.clone(), DataPoint::new(1, 1.0)),
                Row::with_labels("partitioned", labels.clone(), DataPoint::new(2, 2.0)),
            ])
            .unwrap();

        let series_id = storage
            .registry
            .read()
            .resolve_existing("partitioned", &labels)
            .unwrap()
            .series_id;

        let sealed = storage.sealed_chunks.read();
        let chunks = sealed.get(&series_id).unwrap();
        assert_eq!(
            chunks.len(),
            1,
            "partition transition should seal current chunk"
        );
        assert_eq!(chunks[0].header.min_ts, 1);
        assert_eq!(chunks[0].header.max_ts, 1);

        let active = storage.active_builders.read();
        assert_eq!(active.get(&series_id).unwrap().builder.len(), 1);
    }

    #[test]
    fn retention_window_hides_points_older_than_latest_minus_window() {
        let storage = StorageBuilder::new()
            .with_retention(Duration::from_secs(1))
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .build()
            .unwrap();

        storage
            .insert_rows(&[Row::new("retention_metric", DataPoint::new(100, 1.0))])
            .unwrap();
        storage
            .insert_rows(&[Row::new("retention_metric", DataPoint::new(102, 2.0))])
            .unwrap();

        let points = storage.select("retention_metric", &[], 0, 200).unwrap();
        assert_eq!(points, vec![DataPoint::new(102, 2.0)]);
    }

    #[test]
    fn default_retention_rejects_out_of_window_writes() {
        let retention_secs = Duration::from_secs(14 * 24 * 3600).as_secs() as i64;
        let storage = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .build()
            .unwrap();

        storage
            .insert_rows(&[Row::new(
                "default_retention_metric",
                DataPoint::new(retention_secs + 1, 1.0),
            )])
            .unwrap();

        let err = storage
            .insert_rows(&[Row::new("default_retention_metric", DataPoint::new(0, 0.0))])
            .unwrap_err();
        assert!(matches!(err, TsinkError::OutOfRetention { timestamp: 0 }));
    }

    #[test]
    fn explicitly_setting_default_retention_rejects_out_of_window_writes() {
        let retention = Duration::from_secs(14 * 24 * 3600);
        let storage = StorageBuilder::new()
            .with_retention(retention)
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .build()
            .unwrap();

        storage
            .insert_rows(&[Row::new(
                "explicit_default_retention_metric",
                DataPoint::new(retention.as_secs() as i64 + 1, 1.0),
            )])
            .unwrap();

        let err = storage
            .insert_rows(&[Row::new(
                "explicit_default_retention_metric",
                DataPoint::new(0, 0.0),
            )])
            .unwrap_err();
        assert!(matches!(err, TsinkError::OutOfRetention { timestamp: 0 }));
    }

    #[test]
    fn disabling_retention_enforcement_never_expires_or_rejects_points() {
        let storage = StorageBuilder::new()
            .with_retention(Duration::from_secs(1))
            .with_retention_enforced(false)
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .build()
            .unwrap();

        storage
            .insert_rows(&[Row::new("no_retention_metric", DataPoint::new(100, 1.0))])
            .unwrap();
        storage
            .insert_rows(&[Row::new("no_retention_metric", DataPoint::new(102, 2.0))])
            .unwrap();
        storage
            .insert_rows(&[Row::new("no_retention_metric", DataPoint::new(0, 0.0))])
            .unwrap();

        let points = storage.select("no_retention_metric", &[], 0, 200).unwrap();
        assert_eq!(
            points,
            vec![
                DataPoint::new(0, 0.0),
                DataPoint::new(100, 1.0),
                DataPoint::new(102, 2.0)
            ]
        );
    }

    #[test]
    fn timestamp_precision_changes_retention_unit_conversion() {
        let seconds_storage = StorageBuilder::new()
            .with_retention(Duration::from_secs(1))
            .with_timestamp_precision(TimestampPrecision::Seconds)
            .build()
            .unwrap();
        seconds_storage
            .insert_rows(&[Row::new("seconds", DataPoint::new(0, 1.0))])
            .unwrap();
        seconds_storage
            .insert_rows(&[Row::new("seconds", DataPoint::new(2, 2.0))])
            .unwrap();
        let seconds_points = seconds_storage.select("seconds", &[], 0, 10).unwrap();
        assert_eq!(seconds_points, vec![DataPoint::new(2, 2.0)]);

        let millis_storage = StorageBuilder::new()
            .with_retention(Duration::from_secs(1))
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .build()
            .unwrap();
        millis_storage
            .insert_rows(&[Row::new("millis", DataPoint::new(0, 1.0))])
            .unwrap();
        millis_storage
            .insert_rows(&[Row::new("millis", DataPoint::new(2, 2.0))])
            .unwrap();
        let millis_points = millis_storage.select("millis", &[], 0, 10).unwrap();
        assert_eq!(millis_points.len(), 2);
    }

    #[test]
    fn write_limiter_respects_configured_timeout() {
        let storage = ChunkStorage::new_with_data_path_and_options(
            8,
            None,
            None,
            None,
            1,
            ChunkStorageOptions {
                retention_window: i64::MAX,
                retention_enforced: false,
                partition_window: i64::MAX,
                max_writers: 1,
                write_timeout: Duration::ZERO,
            },
        );

        let _held_permit = storage.write_limiter.acquire();
        let err = storage
            .insert_rows(&[Row::new("write_timeout_metric", DataPoint::new(1, 1.0))])
            .unwrap_err();
        assert!(matches!(
            err,
            TsinkError::WriteTimeout {
                timeout_ms: 0,
                workers: 1
            }
        ));
    }

    #[test]
    fn close_blocks_until_in_flight_writer_releases_permit() {
        use std::sync::Arc;
        use std::sync::mpsc;
        use std::thread;

        let storage = Arc::new(ChunkStorage::new_with_data_path_and_options(
            8,
            None,
            None,
            None,
            1,
            ChunkStorageOptions {
                retention_window: i64::MAX,
                retention_enforced: false,
                partition_window: i64::MAX,
                max_writers: 1,
                write_timeout: Duration::from_secs(2),
            },
        ));
        let labels = vec![Label::new("host", "a")];

        let held_permit = storage.write_limiter.acquire();

        let writer_storage = Arc::clone(&storage);
        let writer_labels = labels.clone();
        let (writer_tx, writer_rx) = mpsc::channel();
        let writer = thread::spawn(move || {
            let result = writer_storage.insert_rows(&[Row::with_labels(
                "close_race_metric",
                writer_labels,
                DataPoint::new(1, 1.0),
            )]);
            writer_tx.send(result).unwrap();
        });

        // Writer should be blocked waiting for permit acquisition.
        assert!(writer_rx.recv_timeout(Duration::from_millis(100)).is_err());

        let close_storage = Arc::clone(&storage);
        let (close_tx, close_rx) = mpsc::channel();
        let closer = thread::spawn(move || {
            let result = close_storage.close();
            close_tx.send(result).unwrap();
        });

        // Close should wait until in-flight writers release permits.
        assert!(close_rx.recv_timeout(Duration::from_millis(100)).is_err());

        drop(held_permit);

        let close_result = close_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(close_result.is_ok());

        let writer_result = writer_rx.recv_timeout(Duration::from_secs(1)).unwrap();
        assert!(matches!(writer_result, Err(TsinkError::StorageClosed)));

        writer.join().unwrap();
        closer.join().unwrap();
    }

    fn make_persisted_numeric_chunk(series_id: u64, points: &[(i64, f64)]) -> Chunk {
        assert!(!points.is_empty());
        let chunk_points = points
            .iter()
            .map(|(ts, value)| ChunkPoint {
                ts: *ts,
                value: Value::F64(*value),
            })
            .collect::<Vec<_>>();
        let encoded = Encoder::encode_chunk_points(&chunk_points, ValueLane::Numeric).unwrap();

        Chunk {
            header: ChunkHeader {
                series_id,
                lane: ValueLane::Numeric,
                point_count: chunk_points.len() as u16,
                min_ts: chunk_points.first().unwrap().ts,
                max_ts: chunk_points.last().unwrap().ts,
                ts_codec: encoded.ts_codec,
                value_codec: encoded.value_codec,
            },
            points: chunk_points,
            encoded_payload: encoded.payload,
        }
    }
}
