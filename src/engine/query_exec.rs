use std::borrow::Cow;

use roaring::RoaringTreemap;

use crate::engine::binio::{read_u32_at, read_u8_at};
use crate::engine::query::{
    decode_chunk_points_in_range_into, decode_encoded_chunk_payload_in_range_into,
    EncodedChunkDescriptor,
};
use crate::engine::segment::{chunk_payload_uses_zstd, decompress_chunk_payload_zstd};
use crate::query_aggregation::{
    aggregate_series, downsample_points, downsample_points_with_custom,
};
use crate::query_matcher::CompiledSeriesMatcher;
use crate::query_selection::{PreparedSeriesSelection, SeriesSelectionBackend};
use crate::storage::SeriesMatcherOp;
use crate::Aggregation;

use super::*;

impl ChunkStorage {
    fn collect_points_for_series_into(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        out: &mut Vec<DataPoint>,
    ) -> Result<()> {
        if self.try_collect_points_for_series_with_merge(series_id, start, end, out)? {
            self.observability
                .query
                .merge_path_queries_total
                .fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        self.observability
            .query
            .append_sort_path_queries_total
            .fetch_add(1, Ordering::Relaxed);
        self.collect_points_for_series_append_sort_path(series_id, start, end, out)
    }

    fn try_collect_points_for_series_with_merge(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        out: &mut Vec<DataPoint>,
    ) -> Result<bool> {
        let _visibility_guard = self.flush_visibility_lock.read();
        let mut has_previous_chunk = false;
        let mut has_previous_persisted_chunk = false;
        let mut previous_max_ts = i64::MIN;
        let mut previous_persisted_max_ts = i64::MIN;
        let mut requires_output_validation = false;
        let mut requires_timestamp_dedupe = false;
        let mut requires_exact_dedupe = false;
        let mut persisted_source_sorted = true;
        let mut sealed_source_sorted = true;
        let mut estimated_points = 0usize;

        {
            let persisted_index = self.persisted_index.read();
            let mut persisted_chunks = Vec::<PersistedChunkRef>::new();

            if let Some(chunks) = persisted_index.chunk_refs.get(&series_id) {
                let end_idx = chunks.partition_point(|chunk| chunk.min_ts < end);
                persisted_chunks.reserve(end_idx);

                let mut previous_persisted_source_max_ts = i64::MIN;
                let mut has_previous_persisted_source_chunk = false;
                let mut overlapping_persisted_cluster_level = 0u8;
                for chunk_ref in &chunks[..end_idx] {
                    if chunk_ref.max_ts < start {
                        continue;
                    }

                    let overlaps_previous_persisted_source = has_previous_persisted_source_chunk
                        && chunk_ref.min_ts <= previous_persisted_source_max_ts;

                    if has_previous_chunk
                        && chunk_ref.min_ts <= previous_max_ts
                        && has_previous_persisted_chunk
                        && chunk_ref.min_ts <= previous_persisted_max_ts
                    {
                        requires_exact_dedupe = true;
                    }
                    if has_previous_persisted_source_chunk
                        && chunk_ref.min_ts < previous_persisted_source_max_ts
                    {
                        persisted_source_sorted = false;
                    }
                    if overlaps_previous_persisted_source {
                        if chunk_ref.level != overlapping_persisted_cluster_level {
                            requires_timestamp_dedupe = true;
                        }
                        overlapping_persisted_cluster_level =
                            overlapping_persisted_cluster_level.max(chunk_ref.level);
                    } else {
                        overlapping_persisted_cluster_level = chunk_ref.level;
                    }

                    has_previous_chunk = true;
                    has_previous_persisted_chunk = true;
                    previous_max_ts = previous_max_ts.max(chunk_ref.max_ts);
                    previous_persisted_max_ts = previous_persisted_max_ts.max(chunk_ref.max_ts);
                    previous_persisted_source_max_ts =
                        previous_persisted_source_max_ts.max(chunk_ref.max_ts);
                    has_previous_persisted_source_chunk = true;
                    estimated_points =
                        estimated_points.saturating_add(chunk_ref.point_count as usize);
                    persisted_chunks.push(*chunk_ref);
                }
            }
            // Hold active read lock while reading sealed+active to prevent observing a transient
            // "moved out of active but not yet visible in sealed" flush transition.
            let active = self.active_shard(series_id).read();
            let sealed = self.sealed_shard(series_id).read();
            let mut sealed_chunks = Vec::<&Chunk>::new();
            if let Some(chunks) = sealed.get(&series_id) {
                let end_bound = SealedChunkKey::upper_bound_for_min_ts(end);
                let mut previous_sealed_source_max_ts = i64::MIN;
                let mut has_previous_sealed_source_chunk = false;
                for (_, chunk) in chunks.range(..end_bound) {
                    if chunk.header.max_ts < start {
                        continue;
                    }

                    if has_previous_chunk
                        && chunk.header.min_ts <= previous_max_ts
                        && has_previous_persisted_chunk
                        && chunk.header.min_ts <= previous_persisted_max_ts
                    {
                        requires_exact_dedupe = true;
                    }
                    if has_previous_sealed_source_chunk
                        && chunk.header.min_ts < previous_sealed_source_max_ts
                    {
                        sealed_source_sorted = false;
                    }

                    has_previous_chunk = true;
                    previous_max_ts = previous_max_ts.max(chunk.header.max_ts);
                    previous_sealed_source_max_ts =
                        previous_sealed_source_max_ts.max(chunk.header.max_ts);
                    has_previous_sealed_source_chunk = true;

                    // Chunks without encoded payload may be ad-hoc/manual and not guaranteed sorted.
                    if chunk.points.len() > 1 && chunk.encoded_payload.is_empty() {
                        requires_output_validation = true;
                    }
                    estimated_points =
                        estimated_points.saturating_add(chunk.header.point_count as usize);
                    sealed_chunks.push(chunk);
                }
            }

            let mut active_points: &[ChunkPoint] = &[];
            if let Some(state) = active.get(&series_id) {
                active_points = state.builder.points();
                let mut previous_active_ts = i64::MIN;
                let mut has_previous_active = false;

                for point in state.builder.points() {
                    if point.ts < start || point.ts >= end {
                        continue;
                    }

                    if has_previous_persisted_chunk && point.ts <= previous_persisted_max_ts {
                        requires_exact_dedupe = true;
                    }
                    if has_previous_active && point.ts < previous_active_ts {
                        requires_output_validation = true;
                    }

                    has_previous_active = true;
                    previous_active_ts = point.ts;
                    estimated_points = estimated_points.saturating_add(1);
                }
            }
            if requires_output_validation || !persisted_source_sorted || !sealed_source_sorted {
                return Ok(false);
            }

            out.clear();
            out.reserve(estimated_points);

            let mut persisted_cursor = PersistedSourceMergeCursor::new(
                persisted_chunks,
                persisted_index.segment_maps.as_slice(),
                start,
                end,
            );
            let mut sealed_cursor = SealedSourceMergeCursor::new(sealed_chunks, start, end);
            let mut active_cursor = ActiveSourceMergeCursor::new(active_points, start, end);

            merge_sorted_query_sources_into(
                &mut persisted_cursor,
                &mut sealed_cursor,
                &mut active_cursor,
                out,
            )?;
        }

        self.apply_retention_filter(out);
        if requires_timestamp_dedupe {
            dedupe_last_value_per_timestamp(out);
        } else if requires_exact_dedupe {
            dedupe_exact_duplicate_points(out);
        }
        Ok(true)
    }

    fn collect_points_for_series_append_sort_path(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
        out: &mut Vec<DataPoint>,
    ) -> Result<()> {
        let _visibility_guard = self.flush_visibility_lock.read();
        out.clear();
        let mut has_overlap = false;
        let mut has_previous_chunk = false;
        let mut has_previous_persisted_chunk = false;
        let mut previous_max_ts = i64::MIN;
        let mut previous_persisted_max_ts = i64::MIN;
        let mut requires_output_validation = false;
        let mut requires_timestamp_dedupe = false;
        let mut requires_exact_dedupe = false;
        let mut has_previous_persisted_source_chunk = false;
        let mut previous_persisted_source_max_ts = i64::MIN;
        let mut overlapping_persisted_cluster_level = 0u8;

        {
            let persisted_index = self.persisted_index.read();

            if let Some(chunks) = persisted_index.chunk_refs.get(&series_id) {
                let end_idx = chunks.partition_point(|chunk| chunk.min_ts < end);
                for chunk_ref in &chunks[..end_idx] {
                    if chunk_ref.max_ts < start {
                        continue;
                    }

                    let overlaps_previous_persisted_source = has_previous_persisted_source_chunk
                        && chunk_ref.min_ts <= previous_persisted_source_max_ts;

                    if has_previous_chunk && chunk_ref.min_ts <= previous_max_ts {
                        has_overlap = true;
                        if has_previous_persisted_chunk
                            && chunk_ref.min_ts <= previous_persisted_max_ts
                        {
                            requires_exact_dedupe = true;
                        }
                    }
                    if overlaps_previous_persisted_source {
                        if chunk_ref.level != overlapping_persisted_cluster_level {
                            requires_timestamp_dedupe = true;
                        }
                        overlapping_persisted_cluster_level =
                            overlapping_persisted_cluster_level.max(chunk_ref.level);
                    } else {
                        overlapping_persisted_cluster_level = chunk_ref.level;
                    }

                    has_previous_chunk = true;
                    has_previous_persisted_chunk = true;
                    has_previous_persisted_source_chunk = true;
                    previous_max_ts = previous_max_ts.max(chunk_ref.max_ts);
                    previous_persisted_max_ts = previous_persisted_max_ts.max(chunk_ref.max_ts);
                    previous_persisted_source_max_ts =
                        previous_persisted_source_max_ts.max(chunk_ref.max_ts);

                    let payload = persisted_chunk_payload(
                        persisted_index.segment_maps.as_slice(),
                        chunk_ref,
                    )?;
                    decode_encoded_chunk_payload_in_range_into(
                        EncodedChunkDescriptor {
                            lane: chunk_ref.lane,
                            ts_codec: chunk_ref.ts_codec,
                            value_codec: chunk_ref.value_codec,
                            point_count: chunk_ref.point_count as usize,
                        },
                        payload.as_ref(),
                        start,
                        end,
                        out,
                    )?;
                }
            }
        }

        // Hold active read lock while reading sealed+active to prevent observing a transient
        // "moved out of active but not yet visible in sealed" flush transition.
        let active = self.active_shard(series_id).read();
        {
            let sealed = self.sealed_shard(series_id).read();
            if let Some(chunks) = sealed.get(&series_id) {
                let end_bound = SealedChunkKey::upper_bound_for_min_ts(end);
                for (_, chunk) in chunks.range(..end_bound) {
                    if chunk.header.max_ts < start {
                        continue;
                    }

                    if has_previous_chunk && chunk.header.min_ts <= previous_max_ts {
                        has_overlap = true;
                        if has_previous_persisted_chunk
                            && chunk.header.min_ts <= previous_persisted_max_ts
                        {
                            requires_exact_dedupe = true;
                        }
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
                if has_previous_persisted_chunk && point.ts <= previous_persisted_max_ts {
                    requires_exact_dedupe = true;
                }
                if has_previous_active && point.ts < previous_active_ts {
                    requires_output_validation = true;
                }

                has_previous_active = true;
                previous_active_ts = point.ts;
                out.push(DataPoint::new(point.ts, point.value.clone()));
            }
        }

        self.apply_retention_filter(out);

        if has_overlap || requires_output_validation {
            if !points_are_sorted_by_timestamp(out) {
                out.sort_by_key(|point| point.timestamp);
            }
            if requires_timestamp_dedupe {
                dedupe_last_value_per_timestamp(out);
            } else if requires_exact_dedupe {
                dedupe_exact_duplicate_points(out);
            }
        }
        Ok(())
    }

    pub(super) fn collect_points_for_series(
        &self,
        series_id: SeriesId,
        start: i64,
        end: i64,
    ) -> Result<Vec<DataPoint>> {
        let mut out = Vec::new();
        self.collect_points_for_series_into(series_id, start, end, &mut out)?;
        Ok(out)
    }

    pub(super) fn select_api(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
    ) -> Result<Vec<DataPoint>> {
        self.observability
            .query
            .select_calls_total
            .fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();

        let result = (|| -> Result<Vec<DataPoint>> {
            self.ensure_open()?;
            Self::validate_select_request(metric, labels, start, end)?;

            let mut out = Vec::new();
            self.select_into_impl(metric, labels, start, end, &mut out)?;
            Ok(out)
        })();

        self.observability
            .query
            .select_duration_nanos_total
            .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);

        match result {
            Ok(points) => {
                self.observability
                    .query
                    .select_points_returned_total
                    .fetch_add(saturating_u64_from_usize(points.len()), Ordering::Relaxed);
                Ok(points)
            }
            Err(err) => {
                self.observability
                    .query
                    .select_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
        }
    }

    pub(super) fn select_into_api(
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

    pub(super) fn select_with_options_api(
        &self,
        metric: &str,
        opts: QueryOptions,
    ) -> Result<Vec<DataPoint>> {
        self.observability
            .query
            .select_with_options_calls_total
            .fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();

        let result = (|| -> Result<Vec<DataPoint>> {
            self.ensure_open()?;
            validate_metric(metric)?;
            validate_labels(&opts.labels)?;

            if opts.start >= opts.end {
                return Err(TsinkError::InvalidTimeRange {
                    start: opts.start,
                    end: opts.end,
                });
            }

            if let Some(downsample) = opts.downsample {
                if downsample.interval <= 0 {
                    return Err(TsinkError::InvalidConfiguration(
                        "downsample interval must be positive".to_string(),
                    ));
                }
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
        })();

        self.observability
            .query
            .select_with_options_duration_nanos_total
            .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);

        match result {
            Ok(points) => {
                self.observability
                    .query
                    .select_with_options_points_returned_total
                    .fetch_add(saturating_u64_from_usize(points.len()), Ordering::Relaxed);
                Ok(points)
            }
            Err(err) => {
                self.observability
                    .query
                    .select_with_options_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
        }
    }

    pub(super) fn select_all_api(
        &self,
        metric: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
        self.observability
            .query
            .select_all_calls_total
            .fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();

        let result = (|| -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
            self.ensure_open()?;
            validate_metric(metric)?;

            if start >= end {
                return Err(TsinkError::InvalidTimeRange { start, end });
            }

            let series_ids = self.registry.read().series_ids_for_metric(metric);
            if series_ids.is_empty() {
                return Ok(Vec::new());
            }

            let series_with_labels = {
                let registry = self.registry.read();
                series_ids
                    .into_iter()
                    .map(|series_id| {
                        let labels = registry
                            .decode_series_key(series_id)
                            .map(|key| key.labels)
                            .unwrap_or_default();
                        (series_id, labels)
                    })
                    .collect::<Vec<_>>()
            };

            let mut out = series_with_labels
                .into_par_iter()
                .map(|(series_id, labels)| {
                    let points = self.collect_points_for_series(series_id, start, end)?;
                    if points.is_empty() {
                        Ok(None)
                    } else {
                        Ok(Some((labels, points)))
                    }
                })
                .collect::<Result<Vec<_>>>()?
                .into_iter()
                .flatten()
                .collect::<Vec<_>>();

            out.sort_by(|a, b| a.0.cmp(&b.0));
            Ok(out)
        })();

        self.observability
            .query
            .select_all_duration_nanos_total
            .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);

        match result {
            Ok(series) => {
                let points_returned = series.iter().map(|(_, points)| points.len()).sum::<usize>();
                self.observability
                    .query
                    .select_all_series_returned_total
                    .fetch_add(saturating_u64_from_usize(series.len()), Ordering::Relaxed);
                self.observability
                    .query
                    .select_all_points_returned_total
                    .fetch_add(
                        saturating_u64_from_usize(points_returned),
                        Ordering::Relaxed,
                    );
                Ok(series)
            }
            Err(err) => {
                self.observability
                    .query
                    .select_all_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
        }
    }

    pub(super) fn list_metrics_api(&self) -> Result<Vec<MetricSeries>> {
        self.ensure_open()?;
        let materialized_series_ids = self
            .materialized_series
            .read()
            .iter()
            .copied()
            .collect::<Vec<_>>();
        Ok(self.metric_series_for_ids(materialized_series_ids))
    }

    pub(super) fn list_metrics_with_wal_api(&self) -> Result<Vec<MetricSeries>> {
        self.ensure_open()?;
        let all_series_ids = self.registry.read().all_series_ids();
        Ok(self.metric_series_for_ids(all_series_ids))
    }

    pub(super) fn select_series_api(
        &self,
        selection: &SeriesSelection,
    ) -> Result<Vec<MetricSeries>> {
        self.observability
            .query
            .select_series_calls_total
            .fetch_add(1, Ordering::Relaxed);
        let started = Instant::now();

        let result = (|| -> Result<Vec<MetricSeries>> {
            self.ensure_open()?;
            self.select_series_impl(selection)
        })();

        self.observability
            .query
            .select_series_duration_nanos_total
            .fetch_add(elapsed_nanos_u64(started), Ordering::Relaxed);

        match result {
            Ok(series) => {
                self.observability
                    .query
                    .select_series_returned_total
                    .fetch_add(saturating_u64_from_usize(series.len()), Ordering::Relaxed);
                Ok(series)
            }
            Err(err) => {
                self.observability
                    .query
                    .select_series_errors_total
                    .fetch_add(1, Ordering::Relaxed);
                Err(err)
            }
        }
    }

    fn intersect_candidates(candidates: &mut BTreeSet<SeriesId>, filter: Option<&RoaringTreemap>) {
        match filter {
            Some(filter) => candidates.retain(|series_id| filter.contains(*series_id)),
            None => candidates.clear(),
        }
    }

    fn subtract_candidates(candidates: &mut BTreeSet<SeriesId>, filter: Option<&RoaringTreemap>) {
        if let Some(filter) = filter {
            candidates.retain(|series_id| !filter.contains(*series_id));
        }
    }

    fn bitmap_to_series_set(bitmap: &RoaringTreemap) -> BTreeSet<SeriesId> {
        bitmap.iter().collect()
    }

    fn apply_postings_matcher_to_candidates(
        registry: &SeriesRegistry,
        candidates: &mut BTreeSet<SeriesId>,
        matcher: &CompiledSeriesMatcher,
    ) -> Result<()> {
        if candidates.is_empty() {
            return Ok(());
        }

        if matcher.name == "__name__" {
            match matcher.op {
                SeriesMatcherOp::Equal => {
                    Self::intersect_candidates(
                        candidates,
                        registry.series_id_postings_for_metric(&matcher.value),
                    );
                }
                SeriesMatcherOp::NotEqual => {
                    Self::subtract_candidates(
                        candidates,
                        registry.series_id_postings_for_metric(&matcher.value),
                    );
                }
                SeriesMatcherOp::RegexMatch | SeriesMatcherOp::RegexNoMatch => {
                    let Some(regex) = matcher.regex.as_ref() else {
                        return Err(TsinkError::InvalidConfiguration(
                            "regex matcher missing compiled regex".to_string(),
                        ));
                    };

                    let mut matched = RoaringTreemap::new();
                    for (metric, series_ids) in registry.metric_postings_entries() {
                        if regex.is_match(metric) {
                            for series_id in series_ids.iter() {
                                matched.insert(series_id);
                            }
                        }
                    }

                    if matcher.op == SeriesMatcherOp::RegexMatch {
                        Self::intersect_candidates(candidates, Some(&matched));
                    } else {
                        Self::subtract_candidates(candidates, Some(&matched));
                    }
                }
            }

            return Ok(());
        }

        match matcher.op {
            SeriesMatcherOp::Equal => {
                Self::intersect_candidates(
                    candidates,
                    registry.postings_for_label(&matcher.name, &matcher.value),
                );
            }
            SeriesMatcherOp::NotEqual => {
                Self::subtract_candidates(
                    candidates,
                    registry.postings_for_label(&matcher.name, &matcher.value),
                );
            }
            SeriesMatcherOp::RegexMatch | SeriesMatcherOp::RegexNoMatch => {
                let Some(label_name_id) = registry.label_name_id(&matcher.name) else {
                    if matcher.op == SeriesMatcherOp::RegexMatch {
                        candidates.clear();
                    }
                    return Ok(());
                };

                let Some(regex) = matcher.regex.as_ref() else {
                    return Err(TsinkError::InvalidConfiguration(
                        "regex matcher missing compiled regex".to_string(),
                    ));
                };

                let mut matched = RoaringTreemap::new();
                for (pair, series_ids) in registry.postings_entries() {
                    if pair.name_id != label_name_id {
                        continue;
                    }
                    let Some(label_value) = registry.label_value_by_id(pair.value_id) else {
                        continue;
                    };
                    if regex.is_match(label_value) {
                        for series_id in series_ids.iter() {
                            matched.insert(series_id);
                        }
                    }
                }

                if matcher.op == SeriesMatcherOp::RegexMatch {
                    Self::intersect_candidates(candidates, Some(&matched));
                } else {
                    Self::subtract_candidates(candidates, Some(&matched));
                }
            }
        }

        Ok(())
    }

    fn select_series_candidate_ids(
        &self,
        selection: &SeriesSelection,
        compiled_matchers: &[CompiledSeriesMatcher],
    ) -> Result<Vec<SeriesId>> {
        let registry = self.registry.read();

        let mut candidates = if let Some(metric) = selection.metric.as_ref() {
            registry
                .series_id_postings_for_metric(metric)
                .map(Self::bitmap_to_series_set)
                .unwrap_or_default()
        } else {
            registry
                .all_series_ids()
                .into_iter()
                .collect::<BTreeSet<_>>()
        };

        for matcher in compiled_matchers {
            Self::apply_postings_matcher_to_candidates(&registry, &mut candidates, matcher)?;
            if candidates.is_empty() {
                break;
            }
        }

        Ok(candidates.into_iter().collect())
    }

    fn series_ids_with_data_in_time_range(
        &self,
        series_ids: Vec<SeriesId>,
        start: i64,
        end: i64,
    ) -> Vec<SeriesId> {
        if series_ids.is_empty() {
            return series_ids;
        }

        let _visibility_guard = self.flush_visibility_lock.read();
        let persisted_index = self.persisted_index.read();
        let mut filtered = Vec::with_capacity(series_ids.len());

        for series_id in series_ids {
            let persisted_overlap =
                persisted_index
                    .chunk_refs
                    .get(&series_id)
                    .is_some_and(|chunks| {
                        let end_idx = chunks.partition_point(|chunk| chunk.min_ts < end);
                        chunks[..end_idx]
                            .iter()
                            .any(|chunk| chunk.max_ts >= start && chunk.min_ts < end)
                    });

            if persisted_overlap {
                filtered.push(series_id);
                continue;
            }

            let active = self.active_shard(series_id).read();
            {
                let sealed = self.sealed_shard(series_id).read();
                let sealed_overlap = sealed.get(&series_id).is_some_and(|chunks| {
                    let end_bound = SealedChunkKey::upper_bound_for_min_ts(end);
                    chunks
                        .range(..end_bound)
                        .any(|(_, chunk)| chunk.header.max_ts >= start && chunk.header.min_ts < end)
                });
                if sealed_overlap {
                    filtered.push(series_id);
                    continue;
                }
            }

            let active_overlap = active.get(&series_id).is_some_and(|state| {
                state
                    .builder
                    .points()
                    .iter()
                    .any(|point| point.ts >= start && point.ts < end)
            });
            if active_overlap {
                filtered.push(series_id);
            }
        }

        filtered
    }

    pub(super) fn select_series_impl(
        &self,
        selection: &SeriesSelection,
    ) -> Result<Vec<MetricSeries>> {
        crate::query_selection::execute_series_selection(
            &PostingsSeriesSelectionBackend { storage: self },
            selection,
        )
    }

    pub(super) fn validate_select_request(
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
    ) -> Result<()> {
        validate_metric(metric)?;
        validate_labels(labels)?;
        if start >= end {
            return Err(TsinkError::InvalidTimeRange { start, end });
        }
        Ok(())
    }

    pub(super) fn select_into_impl(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        out: &mut Vec<DataPoint>,
    ) -> Result<()> {
        let Some(series_id) = self
            .registry
            .read()
            .resolve_existing(metric, labels)
            .map(|resolution| resolution.series_id)
        else {
            out.clear();
            return Ok(());
        };
        self.collect_points_for_series_into(series_id, start, end, out)
    }
}

struct PostingsSeriesSelectionBackend<'a> {
    storage: &'a ChunkStorage,
}

impl SeriesSelectionBackend for PostingsSeriesSelectionBackend<'_> {
    type Candidate = SeriesId;

    fn candidate_items(
        &self,
        selection: &SeriesSelection,
        prepared: &PreparedSeriesSelection,
    ) -> Result<Vec<Self::Candidate>> {
        self.storage
            .select_series_candidate_ids(selection, &prepared.compiled_matchers)
    }

    fn retain_items_in_time_range(
        &self,
        items: &mut Vec<Self::Candidate>,
        start: i64,
        end: i64,
    ) -> Result<()> {
        let filtered =
            self.storage
                .series_ids_with_data_in_time_range(std::mem::take(items), start, end);
        *items = filtered;
        Ok(())
    }

    fn materialize_items(&self, items: Vec<Self::Candidate>) -> Result<Vec<MetricSeries>> {
        Ok(self.storage.metric_series_for_ids(items))
    }
}

struct PersistedSourceMergeCursor<'a> {
    chunk_refs: Vec<PersistedChunkRef>,
    segment_maps: &'a [Arc<PlatformMmap>],
    start: i64,
    end: i64,
    next_chunk_idx: usize,
    current_points: Vec<DataPoint>,
    next_point_idx: usize,
}

impl<'a> PersistedSourceMergeCursor<'a> {
    fn new(
        chunk_refs: Vec<PersistedChunkRef>,
        segment_maps: &'a [Arc<PlatformMmap>],
        start: i64,
        end: i64,
    ) -> Self {
        Self {
            chunk_refs,
            segment_maps,
            start,
            end,
            next_chunk_idx: 0,
            current_points: Vec::new(),
            next_point_idx: 0,
        }
    }

    fn ensure_head(&mut self) -> Result<Option<&DataPoint>> {
        loop {
            if self.next_point_idx < self.current_points.len() {
                return Ok(self.current_points.get(self.next_point_idx));
            }
            if self.next_chunk_idx >= self.chunk_refs.len() {
                return Ok(None);
            }

            let chunk_ref = self.chunk_refs[self.next_chunk_idx];
            self.next_chunk_idx = self.next_chunk_idx.saturating_add(1);
            self.current_points.clear();
            self.next_point_idx = 0;

            let payload = persisted_chunk_payload(self.segment_maps, &chunk_ref)?;
            decode_encoded_chunk_payload_in_range_into(
                EncodedChunkDescriptor {
                    lane: chunk_ref.lane,
                    ts_codec: chunk_ref.ts_codec,
                    value_codec: chunk_ref.value_codec,
                    point_count: chunk_ref.point_count as usize,
                },
                payload.as_ref(),
                self.start,
                self.end,
                &mut self.current_points,
            )?;
        }
    }

    fn peek_timestamp(&mut self) -> Result<Option<i64>> {
        Ok(self.ensure_head()?.map(|point| point.timestamp))
    }

    fn pop_point(&mut self) -> Result<Option<DataPoint>> {
        if self.ensure_head()?.is_none() {
            return Ok(None);
        }

        let point = self.current_points[self.next_point_idx].clone();
        self.next_point_idx = self.next_point_idx.saturating_add(1);
        Ok(Some(point))
    }
}

struct SealedSourceMergeCursor<'a> {
    chunks: Vec<&'a Chunk>,
    start: i64,
    end: i64,
    next_chunk_idx: usize,
    current_points: Vec<DataPoint>,
    next_point_idx: usize,
}

impl<'a> SealedSourceMergeCursor<'a> {
    fn new(chunks: Vec<&'a Chunk>, start: i64, end: i64) -> Self {
        Self {
            chunks,
            start,
            end,
            next_chunk_idx: 0,
            current_points: Vec::new(),
            next_point_idx: 0,
        }
    }

    fn ensure_head(&mut self) -> Result<Option<&DataPoint>> {
        loop {
            if self.next_point_idx < self.current_points.len() {
                return Ok(self.current_points.get(self.next_point_idx));
            }
            if self.next_chunk_idx >= self.chunks.len() {
                return Ok(None);
            }

            let chunk = self.chunks[self.next_chunk_idx];
            self.next_chunk_idx = self.next_chunk_idx.saturating_add(1);
            self.current_points.clear();
            self.next_point_idx = 0;
            decode_chunk_points_in_range_into(
                chunk,
                self.start,
                self.end,
                &mut self.current_points,
            )?;
        }
    }

    fn peek_timestamp(&mut self) -> Result<Option<i64>> {
        Ok(self.ensure_head()?.map(|point| point.timestamp))
    }

    fn pop_point(&mut self) -> Result<Option<DataPoint>> {
        if self.ensure_head()?.is_none() {
            return Ok(None);
        }

        let point = self.current_points[self.next_point_idx].clone();
        self.next_point_idx = self.next_point_idx.saturating_add(1);
        Ok(Some(point))
    }
}

struct ActiveSourceMergeCursor<'a> {
    points: &'a [ChunkPoint],
    start: i64,
    end: i64,
    next_point_idx: usize,
}

impl<'a> ActiveSourceMergeCursor<'a> {
    fn new(points: &'a [ChunkPoint], start: i64, end: i64) -> Self {
        Self {
            points,
            start,
            end,
            next_point_idx: 0,
        }
    }

    fn seek_in_range(&mut self) {
        while self.next_point_idx < self.points.len()
            && self.points[self.next_point_idx].ts < self.start
        {
            self.next_point_idx = self.next_point_idx.saturating_add(1);
        }
    }

    fn peek_timestamp(&mut self) -> Option<i64> {
        self.seek_in_range();
        let point = self.points.get(self.next_point_idx)?;
        (point.ts < self.end).then_some(point.ts)
    }

    fn pop_point(&mut self) -> Option<DataPoint> {
        self.seek_in_range();
        let point = self.points.get(self.next_point_idx)?;
        if point.ts >= self.end {
            return None;
        }
        self.next_point_idx = self.next_point_idx.saturating_add(1);
        Some(DataPoint::new(point.ts, point.value.clone()))
    }
}

fn merge_sorted_query_sources_into(
    persisted: &mut PersistedSourceMergeCursor<'_>,
    sealed: &mut SealedSourceMergeCursor<'_>,
    active: &mut ActiveSourceMergeCursor<'_>,
    out: &mut Vec<DataPoint>,
) -> Result<()> {
    enum Source {
        Persisted,
        Sealed,
        Active,
    }

    loop {
        let mut selected = None;
        let mut selected_ts = i64::MAX;

        if let Some(ts) = persisted.peek_timestamp()? {
            selected = Some(Source::Persisted);
            selected_ts = ts;
        }
        if let Some(ts) = sealed.peek_timestamp()? {
            if selected.is_none() || ts < selected_ts {
                selected = Some(Source::Sealed);
                selected_ts = ts;
            }
        }
        if let Some(ts) = active.peek_timestamp() {
            if selected.is_none() || ts < selected_ts {
                selected = Some(Source::Active);
            }
        }

        match selected {
            Some(Source::Persisted) => {
                if let Some(point) = persisted.pop_point()? {
                    out.push(point);
                }
            }
            Some(Source::Sealed) => {
                if let Some(point) = sealed.pop_point()? {
                    out.push(point);
                }
            }
            Some(Source::Active) => {
                if let Some(point) = active.pop_point() {
                    out.push(point);
                }
            }
            None => break,
        }
    }

    Ok(())
}

fn persisted_chunk_payload<'a>(
    persisted_segment_maps: &'a [Arc<PlatformMmap>],
    chunk_ref: &PersistedChunkRef,
) -> Result<Cow<'a, [u8]>> {
    let Some(mapped_segment) = persisted_segment_maps.get(chunk_ref.segment_slot) else {
        return Err(TsinkError::DataCorruption(format!(
            "missing mapped segment slot {}",
            chunk_ref.segment_slot
        )));
    };
    let bytes = mapped_segment.as_slice();

    let offset = usize::try_from(chunk_ref.chunk_offset).map_err(|_| {
        TsinkError::DataCorruption(format!(
            "chunk offset {} exceeds usize",
            chunk_ref.chunk_offset
        ))
    })?;
    let record_len = usize::try_from(chunk_ref.chunk_len).map_err(|_| {
        TsinkError::DataCorruption(format!(
            "chunk length {} exceeds usize",
            chunk_ref.chunk_len
        ))
    })?;
    let record_end = offset.saturating_add(record_len);
    if record_end > bytes.len() {
        return Err(TsinkError::DataCorruption(format!(
            "chunk at offset {} length {} exceeds mapped file size {}",
            chunk_ref.chunk_offset,
            chunk_ref.chunk_len,
            bytes.len()
        )));
    }

    let record = &bytes[offset..record_end];
    if record.len() < 42 {
        return Err(TsinkError::DataCorruption(
            "chunk record too short for header".to_string(),
        ));
    }

    let body_len = usize::try_from(read_u32_at(record, 0)?).unwrap_or(usize::MAX);
    if body_len.saturating_add(4) != record.len() {
        return Err(TsinkError::DataCorruption(format!(
            "chunk record length mismatch at offset {}",
            chunk_ref.chunk_offset
        )));
    }

    let payload_len = usize::try_from(read_u32_at(record, 38)?).unwrap_or(usize::MAX);
    let payload_start = 42usize;
    let payload_end = payload_start.saturating_add(payload_len);
    let chunk_flags = read_u8_at(record, 19)?;

    if payload_end.saturating_add(4) != record.len() {
        return Err(TsinkError::DataCorruption(format!(
            "chunk payload length mismatch at offset {}",
            chunk_ref.chunk_offset
        )));
    }

    let payload = &record[payload_start..payload_end];
    if chunk_payload_uses_zstd(chunk_flags)? {
        return Ok(Cow::Owned(decompress_chunk_payload_zstd(payload)?));
    }

    Ok(Cow::Borrowed(payload))
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

    points.dedup_by(|current, next| {
        if current.timestamp == next.timestamp {
            // `dedup_by` removes `next`; swap first so the latest value survives.
            std::mem::swap(current, next);
            true
        } else {
            false
        }
    });
}

fn dedupe_exact_duplicate_points(points: &mut Vec<DataPoint>) {
    if points.len() < 2 {
        return;
    }

    points.dedup_by(|current, next| {
        current.timestamp == next.timestamp && current.value == next.value
    });
}
