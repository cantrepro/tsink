use crate::engine::query::TieredQueryPlan;
use crate::validation::{validate_labels, validate_metric};
use crate::{MetricSeries, Result, Row, TsinkError};

use super::{
    shard_window_fnv1a_update, shard_window_hash_data_point, shard_window_series_identity_key,
    sort_data_points_for_shard_window, validate_query_rows_scan_options,
    validate_shard_window_request, validate_shard_window_scan_options, ChunkStorage,
    MetadataShardScope, PersistedTierFetchStats, QueryRowsPage, QueryRowsScanOptions,
    RawSeriesScanPage, SeriesId, ShardWindowDigest, ShardWindowRowsPage, ShardWindowScanOptions,
    SHARD_WINDOW_FNV_OFFSET_BASIS,
};

impl ChunkStorage {
    fn shard_scan_series_entries(
        &self,
        shard: u32,
        shard_count: u32,
        operation: &'static str,
    ) -> Result<Vec<ShardScanSeriesEntry>> {
        let metadata = self.metadata_shard_scope_context();
        let series_query = self.series_query_context();
        let scope = MetadataShardScope::new(shard_count, vec![shard]);
        let candidate_series_ids = metadata.live_series_ids_for_scope(&scope, operation)?;

        let mut entries = Vec::with_capacity(candidate_series_ids.len());
        for series_id in candidate_series_ids {
            let Some(series) = series_query.metric_series(series_id) else {
                continue;
            };
            let identity_key =
                shard_window_series_identity_key(series.name.as_str(), &series.labels);
            entries.push(ShardScanSeriesEntry {
                series_id,
                series,
                identity_key,
            });
        }
        entries.sort_by(|left, right| left.identity_key.cmp(&right.identity_key));
        Ok(entries)
    }

    fn scan_resolved_series_rows_with_plan(
        &self,
        resolved: &[(MetricSeries, Option<SeriesId>)],
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        options: QueryRowsScanOptions,
    ) -> Result<QueryRowsPage> {
        let context = self.series_query_context();
        let max_rows = options.max_rows;
        let row_offset = options.row_offset.unwrap_or(0);

        let mut response = QueryRowsPage {
            rows_scanned: 0,
            truncated: false,
            next_row_offset: None,
            rows: Vec::new(),
        };
        let mut stream_row_offset = 0u64;
        let mut persisted_stats = PersistedTierFetchStats::default();

        for (index, (series, series_id)) in resolved.iter().enumerate() {
            let page = match series_id {
                Some(series_id) => context.collect_raw_series_page(
                    *series_id,
                    start,
                    end,
                    plan,
                    row_offset.saturating_sub(stream_row_offset),
                    max_rows.and_then(|max| max.checked_sub(response.rows.len())),
                )?,
                None => RawSeriesScanPage::default(),
            };
            persisted_stats.accumulate(page.stats);
            stream_row_offset = stream_row_offset.saturating_add(page.final_rows_seen);

            if !page.points.is_empty() {
                response.rows_scanned = response
                    .rows_scanned
                    .saturating_add(u64::try_from(page.points.len()).unwrap_or(u64::MAX));
                response.rows.extend(page.points.into_iter().map(|point| {
                    Row::with_labels(series.name.clone(), series.labels.clone(), point)
                }));
            }

            if !page.reached_end {
                response.truncated = true;
                response.next_row_offset = Some(stream_row_offset);
                break;
            }

            if max_rows.is_some_and(|max| response.rows.len() >= max) {
                if index + 1 < resolved.len() {
                    response.truncated = true;
                    response.next_row_offset = Some(stream_row_offset);
                }
                break;
            }
        }

        self.record_query_tier_plan(plan);
        self.record_persisted_tier_fetch_stats(persisted_stats);
        Ok(response)
    }

    pub(in crate::engine::storage_engine) fn compute_shard_window_digest_api(
        &self,
        shard: u32,
        shard_count: u32,
        window_start: i64,
        window_end: i64,
    ) -> Result<ShardWindowDigest> {
        let context = self.series_query_context();
        self.ensure_open()?;
        validate_shard_window_request(shard, shard_count, window_start, window_end)?;
        self.request_background_persisted_refresh_if_needed();

        let mut points = Vec::new();
        let mut point_hashes = Vec::new();
        let mut fingerprint = SHARD_WINDOW_FNV_OFFSET_BASIS;
        let mut series_count = 0u64;
        let mut point_count = 0u64;
        let plan = context.query_tier_plan(window_start, window_end);
        let mut persisted_stats = PersistedTierFetchStats::default();

        for entry in
            self.shard_scan_series_entries(shard, shard_count, "compute_shard_window_digest")?
        {
            let stats = context.collect_points_for_series_into(
                entry.series_id,
                window_start,
                window_end,
                plan,
                &mut points,
            )?;
            persisted_stats.accumulate(stats);
            if points.is_empty() {
                continue;
            }

            point_hashes.clear();
            point_hashes.extend(points.iter().map(shard_window_hash_data_point));
            point_hashes.sort_unstable();

            shard_window_fnv1a_update(&mut fingerprint, entry.identity_key.as_bytes());
            shard_window_fnv1a_update(
                &mut fingerprint,
                &u64::try_from(point_hashes.len())
                    .unwrap_or(u64::MAX)
                    .to_le_bytes(),
            );
            for point_hash in &point_hashes {
                shard_window_fnv1a_update(&mut fingerprint, &point_hash.to_le_bytes());
            }

            series_count = series_count.saturating_add(1);
            point_count =
                point_count.saturating_add(u64::try_from(point_hashes.len()).unwrap_or(u64::MAX));
        }

        self.record_query_tier_plan(plan);
        self.record_persisted_tier_fetch_stats(persisted_stats);
        Ok(ShardWindowDigest {
            shard,
            shard_count,
            window_start,
            window_end,
            series_count,
            point_count,
            fingerprint,
        })
    }

    pub(in crate::engine::storage_engine) fn scan_shard_window_rows_api(
        &self,
        shard: u32,
        shard_count: u32,
        window_start: i64,
        window_end: i64,
        options: ShardWindowScanOptions,
    ) -> Result<ShardWindowRowsPage> {
        let context = self.series_query_context();
        self.ensure_open()?;
        validate_shard_window_request(shard, shard_count, window_start, window_end)?;
        validate_shard_window_scan_options(options)?;
        self.request_background_persisted_refresh_if_needed();

        let max_series =
            u64::try_from(options.max_series.unwrap_or(usize::MAX)).unwrap_or(u64::MAX);
        let max_rows = options.max_rows.unwrap_or(usize::MAX);
        let row_offset = options.row_offset.unwrap_or(0);

        let mut response = ShardWindowRowsPage {
            shard,
            shard_count,
            window_start,
            window_end,
            series_scanned: 0,
            rows_scanned: 0,
            truncated: false,
            next_row_offset: None,
            rows: Vec::new(),
        };

        let mut points = Vec::new();
        let mut stream_row_offset = 0u64;
        let mut remaining_series_budget = max_series;
        let plan = context.query_tier_plan(window_start, window_end);
        let mut persisted_stats = PersistedTierFetchStats::default();
        'series_scan: for entry in
            self.shard_scan_series_entries(shard, shard_count, "scan_shard_window_rows")?
        {
            let stats = context.collect_points_for_series_into(
                entry.series_id,
                window_start,
                window_end,
                plan,
                &mut points,
            )?;
            persisted_stats.accumulate(stats);
            if points.is_empty() {
                continue;
            }

            sort_data_points_for_shard_window(&mut points);

            let mut counted_series_for_budget = false;
            for point in points.iter() {
                if stream_row_offset < row_offset {
                    stream_row_offset = stream_row_offset.saturating_add(1);
                    continue;
                }

                if !counted_series_for_budget {
                    if remaining_series_budget == 0 {
                        response.truncated = true;
                        response.next_row_offset = Some(stream_row_offset);
                        break 'series_scan;
                    }
                    remaining_series_budget = remaining_series_budget.saturating_sub(1);
                    response.series_scanned = response.series_scanned.saturating_add(1);
                    counted_series_for_budget = true;
                }

                if response.rows.len() >= max_rows {
                    response.truncated = true;
                    response.next_row_offset = Some(stream_row_offset);
                    break 'series_scan;
                }

                response.rows_scanned = response.rows_scanned.saturating_add(1);
                response.rows.push(Row::with_labels(
                    entry.series.name.clone(),
                    entry.series.labels.clone(),
                    point.clone(),
                ));
                stream_row_offset = stream_row_offset.saturating_add(1);
            }
        }

        self.record_query_tier_plan(plan);
        self.record_persisted_tier_fetch_stats(persisted_stats);
        Ok(response)
    }

    pub(in crate::engine::storage_engine) fn scan_series_rows_api(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        options: QueryRowsScanOptions,
    ) -> Result<QueryRowsPage> {
        let context = self.series_query_context();
        self.ensure_open()?;
        if start >= end {
            return Err(TsinkError::InvalidTimeRange { start, end });
        }
        validate_query_rows_scan_options(options)?;
        self.request_background_persisted_refresh_if_needed();

        for item in series {
            validate_metric(&item.name)?;
            validate_labels(&item.labels)?;
        }
        let resolved = context.resolve_series_batch(series);
        let plan = context.query_tier_plan(start, end);
        self.scan_resolved_series_rows_with_plan(&resolved, start, end, plan, options)
    }

    pub(in crate::engine::storage_engine) fn scan_metric_rows_api(
        &self,
        metric: &str,
        start: i64,
        end: i64,
        options: QueryRowsScanOptions,
    ) -> Result<QueryRowsPage> {
        let context = self.series_query_context();
        self.ensure_open()?;
        validate_metric(metric)?;
        if start >= end {
            return Err(TsinkError::InvalidTimeRange { start, end });
        }
        validate_query_rows_scan_options(options)?;
        self.request_background_persisted_refresh_if_needed();

        let mut resolved = context
            .resolved_series_for_metric(metric)
            .into_iter()
            .map(|(series_id, series)| (series, Some(series_id)))
            .collect::<Vec<_>>();
        if resolved.is_empty() {
            return Ok(QueryRowsPage {
                rows_scanned: 0,
                truncated: false,
                next_row_offset: None,
                rows: Vec::new(),
            });
        }
        resolved.sort_by(|a, b| a.0.labels.cmp(&b.0.labels));

        let plan = context.query_tier_plan(start, end);
        self.scan_resolved_series_rows_with_plan(&resolved, start, end, plan, options)
    }
}

struct ShardScanSeriesEntry {
    series_id: SeriesId,
    series: MetricSeries,
    identity_key: String,
}
