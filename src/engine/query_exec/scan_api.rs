use crate::engine::query::TieredQueryPlan;
use crate::validation::{validate_labels, validate_metric};
use crate::{MetricSeries, Result, Row, SeriesMatcher, SeriesSelection, TsinkError};

use super::{
    modeled_metric_series_retained_bytes, modeled_metric_series_shape_retained_bytes,
    modeled_points_retained_bytes, modeled_row_parts_returned_bytes, modeled_string_capacity_bytes,
    modeled_vec_capacity_bytes, modeled_vec_growth_capacity_upper, shard_window_fnv1a_update,
    shard_window_hash_data_point, sort_data_points_for_shard_window_with_execution,
    validate_query_rows_scan_options, validate_shard_window_request,
    validate_shard_window_scan_options, ChunkStorage, MetadataShardScope, PersistedTierFetchStats,
    QueryExecution, QueryRowsExecutionResult, QueryRowsPage, QueryRowsScanOptions,
    RawSeriesScanPage, RuntimeMetadataCandidatePlan, SeriesId, ShardWindowDigest,
    ShardWindowRowsExecutionResult, ShardWindowRowsPage, ShardWindowScanOptions,
    SHARD_WINDOW_FNV_OFFSET_BASIS,
};

fn modeled_resolved_series_retained_bytes(series: &[MetricSeries]) -> u64 {
    modeled_vec_capacity_bytes::<(MetricSeries, Option<SeriesId>)>(series.len()).saturating_add(
        series.iter().fold(0u64, |bytes, item| {
            let label_text_bytes = item.labels.iter().fold(0usize, |total, label| {
                total
                    .saturating_add(label.name.len())
                    .saturating_add(label.value.len())
            });
            bytes.saturating_add(modeled_metric_series_shape_retained_bytes(
                item.name.len(),
                item.labels.len(),
                label_text_bytes,
            ))
        }),
    )
}

fn modeled_metric_matcher_selection_retained_bytes(
    metric_capacity: usize,
    matcher_capacity: usize,
    matchers: &[SeriesMatcher],
) -> u64 {
    modeled_string_capacity_bytes(metric_capacity)
        .saturating_add(modeled_vec_capacity_bytes::<SeriesMatcher>(
            matcher_capacity,
        ))
        .saturating_add(matchers.iter().fold(0u64, |bytes, matcher| {
            bytes
                .saturating_add(modeled_string_capacity_bytes(matcher.name.capacity()))
                .saturating_add(modeled_string_capacity_bytes(matcher.value.capacity()))
        }))
}

fn modeled_metric_matcher_selection_clone_upper_bytes(
    metric: &str,
    matchers: &[SeriesMatcher],
) -> u64 {
    modeled_string_capacity_bytes(metric.len())
        .saturating_add(modeled_vec_capacity_bytes::<SeriesMatcher>(matchers.len()))
        .saturating_add(matchers.iter().fold(0u64, |bytes, matcher| {
            bytes
                .saturating_add(modeled_string_capacity_bytes(matcher.name.capacity()))
                .saturating_add(modeled_string_capacity_bytes(matcher.value.capacity()))
        }))
}

fn modeled_resolved_metric_rows_retained_bytes(
    resolved: &Vec<(MetricSeries, Option<SeriesId>)>,
) -> u64 {
    modeled_vec_capacity_bytes::<(MetricSeries, Option<SeriesId>)>(resolved.capacity())
        .saturating_add(resolved.iter().fold(0u64, |bytes, (series, _)| {
            bytes.saturating_add(modeled_metric_series_retained_bytes(series))
        }))
}

fn charge_projected_row_before_materialization(
    execution: &QueryExecution,
    metric: &str,
    labels: &[crate::Label],
    point: &crate::DataPoint,
    excluded_output_label: Option<&str>,
) -> Result<()> {
    let excluded_label_bytes = excluded_output_label.map_or(0u64, |excluded_name| {
        labels.iter().fold(0u64, |bytes, label| {
            if label.name != excluded_name {
                return bytes;
            }
            bytes
                .saturating_add(
                    u64::try_from(std::mem::size_of::<crate::Label>()).unwrap_or(u64::MAX),
                )
                .saturating_add(u64::try_from(label.name.len()).unwrap_or(u64::MAX))
                .saturating_add(u64::try_from(label.value.len()).unwrap_or(u64::MAX))
        })
    });
    execution.checkpoint()?;
    execution.charge_samples_returned(1)?;
    execution.charge_returned_bytes(
        modeled_row_parts_returned_bytes(metric, labels, point)
            .saturating_sub(excluded_label_bytes),
    )?;
    Ok(())
}

fn shard_scan_identity_key_len(
    metric_bytes: usize,
    label_count: usize,
    label_text_bytes: usize,
) -> usize {
    2usize
        .saturating_add(metric_bytes.min(crate::label::MAX_METRIC_NAME_LEN))
        .saturating_add(label_count.saturating_mul(4))
        .saturating_add(label_text_bytes)
        .saturating_mul(2)
}

fn shard_scan_identity_key(metric: &str, labels: &[crate::Label]) -> String {
    fn push_hex_byte(output: &mut String, byte: u8) {
        const HEX: &[u8; 16] = b"0123456789abcdef";
        output.push(char::from(HEX[(byte >> 4) as usize]));
        output.push(char::from(HEX[(byte & 0x0f) as usize]));
    }

    fn push_hex_bytes(output: &mut String, bytes: &[u8]) {
        for byte in bytes {
            push_hex_byte(output, *byte);
        }
    }

    debug_assert!(labels.windows(2).all(|pair| pair[0] <= pair[1]));
    let metric_bytes = metric.as_bytes();
    let metric_len = metric_bytes.len().min(crate::label::MAX_METRIC_NAME_LEN);
    let canonical_len = 2usize
        .saturating_add(metric_len)
        .saturating_add(labels.iter().fold(0usize, |bytes, label| {
            if !label.is_valid() {
                return bytes;
            }
            bytes
                .saturating_add(4)
                .saturating_add(label.name.len().min(u16::MAX as usize))
                .saturating_add(label.value.len().min(u16::MAX as usize))
        }));
    let mut output = String::with_capacity(canonical_len.saturating_mul(2));
    push_hex_bytes(&mut output, &(metric_len as u16).to_le_bytes());
    push_hex_bytes(&mut output, &metric_bytes[..metric_len]);
    for label in labels {
        if !label.is_valid() {
            continue;
        }
        let name_bytes = label.name.as_bytes();
        let name_len = name_bytes.len().min(u16::MAX as usize);
        push_hex_bytes(&mut output, &(name_len as u16).to_le_bytes());
        push_hex_bytes(&mut output, &name_bytes[..name_len]);

        let value_bytes = label.value.as_bytes();
        let value_len = value_bytes.len().min(u16::MAX as usize);
        push_hex_bytes(&mut output, &(value_len as u16).to_le_bytes());
        push_hex_bytes(&mut output, &value_bytes[..value_len]);
    }
    output
}

fn modeled_shard_scan_entries_retained_bytes(entries: &Vec<ShardScanSeriesEntry>) -> u64 {
    modeled_vec_capacity_bytes::<ShardScanSeriesEntry>(entries.capacity()).saturating_add(
        entries.iter().fold(0u64, |bytes, entry| {
            bytes
                .saturating_add(modeled_metric_series_retained_bytes(&entry.series))
                .saturating_add(modeled_string_capacity_bytes(entry.identity_key.capacity()))
        }),
    )
}

impl ChunkStorage {
    pub(in crate::engine::storage_engine) fn shard_scan_series_entries(
        &self,
        shard: u32,
        shard_count: u32,
        operation: &'static str,
        execution: &QueryExecution,
    ) -> Result<(
        Vec<ShardScanSeriesEntry>,
        crate::QueryMemoryReservation,
        crate::QueryMemoryReservation,
    )> {
        let candidate_reservation = self.reserve_metadata_candidate_working_set(execution)?;
        let metadata = self.metadata_shard_scope_context();
        let series_query = self.series_query_context();
        let scope = MetadataShardScope::new(shard_count, vec![shard]);
        execution.ensure_pattern_expansion(
            u64::try_from(self.catalog.registry.read().series_count()).unwrap_or(u64::MAX),
        )?;
        let candidate_series_ids = metadata.live_series_ids_for_scope(&scope, operation)?;
        execution.checkpoint()?;
        execution.charge_pattern_expansion(
            u64::try_from(candidate_series_ids.len()).unwrap_or(u64::MAX),
        )?;
        execution
            .charge_series_matched(u64::try_from(candidate_series_ids.len()).unwrap_or(u64::MAX))?;
        execution.observe_intermediate_vector_size(
            u64::try_from(candidate_series_ids.len()).unwrap_or(u64::MAX),
        )?;

        let mut entries_bytes = modeled_vec_capacity_bytes::<ShardScanSeriesEntry>(
            modeled_vec_growth_capacity_upper(candidate_series_ids.len()),
        );
        {
            let registry = self.catalog.registry.read();
            for series_id in &candidate_series_ids {
                execution.checkpoint()?;
                let Some((metric_bytes, label_count, label_text_bytes)) =
                    registry.decoded_series_key_shape(*series_id)
                else {
                    continue;
                };
                entries_bytes = entries_bytes
                    .saturating_add(modeled_metric_series_shape_retained_bytes(
                        metric_bytes,
                        label_count,
                        label_text_bytes,
                    ))
                    .saturating_add(modeled_string_capacity_bytes(
                        modeled_vec_growth_capacity_upper(shard_scan_identity_key_len(
                            metric_bytes,
                            label_count,
                            label_text_bytes,
                        )),
                    ));
            }
        }
        let mut entries_reservation = execution.reserve_memory(entries_bytes)?;
        let mut entries = Vec::with_capacity(candidate_series_ids.len());
        for series_id in candidate_series_ids {
            execution.checkpoint()?;
            let Some(series) = series_query.metric_series(series_id) else {
                continue;
            };
            let identity_key = shard_scan_identity_key(series.name.as_str(), &series.labels);
            entries.push(ShardScanSeriesEntry {
                series_id,
                series,
                identity_key,
            });
        }
        entries_reservation.resize(modeled_shard_scan_entries_retained_bytes(&entries))?;
        execution.checkpoint()?;
        entries.sort_unstable_by(|left, right| left.identity_key.cmp(&right.identity_key));
        execution.checkpoint()?;
        Ok((entries, candidate_reservation, entries_reservation))
    }

    #[allow(clippy::too_many_arguments)]
    fn scan_resolved_series_rows_with_plan(
        &self,
        resolved: &[(MetricSeries, Option<SeriesId>)],
        start: i64,
        end: i64,
        plan: TieredQueryPlan,
        options: QueryRowsScanOptions,
        excluded_output_label: Option<&str>,
        execution: &QueryExecution,
    ) -> Result<QueryRowsExecutionResult> {
        let context = self.series_query_context();
        let max_rows = options.max_rows;
        let row_offset = options.row_offset.unwrap_or(0);

        let mut row_reservation = execution.reserve_memory(0)?;
        let mut response = QueryRowsPage {
            rows_scanned: 0,
            truncated: false,
            next_row_offset: None,
            rows: Vec::new(),
        };
        let mut stream_row_offset = 0u64;
        let mut persisted_stats = PersistedTierFetchStats::default();

        for (index, (series, series_id)) in resolved.iter().enumerate() {
            execution.checkpoint()?;
            let mut page = match series_id {
                Some(series_id) => context.collect_raw_series_page(
                    *series_id,
                    start,
                    end,
                    plan,
                    row_offset.saturating_sub(stream_row_offset),
                    max_rows.and_then(|max| max.checked_sub(response.rows.len())),
                    Some(execution),
                    false,
                )?,
                None => RawSeriesScanPage::default(),
            };
            let raw_points_reservation = page.take_query_reservation();
            let RawSeriesScanPage {
                points,
                final_rows_seen,
                reached_end,
                stats,
                query_reservation: _,
            } = page;
            persisted_stats.accumulate(stats);
            stream_row_offset = stream_row_offset.saturating_add(final_rows_seen);

            if !points.is_empty() {
                let raw_points_reservation = raw_points_reservation.ok_or_else(|| {
                    TsinkError::Other(
                        "row scan received raw points without a query-memory reservation"
                            .to_string(),
                    )
                })?;
                row_reservation.resize(super::modeled_rows_append_upper_bytes(
                    &response.rows,
                    &series.name,
                    &series.labels,
                    &points,
                ))?;
                response.rows_scanned = response
                    .rows_scanned
                    .saturating_add(u64::try_from(points.len()).unwrap_or(u64::MAX));
                for point in points {
                    charge_projected_row_before_materialization(
                        execution,
                        &series.name,
                        &series.labels,
                        &point,
                        excluded_output_label,
                    )?;
                    let labels = match excluded_output_label {
                        Some(excluded_name) => {
                            let mut labels = Vec::with_capacity(series.labels.len());
                            labels.extend(
                                series
                                    .labels
                                    .iter()
                                    .filter(|label| label.name != excluded_name)
                                    .cloned(),
                            );
                            labels
                        }
                        None => series.labels.clone(),
                    };
                    response
                        .rows
                        .push(Row::with_labels(series.name.clone(), labels, point));
                }
                let mut source_reservations = [row_reservation, raw_points_reservation];
                row_reservation = match execution.coalesce_memory_reservation_array(
                    &mut source_reservations,
                    super::modeled_query_rows_retained_bytes(&response.rows),
                ) {
                    Ok(reservation) => reservation,
                    Err(error) => {
                        // Keep both source guards live until the materialized rows are gone.
                        drop(response);
                        let error = match error {
                            crate::query_budget::QueryMemoryCoalesceError::Budget(error) => {
                                error.into()
                            }
                            crate::query_budget::QueryMemoryCoalesceError::InvalidReservations => {
                                TsinkError::Other(
                                    "row scan received an incompatible raw-page query-memory reservation"
                                        .to_string(),
                                )
                            }
                        };
                        drop(source_reservations);
                        return Err(error);
                    }
                };
            } else {
                drop(points);
                drop(raw_points_reservation);
            }

            if !reached_end {
                response.truncated = true;
                response.next_row_offset = Some(stream_row_offset);
                break;
            }

            // A full exact page ends only when no later identities remain. Otherwise continue
            // with a zero-row logical limit to look for one real row; empty trailing identities
            // must not turn an exact final page into a conservative extra continuation page.
            if max_rows.is_some_and(|max| response.rows.len() >= max) && index + 1 >= resolved.len()
            {
                break;
            }
        }

        self.record_query_tier_plan(plan);
        self.record_persisted_tier_fetch_stats(persisted_stats);
        Ok(QueryRowsExecutionResult::accounted(
            response,
            row_reservation,
        ))
    }

    pub(in crate::engine::storage_engine) fn compute_shard_window_digest_api(
        &self,
        shard: u32,
        shard_count: u32,
        window_start: i64,
        window_end: i64,
        execution: &QueryExecution,
    ) -> Result<ShardWindowDigest> {
        let context = self.series_query_context();
        execution.checkpoint()?;
        self.ensure_open()?;
        validate_shard_window_request(shard, shard_count, window_start, window_end)?;
        self.request_background_persisted_refresh_if_needed();

        let mut points = Vec::new();
        let mut points_reservation = execution.reserve_memory(0)?;
        let mut point_hashes = Vec::new();
        let mut point_hash_reservation = execution.reserve_memory(0)?;
        let mut fingerprint = SHARD_WINDOW_FNV_OFFSET_BASIS;
        let mut series_count = 0u64;
        let mut point_count = 0u64;
        let plan = context.query_tier_plan(window_start, window_end);
        let mut persisted_stats = PersistedTierFetchStats::default();

        let (entries, _candidate_reservation, _entries_reservation) = self
            .shard_scan_series_entries(
                shard,
                shard_count,
                "compute_shard_window_digest",
                execution,
            )?;
        for entry in entries {
            execution.checkpoint()?;
            let stats = context.collect_points_for_series_into(
                entry.series_id,
                window_start,
                window_end,
                plan,
                &mut points,
                Some(execution),
                false,
            )?;
            persisted_stats.accumulate(stats);
            points_reservation.resize(modeled_points_retained_bytes(&points))?;
            if points.is_empty() {
                continue;
            }

            point_hashes.clear();
            execution.observe_intermediate_vector_size(
                u64::try_from(points.len()).unwrap_or(u64::MAX),
            )?;
            let requested_hash_capacity = points
                .len()
                .checked_next_power_of_two()
                .unwrap_or(usize::MAX)
                .max(4);
            point_hash_reservation
                .resize(modeled_vec_capacity_bytes::<u64>(requested_hash_capacity))?;
            for point in &points {
                execution.checkpoint()?;
                point_hashes.push(shard_window_hash_data_point(point)?);
            }
            point_hash_reservation
                .resize(modeled_vec_capacity_bytes::<u64>(point_hashes.capacity()))?;
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

        drop(point_hashes);
        drop(point_hash_reservation);
        drop(points);
        drop(points_reservation);
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
        execution: &QueryExecution,
    ) -> Result<ShardWindowRowsExecutionResult> {
        let context = self.series_query_context();
        execution.checkpoint()?;
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
        let mut points_reservation = execution.reserve_memory(0)?;
        let mut stream_row_offset = 0u64;
        let mut remaining_series_budget = max_series;
        let plan = context.query_tier_plan(window_start, window_end);
        let mut persisted_stats = PersistedTierFetchStats::default();
        let mut row_reservation = execution.reserve_memory(0)?;
        let (entries, _candidate_reservation, _entries_reservation) = self
            .shard_scan_series_entries(shard, shard_count, "scan_shard_window_rows", execution)?;
        'series_scan: for entry in entries {
            execution.checkpoint()?;
            let stats = context.collect_points_for_series_into(
                entry.series_id,
                window_start,
                window_end,
                plan,
                &mut points,
                Some(execution),
                false,
            )?;
            persisted_stats.accumulate(stats);
            points_reservation.resize(modeled_points_retained_bytes(&points))?;
            if points.is_empty() {
                continue;
            }

            sort_data_points_for_shard_window_with_execution(&mut points, execution)?;
            row_reservation.resize(super::modeled_rows_append_upper_bytes(
                &response.rows,
                &entry.series.name,
                &entry.series.labels,
                &points,
            ))?;

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
                self.charge_row_before_materialization(
                    execution,
                    &entry.series.name,
                    &entry.series.labels,
                    point,
                )?;
                response.rows.push(Row::with_labels(
                    entry.series.name.clone(),
                    entry.series.labels.clone(),
                    point.clone(),
                ));
                stream_row_offset = stream_row_offset.saturating_add(1);
            }
        }

        drop(points);
        drop(points_reservation);
        row_reservation.resize(super::modeled_query_rows_retained_bytes(&response.rows))?;
        self.record_query_tier_plan(plan);
        self.record_persisted_tier_fetch_stats(persisted_stats);
        Ok(ShardWindowRowsExecutionResult::accounted(
            response,
            row_reservation,
        ))
    }

    pub(in crate::engine::storage_engine) fn scan_series_rows_result_api(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        options: QueryRowsScanOptions,
        execution: &QueryExecution,
    ) -> Result<QueryRowsExecutionResult> {
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
        execution
            .observe_intermediate_vector_size(u64::try_from(series.len()).unwrap_or(u64::MAX))?;
        // Resolution clones every requested identity before scanning. Admit that complete
        // retained shape before `resolve_series_batch` allocates; the reservation stays live
        // until scanning no longer needs the cloned identities.
        let _resolved_series_reservation =
            execution.reserve_memory(modeled_resolved_series_retained_bytes(series))?;
        let resolved = context.resolve_series_batch(series);
        let existing_occurrences = resolved
            .iter()
            .filter(|(_, series_id)| series_id.is_some())
            .count();
        let matched_series_ids_reservation = execution
            .reserve_memory(modeled_vec_capacity_bytes::<SeriesId>(existing_occurrences))?;
        let mut matched_series_ids = Vec::with_capacity(existing_occurrences);
        matched_series_ids.extend(
            resolved
                .iter()
                .filter_map(|(_, series_id)| series_id.as_ref().copied()),
        );
        matched_series_ids.sort_unstable();
        matched_series_ids.dedup();
        execution
            .charge_series_matched(u64::try_from(matched_series_ids.len()).unwrap_or(u64::MAX))?;
        drop(matched_series_ids);
        drop(matched_series_ids_reservation);
        let plan = context.query_tier_plan(start, end);
        self.scan_resolved_series_rows_with_plan(
            &resolved, start, end, plan, options, None, execution,
        )
    }

    pub(in crate::engine::storage_engine) fn scan_metric_rows_result_api(
        &self,
        metric: &str,
        start: i64,
        end: i64,
        options: QueryRowsScanOptions,
        execution: &QueryExecution,
    ) -> Result<QueryRowsExecutionResult> {
        let context = self.series_query_context();
        self.ensure_open()?;
        options.validate_metric_row_request(metric, start, end)?;
        self.request_background_persisted_refresh_if_needed();

        let mut resolved_with_reservation =
            context.resolved_series_for_metric(metric, execution)?;
        resolved_with_reservation
            .series
            .sort_unstable_by(|a, b| a.0.labels.cmp(&b.0.labels));

        let plan = context.query_tier_plan(start, end);
        let result = self.scan_resolved_series_rows_with_plan(
            &resolved_with_reservation.series,
            start,
            end,
            plan,
            options,
            None,
            execution,
        );
        drop(resolved_with_reservation);
        result
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::engine::storage_engine) fn scan_metric_rows_with_matchers_result_api(
        &self,
        metric: &str,
        matchers: &[SeriesMatcher],
        excluded_output_label: Option<&str>,
        start: i64,
        end: i64,
        options: QueryRowsScanOptions,
        execution: &QueryExecution,
    ) -> Result<QueryRowsExecutionResult> {
        let context = self.series_query_context();
        self.ensure_open()?;
        options.validate_metric_row_request(metric, start, end)?;
        crate::storage::validate_series_matcher_shapes(matchers).map_err(TsinkError::from)?;
        crate::storage::validate_metric_row_output_projection(matchers, excluded_output_label)?;
        self.request_background_persisted_refresh_if_needed();
        execution.checkpoint()?;

        // The candidate planner consumes an owned `SeriesSelection`. Admit the complete cloned
        // metric/matcher shape before constructing it; callers retain ownership of the borrowed
        // matchers and are not charged for that input allocation here.
        let mut selection_reservation = execution.reserve_memory(
            modeled_metric_matcher_selection_clone_upper_bytes(metric, matchers),
        )?;
        let selection = SeriesSelection {
            metric: Some(metric.to_string()),
            matchers: matchers.to_vec(),
            start: None,
            end: None,
        };
        selection_reservation.resize(modeled_metric_matcher_selection_retained_bytes(
            selection
                .metric
                .as_ref()
                .map_or(0, |selected_metric| selected_metric.capacity()),
            selection.matchers.capacity(),
            &selection.matchers,
        ))?;

        // Keep the selection clone, matcher programs, and complete candidate working set live
        // together. Candidate planning applies both the exact metric and every supplied matcher,
        // so the resulting unique ID set is the one canonical `series_matched` charge.
        let candidate_reservation = self.reserve_metadata_candidate_working_set(execution)?;
        #[cfg(test)]
        let prepared = {
            let before_regex_compile = || self.invoke_metadata_matcher_regex_compile_hook();
            crate::query_selection::prepare_series_selection_with_execution(
                &selection,
                execution,
                Some(&before_regex_compile),
            )?
        };
        #[cfg(not(test))]
        let prepared = crate::query_selection::prepare_series_selection_with_execution(
            &selection, execution, None,
        )?;
        let candidate_plan = self.runtime_metadata_candidate_plan(
            &selection,
            &prepared.compiled_matchers,
            None,
            execution,
        )?;
        #[cfg(test)]
        self.record_runtime_metadata_candidate_plan_hooks(
            &candidate_plan,
            &prepared.compiled_matchers,
        );
        let RuntimeMetadataCandidatePlan {
            candidate_series_ids,
            ..
        } = candidate_plan;
        let candidate_count = candidate_series_ids.len();
        execution.charge_series_matched(candidate_count)?;
        execution.observe_intermediate_vector_size(candidate_count)?;

        let candidate_len = usize::try_from(candidate_count).map_err(|_| {
            TsinkError::Other(
                "matcher-aware metric row scan candidate count exceeds the supported range"
                    .to_string(),
            )
        })?;
        let resolved_capacity = modeled_vec_growth_capacity_upper(candidate_len);
        let mut resolved_bytes =
            modeled_vec_capacity_bytes::<(MetricSeries, Option<SeriesId>)>(resolved_capacity);
        {
            let registry = self.catalog.registry.read();
            for series_id in candidate_series_ids.iter() {
                execution.checkpoint()?;
                let Some((metric_bytes, label_count, label_text_bytes)) =
                    registry.decoded_series_key_shape(series_id)
                else {
                    continue;
                };
                resolved_bytes =
                    resolved_bytes.saturating_add(modeled_metric_series_shape_retained_bytes(
                        metric_bytes,
                        label_count,
                        label_text_bytes,
                    ));
            }
        }
        let mut resolved_reservation = execution.reserve_memory(resolved_bytes)?;
        let mut resolved = Vec::new();
        resolved.try_reserve(candidate_len).map_err(|error| {
            TsinkError::Other(format!(
                "failed to reserve matcher-aware metric row identities: {error}"
            ))
        })?;
        {
            let registry = self.catalog.registry.read();
            for series_id in candidate_series_ids.iter() {
                execution.checkpoint()?;
                let Some(series_key) = registry.decode_series_key(series_id) else {
                    continue;
                };
                resolved.push((
                    MetricSeries {
                        name: series_key.metric,
                        labels: series_key.labels,
                    },
                    Some(series_id),
                ));
            }
        }
        resolved_reservation.resize(modeled_resolved_metric_rows_retained_bytes(&resolved))?;
        execution.checkpoint()?;
        resolved.sort_unstable_by(|left, right| left.0.labels.cmp(&right.0.labels));
        execution.checkpoint()?;

        let plan = context.query_tier_plan(start, end);
        let result = self.scan_resolved_series_rows_with_plan(
            &resolved,
            start,
            end,
            plan,
            options,
            excluded_output_label,
            execution,
        );
        drop(resolved);
        drop(resolved_reservation);
        drop(candidate_series_ids);
        drop(prepared);
        drop(candidate_reservation);
        drop(selection);
        drop(selection_reservation);
        result
    }
}

#[derive(Debug)]
pub(in crate::engine::storage_engine) struct ShardScanSeriesEntry {
    series_id: SeriesId,
    series: MetricSeries,
    identity_key: String,
}
