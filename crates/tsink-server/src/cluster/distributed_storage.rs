use super::query::{
    modeled_metric_series_vec_retained_bytes, modeled_points_vec_retained_bytes,
    modeled_series_points_vec_retained_bytes, ReadFanoutError, ReadFanoutExecutor,
    ReadFanoutResponse, ReadFanoutResponseMetadata, SeriesPoints,
};
use super::rpc::RpcClient;
use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::runtime::Handle;
use tsink::{
    DataPoint, DeleteSeriesResult, EffectiveStorageLimits, Label, MetricSeries, QueryBudget,
    QueryCancellationToken, QueryExecution, QueryExecutionAccounting, QueryMemoryReservation,
    QueryOptions, QueryWorkLimits, Result as TsinkResult, Row, SelectManyExecutionResult,
    SelectSeriesExecutionResult, SeriesMatcher, SeriesMatcherOp, SeriesSelection, Storage,
    StorageObservabilitySnapshot, TsinkError,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DistributedStorageCacheConfig {
    pub cache_select_series: bool,
    pub cache_select_points: bool,
    pub cache_select_all: bool,
}

impl Default for DistributedStorageCacheConfig {
    fn default() -> Self {
        Self {
            cache_select_series: true,
            cache_select_points: true,
            cache_select_all: true,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DistributedStorageCacheSnapshot {
    pub select_series_hits: u64,
    pub select_series_misses: u64,
    pub select_points_hits: u64,
    pub select_points_misses: u64,
    pub select_all_hits: u64,
    pub select_all_misses: u64,
}

type SelectAllCacheValue = Vec<(Vec<Label>, Vec<DataPoint>)>;

const DISTRIBUTED_METADATA_ALLOCATION_ALLOWANCE_BYTES: u64 = 64;

struct DistributedReadMetadataState {
    metadata: ReadFanoutResponseMetadata,
    reservation: Option<QueryMemoryReservation>,
}

pub(crate) struct AccountedDistributedReadMetadata {
    pub(crate) metadata: ReadFanoutResponseMetadata,
    _reservation: Option<QueryMemoryReservation>,
}

impl AccountedDistributedReadMetadata {
    #[cfg(test)]
    fn reserved_memory_bytes(&self) -> u64 {
        self._reservation
            .as_ref()
            .map_or(0, QueryMemoryReservation::bytes)
    }
}

fn modeled_select_all_output_slots(capacity: usize) -> u64 {
    if capacity == 0 {
        return 0;
    }
    u64::try_from(capacity)
        .unwrap_or(u64::MAX)
        .saturating_mul(
            u64::try_from(std::mem::size_of::<(Vec<Label>, Vec<DataPoint>)>()).unwrap_or(u64::MAX),
        )
        .saturating_add(64)
}

fn distributed_collection_growth_capacity_upper(len: usize) -> usize {
    if len == 0 {
        0
    } else if len <= 4 {
        4
    } else {
        len.checked_next_power_of_two().unwrap_or(usize::MAX)
    }
}

fn modeled_distributed_metadata_string_bytes(value: &str) -> u64 {
    if value.is_empty() {
        0
    } else {
        u64::try_from(value.len())
            .unwrap_or(u64::MAX)
            .saturating_add(DISTRIBUTED_METADATA_ALLOCATION_ALLOWANCE_BYTES)
    }
}

fn modeled_distributed_metadata_retained_bytes(metadata: &ReadFanoutResponseMetadata) -> u64 {
    let vector_bytes = if metadata.warnings.capacity() == 0 {
        0
    } else {
        u64::try_from(metadata.warnings.capacity())
            .unwrap_or(u64::MAX)
            .saturating_mul(u64::try_from(std::mem::size_of::<String>()).unwrap_or(u64::MAX))
            .saturating_add(DISTRIBUTED_METADATA_ALLOCATION_ALLOWANCE_BYTES)
    };
    vector_bytes.saturating_add(metadata.warnings.iter().fold(0u64, |bytes, warning| {
        bytes.saturating_add(if warning.capacity() == 0 {
            0
        } else {
            u64::try_from(warning.capacity())
                .unwrap_or(u64::MAX)
                .saturating_add(DISTRIBUTED_METADATA_ALLOCATION_ALLOWANCE_BYTES)
        })
    }))
}

fn modeled_distributed_metadata_merge_upper_bytes(
    current: &ReadFanoutResponseMetadata,
    incoming: &ReadFanoutResponseMetadata,
) -> u64 {
    let missing_count = incoming
        .warnings
        .iter()
        .filter(|warning| !current.warnings.iter().any(|item| item == *warning))
        .count();
    let future_len = current.warnings.len().saturating_add(missing_count);
    let future_capacity = current
        .warnings
        .capacity()
        .max(distributed_collection_growth_capacity_upper(future_len));
    let vector_bytes = if future_capacity == 0 {
        0
    } else {
        u64::try_from(future_capacity)
            .unwrap_or(u64::MAX)
            .saturating_mul(u64::try_from(std::mem::size_of::<String>()).unwrap_or(u64::MAX))
            .saturating_add(DISTRIBUTED_METADATA_ALLOCATION_ALLOWANCE_BYTES)
    };
    vector_bytes
        .saturating_add(current.warnings.iter().fold(0u64, |bytes, warning| {
            bytes.saturating_add(if warning.capacity() == 0 {
                0
            } else {
                u64::try_from(warning.capacity())
                    .unwrap_or(u64::MAX)
                    .saturating_add(DISTRIBUTED_METADATA_ALLOCATION_ALLOWANCE_BYTES)
            })
        }))
        .saturating_add(incoming.warnings.iter().fold(0u64, |bytes, warning| {
            if current.warnings.iter().any(|item| item == warning) {
                bytes
            } else {
                bytes.saturating_add(modeled_distributed_metadata_string_bytes(warning))
            }
        }))
}

#[derive(Debug, Default)]
struct DistributedStorageCache {
    select_series: HashMap<SeriesSelectionCacheKey, Vec<MetricSeries>>,
    select_points: HashMap<SelectPointsCacheKey, Vec<DataPoint>>,
    select_all: HashMap<SelectAllCacheKey, SelectAllCacheValue>,
    snapshot: DistributedStorageCacheSnapshot,
}

#[derive(Clone)]
pub struct DistributedPromqlReadBridge {
    runtime_handle: Handle,
}

impl DistributedPromqlReadBridge {
    /// PromQL still executes against the synchronous `Storage` trait. Public query handlers and
    /// rule evaluation run the engine inside `spawn_blocking`, and this bridge is the only place
    /// where those sync storage calls hop back onto the async runtime for cluster read fanout.
    pub fn from_current_runtime() -> Self {
        Self {
            runtime_handle: Handle::current(),
        }
    }

    fn block_on<T>(&self, future: impl Future<Output = T>) -> T {
        self.runtime_handle.block_on(future)
    }
}

#[derive(Clone)]
pub struct DistributedStorageAdapter {
    local_storage: Arc<dyn Storage>,
    rpc_client: RpcClient,
    read_fanout: ReadFanoutExecutor,
    ring_version: u64,
    read_bridge: DistributedPromqlReadBridge,
    cache_config: DistributedStorageCacheConfig,
    cache: Arc<Mutex<DistributedStorageCache>>,
    read_metadata: Arc<Mutex<DistributedReadMetadataState>>,
}

impl DistributedStorageAdapter {
    pub fn new(
        local_storage: Arc<dyn Storage>,
        rpc_client: RpcClient,
        read_fanout: ReadFanoutExecutor,
        ring_version: u64,
        read_bridge: DistributedPromqlReadBridge,
    ) -> Self {
        Self {
            local_storage,
            rpc_client,
            read_fanout: read_fanout.clone(),
            ring_version: ring_version.max(1),
            read_bridge,
            cache_config: DistributedStorageCacheConfig::default(),
            cache: Arc::new(Mutex::new(DistributedStorageCache::default())),
            read_metadata: Arc::new(Mutex::new(DistributedReadMetadataState {
                metadata: ReadFanoutResponseMetadata {
                    consistency: read_fanout.read_consistency_mode(),
                    partial_response_policy: read_fanout.read_partial_response_policy(),
                    partial_response: false,
                    warnings: Vec::new(),
                },
                reservation: None,
            })),
        }
    }

    #[allow(dead_code)]
    pub fn with_cache_config(mut self, config: DistributedStorageCacheConfig) -> Self {
        self.cache_config = config;
        self
    }

    #[cfg(test)]
    pub fn read_metadata_snapshot(&self) -> ReadFanoutResponseMetadata {
        match self.read_metadata.lock() {
            Ok(state) => state.metadata.clone(),
            Err(_) => ReadFanoutResponseMetadata {
                consistency: self.read_fanout.read_consistency_mode(),
                partial_response_policy: self.read_fanout.read_partial_response_policy(),
                partial_response: false,
                warnings: Vec::new(),
            },
        }
    }

    pub(crate) fn take_accounted_read_metadata(
        &self,
    ) -> TsinkResult<AccountedDistributedReadMetadata> {
        let mut state = self.lock_metadata()?;
        let retained_bytes = modeled_distributed_metadata_retained_bytes(&state.metadata);
        if retained_bytes > 0 && state.reservation.is_none() {
            return Err(TsinkError::Other(
                "distributed PromQL metadata omitted its retained-memory reservation".to_string(),
            ));
        }
        let metadata = std::mem::replace(
            &mut state.metadata,
            ReadFanoutResponseMetadata {
                consistency: self.read_fanout.read_consistency_mode(),
                partial_response_policy: self.read_fanout.read_partial_response_policy(),
                partial_response: false,
                warnings: Vec::new(),
            },
        );
        Ok(AccountedDistributedReadMetadata {
            metadata,
            _reservation: state.reservation.take(),
        })
    }

    #[allow(dead_code)]
    pub fn cache_snapshot(&self) -> DistributedStorageCacheSnapshot {
        match self.cache.lock() {
            Ok(cache) => cache.snapshot,
            Err(_) => DistributedStorageCacheSnapshot::default(),
        }
    }

    fn lock_cache(&self) -> TsinkResult<MutexGuard<'_, DistributedStorageCache>> {
        self.cache.lock().map_err(|err| TsinkError::LockPoisoned {
            resource: format!("distributed-storage-cache: {err}"),
        })
    }

    fn lock_metadata(&self) -> TsinkResult<MutexGuard<'_, DistributedReadMetadataState>> {
        self.read_metadata
            .lock()
            .map_err(|err| TsinkError::LockPoisoned {
                resource: format!("distributed-storage-metadata: {err}"),
            })
    }

    fn record_metadata(
        &self,
        metadata: &ReadFanoutResponseMetadata,
        execution: Option<&QueryExecution>,
    ) -> TsinkResult<()> {
        let mut state = self.lock_metadata()?;
        if let Some(execution) = execution {
            execution.checkpoint().map_err(TsinkError::from)?;
            let upper = modeled_distributed_metadata_merge_upper_bytes(&state.metadata, metadata);
            if upper > 0 {
                if let Some(reservation) = &mut state.reservation {
                    reservation.resize(upper).map_err(TsinkError::from)?;
                } else {
                    state.reservation =
                        Some(execution.reserve_memory(upper).map_err(TsinkError::from)?);
                }
            }
        }
        state.metadata.partial_response |= metadata.partial_response;
        for warning in &metadata.warnings {
            if !state.metadata.warnings.iter().any(|item| item == warning) {
                state.metadata.warnings.push(warning.clone());
            }
        }
        // Warning strings are the complete comparison keys, so unstable sorting is deterministic
        // here and avoids stable-sort scratch allocation.
        state.metadata.warnings.sort_unstable();
        let retained_bytes = modeled_distributed_metadata_retained_bytes(&state.metadata);
        if let Some(reservation) = &mut state.reservation {
            reservation
                .resize(retained_bytes)
                .map_err(TsinkError::from)?;
        }
        Ok(())
    }

    fn finish_fanout<T>(&self, response: TsinkResult<ReadFanoutResponse<T>>) -> TsinkResult<T> {
        let response = response?;
        self.record_metadata(&response.metadata, None)?;
        Ok(response.value)
    }

    fn select_series_distributed(
        &self,
        selection: &SeriesSelection,
    ) -> TsinkResult<Vec<MetricSeries>> {
        self.finish_fanout(
            self.read_bridge
                .block_on(self.read_fanout.select_series_with_ring_version_detailed(
                    &self.local_storage,
                    &self.rpc_client,
                    selection,
                    self.ring_version,
                ))
                .map_err(map_read_fanout_error),
        )
    }

    fn select_series_distributed_with_execution(
        &self,
        selection: &SeriesSelection,
        execution: &QueryExecution,
    ) -> TsinkResult<Vec<MetricSeries>> {
        self.select_series_distributed_result_with_execution(selection, execution)
            .map(SelectSeriesExecutionResult::into_series)
    }

    fn select_series_distributed_result_with_execution(
        &self,
        selection: &SeriesSelection,
        execution: &QueryExecution,
    ) -> TsinkResult<SelectSeriesExecutionResult> {
        let response = self
            .read_bridge
            .block_on(
                self.read_fanout
                    .select_series_with_ring_version_detailed_accounted_with_execution(
                        &self.local_storage,
                        &self.rpc_client,
                        selection,
                        self.ring_version,
                        execution,
                    ),
            )
            .map_err(map_read_fanout_error)?;
        execution.checkpoint().map_err(TsinkError::from)?;
        self.record_metadata(&response.metadata, Some(execution))?;
        let mut accounted = response.value;
        let reservation = accounted.take_reservation().ok_or_else(|| {
            TsinkError::Other(
                "distributed select_series omitted its retained-result reservation".to_string(),
            )
        })?;
        Ok(SelectSeriesExecutionResult::accounted(
            std::mem::take(&mut accounted.series),
            reservation,
        ))
    }

    fn list_metrics_distributed(&self) -> TsinkResult<Vec<MetricSeries>> {
        self.finish_fanout(
            self.read_bridge
                .block_on(self.read_fanout.list_metrics_with_ring_version_detailed(
                    &self.local_storage,
                    &self.rpc_client,
                    self.ring_version,
                ))
                .map_err(map_read_fanout_error),
        )
    }

    fn list_metrics_distributed_result_with_execution(
        &self,
        execution: &QueryExecution,
    ) -> TsinkResult<SelectSeriesExecutionResult> {
        let response = self
            .read_bridge
            .block_on(
                self.read_fanout
                    .list_metrics_with_ring_version_detailed_accounted_with_execution(
                        &self.local_storage,
                        &self.rpc_client,
                        self.ring_version,
                        execution,
                    ),
            )
            .map_err(map_read_fanout_error)?;
        execution.checkpoint().map_err(TsinkError::from)?;
        self.record_metadata(&response.metadata, Some(execution))?;
        let mut accounted = response.value;
        let reservation = accounted.take_reservation().ok_or_else(|| {
            TsinkError::Other(
                "distributed list_metrics omitted its retained-result reservation".to_string(),
            )
        })?;
        Ok(SelectSeriesExecutionResult::accounted(
            std::mem::take(&mut accounted.series),
            reservation,
        ))
    }

    fn select_points_distributed(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
    ) -> TsinkResult<Vec<super::query::SeriesPoints>> {
        self.finish_fanout(
            self.read_bridge
                .block_on(
                    self.read_fanout
                        .select_points_for_series_with_ring_version_detailed(
                            &self.local_storage,
                            &self.rpc_client,
                            series,
                            start,
                            end,
                            self.ring_version,
                        ),
                )
                .map_err(map_read_fanout_error),
        )
    }

    fn select_points_distributed_with_execution(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        execution: &QueryExecution,
    ) -> TsinkResult<Vec<SeriesPoints>> {
        self.select_points_distributed_result_with_execution(series, start, end, execution)
            .map(SelectManyExecutionResult::into_series)
    }

    fn select_points_distributed_result_with_execution(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        execution: &QueryExecution,
    ) -> TsinkResult<SelectManyExecutionResult> {
        let response = self
            .read_bridge
            .block_on(
                self.read_fanout
                    .select_points_for_series_with_ring_version_detailed_accounted_with_execution(
                        &self.local_storage,
                        &self.rpc_client,
                        series,
                        start,
                        end,
                        self.ring_version,
                        execution,
                    ),
            )
            .map_err(map_read_fanout_error)?;
        execution.checkpoint().map_err(TsinkError::from)?;
        self.record_metadata(&response.metadata, Some(execution))?;
        let mut accounted = response.value;
        let reservation = accounted.take_reservation().ok_or_else(|| {
            TsinkError::Other(
                "distributed select_batch omitted its retained-result reservation".to_string(),
            )
        })?;
        Ok(SelectManyExecutionResult::accounted(
            std::mem::take(&mut accounted.series),
            std::mem::take(&mut accounted.matched),
            reservation,
        ))
    }

    fn cache_read<K, V>(
        &self,
        enabled: bool,
        map: impl FnOnce(&DistributedStorageCache) -> &HashMap<K, V>,
        key: &K,
        on_hit: impl FnOnce(&mut DistributedStorageCacheSnapshot),
    ) -> TsinkResult<Option<V>>
    where
        K: Eq + Hash,
        V: Clone,
    {
        if !enabled {
            return Ok(None);
        }
        let mut cache = self.lock_cache()?;
        if let Some(value) = map(&cache).get(key).cloned() {
            on_hit(&mut cache.snapshot);
            return Ok(Some(value));
        }
        Ok(None)
    }

    fn cache_write<K, V>(
        &self,
        enabled: bool,
        map: impl FnOnce(&mut DistributedStorageCache) -> &mut HashMap<K, V>,
        key: K,
        value: V,
        on_miss: impl FnOnce(&mut DistributedStorageCacheSnapshot),
    ) -> TsinkResult<()>
    where
        K: Eq + Hash,
    {
        if !enabled {
            return Ok(());
        }
        let mut cache = self.lock_cache()?;
        on_miss(&mut cache.snapshot);
        map(&mut cache).insert(key, value);
        Ok(())
    }

    fn invalidate_cache(&self) -> TsinkResult<()> {
        let mut cache = self.lock_cache()?;
        cache.select_series.clear();
        cache.select_points.clear();
        cache.select_all.clear();
        Ok(())
    }
}

impl Storage for DistributedStorageAdapter {
    fn select_many_execution_accounting(&self) -> QueryExecutionAccounting {
        QueryExecutionAccounting::Complete
    }

    fn select_series_execution_accounting(&self) -> QueryExecutionAccounting {
        QueryExecutionAccounting::Complete
    }

    fn query_budget(&self) -> Option<QueryBudget> {
        self.local_storage.query_budget()
    }

    fn insert_rows(&self, rows: &[Row]) -> TsinkResult<()> {
        self.local_storage.insert_rows(rows)
    }

    fn select(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
    ) -> TsinkResult<Vec<DataPoint>> {
        let key = SelectPointsCacheKey::new(metric, labels, start, end);
        if let Some(points) = self.cache_read(
            self.cache_config.cache_select_points,
            |cache| &cache.select_points,
            &key,
            |snapshot| snapshot.select_points_hits = snapshot.select_points_hits.saturating_add(1),
        )? {
            return Ok(points);
        }

        let series = MetricSeries {
            name: metric.to_string(),
            labels: labels.to_vec(),
        };
        let mut points = self
            .select_points_distributed(std::slice::from_ref(&series), start, end)?
            .into_iter()
            .next()
            .map(|item| item.points)
            .unwrap_or_default();
        points.sort_by_key(|point| point.timestamp);

        self.cache_write(
            self.cache_config.cache_select_points,
            |cache| &mut cache.select_points,
            key,
            points.clone(),
            |snapshot| {
                snapshot.select_points_misses = snapshot.select_points_misses.saturating_add(1)
            },
        )?;

        Ok(points)
    }

    fn select_with_execution(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        execution: &QueryExecution,
    ) -> TsinkResult<Vec<DataPoint>> {
        execution.checkpoint().map_err(TsinkError::from)?;
        let series = MetricSeries {
            name: metric.to_string(),
            labels: labels.to_vec(),
        };
        let mut selected = self.select_points_distributed_with_execution(
            std::slice::from_ref(&series),
            start,
            end,
            execution,
        )?;
        let mut reservation = execution
            .reserve_memory(modeled_series_points_vec_retained_bytes(&selected))
            .map_err(TsinkError::from)?;
        let mut points = selected.pop().map(|item| item.points).unwrap_or_default();
        points.sort_by_key(|point| point.timestamp);
        drop(selected);
        reservation
            .resize(modeled_points_vec_retained_bytes(&points))
            .map_err(TsinkError::from)?;
        Ok(points)
    }

    fn select_many_with_execution(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        execution: &QueryExecution,
    ) -> TsinkResult<Vec<SeriesPoints>> {
        execution.checkpoint().map_err(TsinkError::from)?;
        self.select_points_distributed_with_execution(series, start, end, execution)
    }

    fn select_many_with_execution_result(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        execution: &QueryExecution,
    ) -> TsinkResult<SelectManyExecutionResult> {
        execution.checkpoint().map_err(TsinkError::from)?;
        self.select_points_distributed_result_with_execution(series, start, end, execution)
    }

    fn select_with_options(
        &self,
        _metric: &str,
        _opts: QueryOptions,
    ) -> TsinkResult<Vec<DataPoint>> {
        Err(TsinkError::InvalidConfiguration(
            "select_with_options is not supported by distributed PromQL storage adapter"
                .to_string(),
        ))
    }

    fn select_with_options_with_execution(
        &self,
        _metric: &str,
        _opts: QueryOptions,
        execution: &QueryExecution,
    ) -> TsinkResult<Vec<DataPoint>> {
        execution.checkpoint().map_err(TsinkError::from)?;
        Err(TsinkError::InvalidConfiguration(
            "select_with_options is not supported by distributed PromQL storage adapter"
                .to_string(),
        ))
    }

    fn select_all(
        &self,
        metric: &str,
        start: i64,
        end: i64,
    ) -> TsinkResult<Vec<(Vec<Label>, Vec<DataPoint>)>> {
        let key = SelectAllCacheKey {
            metric: metric.to_string(),
            start,
            end,
        };
        if let Some(rows) = self.cache_read(
            self.cache_config.cache_select_all,
            |cache| &cache.select_all,
            &key,
            |snapshot| snapshot.select_all_hits = snapshot.select_all_hits.saturating_add(1),
        )? {
            return Ok(rows);
        }

        let selection = SeriesSelection::new()
            .with_metric(metric.to_string())
            .with_time_range(start, end);
        let series = self.select_series(&selection)?;
        if series.is_empty() {
            self.cache_write(
                self.cache_config.cache_select_all,
                |cache| &mut cache.select_all,
                key,
                Vec::new(),
                |snapshot| {
                    snapshot.select_all_misses = snapshot.select_all_misses.saturating_add(1)
                },
            )?;
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for item in self.select_points_distributed(&series, start, end)? {
            if item.points.is_empty() {
                continue;
            }
            out.push((item.series.labels, item.points));
        }
        out.sort_by(|left, right| left.0.cmp(&right.0));

        self.cache_write(
            self.cache_config.cache_select_all,
            |cache| &mut cache.select_all,
            key,
            out.clone(),
            |snapshot| snapshot.select_all_misses = snapshot.select_all_misses.saturating_add(1),
        )?;
        Ok(out)
    }

    fn select_all_with_execution(
        &self,
        metric: &str,
        start: i64,
        end: i64,
        execution: &QueryExecution,
    ) -> TsinkResult<Vec<(Vec<Label>, Vec<DataPoint>)>> {
        execution.checkpoint().map_err(TsinkError::from)?;
        let selection = SeriesSelection::new()
            .with_metric(metric.to_string())
            .with_time_range(start, end);
        let series = self.select_series_distributed_with_execution(&selection, execution)?;
        let _series_reservation = execution
            .reserve_memory(modeled_metric_series_vec_retained_bytes(&series))
            .map_err(TsinkError::from)?;
        if series.is_empty() {
            return Ok(Vec::new());
        }

        let selected =
            self.select_points_distributed_with_execution(&series, start, end, execution)?;
        let _points_reservation = execution
            .reserve_memory(modeled_series_points_vec_retained_bytes(&selected))
            .map_err(TsinkError::from)?;
        let _out_slots_reservation = execution
            .reserve_memory(modeled_select_all_output_slots(selected.len()))
            .map_err(TsinkError::from)?;
        let mut out = Vec::with_capacity(selected.len());
        for item in selected {
            if item.points.is_empty() {
                continue;
            }
            out.push((item.series.labels, item.points));
        }
        out.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(out)
    }

    fn list_metrics(&self) -> TsinkResult<Vec<MetricSeries>> {
        let Some(execution) =
            self.begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())?
        else {
            // A third-party compatibility backend may not expose a query budget. Preserve that
            // legacy behavior explicitly; finite built-in profiles always take the accounted path.
            return self.list_metrics_distributed();
        };
        let result = self
            .list_metrics_distributed_result_with_execution(&execution)
            .map(SelectSeriesExecutionResult::into_series);
        // This compatibility call owns the execution and cannot return fanout metadata. Move the
        // metadata guard out on every result path so warning/partial-response memory cannot keep
        // the self-admitted query lease alive. Execution-aware callers retain the separate take
        // contract because their response adapters consume the warnings.
        let accounted_metadata = self.take_accounted_read_metadata();
        match (result, accounted_metadata) {
            (Ok(series), Ok(metadata)) => {
                drop(metadata);
                Ok(series)
            }
            (Err(error), _) => Err(error),
            (Ok(_), Err(error)) => Err(error),
        }
    }

    fn list_metrics_with_execution(
        &self,
        execution: &QueryExecution,
    ) -> TsinkResult<Vec<MetricSeries>> {
        self.list_metrics_with_execution_result(execution)
            .map(SelectSeriesExecutionResult::into_series)
    }

    fn list_metrics_with_execution_result(
        &self,
        execution: &QueryExecution,
    ) -> TsinkResult<SelectSeriesExecutionResult> {
        execution.checkpoint().map_err(TsinkError::from)?;
        self.list_metrics_distributed_result_with_execution(execution)
    }

    fn list_metrics_execution_accounting(&self) -> QueryExecutionAccounting {
        QueryExecutionAccounting::Complete
    }

    fn list_metrics_with_wal(&self) -> TsinkResult<Vec<MetricSeries>> {
        self.list_metrics()
    }

    fn list_metrics_with_wal_with_execution(
        &self,
        execution: &QueryExecution,
    ) -> TsinkResult<Vec<MetricSeries>> {
        self.list_metrics_with_wal_with_execution_result(execution)
            .map(SelectSeriesExecutionResult::into_series)
    }

    fn list_metrics_with_wal_with_execution_result(
        &self,
        execution: &QueryExecution,
    ) -> TsinkResult<SelectSeriesExecutionResult> {
        execution.checkpoint().map_err(TsinkError::from)?;
        self.list_metrics_distributed_result_with_execution(execution)
    }

    fn list_metrics_with_wal_execution_accounting(&self) -> QueryExecutionAccounting {
        QueryExecutionAccounting::Complete
    }

    fn select_series(&self, selection: &SeriesSelection) -> TsinkResult<Vec<MetricSeries>> {
        selection.validate().map_err(TsinkError::from)?;
        let key = SeriesSelectionCacheKey::from_selection(selection);
        if let Some(series) = self.cache_read(
            self.cache_config.cache_select_series,
            |cache| &cache.select_series,
            &key,
            |snapshot| snapshot.select_series_hits = snapshot.select_series_hits.saturating_add(1),
        )? {
            return Ok(series);
        }

        let series = self.select_series_distributed(selection)?;
        self.cache_write(
            self.cache_config.cache_select_series,
            |cache| &mut cache.select_series,
            key,
            series.clone(),
            |snapshot| {
                snapshot.select_series_misses = snapshot.select_series_misses.saturating_add(1)
            },
        )?;
        Ok(series)
    }

    fn select_series_with_execution(
        &self,
        selection: &SeriesSelection,
        execution: &QueryExecution,
    ) -> TsinkResult<Vec<MetricSeries>> {
        self.select_series_with_execution_result(selection, execution)
            .map(SelectSeriesExecutionResult::into_series)
    }

    fn select_series_with_execution_result(
        &self,
        selection: &SeriesSelection,
        execution: &QueryExecution,
    ) -> TsinkResult<SelectSeriesExecutionResult> {
        execution.checkpoint().map_err(TsinkError::from)?;
        selection.validate_shape().map_err(TsinkError::from)?;
        self.select_series_distributed_result_with_execution(selection, execution)
    }

    fn delete_series(&self, selection: &SeriesSelection) -> TsinkResult<DeleteSeriesResult> {
        let result = self.local_storage.delete_series(selection)?;
        self.invalidate_cache()?;
        Ok(result)
    }

    fn memory_used(&self) -> usize {
        self.local_storage.memory_used()
    }

    fn memory_budget(&self) -> usize {
        self.local_storage.memory_budget()
    }

    fn effective_storage_limits(&self) -> EffectiveStorageLimits {
        self.local_storage.effective_storage_limits()
    }

    fn resource_configuration_snapshot(&self) -> tsink::ResourceConfigurationSnapshot {
        self.local_storage.resource_configuration_snapshot()
    }

    fn observability_snapshot(&self) -> StorageObservabilitySnapshot {
        self.local_storage.observability_snapshot()
    }

    fn apply_rollup_policies(
        &self,
        policies: Vec<tsink::RollupPolicy>,
    ) -> TsinkResult<tsink::RollupObservabilitySnapshot> {
        self.local_storage.apply_rollup_policies(policies)
    }

    fn trigger_rollup_run(&self) -> TsinkResult<tsink::RollupObservabilitySnapshot> {
        self.local_storage.trigger_rollup_run()
    }

    fn snapshot(&self, destination: &Path) -> TsinkResult<()> {
        self.local_storage.snapshot(destination)
    }

    fn close(&self) -> TsinkResult<()> {
        self.local_storage.close()
    }
}

fn map_read_fanout_error(err: ReadFanoutError) -> TsinkError {
    match err {
        ReadFanoutError::QueryBudget { error } => TsinkError::QueryBudget(error),
        ReadFanoutError::InvalidRequest { message }
        | ReadFanoutError::MergeLimitExceeded { message } => {
            TsinkError::InvalidConfiguration(message)
        }
        other => TsinkError::Other(format!("distributed read fanout failed: {other}")),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SelectAllCacheKey {
    metric: String,
    start: i64,
    end: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SelectPointsCacheKey {
    metric: String,
    labels: Vec<Label>,
    start: i64,
    end: i64,
}

impl SelectPointsCacheKey {
    fn new(metric: &str, labels: &[Label], start: i64, end: i64) -> Self {
        let mut labels = labels.to_vec();
        labels.sort();
        Self {
            metric: metric.to_string(),
            labels,
            start,
            end,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SeriesSelectionCacheKey {
    metric: Option<String>,
    matchers: Vec<SeriesMatcherCacheKey>,
    start: Option<i64>,
    end: Option<i64>,
}

impl SeriesSelectionCacheKey {
    fn from_selection(selection: &SeriesSelection) -> Self {
        let mut matchers = selection
            .matchers
            .iter()
            .map(SeriesMatcherCacheKey::from_matcher)
            .collect::<Vec<_>>();
        matchers.sort_by(|left, right| {
            left.name
                .cmp(&right.name)
                .then(left.op.cmp(&right.op))
                .then(left.value.cmp(&right.value))
        });
        Self {
            metric: selection.metric.clone(),
            matchers,
            start: selection.start,
            end: selection.end,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SeriesMatcherCacheKey {
    name: String,
    op: u8,
    value: String,
}

impl SeriesMatcherCacheKey {
    fn from_matcher(matcher: &SeriesMatcher) -> Self {
        Self {
            name: matcher.name.clone(),
            op: matcher_op_code(matcher.op),
            value: matcher.value.clone(),
        }
    }
}

fn matcher_op_code(op: SeriesMatcherOp) -> u8 {
    match op {
        SeriesMatcherOp::Equal => 0,
        SeriesMatcherOp::NotEqual => 1,
        SeriesMatcherOp::RegexMatch => 2,
        SeriesMatcherOp::RegexNoMatch => 3,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::config::{ClusterConfig, DEFAULT_CLUSTER_SHARDS};
    use crate::cluster::{ClusterRequestContext, ClusterRuntime};
    use tsink::{
        QueryBudget, QueryBudgetError, QueryBudgetLimits, QueryCancellationToken, QueryLimitReason,
        QueryWorkLimits, StorageBuilder, TimestampPrecision,
    };

    struct CompatibilityStorage {
        inner: Arc<dyn Storage>,
        select_series_entered: Option<Arc<std::sync::Barrier>>,
        select_series_release: Option<Arc<std::sync::Barrier>>,
    }

    impl Storage for CompatibilityStorage {
        fn insert_rows(&self, rows: &[Row]) -> TsinkResult<()> {
            self.inner.insert_rows(rows)
        }

        fn select(
            &self,
            metric: &str,
            labels: &[Label],
            start: i64,
            end: i64,
        ) -> TsinkResult<Vec<DataPoint>> {
            self.inner.select(metric, labels, start, end)
        }

        fn select_with_options(
            &self,
            metric: &str,
            opts: QueryOptions,
        ) -> TsinkResult<Vec<DataPoint>> {
            self.inner.select_with_options(metric, opts)
        }

        fn select_all(
            &self,
            metric: &str,
            start: i64,
            end: i64,
        ) -> TsinkResult<Vec<(Vec<Label>, Vec<DataPoint>)>> {
            self.inner.select_all(metric, start, end)
        }

        fn select_series_in_shards(
            &self,
            selection: &SeriesSelection,
            scope: &tsink::MetadataShardScope,
        ) -> TsinkResult<Vec<MetricSeries>> {
            if let Some(entered) = &self.select_series_entered {
                entered.wait();
            }
            if let Some(release) = &self.select_series_release {
                release.wait();
            }
            self.inner.select_series_in_shards(selection, scope)
        }

        fn close(&self) -> TsinkResult<()> {
            self.inner.close()
        }
    }

    fn make_cluster_context() -> ClusterRequestContext {
        let cfg = ClusterConfig {
            enabled: true,
            node_id: Some("node-a".to_string()),
            bind: Some("127.0.0.1:9301".to_string()),
            seeds: Vec::new(),
            internal_auth_token: Some("cluster-test-token".to_string()),
            ..ClusterConfig::default()
        };
        let runtime = ClusterRuntime::bootstrap(&cfg)
            .expect("cluster runtime should bootstrap")
            .expect("cluster runtime should exist");
        ClusterRequestContext::from_runtime(runtime).expect("cluster context should build")
    }

    fn make_bounded_adapter_with_query_limits(
        rows: &[Row],
        query_limits: QueryBudgetLimits,
    ) -> (Arc<dyn Storage>, Arc<DistributedStorageAdapter>) {
        let storage: Arc<dyn Storage> = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(DEFAULT_CLUSTER_SHARDS)
            .with_query_budget_limits(query_limits)
            .build()
            .expect("storage should build");
        assert_eq!(
            storage.select_many_execution_accounting(),
            QueryExecutionAccounting::Complete
        );
        assert_eq!(
            storage.select_series_execution_accounting(),
            QueryExecutionAccounting::Complete
        );
        assert_eq!(
            storage.select_series_in_shards_execution_accounting(),
            QueryExecutionAccounting::Complete
        );
        storage.insert_rows(rows).expect("insert should succeed");

        let context = make_cluster_context();
        let adapter = Arc::new(DistributedStorageAdapter::new(
            Arc::clone(&storage),
            context.rpc_client,
            context.read_fanout,
            1,
            DistributedPromqlReadBridge::from_current_runtime(),
        ));
        assert_eq!(
            adapter.select_series_execution_accounting(),
            QueryExecutionAccounting::Complete
        );
        (storage, adapter)
    }

    fn make_bounded_adapter(rows: &[Row]) -> (Arc<dyn Storage>, Arc<DistributedStorageAdapter>) {
        make_bounded_adapter_with_query_limits(
            rows,
            QueryBudgetLimits {
                max_concurrent_queries: Some(1),
                max_shared_memory_bytes: Some(32 * 1024 * 1024),
                per_query: QueryWorkLimits {
                    max_memory_bytes: Some(32 * 1024 * 1024),
                    ..QueryWorkLimits::default()
                },
            },
        )
    }

    fn assert_query_limit(error: TsinkError, expected: QueryLimitReason) {
        match error {
            TsinkError::QueryBudget(QueryBudgetError::LimitExceeded(exceeded)) => {
                assert_eq!(exceeded.reason, expected);
            }
            other => panic!("expected {expected} query limit, got {other}"),
        }
    }

    #[tokio::test]
    async fn distributed_metric_row_scan_accounting_remains_unaccounted() {
        let (storage, adapter) = make_bounded_adapter(&[]);
        assert_eq!(
            storage.scan_metric_rows_execution_accounting(),
            QueryExecutionAccounting::Complete,
            "the built-in local storage fixture should expose complete metric-row accounting",
        );
        assert_eq!(
            adapter.scan_metric_rows_execution_accounting(),
            QueryExecutionAccounting::Unaccounted,
            "distributed metric-row scans must fail closed until the adapter has a bounded complete implementation",
        );
    }

    #[tokio::test]
    async fn distributed_series_row_scan_accounting_remains_unaccounted() {
        let (storage, adapter) = make_bounded_adapter(&[]);
        assert_eq!(
            storage.scan_series_rows_execution_accounting(),
            QueryExecutionAccounting::Complete,
            "the built-in local storage fixture should expose complete series-row accounting",
        );
        assert_eq!(
            adapter.scan_series_rows_execution_accounting(),
            QueryExecutionAccounting::Unaccounted,
            "distributed series-row scans must fail closed until fanout has a paginated row RPC",
        );
    }

    fn distributed_list_metrics_rows() -> [Row; 2] {
        [
            Row::with_labels(
                "bounded_distributed_list",
                vec![Label::new("host", "a")],
                DataPoint::new(1_700_000_000_000, 1.0),
            ),
            Row::with_labels(
                "bounded_distributed_list",
                vec![Label::new("host", "b")],
                DataPoint::new(1_700_000_000_000, 2.0),
            ),
        ]
    }

    fn distributed_list_metrics_limits(max_series_matched: u64) -> QueryBudgetLimits {
        QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(32 * 1024 * 1024),
            per_query: QueryWorkLimits {
                max_series_matched: Some(max_series_matched),
                max_memory_bytes: Some(32 * 1024 * 1024),
                ..QueryWorkLimits::default()
            },
        }
    }

    #[tokio::test]
    async fn distributed_list_metrics_detailed_result_retains_complete_guard() {
        let (storage, adapter) = make_bounded_adapter(&distributed_list_metrics_rows());
        assert_eq!(
            adapter.list_metrics_execution_accounting(),
            QueryExecutionAccounting::Complete
        );
        let execution = storage
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
            .expect("query admission should succeed")
            .expect("built-in storage should expose a query budget");
        let adapter_for_call = Arc::clone(&adapter);
        let execution_for_call = execution.clone();
        let result = tokio::task::spawn_blocking(move || {
            adapter_for_call.list_metrics_with_execution_result(&execution_for_call)
        })
        .await
        .expect("distributed list task should join")
        .expect("distributed metric listing should succeed");
        assert_eq!(result.series.len(), 2);
        assert!(result.reserved_memory_bytes() > 0);
        assert_eq!(
            execution.snapshot().memory_reserved_bytes,
            result.reserved_memory_bytes()
        );
        drop(result);
        drop(
            adapter
                .take_accounted_read_metadata()
                .expect("distributed read metadata should remain accounted"),
        );
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }

    #[tokio::test]
    async fn direct_list_metrics_uses_one_execution_at_exact_series_limit() {
        let (storage, adapter) = make_bounded_adapter_with_query_limits(
            &distributed_list_metrics_rows(),
            distributed_list_metrics_limits(2),
        );

        let adapter_for_call = Arc::clone(&adapter);
        let series = tokio::task::spawn_blocking(move || adapter_for_call.list_metrics())
            .await
            .expect("direct list_metrics task should join")
            .expect("the exact series limit should admit");
        assert_eq!(series.len(), 2);

        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.queries_started_total, 1);
        assert_eq!(snapshot.queries_completed_total, 1);
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.peak_active_queries, 1);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }

    #[tokio::test]
    async fn direct_list_metrics_discards_warning_metadata_and_releases_its_query_lease() {
        let (storage, adapter) = make_bounded_adapter(&distributed_list_metrics_rows());
        let warning_metadata = ReadFanoutResponseMetadata {
            consistency: adapter.read_fanout.read_consistency_mode(),
            partial_response_policy: adapter.read_fanout.read_partial_response_policy(),
            partial_response: true,
            warnings: vec!["deterministic direct-list warning".to_string()],
        };
        adapter
            .record_metadata(&warning_metadata, None)
            .expect("warning fixture should be recorded");

        let adapter_for_first = Arc::clone(&adapter);
        let first = tokio::task::spawn_blocking(move || adapter_for_first.list_metrics())
            .await
            .expect("first direct list_metrics task should join")
            .expect("first direct list_metrics call should succeed");
        assert_eq!(first.len(), 2);
        assert!(adapter.read_metadata_snapshot().warnings.is_empty());

        let after_first = storage.query_budget_snapshot();
        assert_eq!(after_first.queries_started_total, 1);
        assert_eq!(after_first.queries_completed_total, 1);
        assert_eq!(after_first.active_queries, 0);
        assert_eq!(after_first.shared_reserved_memory_bytes, 0);
        assert_eq!(after_first.accounting_invariant_violations_total, 0);

        let adapter_for_second = Arc::clone(&adapter);
        let second = tokio::task::spawn_blocking(move || adapter_for_second.list_metrics())
            .await
            .expect("second direct list_metrics task should join")
            .expect("the released one-query envelope must admit the next call");
        assert_eq!(second.len(), 2);

        let after_second = storage.query_budget_snapshot();
        assert_eq!(after_second.queries_started_total, 2);
        assert_eq!(after_second.queries_completed_total, 2);
        assert_eq!(after_second.active_queries, 0);
        assert_eq!(after_second.peak_active_queries, 1);
        assert_eq!(after_second.shared_reserved_memory_bytes, 0);
        assert_eq!(after_second.accounting_invariant_violations_total, 0);
    }

    #[tokio::test]
    async fn execution_aware_wal_list_reuses_the_distributed_query_envelope() {
        let (storage, adapter) = make_bounded_adapter_with_query_limits(
            &distributed_list_metrics_rows(),
            distributed_list_metrics_limits(2),
        );
        let execution = storage
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
            .expect("query should admit")
            .expect("built-in storage should expose its query budget");
        let adapter_for_call = Arc::clone(&adapter);
        let execution_for_call = execution.clone();
        let result = tokio::task::spawn_blocking(move || {
            adapter_for_call.list_metrics_with_wal_with_execution_result(&execution_for_call)
        })
        .await
        .expect("execution-aware WAL list task should join")
        .expect("the supplied envelope must be reused");
        assert_eq!(result.series.len(), 2);
        assert!(result.reserved_memory_bytes() > 0);
        assert_eq!(
            execution.snapshot().memory_reserved_bytes,
            result.reserved_memory_bytes(),
        );
        drop(result);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);

        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.queries_started_total, 1);
        assert_eq!(snapshot.queries_completed_total, 1);
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }

    #[tokio::test]
    async fn execution_aware_list_leaves_warning_guard_for_the_accounted_consumer() {
        let (storage, adapter) = make_bounded_adapter(&distributed_list_metrics_rows());
        let warning_metadata = ReadFanoutResponseMetadata {
            consistency: adapter.read_fanout.read_consistency_mode(),
            partial_response_policy: adapter.read_fanout.read_partial_response_policy(),
            partial_response: true,
            warnings: vec!["accounted server-path warning".to_string()],
        };
        adapter
            .record_metadata(&warning_metadata, None)
            .expect("warning fixture should be recorded");
        let execution = storage
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
            .expect("query should admit")
            .expect("built-in storage should expose its query budget");

        let adapter_for_call = Arc::clone(&adapter);
        let execution_for_call = execution.clone();
        let result = tokio::task::spawn_blocking(move || {
            adapter_for_call.list_metrics_with_wal_with_execution_result(&execution_for_call)
        })
        .await
        .expect("execution-aware list task should join")
        .expect("execution-aware list should preserve accounted metadata");
        let metadata = adapter
            .take_accounted_read_metadata()
            .expect("the server consumer should receive the warning guard");
        assert_eq!(metadata.metadata.warnings, warning_metadata.warnings);
        assert!(result.reserved_memory_bytes() > 0);
        assert!(metadata.reserved_memory_bytes() > 0);
        assert_eq!(
            execution.snapshot().memory_reserved_bytes,
            result
                .reserved_memory_bytes()
                .saturating_add(metadata.reserved_memory_bytes()),
        );

        let metadata_bytes = metadata.reserved_memory_bytes();
        drop(result);
        assert_eq!(execution.snapshot().memory_reserved_bytes, metadata_bytes);
        drop(metadata);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);

        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }

    #[tokio::test]
    async fn direct_list_metrics_returns_structured_one_under_series_rejection() {
        let (storage, adapter) = make_bounded_adapter_with_query_limits(
            &distributed_list_metrics_rows(),
            distributed_list_metrics_limits(1),
        );

        let adapter_for_call = Arc::clone(&adapter);
        let error = tokio::task::spawn_blocking(move || adapter_for_call.list_metrics())
            .await
            .expect("direct list_metrics task should join")
            .expect_err("one under the required series limit should reject");
        assert_query_limit(error, QueryLimitReason::SeriesMatched);

        let snapshot = storage.query_budget_snapshot();
        assert_eq!(snapshot.queries_started_total, 1);
        assert_eq!(snapshot.queries_completed_total, 1);
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.peak_active_queries, 1);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
        assert!(adapter.read_metadata_snapshot().warnings.is_empty());
    }

    #[tokio::test]
    async fn direct_list_metrics_rejects_while_the_only_query_permit_is_held() {
        let (storage, adapter) = make_bounded_adapter(&distributed_list_metrics_rows());
        let held = storage
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
            .expect("held query should admit")
            .expect("built-in storage should expose its query budget");

        let adapter_for_call = Arc::clone(&adapter);
        let error = tokio::task::spawn_blocking(move || adapter_for_call.list_metrics())
            .await
            .expect("direct list_metrics task should join")
            .expect_err("the occupied concurrency envelope should reject");
        assert_query_limit(error, QueryLimitReason::ConcurrentQueries);

        let while_held = storage.query_budget_snapshot();
        assert_eq!(while_held.queries_started_total, 1);
        assert_eq!(while_held.queries_completed_total, 0);
        assert_eq!(while_held.active_queries, 1);
        assert_eq!(while_held.peak_active_queries, 1);
        assert_eq!(while_held.shared_reserved_memory_bytes, 0);
        drop(held);

        let released = storage.query_budget_snapshot();
        assert_eq!(released.queries_started_total, 1);
        assert_eq!(released.queries_completed_total, 1);
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.accounting_invariant_violations_total, 0);
    }

    #[tokio::test]
    async fn select_all_uses_cache_on_repeat_queries() {
        let storage = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(DEFAULT_CLUSTER_SHARDS)
            .build()
            .expect("storage should build");
        storage
            .insert_rows(&[
                Row::with_labels(
                    "up",
                    vec![Label::new("job", "prom")],
                    DataPoint::new(1_700_000_000_000, 1.0),
                ),
                Row::with_labels(
                    "up",
                    vec![Label::new("job", "prom")],
                    DataPoint::new(1_700_000_001_000, 2.0),
                ),
            ])
            .expect("insert should succeed");

        let context = make_cluster_context();
        let adapter = Arc::new(DistributedStorageAdapter::new(
            Arc::clone(&storage),
            context.rpc_client.clone(),
            context.read_fanout.clone(),
            1,
            DistributedPromqlReadBridge::from_current_runtime(),
        ));

        let adapter_for_first = Arc::clone(&adapter);
        let first = tokio::task::spawn_blocking(move || {
            adapter_for_first.select_all("up", 1_700_000_000_000, 1_700_000_002_000)
        })
        .await
        .expect("first select_all task should join")
        .expect("first select_all call should succeed");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].1.len(), 2);

        let adapter_for_second = Arc::clone(&adapter);
        let second = tokio::task::spawn_blocking(move || {
            adapter_for_second.select_all("up", 1_700_000_000_000, 1_700_000_002_000)
        })
        .await
        .expect("second select_all task should join")
        .expect("second select_all call should succeed");
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].1.len(), 2);

        let snapshot = adapter.cache_snapshot();
        assert_eq!(snapshot.select_all_misses, 1);
        assert_eq!(snapshot.select_all_hits, 1);
    }

    #[tokio::test]
    async fn cache_config_can_disable_select_points_cache() {
        let storage = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(DEFAULT_CLUSTER_SHARDS)
            .build()
            .expect("storage should build");
        storage
            .insert_rows(&[Row::with_labels(
                "up",
                vec![Label::new("job", "prom")],
                DataPoint::new(1_700_000_000_000, 1.0),
            )])
            .expect("insert should succeed");

        let context = make_cluster_context();
        let adapter = Arc::new(
            DistributedStorageAdapter::new(
                Arc::clone(&storage),
                context.rpc_client.clone(),
                context.read_fanout.clone(),
                1,
                DistributedPromqlReadBridge::from_current_runtime(),
            )
            .with_cache_config(DistributedStorageCacheConfig {
                cache_select_series: true,
                cache_select_points: false,
                cache_select_all: true,
            }),
        );

        let labels = vec![Label::new("job", "prom")];
        let adapter_for_first = Arc::clone(&adapter);
        let first = tokio::task::spawn_blocking(move || {
            adapter_for_first.select("up", &labels, 1_700_000_000_000, 1_700_000_001_000)
        })
        .await
        .expect("first select task should join")
        .expect("first select call should succeed");
        assert_eq!(first.len(), 1);

        let labels = vec![Label::new("job", "prom")];
        let adapter_for_second = Arc::clone(&adapter);
        let second = tokio::task::spawn_blocking(move || {
            adapter_for_second.select("up", &labels, 1_700_000_000_000, 1_700_000_001_000)
        })
        .await
        .expect("second select task should join")
        .expect("second select call should succeed");
        assert_eq!(second.len(), 1);

        let snapshot = adapter.cache_snapshot();
        assert_eq!(snapshot.select_points_hits, 0);
        assert_eq!(snapshot.select_points_misses, 0);

        let metadata = adapter.read_metadata_snapshot();
        assert_eq!(
            metadata.consistency,
            context.read_fanout.read_consistency_mode()
        );
        assert_eq!(
            metadata.partial_response_policy,
            context.read_fanout.read_partial_response_policy()
        );
    }

    #[tokio::test]
    async fn execution_aware_select_accepts_exact_sample_limit_and_releases_on_one_over() {
        let (storage, adapter) = make_bounded_adapter(&[
            Row::with_labels(
                "bounded_points",
                vec![Label::new("host", "a")],
                DataPoint::new(1_700_000_000_000, 1.0),
            ),
            Row::with_labels(
                "bounded_points",
                vec![Label::new("host", "a")],
                DataPoint::new(1_700_000_001_000, 2.0),
            ),
        ]);

        let exact = adapter
            .begin_query_execution(
                QueryWorkLimits {
                    max_samples_returned: Some(2),
                    ..QueryWorkLimits::default()
                },
                QueryCancellationToken::new(),
            )
            .expect("exact query should admit")
            .expect("adapter should expose its local query budget");
        let exact_for_call = exact.clone();
        let adapter_for_call = Arc::clone(&adapter);
        let points = tokio::task::spawn_blocking(move || {
            adapter_for_call.select_with_execution(
                "bounded_points",
                &[Label::new("host", "a")],
                1_700_000_000_000,
                1_700_000_002_000,
                &exact_for_call,
            )
        })
        .await
        .expect("exact select task should join")
        .expect("exact sample limit should succeed");
        assert_eq!(points.len(), 2);
        assert_eq!(exact.snapshot().samples_returned, 2);
        assert_eq!(exact.snapshot().samples_scanned, 2);
        assert_eq!(exact.snapshot().memory_reserved_bytes, 0);
        let while_exact_is_live = storage.query_budget_snapshot();
        assert_eq!(while_exact_is_live.queries_started_total, 1);
        assert_eq!(while_exact_is_live.active_queries, 1);
        assert_eq!(while_exact_is_live.peak_active_queries, 1);
        assert_eq!(while_exact_is_live.shared_reserved_memory_bytes, 0);
        assert_eq!(
            adapter.cache_snapshot(),
            DistributedStorageCacheSnapshot::default()
        );
        drop(exact);

        let adapter_for_warmup = Arc::clone(&adapter);
        tokio::task::spawn_blocking(move || {
            adapter_for_warmup.select(
                "bounded_points",
                &[Label::new("host", "a")],
                1_700_000_000_000,
                1_700_000_002_000,
            )
        })
        .await
        .expect("cache warmup task should join")
        .expect("compatibility cache warmup should succeed");
        assert_eq!(adapter.cache_snapshot().select_points_misses, 1);

        let one_over = adapter
            .begin_query_execution(
                QueryWorkLimits {
                    max_samples_returned: Some(1),
                    ..QueryWorkLimits::default()
                },
                QueryCancellationToken::new(),
            )
            .expect("one-over query should admit")
            .expect("adapter should expose its local query budget");
        let one_over_for_call = one_over.clone();
        let adapter_for_call = Arc::clone(&adapter);
        let error = tokio::task::spawn_blocking(move || {
            adapter_for_call.select_with_execution(
                "bounded_points",
                &[Label::new("host", "a")],
                1_700_000_000_000,
                1_700_000_002_000,
                &one_over_for_call,
            )
        })
        .await
        .expect("one-over select task should join")
        .expect_err("one-over sample limit should fail");
        assert_query_limit(error, QueryLimitReason::SamplesReturned);
        assert_eq!(one_over.snapshot().memory_reserved_bytes, 0);
        assert_eq!(adapter.cache_snapshot().select_points_hits, 0);
        assert_eq!(
            storage.query_budget_snapshot().shared_reserved_memory_bytes,
            0
        );
        drop(one_over);

        let released = storage.query_budget_snapshot();
        assert_eq!(released.queries_started_total, 3);
        assert_eq!(released.queries_completed_total, 3);
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.accounting_invariant_violations_total, 0);
    }

    #[tokio::test]
    async fn execution_aware_series_selection_enforces_exact_and_one_over_limits() {
        let (storage, adapter) = make_bounded_adapter(&[
            Row::with_labels(
                "bounded_series",
                vec![Label::new("host", "a")],
                DataPoint::new(1_700_000_000_000, 1.0),
            ),
            Row::with_labels(
                "bounded_series",
                vec![Label::new("host", "b")],
                DataPoint::new(1_700_000_000_000, 2.0),
            ),
        ]);
        let selection = SeriesSelection::new().with_metric("bounded_series");

        let exact = adapter
            .begin_query_execution(
                QueryWorkLimits {
                    max_series_matched: Some(2),
                    ..QueryWorkLimits::default()
                },
                QueryCancellationToken::new(),
            )
            .expect("exact query should admit")
            .expect("adapter should expose its local query budget");
        let exact_for_call = exact.clone();
        let adapter_for_call = Arc::clone(&adapter);
        let selection_for_call = selection.clone();
        let selected = tokio::task::spawn_blocking(move || {
            adapter_for_call
                .select_series_with_execution_result(&selection_for_call, &exact_for_call)
        })
        .await
        .expect("exact series task should join")
        .expect("exact series limit should succeed");
        assert_eq!(selected.series.len(), 2);
        assert!(selected.reserved_memory_bytes() > 0);
        assert!(exact.snapshot().memory_reserved_bytes > 0);
        assert_eq!(exact.snapshot().series_matched, 2);
        drop(selected);
        assert_eq!(exact.snapshot().memory_reserved_bytes, 0);
        drop(exact);

        let one_over = adapter
            .begin_query_execution(
                QueryWorkLimits {
                    max_series_matched: Some(1),
                    ..QueryWorkLimits::default()
                },
                QueryCancellationToken::new(),
            )
            .expect("one-over query should admit")
            .expect("adapter should expose its local query budget");
        let one_over_for_call = one_over.clone();
        let adapter_for_call = Arc::clone(&adapter);
        let error = tokio::task::spawn_blocking(move || {
            adapter_for_call.select_series_with_execution(&selection, &one_over_for_call)
        })
        .await
        .expect("one-over series task should join")
        .expect_err("one-over series limit should fail");
        assert_query_limit(error, QueryLimitReason::SeriesMatched);
        assert_eq!(one_over.snapshot().memory_reserved_bytes, 0);
        drop(one_over);

        let released = storage.query_budget_snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.accounting_invariant_violations_total, 0);
        assert_eq!(
            adapter.cache_snapshot(),
            DistributedStorageCacheSnapshot::default()
        );
    }

    #[tokio::test]
    async fn execution_aware_select_enforces_exact_and_one_over_returned_bytes() {
        let (storage, adapter) = make_bounded_adapter(&[Row::with_labels(
            "bounded_bytes",
            vec![Label::new("host", "a")],
            DataPoint::new(1_700_000_000_000, "payload"),
        )]);

        let calibration = adapter
            .begin_query_execution(QueryWorkLimits::default(), QueryCancellationToken::new())
            .expect("calibration query should admit")
            .expect("adapter should expose its local query budget");
        let calibration_for_call = calibration.clone();
        let adapter_for_call = Arc::clone(&adapter);
        tokio::task::spawn_blocking(move || {
            adapter_for_call.select_with_execution(
                "bounded_bytes",
                &[Label::new("host", "a")],
                1_700_000_000_000,
                1_700_000_001_000,
                &calibration_for_call,
            )
        })
        .await
        .expect("calibration task should join")
        .expect("calibration query should succeed");
        let exact_bytes = calibration.snapshot().returned_bytes;
        assert!(exact_bytes > 1);
        drop(calibration);

        let exact = adapter
            .begin_query_execution(
                QueryWorkLimits {
                    max_returned_bytes: Some(exact_bytes),
                    ..QueryWorkLimits::default()
                },
                QueryCancellationToken::new(),
            )
            .expect("exact query should admit")
            .expect("adapter should expose its local query budget");
        let exact_for_call = exact.clone();
        let adapter_for_call = Arc::clone(&adapter);
        tokio::task::spawn_blocking(move || {
            adapter_for_call.select_with_execution(
                "bounded_bytes",
                &[Label::new("host", "a")],
                1_700_000_000_000,
                1_700_000_001_000,
                &exact_for_call,
            )
        })
        .await
        .expect("exact returned-byte task should join")
        .expect("exact returned-byte limit should succeed");
        assert_eq!(exact.snapshot().returned_bytes, exact_bytes);
        drop(exact);

        let one_over = adapter
            .begin_query_execution(
                QueryWorkLimits {
                    max_returned_bytes: Some(exact_bytes - 1),
                    ..QueryWorkLimits::default()
                },
                QueryCancellationToken::new(),
            )
            .expect("one-over query should admit")
            .expect("adapter should expose its local query budget");
        let one_over_for_call = one_over.clone();
        let adapter_for_call = Arc::clone(&adapter);
        let error = tokio::task::spawn_blocking(move || {
            adapter_for_call.select_with_execution(
                "bounded_bytes",
                &[Label::new("host", "a")],
                1_700_000_000_000,
                1_700_000_001_000,
                &one_over_for_call,
            )
        })
        .await
        .expect("one-over returned-byte task should join")
        .expect_err("one-over returned-byte limit should fail");
        assert_query_limit(error, QueryLimitReason::ReturnedBytes);
        assert_eq!(one_over.snapshot().memory_reserved_bytes, 0);
        drop(one_over);

        let released = storage.query_budget_snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.accounting_invariant_violations_total, 0);
    }

    #[tokio::test]
    async fn metadata_compatibility_remains_unbounded_but_bounded_fails_closed() {
        let inner: Arc<dyn Storage> = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(DEFAULT_CLUSTER_SHARDS)
            .build()
            .expect("storage should build");
        inner
            .insert_rows(&[
                Row::with_labels(
                    "compatibility_series",
                    vec![Label::new("host", "a")],
                    DataPoint::new(1_700_000_000_000, 1.0),
                ),
                Row::with_labels(
                    "compatibility_series",
                    vec![Label::new("host", "b")],
                    DataPoint::new(1_700_000_000_000, 2.0),
                ),
            ])
            .expect("insert should succeed");
        let compatibility: Arc<dyn Storage> = Arc::new(CompatibilityStorage {
            inner,
            select_series_entered: None,
            select_series_release: None,
        });
        assert!(compatibility.query_budget().is_none());
        assert_eq!(
            compatibility.select_series_in_shards_execution_accounting(),
            QueryExecutionAccounting::Unaccounted
        );

        let context = make_cluster_context();
        let adapter = Arc::new(DistributedStorageAdapter::new(
            compatibility,
            context.rpc_client,
            context.read_fanout,
            1,
            DistributedPromqlReadBridge::from_current_runtime(),
        ));
        let budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(32 * 1024 * 1024),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(32 * 1024 * 1024),
                ..QueryWorkLimits::default()
            },
        })
        .expect("query budget should build");
        let selection = SeriesSelection::new().with_metric("compatibility_series");

        let adapter_for_call = Arc::clone(&adapter);
        let selection_for_call = selection.clone();
        let selected = tokio::task::spawn_blocking(move || {
            adapter_for_call.select_series(&selection_for_call)
        })
        .await
        .expect("unbounded compatibility task should join")
        .expect("legacy unbounded compatibility should succeed");
        assert_eq!(selected.len(), 2);

        let execution = budget
            .begin_query_with(
                QueryWorkLimits {
                    max_series_matched: Some(2),
                    ..QueryWorkLimits::default()
                },
                QueryCancellationToken::new(),
            )
            .expect("bounded query should admit");
        let execution_for_call = execution.clone();
        let adapter_for_call = Arc::clone(&adapter);
        let error = tokio::task::spawn_blocking(move || {
            adapter_for_call.select_series_with_execution(&selection, &execution_for_call)
        })
        .await
        .expect("bounded compatibility task should join")
        .expect_err("bounded compatibility must fail closed");
        assert!(error
            .to_string()
            .contains("requires complete result accounting"));
        assert_eq!(execution.snapshot().series_matched, 0);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);

        let released = budget.snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.accounting_invariant_violations_total, 0);
    }

    #[tokio::test]
    async fn bounded_metadata_fails_closed_for_unaccounted_compatibility_storage() {
        let inner: Arc<dyn Storage> = StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .with_metadata_shard_count(DEFAULT_CLUSTER_SHARDS)
            .build()
            .expect("storage should build");
        inner
            .insert_rows(&[
                Row::with_labels(
                    "concurrent_compatibility_series",
                    vec![Label::new("host", "a")],
                    DataPoint::new(1_700_000_000_000, 1.0),
                ),
                Row::with_labels(
                    "concurrent_compatibility_series",
                    vec![Label::new("host", "b")],
                    DataPoint::new(1_700_000_000_000, 2.0),
                ),
            ])
            .expect("insert should succeed");
        let compatibility: Arc<dyn Storage> = Arc::new(CompatibilityStorage {
            inner,
            select_series_entered: None,
            select_series_release: None,
        });

        let context = make_cluster_context();
        let adapter = Arc::new(DistributedStorageAdapter::new(
            compatibility,
            context.rpc_client,
            context.read_fanout,
            1,
            DistributedPromqlReadBridge::from_current_runtime(),
        ));
        let budget =
            QueryBudget::new(QueryBudgetLimits::default()).expect("query budget should build");
        let execution = budget
            .begin_query_with(
                QueryWorkLimits {
                    max_series_matched: Some(2),
                    ..QueryWorkLimits::default()
                },
                QueryCancellationToken::new(),
            )
            .expect("query should admit");

        let adapter_for_call = Arc::clone(&adapter);
        let query_execution = execution.clone();
        let error = tokio::task::spawn_blocking(move || {
            adapter_for_call.select_series_with_execution(
                &SeriesSelection::new().with_metric("concurrent_compatibility_series"),
                &query_execution,
            )
        })
        .await
        .expect("compatibility query task should join")
        .expect_err("bounded metadata must reject an unaccounted local backend");
        assert!(
            error
                .to_string()
                .contains("requires complete result accounting"),
            "unexpected compatibility error: {error}"
        );
        assert_eq!(execution.snapshot().series_matched, 0);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);

        let released = budget.snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
        assert_eq!(released.accounting_invariant_violations_total, 0);
    }

    #[tokio::test]
    async fn distributed_metadata_warning_guard_has_exact_boundary_and_survives_snapshot_move() {
        let (storage, adapter) = make_bounded_adapter(&[]);
        let metadata = ReadFanoutResponseMetadata {
            consistency: adapter.read_fanout.read_consistency_mode(),
            partial_response_policy: adapter.read_fanout.read_partial_response_policy(),
            partial_response: true,
            warnings: vec!["bounded peer warning".repeat(8)],
        };
        let exact_bytes = modeled_distributed_metadata_merge_upper_bytes(
            &adapter.read_metadata_snapshot(),
            &metadata,
        );
        assert!(exact_bytes > 1);
        let execution = adapter
            .begin_query_execution(
                QueryWorkLimits {
                    max_memory_bytes: Some(exact_bytes),
                    ..QueryWorkLimits::default()
                },
                QueryCancellationToken::new(),
            )
            .expect("metadata query should admit")
            .expect("adapter should expose its local query budget");

        adapter
            .record_metadata(&metadata, Some(&execution))
            .expect("exact metadata envelope should succeed");
        let accounted = adapter
            .take_accounted_read_metadata()
            .expect("accounted metadata should move out of the adapter");
        assert_eq!(accounted.metadata.warnings, metadata.warnings);
        assert!(accounted.reserved_memory_bytes() > 0);
        assert!(accounted.reserved_memory_bytes() <= exact_bytes);
        assert!(execution.snapshot().memory_reserved_bytes > 0);

        drop(adapter);
        assert!(
            execution.snapshot().memory_reserved_bytes > 0,
            "the moved metadata guard must outlive the adapter"
        );
        drop(accounted);
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        drop(execution);
        let released = storage.query_budget_snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);

        let (one_under_storage, one_under_adapter) = make_bounded_adapter(&[]);
        let one_under_metadata = ReadFanoutResponseMetadata {
            consistency: one_under_adapter.read_fanout.read_consistency_mode(),
            partial_response_policy: one_under_adapter.read_fanout.read_partial_response_policy(),
            partial_response: true,
            warnings: vec!["bounded peer warning".repeat(8)],
        };
        let required = modeled_distributed_metadata_merge_upper_bytes(
            &one_under_adapter.read_metadata_snapshot(),
            &one_under_metadata,
        );
        let one_under = one_under_adapter
            .begin_query_execution(
                QueryWorkLimits {
                    max_memory_bytes: Some(required - 1),
                    ..QueryWorkLimits::default()
                },
                QueryCancellationToken::new(),
            )
            .expect("one-under metadata query should admit")
            .expect("adapter should expose its local query budget");
        let error = one_under_adapter
            .record_metadata(&one_under_metadata, Some(&one_under))
            .expect_err("one-under metadata envelope must fail before cloning");
        assert_query_limit(error, QueryLimitReason::PerQueryMemoryBytes);
        assert!(one_under_adapter
            .read_metadata_snapshot()
            .warnings
            .is_empty());
        assert_eq!(one_under.snapshot().memory_reserved_bytes, 0);
        drop(one_under);
        let released = one_under_storage.query_budget_snapshot();
        assert_eq!(released.active_queries, 0);
        assert_eq!(released.shared_reserved_memory_bytes, 0);
    }

    #[tokio::test]
    async fn public_metadata_snapshot_rejects_nonempty_unaccounted_warnings() {
        let (_storage, adapter) = make_bounded_adapter(&[]);
        let metadata = ReadFanoutResponseMetadata {
            consistency: adapter.read_fanout.read_consistency_mode(),
            partial_response_policy: adapter.read_fanout.read_partial_response_policy(),
            partial_response: true,
            warnings: vec!["legacy warning".to_string()],
        };
        adapter
            .record_metadata(&metadata, None)
            .expect("legacy metadata merge should remain compatible");

        let error = match adapter.take_accounted_read_metadata() {
            Ok(_) => panic!("public snapshot must fail closed without a warning reservation"),
            Err(error) => error,
        };
        assert!(matches!(error, TsinkError::Other(_)));
        assert_eq!(adapter.read_metadata_snapshot().warnings, metadata.warnings);
    }
}
