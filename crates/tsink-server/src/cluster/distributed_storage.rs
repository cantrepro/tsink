use super::query::{
    ReadFanoutError, ReadFanoutExecutor, ReadFanoutResponse, ReadFanoutResponseMetadata,
};
use super::rpc::RpcClient;
use std::collections::HashMap;
use std::future::Future;
use std::hash::Hash;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::runtime::Handle;
use tsink::{
    DataPoint, DeleteSeriesResult, Label, MetricSeries, QueryOptions, Result as TsinkResult, Row,
    SeriesMatcher, SeriesMatcherOp, SeriesSelection, Storage, StorageObservabilitySnapshot,
    TsinkError,
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
    read_metadata: Arc<Mutex<ReadFanoutResponseMetadata>>,
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
            read_metadata: Arc::new(Mutex::new(ReadFanoutResponseMetadata {
                consistency: read_fanout.read_consistency_mode(),
                partial_response_policy: read_fanout.read_partial_response_policy(),
                partial_response: false,
                warnings: Vec::new(),
            })),
        }
    }

    #[allow(dead_code)]
    pub fn with_cache_config(mut self, config: DistributedStorageCacheConfig) -> Self {
        self.cache_config = config;
        self
    }

    pub fn read_metadata_snapshot(&self) -> ReadFanoutResponseMetadata {
        match self.read_metadata.lock() {
            Ok(metadata) => metadata.clone(),
            Err(_) => ReadFanoutResponseMetadata {
                consistency: self.read_fanout.read_consistency_mode(),
                partial_response_policy: self.read_fanout.read_partial_response_policy(),
                partial_response: false,
                warnings: Vec::new(),
            },
        }
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

    fn lock_metadata(&self) -> TsinkResult<MutexGuard<'_, ReadFanoutResponseMetadata>> {
        self.read_metadata
            .lock()
            .map_err(|err| TsinkError::LockPoisoned {
                resource: format!("distributed-storage-metadata: {err}"),
            })
    }

    fn record_metadata(&self, metadata: &ReadFanoutResponseMetadata) -> TsinkResult<()> {
        let mut current = self.lock_metadata()?;
        current.partial_response |= metadata.partial_response;
        for warning in &metadata.warnings {
            if !current.warnings.iter().any(|item| item == warning) {
                current.warnings.push(warning.clone());
            }
        }
        current.warnings.sort();
        Ok(())
    }

    fn finish_fanout<T>(&self, response: TsinkResult<ReadFanoutResponse<T>>) -> TsinkResult<T> {
        let response = response?;
        self.record_metadata(&response.metadata)?;
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
        points.sort_by(|left, right| left.timestamp.cmp(&right.timestamp));

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

    fn list_metrics(&self) -> TsinkResult<Vec<MetricSeries>> {
        self.list_metrics_distributed()
    }

    fn list_metrics_with_wal(&self) -> TsinkResult<Vec<MetricSeries>> {
        self.list_metrics()
    }

    fn select_series(&self, selection: &SeriesSelection) -> TsinkResult<Vec<MetricSeries>> {
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
    use tsink::{StorageBuilder, TimestampPrecision};

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
}
