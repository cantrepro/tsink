use crate::prom_remote::MetricType;
use crate::prom_write::NormalizedMetricMetadataUpdate;
use serde::de::{DeserializeSeed, Error as DeError, IgnoredAny, MapAccess, SeqAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::value::RawValue;
use std::cmp::Ordering as CmpOrdering;
use std::collections::BTreeMap;
use std::fmt;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tsink::{QueryBudgetError, QueryExecution, QueryMemoryReservation};

const METADATA_STORE_FILE_NAME: &str = "metric-metadata-store.json";
const METADATA_STORE_MAGIC: &str = "tsink-metric-metadata-store";
const METADATA_STORE_SCHEMA_VERSION: u16 = 1;
const METADATA_STORE_PREFIX: &[u8] =
    b"{\"magic\":\"tsink-metric-metadata-store\",\"schema_version\":1,\"entries\":[";
const METADATA_STORE_SUFFIX: &[u8] = b"]}\n";
const METADATA_ALLOCATION_ALLOWANCE_BYTES: usize = 64;
const METADATA_BTREE_ENTRY_ALLOWANCE_BYTES: usize = 64;
const METADATA_STARTUP_RECORD_JSON_OVERHEAD_BYTES: usize = 256;
const METADATA_QUERY_LOCK_MAX_ATTEMPTS: usize = 1_000;
const METADATA_QUERY_LOCK_RETRY_INTERVAL: Duration = Duration::from_millis(1);

type MetricMetadataKey = (String, String);
type MetricMetadataEntries = BTreeMap<MetricMetadataKey, MetricMetadataRecord>;
type MetricMetadataReadGuard<'a> = RwLockReadGuard<'a, MetricMetadataStoreState>;
type MetricMetadataWriteGuard<'a> = RwLockWriteGuard<'a, MetricMetadataStoreState>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MetricMetadataStoreConfig {
    pub max_entries: usize,
    pub max_record_bytes: usize,
    pub max_update_batch_entries: usize,
    pub max_update_batch_bytes: usize,
    pub max_retained_bytes: usize,
    pub max_durable_file_bytes: usize,
    pub max_startup_transient_bytes: usize,
    pub max_write_transient_bytes: usize,
    pub max_query_records: usize,
    pub max_query_result_bytes: usize,
}

impl Default for MetricMetadataStoreConfig {
    fn default() -> Self {
        Self {
            max_entries: 100_000,
            max_record_bytes: 64 * 1024,
            max_update_batch_entries: 512,
            max_update_batch_bytes: 4 * 1024 * 1024,
            max_retained_bytes: 64 * 1024 * 1024,
            max_durable_file_bytes: 16 * 1024 * 1024,
            max_startup_transient_bytes: 96 * 1024 * 1024,
            max_write_transient_bytes: 32 * 1024 * 1024,
            max_query_records: 10_000,
            max_query_result_bytes: 16 * 1024 * 1024,
        }
    }
}

impl MetricMetadataStoreConfig {
    pub fn validate(self) -> Result<Self, String> {
        let positive = [
            ("max_entries", self.max_entries),
            ("max_record_bytes", self.max_record_bytes),
            ("max_update_batch_entries", self.max_update_batch_entries),
            ("max_update_batch_bytes", self.max_update_batch_bytes),
            ("max_retained_bytes", self.max_retained_bytes),
            ("max_durable_file_bytes", self.max_durable_file_bytes),
            (
                "max_startup_transient_bytes",
                self.max_startup_transient_bytes,
            ),
            ("max_write_transient_bytes", self.max_write_transient_bytes),
            ("max_query_records", self.max_query_records),
            ("max_query_result_bytes", self.max_query_result_bytes),
        ];
        if let Some((field, _)) = positive.into_iter().find(|(_, value)| *value == 0) {
            return Err(format!(
                "metric metadata store {field} must be greater than zero"
            ));
        }
        if self.max_update_batch_entries > self.max_entries {
            return Err(
                "metric metadata store max_update_batch_entries cannot exceed max_entries"
                    .to_string(),
            );
        }
        if self.max_record_bytes > self.max_update_batch_bytes {
            return Err(
                "metric metadata store max_record_bytes cannot exceed max_update_batch_bytes"
                    .to_string(),
            );
        }
        if self.max_retained_bytes < modeled_empty_store_retained_bytes() {
            return Err(
                "metric metadata store max_retained_bytes is below the empty-store model"
                    .to_string(),
            );
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MetricMetadataRecord {
    pub tenant_id: String,
    pub metric_family_name: String,
    pub metric_type: i32,
    pub help: String,
    pub unit: String,
    pub updated_unix_ms: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(not(test), allow(dead_code))]
pub struct MetricMetadataStoreMetricsSnapshot {
    pub limits: MetricMetadataStoreConfig,
    pub entries: u64,
    pub retained_bytes: u64,
    pub peak_retained_bytes: u64,
    pub durable_file_bytes: u64,
    pub transient_bytes: u64,
    pub peak_transient_bytes: u64,
    pub query_result_bytes: u64,
    pub peak_query_result_bytes: u64,
    pub rejections_total: u64,
    pub entry_rejections_total: u64,
    pub record_rejections_total: u64,
    pub update_batch_rejections_total: u64,
    pub retained_rejections_total: u64,
    pub durable_file_rejections_total: u64,
    pub transient_rejections_total: u64,
    pub query_rejections_total: u64,
    pub persistence_rejections_total: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MetricMetadataStoreErrorCode {
    EntryLimit,
    RecordInputBytesLimit,
    UpdateBatchEntriesLimit,
    UpdateBatchBytesLimit,
    RetainedBytesLimit,
    DurableFileBytesLimit,
    StartupDurableFileBytesLimit,
    StartupRecordBytesLimit,
    StartupRetainedBytesLimit,
    StartupTransientBytesLimit,
    WriteTransientBytesLimit,
    QueryRecordsLimit,
    QueryResultBytesLimit,
    Allocation,
}

impl MetricMetadataStoreErrorCode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::EntryLimit => "entry_limit",
            Self::RecordInputBytesLimit => "record_input_bytes_limit",
            Self::UpdateBatchEntriesLimit => "update_batch_entries_limit",
            Self::UpdateBatchBytesLimit => "update_batch_bytes_limit",
            Self::RetainedBytesLimit => "retained_bytes_limit",
            Self::DurableFileBytesLimit => "durable_file_bytes_limit",
            Self::StartupDurableFileBytesLimit => "startup_durable_file_bytes_limit",
            Self::StartupRecordBytesLimit => "startup_record_bytes_limit",
            Self::StartupRetainedBytesLimit => "startup_retained_bytes_limit",
            Self::StartupTransientBytesLimit => "startup_transient_bytes_limit",
            Self::WriteTransientBytesLimit => "write_transient_bytes_limit",
            Self::QueryRecordsLimit => "query_records_limit",
            Self::QueryResultBytesLimit => "query_result_bytes_limit",
            Self::Allocation => "allocation",
        }
    }

    fn from_str(value: &str) -> Option<Self> {
        Some(match value {
            "entry_limit" => Self::EntryLimit,
            "record_input_bytes_limit" => Self::RecordInputBytesLimit,
            "update_batch_entries_limit" => Self::UpdateBatchEntriesLimit,
            "update_batch_bytes_limit" => Self::UpdateBatchBytesLimit,
            "retained_bytes_limit" => Self::RetainedBytesLimit,
            "durable_file_bytes_limit" => Self::DurableFileBytesLimit,
            "startup_durable_file_bytes_limit" => Self::StartupDurableFileBytesLimit,
            "startup_record_bytes_limit" => Self::StartupRecordBytesLimit,
            "startup_retained_bytes_limit" => Self::StartupRetainedBytesLimit,
            "startup_transient_bytes_limit" => Self::StartupTransientBytesLimit,
            "write_transient_bytes_limit" => Self::WriteTransientBytesLimit,
            "query_records_limit" => Self::QueryRecordsLimit,
            "query_result_bytes_limit" => Self::QueryResultBytesLimit,
            "allocation" => Self::Allocation,
            _ => return None,
        })
    }
}

#[derive(Debug)]
#[cfg_attr(not(test), allow(dead_code))]
pub enum MetricMetadataQueryError {
    Budget(QueryBudgetError),
    StoreUnavailable,
    RecordLimit,
    ResultLimit,
    Allocation,
}

impl fmt::Display for MetricMetadataQueryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Budget(error) => {
                write!(formatter, "metric metadata query budget failed: {error}")
            }
            Self::StoreUnavailable => formatter.write_str("metric metadata store is unavailable"),
            Self::RecordLimit => formatter.write_str("metric metadata query record limit exceeded"),
            Self::ResultLimit => {
                formatter.write_str("metric metadata query result byte limit exceeded")
            }
            Self::Allocation => formatter.write_str("metric metadata query allocation failed"),
        }
    }
}

impl std::error::Error for MetricMetadataQueryError {}

impl From<QueryBudgetError> for MetricMetadataQueryError {
    fn from(error: QueryBudgetError) -> Self {
        Self::Budget(error)
    }
}

impl MetricMetadataQueryError {
    #[must_use]
    #[cfg_attr(not(test), allow(dead_code))]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Budget(_) => "query_budget",
            Self::StoreUnavailable => "store_unavailable",
            Self::RecordLimit => "query_records_limit",
            Self::ResultLimit => "query_result_bytes_limit",
            Self::Allocation => "allocation",
        }
    }
}

/// Metadata records whose cloned heap remains charged to the supplied query execution.
#[derive(Debug)]
#[must_use = "dropping the result releases its retained query-memory reservation"]
#[cfg_attr(not(test), allow(dead_code))]
pub struct AccountedMetricMetadataQueryResult {
    records: Vec<MetricMetadataRecord>,
    reservation: Option<QueryMemoryReservation>,
    accounting: Arc<MetricMetadataStoreAccounting>,
    accounted_bytes: u64,
}

#[cfg_attr(not(test), allow(dead_code))]
impl AccountedMetricMetadataQueryResult {
    #[must_use]
    pub fn records(&self) -> &[MetricMetadataRecord] {
        &self.records
    }

    #[must_use]
    pub fn reserved_memory_bytes(&self) -> u64 {
        self.reservation
            .as_ref()
            .map_or(0, QueryMemoryReservation::bytes)
    }
}

impl Drop for AccountedMetricMetadataQueryResult {
    fn drop(&mut self) {
        // Release the execution-owned guard first. The store gauge can conservatively overlap
        // that release, but it must never report the bytes free while the query guard is live.
        drop(self.reservation.take());
        self.accounting.release_query_bytes(self.accounted_bytes);
    }
}

#[derive(Debug)]
struct MetricMetadataStoreState {
    entries: MetricMetadataEntries,
    retained_bytes: usize,
}

impl MetricMetadataStoreState {
    fn empty() -> Self {
        Self {
            entries: BTreeMap::new(),
            retained_bytes: modeled_empty_store_retained_bytes(),
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum MetadataStoreRejection {
    Entry,
    Record,
    UpdateBatch,
    Retained,
    DurableFile,
    Transient,
    Query,
    Persistence,
}

#[derive(Debug, Default)]
#[cfg_attr(not(test), allow(dead_code))]
struct MetricMetadataStoreAccounting {
    current_state_epoch: AtomicU64,
    entries: AtomicU64,
    retained_bytes: AtomicU64,
    peak_retained_bytes: AtomicU64,
    durable_file_bytes: AtomicU64,
    transient_bytes: AtomicU64,
    peak_transient_bytes: AtomicU64,
    query_result_bytes: AtomicU64,
    peak_query_result_bytes: AtomicU64,
    rejections_total: AtomicU64,
    entry_rejections_total: AtomicU64,
    record_rejections_total: AtomicU64,
    update_batch_rejections_total: AtomicU64,
    retained_rejections_total: AtomicU64,
    durable_file_rejections_total: AtomicU64,
    transient_rejections_total: AtomicU64,
    query_rejections_total: AtomicU64,
    persistence_rejections_total: AtomicU64,
}

impl MetricMetadataStoreAccounting {
    fn saturating_u64(value: usize) -> u64 {
        u64::try_from(value).unwrap_or(u64::MAX)
    }

    fn increment(counter: &AtomicU64) {
        let _ = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |value| {
            Some(value.saturating_add(1))
        });
    }

    fn observe_peak(counter: &AtomicU64, observed: u64) {
        let _ = counter.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            (observed > current).then_some(observed)
        });
    }

    fn publish_current_state(&self, entries: usize, retained_bytes: usize) {
        // Writers are serialized by the store write lock. The odd/even epoch keeps the pair
        // coherent for lock-free operator snapshots without making those snapshots wait on store
        // serialization or durable I/O.
        self.current_state_epoch.fetch_add(1, Ordering::AcqRel);
        self.entries
            .store(Self::saturating_u64(entries), Ordering::Release);
        let retained_bytes = Self::saturating_u64(retained_bytes);
        self.retained_bytes.store(retained_bytes, Ordering::Release);
        Self::observe_peak(&self.peak_retained_bytes, retained_bytes);
        self.current_state_epoch.fetch_add(1, Ordering::Release);
    }

    fn current_state(&self) -> (u64, u64) {
        loop {
            let before = self.current_state_epoch.load(Ordering::Acquire);
            if before & 1 != 0 {
                std::hint::spin_loop();
                continue;
            }
            let entries = self.entries.load(Ordering::Acquire);
            let retained_bytes = self.retained_bytes.load(Ordering::Acquire);
            let after = self.current_state_epoch.load(Ordering::Acquire);
            if before == after {
                return (entries, retained_bytes);
            }
        }
    }

    fn observe_startup_peak(&self, bytes: usize) {
        Self::observe_peak(&self.peak_transient_bytes, Self::saturating_u64(bytes));
    }

    fn record_rejection(&self, rejection: MetadataStoreRejection) {
        Self::increment(&self.rejections_total);
        let counter = match rejection {
            MetadataStoreRejection::Entry => &self.entry_rejections_total,
            MetadataStoreRejection::Record => &self.record_rejections_total,
            MetadataStoreRejection::UpdateBatch => &self.update_batch_rejections_total,
            MetadataStoreRejection::Retained => &self.retained_rejections_total,
            MetadataStoreRejection::DurableFile => &self.durable_file_rejections_total,
            MetadataStoreRejection::Transient => &self.transient_rejections_total,
            MetadataStoreRejection::Query => &self.query_rejections_total,
            MetadataStoreRejection::Persistence => &self.persistence_rejections_total,
        };
        Self::increment(counter);
    }

    fn reserve_transient(
        self: &Arc<Self>,
        bytes: usize,
        limit: usize,
    ) -> Result<MetadataTransientReservation, ()> {
        let requested = Self::saturating_u64(bytes);
        let limit = Self::saturating_u64(limit);
        let mut current = self.transient_bytes.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_add(requested) else {
                self.record_rejection(MetadataStoreRejection::Transient);
                return Err(());
            };
            if next > limit {
                self.record_rejection(MetadataStoreRejection::Transient);
                return Err(());
            }
            match self.transient_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    Self::observe_peak(&self.peak_transient_bytes, next);
                    return Ok(MetadataTransientReservation {
                        accounting: Arc::clone(self),
                        bytes: requested,
                        limit,
                    });
                }
                Err(observed) => current = observed,
            }
        }
    }

    fn retain_query_bytes(&self, bytes: u64) {
        if bytes == 0 {
            return;
        }
        let current = self.query_result_bytes.fetch_add(bytes, Ordering::AcqRel);
        Self::observe_peak(&self.peak_query_result_bytes, current.saturating_add(bytes));
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn release_query_bytes(&self, bytes: u64) {
        if bytes > 0 {
            self.query_result_bytes.fetch_sub(bytes, Ordering::AcqRel);
        }
    }
}

#[derive(Debug)]
struct MetadataTransientReservation {
    accounting: Arc<MetricMetadataStoreAccounting>,
    bytes: u64,
    limit: u64,
}

impl MetadataTransientReservation {
    fn resize(&mut self, requested_bytes: usize) -> Result<(), ()> {
        let requested = MetricMetadataStoreAccounting::saturating_u64(requested_bytes);
        if requested == self.bytes {
            return Ok(());
        }
        if requested < self.bytes {
            self.accounting
                .transient_bytes
                .fetch_sub(self.bytes - requested, Ordering::AcqRel);
            self.bytes = requested;
            return Ok(());
        }

        let additional = requested - self.bytes;
        let mut current = self.accounting.transient_bytes.load(Ordering::Acquire);
        loop {
            let Some(next) = current.checked_add(additional) else {
                self.accounting
                    .record_rejection(MetadataStoreRejection::Transient);
                return Err(());
            };
            if next > self.limit {
                self.accounting
                    .record_rejection(MetadataStoreRejection::Transient);
                return Err(());
            }
            match self.accounting.transient_bytes.compare_exchange_weak(
                current,
                next,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => {
                    self.bytes = requested;
                    MetricMetadataStoreAccounting::observe_peak(
                        &self.accounting.peak_transient_bytes,
                        next,
                    );
                    return Ok(());
                }
                Err(observed) => current = observed,
            }
        }
    }
}

impl Drop for MetadataTransientReservation {
    fn drop(&mut self) {
        if self.bytes > 0 {
            self.accounting
                .transient_bytes
                .fetch_sub(self.bytes, Ordering::AcqRel);
        }
    }
}

#[derive(Debug)]
pub struct MetricMetadataStore {
    path: Option<PathBuf>,
    local_disk_budget: Option<Arc<tsink::LocalDiskBudget>>,
    config: MetricMetadataStoreConfig,
    state: RwLock<MetricMetadataStoreState>,
    accounting: Arc<MetricMetadataStoreAccounting>,
}

impl MetricMetadataStore {
    #[allow(dead_code)]
    pub fn in_memory() -> Self {
        Self::in_memory_with_config(MetricMetadataStoreConfig::default())
            .expect("default metric metadata store config must be valid")
    }

    #[allow(dead_code)]
    pub fn in_memory_with_config(config: MetricMetadataStoreConfig) -> Result<Self, String> {
        let config = config.validate()?;
        let state = MetricMetadataStoreState::empty();
        let accounting = Arc::new(MetricMetadataStoreAccounting::default());
        accounting.publish_current_state(state.entries.len(), state.retained_bytes);
        Ok(Self {
            path: None,
            local_disk_budget: None,
            config,
            state: RwLock::new(state),
            accounting,
        })
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn open(data_path: Option<&Path>) -> Result<Self, String> {
        Self::open_with_disk_budget(data_path, None)
    }

    pub fn open_with_disk_budget(
        data_path: Option<&Path>,
        local_disk_budget: Option<Arc<tsink::LocalDiskBudget>>,
    ) -> Result<Self, String> {
        Self::open_with_config_and_disk_budget(
            data_path,
            MetricMetadataStoreConfig::default(),
            local_disk_budget,
        )
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn open_with_config(
        data_path: Option<&Path>,
        config: MetricMetadataStoreConfig,
    ) -> Result<Self, String> {
        Self::open_with_config_and_disk_budget(data_path, config, None)
    }

    pub fn open_with_config_and_disk_budget(
        data_path: Option<&Path>,
        config: MetricMetadataStoreConfig,
        local_disk_budget: Option<Arc<tsink::LocalDiskBudget>>,
    ) -> Result<Self, String> {
        let config = config.validate()?;
        let path = data_path.map(|path| path.join(METADATA_STORE_FILE_NAME));
        match (path.as_deref(), local_disk_budget.as_ref()) {
            (Some(path), Some(budget)) => {
                budget.cleanup_atomic_write_temps(path).map_err(|err| {
                    format!(
                        "failed to clean metric metadata temporary files for {}: {err}",
                        path.display()
                    )
                })?;
                budget.validate_managed_file_path(path).map_err(|err| {
                    format!(
                        "failed to validate metric metadata store {}: {err}",
                        path.display()
                    )
                })?;
            }
            (None, Some(_)) => {
                return Err(
                    "metric metadata cannot use a local disk budget without a data path"
                        .to_string(),
                )
            }
            _ => {}
        }
        let loaded = if let Some(path) = path.as_ref() {
            load_entries(path, config)?
        } else {
            LoadedMetricMetadataStore {
                state: MetricMetadataStoreState::empty(),
                durable_file_bytes: 0,
                startup_peak_bytes: 0,
            }
        };
        let accounting = Arc::new(MetricMetadataStoreAccounting::default());
        accounting.publish_current_state(loaded.state.entries.len(), loaded.state.retained_bytes);
        accounting.observe_startup_peak(loaded.startup_peak_bytes);
        accounting.durable_file_bytes.store(
            MetricMetadataStoreAccounting::saturating_u64(loaded.durable_file_bytes),
            Ordering::Release,
        );

        Ok(Self {
            path,
            local_disk_budget,
            config,
            state: RwLock::new(loaded.state),
            accounting,
        })
    }

    #[must_use]
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn config(&self) -> MetricMetadataStoreConfig {
        self.config
    }

    /// Returns the stable sidecar limit/allocation code carried by a compatibility write error.
    ///
    /// Disk quota, disk headroom, JSON, and I/O failures deliberately return `None` here because
    /// they retain their native `TsinkError` variants.
    #[must_use]
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn classify_apply_error(error: &tsink::TsinkError) -> Option<MetricMetadataStoreErrorCode> {
        let tsink::TsinkError::Other(message) = error else {
            return None;
        };
        let encoded = message
            .strip_prefix("metric_metadata_store_error=")?
            .split_once(';')?
            .0;
        MetricMetadataStoreErrorCode::from_str(encoded)
    }

    pub fn apply_updates(
        &self,
        tenant_id: &str,
        updates: &[NormalizedMetricMetadataUpdate],
    ) -> tsink::Result<usize> {
        if updates.is_empty() {
            return Ok(0);
        }
        if updates.len() > self.config.max_update_batch_entries {
            return Err(self.limit_error(
                MetadataStoreRejection::UpdateBatch,
                "update batch entry",
                self.config.max_update_batch_entries,
                updates.len(),
            ));
        }

        let mut batch_bytes = 0usize;
        let mut staging_bytes = 0usize;
        for update in updates {
            let record_bytes = modeled_record_input_bytes(
                tenant_id,
                &update.metric_family_name,
                &update.help,
                &update.unit,
            );
            if record_bytes > self.config.max_record_bytes {
                return Err(self.limit_error(
                    MetadataStoreRejection::Record,
                    "record input byte",
                    self.config.max_record_bytes,
                    record_bytes,
                ));
            }
            batch_bytes = batch_bytes.checked_add(record_bytes).ok_or_else(|| {
                self.limit_error(
                    MetadataStoreRejection::UpdateBatch,
                    "update batch byte",
                    self.config.max_update_batch_bytes,
                    usize::MAX,
                )
            })?;
            staging_bytes = staging_bytes
                .checked_add(modeled_staged_entry_bytes(
                    tenant_id,
                    &update.metric_family_name,
                    &update.help,
                    &update.unit,
                ))
                .ok_or_else(|| {
                    self.limit_error(
                        MetadataStoreRejection::Transient,
                        "write transient byte",
                        self.config.max_write_transient_bytes,
                        usize::MAX,
                    )
                })?;
        }
        if batch_bytes > self.config.max_update_batch_bytes {
            return Err(self.limit_error(
                MetadataStoreRejection::UpdateBatch,
                "update batch byte",
                self.config.max_update_batch_bytes,
                batch_bytes,
            ));
        }

        let mut transient = self
            .accounting
            .reserve_transient(staging_bytes, self.config.max_write_transient_bytes)
            .map_err(|()| {
                metadata_limit_error(
                    "write transient byte",
                    self.config.max_write_transient_bytes,
                    staging_bytes,
                )
            })?;
        let mut state = self.write_entries().map_err(tsink::TsinkError::Other)?;
        let mut staged_entries = BTreeMap::<MetricMetadataKey, MetricMetadataRecord>::new();
        let mut changed = 0usize;
        let mut updated_unix_ms = unix_timestamp_millis();
        for update in updates {
            let key = (
                try_clone_string(tenant_id).map_err(|()| self.allocation_error())?,
                try_clone_string(&update.metric_family_name)
                    .map_err(|()| self.allocation_error())?,
            );
            let candidate = MetricMetadataRecord {
                tenant_id: try_clone_string(tenant_id).map_err(|()| self.allocation_error())?,
                metric_family_name: try_clone_string(&update.metric_family_name)
                    .map_err(|()| self.allocation_error())?,
                metric_type: update.metric_type as i32,
                help: try_clone_string(&update.help).map_err(|()| self.allocation_error())?,
                unit: try_clone_string(&update.unit).map_err(|()| self.allocation_error())?,
                updated_unix_ms,
            };
            let existing = staged_entries.get(&key).or_else(|| state.entries.get(&key));
            let existing_matches =
                existing.is_some_and(|existing| records_have_same_metadata(existing, &candidate));
            if existing_matches {
                continue;
            }

            staged_entries.insert(key, candidate);
            changed = changed.saturating_add(1);
            updated_unix_ms = updated_unix_ms.saturating_add(1);
        }

        if changed == 0 {
            return Ok(0);
        }

        let (final_entry_count, final_retained_bytes) =
            modeled_final_state(&state, &staged_entries);
        if final_entry_count > self.config.max_entries {
            return Err(self.limit_error(
                MetadataStoreRejection::Entry,
                "entry",
                self.config.max_entries,
                final_entry_count,
            ));
        }
        if final_retained_bytes > self.config.max_retained_bytes {
            return Err(self.limit_error(
                MetadataStoreRejection::Retained,
                "retained byte",
                self.config.max_retained_bytes,
                final_retained_bytes,
            ));
        }

        let durable_file_bytes = if let Some(path) = self.path.as_ref() {
            let encoded = self.encode_entries(&state.entries, &staged_entries, &mut transient)?;
            let encoded_len = encoded.len();
            let persisted = if let Some(local_disk_budget) = self.local_disk_budget.as_ref() {
                local_disk_budget.write_file_atomically_and_sync_parent(
                    path,
                    &encoded,
                    tsink::DiskCategory::Metadata,
                )
            } else {
                tsink::engine::fs_utils::write_file_atomically_and_sync_parent(path, &encoded)
            };
            if let Err(error) = persisted {
                self.accounting
                    .record_rejection(MetadataStoreRejection::Persistence);
                return Err(error);
            }
            encoded_len
        } else {
            0
        };

        for (key, record) in staged_entries {
            state.entries.insert(key, record);
        }
        state.retained_bytes = final_retained_bytes;
        self.accounting
            .publish_current_state(state.entries.len(), final_retained_bytes);
        self.accounting.durable_file_bytes.store(
            MetricMetadataStoreAccounting::saturating_u64(durable_file_bytes),
            Ordering::Release,
        );
        Ok(changed)
    }

    /// Compatibility query whose bounded cloned result becomes caller-owned immediately.
    ///
    /// This path enforces the store's record and byte ceilings but intentionally cannot retain a
    /// query-memory reservation after returning a plain `Vec`. Bounded engine callers should use
    /// [`Self::query_with_execution_result`].
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn query(
        &self,
        tenant_id: &str,
        metric: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MetricMetadataRecord>, String> {
        self.query_caller_owned(tenant_id, metric, limit)
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn query_caller_owned(
        &self,
        tenant_id: &str,
        metric: Option<&str>,
        limit: usize,
    ) -> Result<Vec<MetricMetadataRecord>, String> {
        let query_limit = self.compatibility_query_limit(limit)?;
        let state = self.read_entries()?;
        let (count, modeled_bytes) =
            modeled_query_result(&state.entries, tenant_id, metric, query_limit, None)
                .map_err(|error| error.to_string())?;
        if modeled_bytes > self.config.max_query_result_bytes {
            self.accounting
                .record_rejection(MetadataStoreRejection::Query);
            return Err(metadata_limit_message(
                "query result byte",
                self.config.max_query_result_bytes,
                modeled_bytes,
            ));
        }
        clone_query_records(&state.entries, tenant_id, metric, count, query_limit, None).map_err(
            |error| {
                self.accounting
                    .record_rejection(MetadataStoreRejection::Query);
                match error {
                    CloneQueryRecordsError::Budget(error) => error.to_string(),
                    CloneQueryRecordsError::Allocation => {
                        "metric metadata query allocation failed".to_string()
                    }
                }
            },
        )
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn query_with_execution_result(
        &self,
        tenant_id: &str,
        metric: Option<&str>,
        limit: usize,
        execution: &QueryExecution,
    ) -> Result<AccountedMetricMetadataQueryResult, MetricMetadataQueryError> {
        execution.checkpoint().map_err(|error| {
            self.accounting
                .record_rejection(MetadataStoreRejection::Query);
            MetricMetadataQueryError::Budget(error)
        })?;
        let query_limit = self.guarded_query_limit(limit)?;
        let state = self
            .read_entries_with_execution(execution)
            .inspect_err(|_| {
                self.accounting
                    .record_rejection(MetadataStoreRejection::Query);
            })?;
        let (count, modeled_bytes) = modeled_query_result(
            &state.entries,
            tenant_id,
            metric,
            query_limit,
            Some(execution),
        )
        .map_err(|error| {
            self.accounting
                .record_rejection(MetadataStoreRejection::Query);
            MetricMetadataQueryError::Budget(error)
        })?;
        if modeled_bytes > self.config.max_query_result_bytes {
            self.accounting
                .record_rejection(MetadataStoreRejection::Query);
            return Err(MetricMetadataQueryError::ResultLimit);
        }
        let modeled_bytes_u64 = MetricMetadataStoreAccounting::saturating_u64(modeled_bytes);
        let mut reservation = execution
            .reserve_memory(modeled_bytes_u64)
            .map_err(|error| {
                self.accounting
                    .record_rejection(MetadataStoreRejection::Query);
                MetricMetadataQueryError::Budget(error)
            })?;
        let records = clone_query_records(
            &state.entries,
            tenant_id,
            metric,
            count,
            query_limit,
            Some(execution),
        )
        .map_err(|error| {
            self.accounting
                .record_rejection(MetadataStoreRejection::Query);
            match error {
                CloneQueryRecordsError::Budget(error) => MetricMetadataQueryError::Budget(error),
                CloneQueryRecordsError::Allocation => MetricMetadataQueryError::Allocation,
            }
        })?;
        let retained_bytes = modeled_owned_query_result_bytes(&records);
        if retained_bytes > self.config.max_query_result_bytes {
            self.accounting
                .record_rejection(MetadataStoreRejection::Query);
            return Err(MetricMetadataQueryError::ResultLimit);
        }
        let retained_bytes_u64 = MetricMetadataStoreAccounting::saturating_u64(retained_bytes);
        reservation.resize(retained_bytes_u64).map_err(|error| {
            self.accounting
                .record_rejection(MetadataStoreRejection::Query);
            MetricMetadataQueryError::Budget(error)
        })?;
        execution
            .observe_intermediate_vector_size(MetricMetadataStoreAccounting::saturating_u64(
                records.len(),
            ))
            .map_err(|error| {
                self.accounting
                    .record_rejection(MetadataStoreRejection::Query);
                MetricMetadataQueryError::Budget(error)
            })?;
        execution.checkpoint().map_err(|error| {
            self.accounting
                .record_rejection(MetadataStoreRejection::Query);
            MetricMetadataQueryError::Budget(error)
        })?;
        self.accounting.retain_query_bytes(retained_bytes_u64);
        Ok(AccountedMetricMetadataQueryResult {
            records,
            reservation: Some(reservation),
            accounting: Arc::clone(&self.accounting),
            accounted_bytes: retained_bytes_u64,
        })
    }

    pub fn snapshot_into(&self, snapshot_path: &Path) -> Result<(), String> {
        let snapshot_file = snapshot_path.join(METADATA_STORE_FILE_NAME);
        let state = self.read_entries()?;
        let mut transient = self
            .accounting
            .reserve_transient(0, self.config.max_write_transient_bytes)
            .map_err(|()| {
                metadata_limit_message(
                    "snapshot transient byte",
                    self.config.max_write_transient_bytes,
                    0,
                )
            })?;
        let encoded = self
            .encode_entries(&state.entries, &BTreeMap::new(), &mut transient)
            .map_err(|error| error.to_string())?;
        tsink::engine::fs_utils::write_file_atomically_and_sync_parent(&snapshot_file, &encoded)
            .map_err(|error| {
                self.accounting
                    .record_rejection(MetadataStoreRejection::Persistence);
                error.to_string()
            })
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub fn metrics_snapshot(&self) -> Result<MetricMetadataStoreMetricsSnapshot, String> {
        let (entries, retained_bytes) = self.accounting.current_state();
        Ok(MetricMetadataStoreMetricsSnapshot {
            limits: self.config,
            entries,
            retained_bytes,
            peak_retained_bytes: self.accounting.peak_retained_bytes.load(Ordering::Acquire),
            durable_file_bytes: self.accounting.durable_file_bytes.load(Ordering::Acquire),
            transient_bytes: self.accounting.transient_bytes.load(Ordering::Acquire),
            peak_transient_bytes: self.accounting.peak_transient_bytes.load(Ordering::Acquire),
            query_result_bytes: self.accounting.query_result_bytes.load(Ordering::Acquire),
            peak_query_result_bytes: self
                .accounting
                .peak_query_result_bytes
                .load(Ordering::Acquire),
            rejections_total: self.accounting.rejections_total.load(Ordering::Acquire),
            entry_rejections_total: self
                .accounting
                .entry_rejections_total
                .load(Ordering::Acquire),
            record_rejections_total: self
                .accounting
                .record_rejections_total
                .load(Ordering::Acquire),
            update_batch_rejections_total: self
                .accounting
                .update_batch_rejections_total
                .load(Ordering::Acquire),
            retained_rejections_total: self
                .accounting
                .retained_rejections_total
                .load(Ordering::Acquire),
            durable_file_rejections_total: self
                .accounting
                .durable_file_rejections_total
                .load(Ordering::Acquire),
            transient_rejections_total: self
                .accounting
                .transient_rejections_total
                .load(Ordering::Acquire),
            query_rejections_total: self
                .accounting
                .query_rejections_total
                .load(Ordering::Acquire),
            persistence_rejections_total: self
                .accounting
                .persistence_rejections_total
                .load(Ordering::Acquire),
        })
    }

    #[cfg(test)]
    pub fn file_path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    fn read_entries(&self) -> Result<MetricMetadataReadGuard<'_>, String> {
        self.state
            .read()
            .map_err(|_| "metric metadata store read lock poisoned".to_string())
    }

    fn read_entries_with_execution(
        &self,
        execution: &QueryExecution,
    ) -> Result<MetricMetadataReadGuard<'_>, MetricMetadataQueryError> {
        for attempt in 0..METADATA_QUERY_LOCK_MAX_ATTEMPTS {
            execution
                .checkpoint()
                .map_err(MetricMetadataQueryError::Budget)?;
            match self.state.try_read() {
                Ok(state) => return Ok(state),
                Err(TryLockError::Poisoned(_)) => {
                    return Err(MetricMetadataQueryError::StoreUnavailable)
                }
                Err(TryLockError::WouldBlock)
                    if attempt.saturating_add(1) < METADATA_QUERY_LOCK_MAX_ATTEMPTS =>
                {
                    std::thread::sleep(METADATA_QUERY_LOCK_RETRY_INTERVAL);
                }
                Err(TryLockError::WouldBlock) => {
                    return Err(MetricMetadataQueryError::StoreUnavailable)
                }
            }
        }
        Err(MetricMetadataQueryError::StoreUnavailable)
    }

    fn write_entries(&self) -> Result<MetricMetadataWriteGuard<'_>, String> {
        self.state
            .write()
            .map_err(|_| "metric metadata store write lock poisoned".to_string())
    }

    #[cfg_attr(not(test), allow(dead_code))]
    fn compatibility_query_limit(&self, requested: usize) -> Result<usize, String> {
        if requested > self.config.max_query_records {
            self.accounting
                .record_rejection(MetadataStoreRejection::Query);
            return Err(metadata_limit_message(
                "query record",
                self.config.max_query_records,
                requested,
            ));
        }
        Ok(requested)
    }

    fn guarded_query_limit(&self, requested: usize) -> Result<usize, MetricMetadataQueryError> {
        if requested > self.config.max_query_records {
            self.accounting
                .record_rejection(MetadataStoreRejection::Query);
            return Err(MetricMetadataQueryError::RecordLimit);
        }
        Ok(requested)
    }

    fn allocation_error(&self) -> tsink::TsinkError {
        self.accounting
            .record_rejection(MetadataStoreRejection::Transient);
        tsink::TsinkError::Other(format!(
            "metric_metadata_store_error={}; metric metadata store allocation failed",
            MetricMetadataStoreErrorCode::Allocation.as_str()
        ))
    }

    fn limit_error(
        &self,
        rejection: MetadataStoreRejection,
        dimension: &'static str,
        limit: usize,
        required: usize,
    ) -> tsink::TsinkError {
        self.accounting.record_rejection(rejection);
        metadata_limit_error(dimension, limit, required)
    }

    fn encode_entries(
        &self,
        entries: &MetricMetadataEntries,
        staged: &MetricMetadataEntries,
        transient: &mut MetadataTransientReservation,
    ) -> tsink::Result<Vec<u8>> {
        match encode_store(
            entries,
            staged,
            self.config.max_durable_file_bytes,
            transient,
        ) {
            Ok(encoded) => Ok(encoded),
            Err(EncodeStoreError::DurableFileLimit { required }) => Err(self.limit_error(
                MetadataStoreRejection::DurableFile,
                "durable file byte",
                self.config.max_durable_file_bytes,
                required,
            )),
            Err(EncodeStoreError::TransientLimit { required }) => Err(metadata_limit_error(
                "write transient byte",
                self.config.max_write_transient_bytes,
                required,
            )),
            Err(EncodeStoreError::Allocation) => Err(self.allocation_error()),
            Err(EncodeStoreError::Json(error)) => Err(error.into()),
        }
    }
}

pub fn metric_type_to_api_string(metric_type: i32) -> &'static str {
    match MetricType::try_from(metric_type) {
        Ok(MetricType::Unknown) => "unknown",
        Ok(MetricType::Counter) => "counter",
        Ok(MetricType::Gauge) => "gauge",
        Ok(MetricType::Histogram) => "histogram",
        Ok(MetricType::Gaugehistogram) => "gaugehistogram",
        Ok(MetricType::Summary) => "summary",
        Ok(MetricType::Info) => "info",
        Ok(MetricType::Stateset) => "stateset",
        Err(_) => "unknown",
    }
}

#[derive(Debug)]
struct LoadedMetricMetadataStore {
    state: MetricMetadataStoreState,
    durable_file_bytes: usize,
    startup_peak_bytes: usize,
}

fn load_entries(
    path: &Path,
    config: MetricMetadataStoreConfig,
) -> Result<LoadedMetricMetadataStore, String> {
    if !path.exists() {
        return Ok(LoadedMetricMetadataStore {
            state: MetricMetadataStoreState::empty(),
            durable_file_bytes: 0,
            startup_peak_bytes: 0,
        });
    }

    let mut file = std::fs::File::open(path).map_err(|error| {
        format!(
            "failed to read metric metadata store {}: {error}",
            path.display()
        )
    })?;
    let file_len = usize::try_from(
        file.metadata()
            .map_err(|error| {
                format!(
                    "failed to inspect metric metadata store {}: {error}",
                    path.display()
                )
            })?
            .len(),
    )
    .unwrap_or(usize::MAX);
    if file_len > config.max_durable_file_bytes {
        return Err(metadata_limit_message(
            "startup durable file byte",
            config.max_durable_file_bytes,
            file_len,
        ));
    }
    let preparse_peak = file_len
        .saturating_mul(2)
        .saturating_add(
            config
                .max_entries
                .saturating_mul(std::mem::size_of::<&RawValue>()),
        )
        .saturating_add(METADATA_ALLOCATION_ALLOWANCE_BYTES.saturating_mul(2))
        .saturating_add(modeled_empty_store_retained_bytes());
    if preparse_peak > config.max_startup_transient_bytes {
        return Err(metadata_limit_message(
            "startup transient byte",
            config.max_startup_transient_bytes,
            preparse_peak,
        ));
    }

    let mut raw = Vec::new();
    raw.try_reserve_exact(file_len)
        .map_err(|_| "metric metadata startup allocation failed".to_string())?;
    {
        let mut bounded_file = (&mut file).take(u64::try_from(file_len).unwrap_or(u64::MAX));
        bounded_file.read_to_end(&mut raw).map_err(|error| {
            format!(
                "failed to read metric metadata store {}: {error}",
                path.display()
            )
        })?;
    }
    let mut extra = [0_u8; 1];
    let grew_while_reading = file.read(&mut extra).map_err(|error| {
        format!(
            "failed to read metric metadata store {}: {error}",
            path.display()
        )
    })? != 0;
    if raw.len() != file_len || grew_while_reading {
        return Err(format!(
            "metric metadata store {} changed size while it was being read",
            path.display()
        ));
    }

    let mut deserializer = serde_json::Deserializer::from_slice(&raw);
    let persisted = RawPersistedStoreSeed {
        max_entries: config.max_entries,
    }
    .deserialize(&mut deserializer)
    .map_err(|err| {
        format!(
            "failed to parse metric metadata store {}: {err}",
            path.display()
        )
    })?;
    deserializer.end().map_err(|err| {
        format!(
            "failed to parse metric metadata store {}: {err}",
            path.display()
        )
    })?;
    if !persisted.magic_valid {
        return Err(format!(
            "metric metadata store {} has unsupported magic",
            path.display()
        ));
    }
    if persisted.schema_version != METADATA_STORE_SCHEMA_VERSION {
        return Err(format!(
            "metric metadata store {} has unsupported schema version {}",
            path.display(),
            persisted.schema_version
        ));
    }

    let raw_reference_bytes = persisted
        .entries
        .capacity()
        .saturating_mul(std::mem::size_of::<&RawValue>())
        .saturating_add(METADATA_ALLOCATION_ALLOWANCE_BYTES);
    let startup_base = raw
        .capacity()
        .saturating_add(METADATA_ALLOCATION_ALLOWANCE_BYTES)
        .saturating_add(raw_reference_bytes);
    let max_record_json_bytes = config
        .max_record_bytes
        .saturating_mul(6)
        .saturating_add(METADATA_STARTUP_RECORD_JSON_OVERHEAD_BYTES);
    let mut state = MetricMetadataStoreState::empty();
    let mut startup_peak_bytes =
        preparse_peak.max(startup_base.saturating_add(state.retained_bytes));
    if startup_peak_bytes > config.max_startup_transient_bytes {
        return Err(metadata_limit_message(
            "startup transient byte",
            config.max_startup_transient_bytes,
            startup_peak_bytes,
        ));
    }
    for raw_entry in persisted.entries {
        let raw_entry_bytes = raw_entry.get().len();
        if raw_entry_bytes > max_record_json_bytes {
            return Err(metadata_limit_message(
                "startup record encoded byte",
                max_record_json_bytes,
                raw_entry_bytes,
            ));
        }
        let decode_peak = startup_base
            .saturating_add(state.retained_bytes)
            .saturating_add(raw_entry_bytes.saturating_mul(2))
            .saturating_add(std::mem::size_of::<MetricMetadataKey>())
            .saturating_add(std::mem::size_of::<MetricMetadataRecord>())
            .saturating_add(METADATA_BTREE_ENTRY_ALLOWANCE_BYTES)
            .saturating_add(METADATA_ALLOCATION_ALLOWANCE_BYTES.saturating_mul(8));
        if decode_peak > config.max_startup_transient_bytes {
            return Err(metadata_limit_message(
                "startup transient byte",
                config.max_startup_transient_bytes,
                decode_peak,
            ));
        }
        startup_peak_bytes = startup_peak_bytes.max(decode_peak);

        let entry: MetricMetadataRecord = serde_json::from_str(raw_entry.get()).map_err(|err| {
            format!(
                "failed to parse metric metadata record in {}: {err}",
                path.display()
            )
        })?;
        let input_bytes = modeled_record_input_bytes(
            &entry.tenant_id,
            &entry.metric_family_name,
            &entry.help,
            &entry.unit,
        );
        if input_bytes > config.max_record_bytes {
            return Err(metadata_limit_message(
                "startup record input byte",
                config.max_record_bytes,
                input_bytes,
            ));
        }
        let key = (
            try_clone_string(&entry.tenant_id)
                .map_err(|()| "metric metadata startup allocation failed".to_string())?,
            try_clone_string(&entry.metric_family_name)
                .map_err(|()| "metric metadata startup allocation failed".to_string())?,
        );
        if state.entries.contains_key(&key) {
            return Err(format!(
                "metric metadata store {} contains a duplicate entry",
                path.display()
            ));
        }
        let next_entry_count = state.entries.len().saturating_add(1);
        if next_entry_count > config.max_entries {
            return Err(metadata_limit_message(
                "startup entry",
                config.max_entries,
                next_entry_count,
            ));
        }
        let next_retained = state
            .retained_bytes
            .saturating_add(modeled_retained_entry_bytes(&entry));
        if next_retained > config.max_retained_bytes {
            return Err(metadata_limit_message(
                "startup retained byte",
                config.max_retained_bytes,
                next_retained,
            ));
        }
        state.entries.insert(key, entry);
        state.retained_bytes = next_retained;
        startup_peak_bytes =
            startup_peak_bytes.max(startup_base.saturating_add(state.retained_bytes));
    }
    Ok(LoadedMetricMetadataStore {
        state,
        durable_file_bytes: raw.len(),
        startup_peak_bytes,
    })
}

fn records_have_same_metadata(left: &MetricMetadataRecord, right: &MetricMetadataRecord) -> bool {
    left.metric_type == right.metric_type && left.help == right.help && left.unit == right.unit
}

fn modeled_empty_store_retained_bytes() -> usize {
    std::mem::size_of::<MetricMetadataEntries>()
}

fn modeled_string_bytes(value: &str) -> usize {
    if value.is_empty() {
        0
    } else {
        value
            .len()
            .saturating_add(METADATA_ALLOCATION_ALLOWANCE_BYTES)
    }
}

fn modeled_record_input_bytes(
    tenant_id: &str,
    metric_family_name: &str,
    help: &str,
    unit: &str,
) -> usize {
    tenant_id
        .len()
        .saturating_add(metric_family_name.len())
        .saturating_add(help.len())
        .saturating_add(unit.len())
}

fn modeled_staged_entry_bytes(
    tenant_id: &str,
    metric_family_name: &str,
    help: &str,
    unit: &str,
) -> usize {
    std::mem::size_of::<(MetricMetadataKey, MetricMetadataRecord)>()
        .saturating_add(METADATA_BTREE_ENTRY_ALLOWANCE_BYTES)
        .saturating_add(modeled_string_bytes(tenant_id).saturating_mul(2))
        .saturating_add(modeled_string_bytes(metric_family_name).saturating_mul(2))
        .saturating_add(modeled_string_bytes(help))
        .saturating_add(modeled_string_bytes(unit))
}

fn modeled_retained_entry_bytes(entry: &MetricMetadataRecord) -> usize {
    modeled_staged_entry_bytes(
        &entry.tenant_id,
        &entry.metric_family_name,
        &entry.help,
        &entry.unit,
    )
}

fn modeled_query_record_bytes(entry: &MetricMetadataRecord) -> usize {
    modeled_string_bytes(&entry.tenant_id)
        .saturating_add(modeled_string_bytes(&entry.metric_family_name))
        .saturating_add(modeled_string_bytes(&entry.help))
        .saturating_add(modeled_string_bytes(&entry.unit))
}

fn modeled_query_vector_bytes(count: usize) -> usize {
    if count == 0 {
        0
    } else {
        count
            .saturating_mul(std::mem::size_of::<MetricMetadataRecord>())
            .saturating_add(METADATA_ALLOCATION_ALLOWANCE_BYTES)
    }
}

fn modeled_query_result(
    entries: &MetricMetadataEntries,
    tenant_id: &str,
    metric: Option<&str>,
    limit: usize,
    execution: Option<&QueryExecution>,
) -> Result<(usize, usize), QueryBudgetError> {
    let mut count = 0usize;
    let mut bytes = 0usize;
    if limit == 0 {
        return Ok((0, 0));
    }
    for ((entry_tenant, entry_metric), entry) in entries {
        if let Some(execution) = execution {
            execution.checkpoint()?;
        }
        if entry_tenant != tenant_id {
            continue;
        }
        if metric.is_some_and(|metric| metric != entry_metric) {
            continue;
        }
        count = count.saturating_add(1);
        bytes = bytes.saturating_add(modeled_query_record_bytes(entry));
        if count >= limit {
            break;
        }
    }
    Ok((
        count,
        bytes.saturating_add(modeled_query_vector_bytes(count)),
    ))
}

fn modeled_owned_string_bytes(value: &String) -> usize {
    if value.capacity() == 0 {
        0
    } else {
        value
            .capacity()
            .saturating_add(METADATA_ALLOCATION_ALLOWANCE_BYTES)
    }
}

fn modeled_owned_query_result_bytes(records: &Vec<MetricMetadataRecord>) -> usize {
    let vector_bytes = if records.capacity() == 0 {
        0
    } else {
        records
            .capacity()
            .saturating_mul(std::mem::size_of::<MetricMetadataRecord>())
            .saturating_add(METADATA_ALLOCATION_ALLOWANCE_BYTES)
    };
    records.iter().fold(vector_bytes, |bytes, record| {
        bytes
            .saturating_add(modeled_owned_string_bytes(&record.tenant_id))
            .saturating_add(modeled_owned_string_bytes(&record.metric_family_name))
            .saturating_add(modeled_owned_string_bytes(&record.help))
            .saturating_add(modeled_owned_string_bytes(&record.unit))
    })
}

fn clone_query_records(
    entries: &MetricMetadataEntries,
    tenant_id: &str,
    metric: Option<&str>,
    count: usize,
    limit: usize,
    execution: Option<&QueryExecution>,
) -> Result<Vec<MetricMetadataRecord>, CloneQueryRecordsError> {
    let mut out = Vec::new();
    out.try_reserve_exact(count)
        .map_err(|_| CloneQueryRecordsError::Allocation)?;
    if limit == 0 {
        return Ok(out);
    }
    for ((entry_tenant, entry_metric), entry) in entries {
        if let Some(execution) = execution {
            execution
                .checkpoint()
                .map_err(CloneQueryRecordsError::Budget)?;
        }
        if entry_tenant != tenant_id {
            continue;
        }
        if metric.is_some_and(|metric| metric != entry_metric) {
            continue;
        }
        out.push(try_clone_record(entry).map_err(|()| CloneQueryRecordsError::Allocation)?);
        if out.len() >= limit {
            break;
        }
    }
    Ok(out)
}

#[derive(Debug)]
enum CloneQueryRecordsError {
    Budget(QueryBudgetError),
    Allocation,
}

fn try_clone_record(entry: &MetricMetadataRecord) -> Result<MetricMetadataRecord, ()> {
    Ok(MetricMetadataRecord {
        tenant_id: try_clone_string(&entry.tenant_id)?,
        metric_family_name: try_clone_string(&entry.metric_family_name)?,
        metric_type: entry.metric_type,
        help: try_clone_string(&entry.help)?,
        unit: try_clone_string(&entry.unit)?,
        updated_unix_ms: entry.updated_unix_ms,
    })
}

fn try_clone_string(value: &str) -> Result<String, ()> {
    let mut cloned = String::new();
    cloned.try_reserve_exact(value.len()).map_err(|_| ())?;
    cloned.push_str(value);
    Ok(cloned)
}

fn modeled_final_state(
    state: &MetricMetadataStoreState,
    staged: &MetricMetadataEntries,
) -> (usize, usize) {
    let mut entry_count = state.entries.len();
    let mut retained_bytes = state.retained_bytes;
    for (key, candidate) in staged {
        if let Some(existing) = state.entries.get(key) {
            retained_bytes = retained_bytes.saturating_sub(modeled_retained_entry_bytes(existing));
        } else {
            entry_count = entry_count.saturating_add(1);
        }
        retained_bytes = retained_bytes.saturating_add(modeled_retained_entry_bytes(candidate));
    }
    (entry_count, retained_bytes)
}

fn metadata_limit_message(dimension: &str, limit: usize, required: usize) -> String {
    let code = match dimension {
        "entry" | "startup entry" => MetricMetadataStoreErrorCode::EntryLimit,
        "record input byte" => MetricMetadataStoreErrorCode::RecordInputBytesLimit,
        "update batch entry" => MetricMetadataStoreErrorCode::UpdateBatchEntriesLimit,
        "update batch byte" => MetricMetadataStoreErrorCode::UpdateBatchBytesLimit,
        "retained byte" => MetricMetadataStoreErrorCode::RetainedBytesLimit,
        "durable file byte" => MetricMetadataStoreErrorCode::DurableFileBytesLimit,
        "startup durable file byte" => MetricMetadataStoreErrorCode::StartupDurableFileBytesLimit,
        "startup record encoded byte" | "startup record input byte" => {
            MetricMetadataStoreErrorCode::StartupRecordBytesLimit
        }
        "startup retained byte" => MetricMetadataStoreErrorCode::StartupRetainedBytesLimit,
        "startup transient byte" => MetricMetadataStoreErrorCode::StartupTransientBytesLimit,
        "write transient byte" | "snapshot transient byte" => {
            MetricMetadataStoreErrorCode::WriteTransientBytesLimit
        }
        "query record" => MetricMetadataStoreErrorCode::QueryRecordsLimit,
        "query result byte" => MetricMetadataStoreErrorCode::QueryResultBytesLimit,
        _ => MetricMetadataStoreErrorCode::Allocation,
    };
    format!(
        "metric_metadata_store_error={}; metric metadata store {dimension} limit exceeded: limit {limit}, required {required}",
        code.as_str()
    )
}

fn metadata_limit_error(
    dimension: &'static str,
    limit: usize,
    required: usize,
) -> tsink::TsinkError {
    tsink::TsinkError::Other(metadata_limit_message(dimension, limit, required))
}

#[derive(Debug, Clone, Copy)]
enum BoundedWriterFailure {
    DurableFileLimit { required: usize },
    TransientLimit { required: usize },
    Allocation,
}

struct BoundedEncodedWriter<'a> {
    bytes: Vec<u8>,
    max_file_bytes: usize,
    base_transient_bytes: usize,
    transient: &'a mut MetadataTransientReservation,
    failure: Option<BoundedWriterFailure>,
}

impl BoundedEncodedWriter<'_> {
    fn encoded_modeled_bytes(encoded_len: usize) -> usize {
        if encoded_len == 0 {
            0
        } else {
            encoded_len.saturating_add(METADATA_ALLOCATION_ALLOWANCE_BYTES)
        }
    }

    fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

impl Write for BoundedEncodedWriter<'_> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        let Some(next_len) = self.bytes.len().checked_add(buffer.len()) else {
            self.failure = Some(BoundedWriterFailure::DurableFileLimit {
                required: usize::MAX,
            });
            return Err(std::io::Error::other(
                "metric metadata durable file limit exceeded",
            ));
        };
        if next_len > self.max_file_bytes {
            self.failure = Some(BoundedWriterFailure::DurableFileLimit { required: next_len });
            return Err(std::io::Error::other(
                "metric metadata durable file limit exceeded",
            ));
        }
        let required_transient = self
            .base_transient_bytes
            .saturating_add(Self::encoded_modeled_bytes(next_len));
        if self.transient.resize(required_transient).is_err() {
            self.failure = Some(BoundedWriterFailure::TransientLimit {
                required: required_transient,
            });
            return Err(std::io::Error::other(
                "metric metadata transient limit exceeded",
            ));
        }
        if self.bytes.try_reserve_exact(buffer.len()).is_err() {
            self.failure = Some(BoundedWriterFailure::Allocation);
            return Err(std::io::Error::other(
                "metric metadata encoding allocation failed",
            ));
        }
        self.bytes.extend_from_slice(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

#[derive(Debug)]
enum EncodeStoreError {
    DurableFileLimit { required: usize },
    TransientLimit { required: usize },
    Allocation,
    Json(serde_json::Error),
}

impl fmt::Display for EncodeStoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DurableFileLimit { required } => write!(
                formatter,
                "metric metadata durable file limit exceeded at {required} bytes"
            ),
            Self::TransientLimit { required } => write!(
                formatter,
                "metric metadata transient limit exceeded at {required} bytes"
            ),
            Self::Allocation => formatter.write_str("metric metadata encoding allocation failed"),
            Self::Json(error) => write!(formatter, "metric metadata encoding failed: {error}"),
        }
    }
}

fn writer_error(
    writer: &BoundedEncodedWriter<'_>,
    json_error: Option<serde_json::Error>,
) -> EncodeStoreError {
    match writer.failure {
        Some(BoundedWriterFailure::DurableFileLimit { required }) => {
            EncodeStoreError::DurableFileLimit { required }
        }
        Some(BoundedWriterFailure::TransientLimit { required }) => {
            EncodeStoreError::TransientLimit { required }
        }
        Some(BoundedWriterFailure::Allocation) => EncodeStoreError::Allocation,
        None => EncodeStoreError::Json(json_error.unwrap_or_else(|| {
            serde_json::Error::io(std::io::Error::other("metric metadata encoding failed"))
        })),
    }
}

struct MergedMetricMetadataRecords<'a> {
    current: std::iter::Peekable<
        std::collections::btree_map::Iter<'a, MetricMetadataKey, MetricMetadataRecord>,
    >,
    staged: std::iter::Peekable<
        std::collections::btree_map::Iter<'a, MetricMetadataKey, MetricMetadataRecord>,
    >,
}

impl<'a> MergedMetricMetadataRecords<'a> {
    fn new(current: &'a MetricMetadataEntries, staged: &'a MetricMetadataEntries) -> Self {
        Self {
            current: current.iter().peekable(),
            staged: staged.iter().peekable(),
        }
    }
}

impl<'a> Iterator for MergedMetricMetadataRecords<'a> {
    type Item = &'a MetricMetadataRecord;

    fn next(&mut self) -> Option<Self::Item> {
        match (self.current.peek(), self.staged.peek()) {
            (Some((current_key, _)), Some((staged_key, _))) => match current_key.cmp(staged_key) {
                CmpOrdering::Less => self.current.next().map(|(_, record)| record),
                CmpOrdering::Equal => {
                    self.current.next();
                    self.staged.next().map(|(_, record)| record)
                }
                CmpOrdering::Greater => self.staged.next().map(|(_, record)| record),
            },
            (Some(_), None) => self.current.next().map(|(_, record)| record),
            (None, Some(_)) => self.staged.next().map(|(_, record)| record),
            (None, None) => None,
        }
    }
}

fn encode_store(
    current: &MetricMetadataEntries,
    staged: &MetricMetadataEntries,
    max_file_bytes: usize,
    transient: &mut MetadataTransientReservation,
) -> Result<Vec<u8>, EncodeStoreError> {
    let base_transient_bytes = usize::try_from(transient.bytes).unwrap_or(usize::MAX);
    let mut writer = BoundedEncodedWriter {
        bytes: Vec::new(),
        max_file_bytes,
        base_transient_bytes,
        transient,
        failure: None,
    };
    writer
        .write_all(METADATA_STORE_PREFIX)
        .map_err(|_| writer_error(&writer, None))?;
    let mut first = true;
    for record in MergedMetricMetadataRecords::new(current, staged) {
        if !first {
            writer
                .write_all(b",")
                .map_err(|_| writer_error(&writer, None))?;
        }
        first = false;
        if let Err(error) = serde_json::to_writer(&mut writer, record) {
            return Err(writer_error(&writer, Some(error)));
        }
    }
    writer
        .write_all(METADATA_STORE_SUFFIX)
        .map_err(|_| writer_error(&writer, None))?;
    Ok(writer.into_bytes())
}

struct RawPersistedMetricMetadataStore<'a> {
    magic_valid: bool,
    schema_version: u16,
    entries: Vec<&'a RawValue>,
}

struct RawPersistedStoreSeed {
    max_entries: usize,
}

impl<'de> DeserializeSeed<'de> for RawPersistedStoreSeed {
    type Value = RawPersistedMetricMetadataStore<'de>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(RawPersistedStoreVisitor {
            max_entries: self.max_entries,
        })
    }
}

struct RawPersistedStoreVisitor {
    max_entries: usize,
}

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "snake_case")]
enum RawStoreField {
    Magic,
    SchemaVersion,
    Entries,
    #[serde(other)]
    Ignore,
}

impl<'de> Visitor<'de> for RawPersistedStoreVisitor {
    type Value = RawPersistedMetricMetadataStore<'de>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a metric metadata store object")
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut magic_valid = None;
        let mut schema_version = None;
        let mut entries = None;
        while let Some(field) = map.next_key::<RawStoreField>()? {
            match field {
                RawStoreField::Magic => {
                    if magic_valid.is_some() {
                        return Err(A::Error::custom(
                            "duplicate metric metadata store magic field",
                        ));
                    }
                    let magic = map.next_value::<&str>()?;
                    magic_valid = Some(magic == METADATA_STORE_MAGIC);
                }
                RawStoreField::SchemaVersion => {
                    if schema_version.is_some() {
                        return Err(A::Error::custom(
                            "duplicate metric metadata schema_version field",
                        ));
                    }
                    schema_version = Some(map.next_value::<u16>()?);
                }
                RawStoreField::Entries => {
                    if entries.is_some() {
                        return Err(A::Error::custom("duplicate metric metadata entries field"));
                    }
                    entries = Some(map.next_value_seed(RawEntriesSeed {
                        max_entries: self.max_entries,
                    })?);
                }
                RawStoreField::Ignore => {
                    map.next_value::<IgnoredAny>()?;
                }
            }
        }
        Ok(RawPersistedMetricMetadataStore {
            magic_valid: magic_valid
                .ok_or_else(|| A::Error::custom("missing metric metadata store magic field"))?,
            schema_version: schema_version
                .ok_or_else(|| A::Error::custom("missing metric metadata schema_version field"))?,
            entries: entries
                .ok_or_else(|| A::Error::custom("missing metric metadata entries field"))?,
        })
    }
}

struct RawEntriesSeed {
    max_entries: usize,
}

impl<'de> DeserializeSeed<'de> for RawEntriesSeed {
    type Value = Vec<&'de RawValue>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(RawEntriesVisitor {
            max_entries: self.max_entries,
        })
    }
}

struct RawEntriesVisitor {
    max_entries: usize,
}

impl<'de> Visitor<'de> for RawEntriesVisitor {
    type Value = Vec<&'de RawValue>;

    fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("a bounded metric metadata entries array")
    }

    fn visit_seq<A>(self, mut sequence: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut entries = Vec::new();
        while let Some(entry) = sequence.next_element::<&'de RawValue>()? {
            if entries.len() >= self.max_entries {
                return Err(A::Error::custom(
                    "metric metadata startup entry limit exceeded",
                ));
            }
            if entries.len() == entries.capacity() {
                entries
                    .try_reserve_exact(1)
                    .map_err(|_| A::Error::custom("metric metadata startup allocation failed"))?;
            }
            entries.push(entry);
        }
        Ok(entries)
    }
}

fn unix_timestamp_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(u128::from(u64::MAX)) as u64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prom_write::NormalizedMetricMetadataUpdate;
    use std::sync::{mpsc, Barrier};
    use std::thread;
    use tempfile::TempDir;

    fn metadata_update(metric_family_name: &str, help: &str) -> NormalizedMetricMetadataUpdate {
        NormalizedMetricMetadataUpdate {
            metric_family_name: metric_family_name.to_string(),
            metric_type: MetricType::Gauge,
            help: help.to_string(),
            unit: "widgets".to_string(),
        }
    }

    fn bounded_test_config() -> MetricMetadataStoreConfig {
        MetricMetadataStoreConfig {
            max_entries: 64,
            max_record_bytes: 4 * 1024,
            max_update_batch_entries: 16,
            max_update_batch_bytes: 16 * 1024,
            max_retained_bytes: 1024 * 1024,
            max_durable_file_bytes: 1024 * 1024,
            max_startup_transient_bytes: 4 * 1024 * 1024,
            max_write_transient_bytes: 2 * 1024 * 1024,
            max_query_records: 64,
            max_query_result_bytes: 1024 * 1024,
        }
    }

    fn query_budget(memory_bytes: u64) -> tsink::QueryBudget {
        tsink::QueryBudget::new(tsink::QueryBudgetLimits {
            max_shared_memory_bytes: Some(memory_bytes),
            per_query: tsink::QueryWorkLimits {
                max_memory_bytes: Some(memory_bytes),
                ..tsink::QueryWorkLimits::default()
            },
            ..tsink::QueryBudgetLimits::default()
        })
        .expect("query budget should build")
    }

    #[test]
    fn metadata_store_config_and_input_boundaries_are_exact() {
        assert!(MetricMetadataStoreConfig::default().validate().is_ok());
        assert!(MetricMetadataStoreConfig {
            max_entries: 0,
            ..MetricMetadataStoreConfig::default()
        }
        .validate()
        .is_err());

        let update = metadata_update("m", "help");
        let exact_record_bytes =
            modeled_record_input_bytes("t", &update.metric_family_name, &update.help, &update.unit);
        let exact = MetricMetadataStore::in_memory_with_config(MetricMetadataStoreConfig {
            max_record_bytes: exact_record_bytes,
            max_update_batch_bytes: exact_record_bytes,
            max_entries: 1,
            max_update_batch_entries: 1,
            ..bounded_test_config()
        })
        .expect("exact record config should build");
        assert_eq!(exact.config().max_record_bytes, exact_record_bytes);
        assert_eq!(
            exact
                .apply_updates("t", std::slice::from_ref(&update))
                .expect("exact record bytes should fit"),
            1
        );

        let below = MetricMetadataStore::in_memory_with_config(MetricMetadataStoreConfig {
            max_record_bytes: exact_record_bytes - 1,
            max_update_batch_bytes: exact_record_bytes,
            ..bounded_test_config()
        })
        .expect("below-bound config should build");
        let error = below
            .apply_updates("t", std::slice::from_ref(&update))
            .expect_err("one byte above the record limit must fail");
        assert!(error.to_string().contains("record input byte limit"));
        assert_eq!(
            MetricMetadataStore::classify_apply_error(&error),
            Some(MetricMetadataStoreErrorCode::RecordInputBytesLimit)
        );
        let snapshot = below.metrics_snapshot().unwrap();
        assert_eq!(snapshot.entries, 0);
        assert_eq!(snapshot.record_rejections_total, 1);
        assert_eq!(snapshot.transient_bytes, 0);

        let entries = MetricMetadataStore::in_memory_with_config(MetricMetadataStoreConfig {
            max_entries: 1,
            max_update_batch_entries: 1,
            ..bounded_test_config()
        })
        .unwrap();
        entries
            .apply_updates("t", &[metadata_update("a", "a")])
            .unwrap();
        let error = entries
            .apply_updates("t", &[metadata_update("b", "b")])
            .expect_err("N+1 entries must fail");
        assert!(error.to_string().contains("entry limit"));
        assert_eq!(
            MetricMetadataStore::classify_apply_error(&error),
            Some(MetricMetadataStoreErrorCode::EntryLimit)
        );
        assert_eq!(entries.metrics_snapshot().unwrap().entries, 1);

        let batch = MetricMetadataStore::in_memory_with_config(MetricMetadataStoreConfig {
            max_entries: 2,
            max_update_batch_entries: 2,
            ..bounded_test_config()
        })
        .unwrap();
        batch
            .apply_updates("t", &[metadata_update("a", "a"), metadata_update("b", "b")])
            .expect("N updates should fit");
        let rejected_batch =
            MetricMetadataStore::in_memory_with_config(MetricMetadataStoreConfig {
                max_entries: 2,
                max_update_batch_entries: 1,
                ..bounded_test_config()
            })
            .unwrap();
        assert!(rejected_batch
            .apply_updates(
                "t",
                &[metadata_update("a", "a"), metadata_update("b", "b"),],
            )
            .expect_err("N+1 batch entries must fail")
            .to_string()
            .contains("update batch entry limit"));

        let byte_updates = [metadata_update("a", "one"), metadata_update("b", "two")];
        let exact_batch_bytes = byte_updates.iter().fold(0usize, |bytes, update| {
            bytes.saturating_add(modeled_record_input_bytes(
                "t",
                &update.metric_family_name,
                &update.help,
                &update.unit,
            ))
        });
        let exact_batch = MetricMetadataStore::in_memory_with_config(MetricMetadataStoreConfig {
            max_update_batch_bytes: exact_batch_bytes,
            max_record_bytes: exact_batch_bytes,
            max_entries: 2,
            max_update_batch_entries: 2,
            ..bounded_test_config()
        })
        .unwrap();
        exact_batch
            .apply_updates("t", &byte_updates)
            .expect("N update bytes should fit");
        let byte_below = MetricMetadataStore::in_memory_with_config(MetricMetadataStoreConfig {
            max_update_batch_bytes: exact_batch_bytes - 1,
            max_record_bytes: exact_batch_bytes - 1,
            max_entries: 2,
            max_update_batch_entries: 2,
            ..bounded_test_config()
        })
        .unwrap();
        assert!(byte_below
            .apply_updates("t", &byte_updates)
            .expect_err("N+1 update bytes must fail")
            .to_string()
            .contains("update batch byte limit"));
    }

    #[test]
    fn retained_bytes_have_exact_boundary_and_replacement_does_not_consume_an_entry() {
        let update = metadata_update("metric", "original");
        let probe = MetricMetadataStore::in_memory_with_config(bounded_test_config()).unwrap();
        probe
            .apply_updates("tenant", std::slice::from_ref(&update))
            .unwrap();
        let exact_retained = usize::try_from(probe.metrics_snapshot().unwrap().retained_bytes)
            .expect("retained bytes should fit usize");

        let exact = MetricMetadataStore::in_memory_with_config(MetricMetadataStoreConfig {
            max_entries: 1,
            max_update_batch_entries: 1,
            max_retained_bytes: exact_retained,
            ..bounded_test_config()
        })
        .unwrap();
        exact
            .apply_updates("tenant", std::slice::from_ref(&update))
            .expect("exact retained bytes should fit");
        assert_eq!(
            exact.metrics_snapshot().unwrap().retained_bytes,
            exact_retained as u64
        );

        let below = MetricMetadataStore::in_memory_with_config(MetricMetadataStoreConfig {
            max_entries: 1,
            max_update_batch_entries: 1,
            max_retained_bytes: exact_retained - 1,
            ..bounded_test_config()
        })
        .unwrap();
        let error = below
            .apply_updates("tenant", std::slice::from_ref(&update))
            .expect_err("N-1 retained bytes must fail");
        assert!(error.to_string().contains("retained byte limit"));
        assert_eq!(
            MetricMetadataStore::classify_apply_error(&error),
            Some(MetricMetadataStoreErrorCode::RetainedBytesLimit)
        );
        let rejected = below.metrics_snapshot().unwrap();
        assert_eq!(rejected.entries, 0);
        assert_eq!(rejected.retained_rejections_total, 1);
        assert_eq!(rejected.transient_bytes, 0);

        let replacement = MetricMetadataStore::in_memory_with_config(MetricMetadataStoreConfig {
            max_entries: 1,
            max_update_batch_entries: 1,
            ..bounded_test_config()
        })
        .unwrap();
        replacement
            .apply_updates("tenant", &[metadata_update("metric", "before")])
            .unwrap();
        replacement
            .apply_updates("tenant", &[metadata_update("metric", "after")])
            .expect("replacement must not consume another entry");
        assert_eq!(replacement.metrics_snapshot().unwrap().entries, 1);
        assert_eq!(
            replacement.query("tenant", Some("metric"), 1).unwrap()[0].help,
            "after"
        );
        assert!(replacement
            .apply_updates("tenant", &[metadata_update("other_", "after")])
            .expect_err("a distinct entry at N+1 must fail")
            .to_string()
            .contains("entry limit"));
    }

    #[test]
    fn durable_and_write_transient_bytes_have_exact_boundaries() {
        let update = metadata_update("metric", "bounded-persistence");
        let probe_dir = TempDir::new().unwrap();
        let probe =
            MetricMetadataStore::open_with_config(Some(probe_dir.path()), bounded_test_config())
                .unwrap();
        probe
            .apply_updates("tenant", std::slice::from_ref(&update))
            .unwrap();
        let probe_snapshot = probe.metrics_snapshot().unwrap();
        let exact_file = usize::try_from(probe_snapshot.durable_file_bytes).unwrap();
        let exact_transient = usize::try_from(probe_snapshot.peak_transient_bytes).unwrap();
        assert!(exact_file > 1);
        assert!(exact_transient > exact_file);
        assert_eq!(probe_snapshot.transient_bytes, 0);

        let exact_dir = TempDir::new().unwrap();
        let exact = MetricMetadataStore::open_with_config(
            Some(exact_dir.path()),
            MetricMetadataStoreConfig {
                max_durable_file_bytes: exact_file,
                max_write_transient_bytes: exact_transient,
                ..bounded_test_config()
            },
        )
        .unwrap();
        exact
            .apply_updates("tenant", std::slice::from_ref(&update))
            .expect("exact durable and transient bytes should fit");
        assert_eq!(exact.metrics_snapshot().unwrap().transient_bytes, 0);

        let file_below_dir = TempDir::new().unwrap();
        let file_below = MetricMetadataStore::open_with_config(
            Some(file_below_dir.path()),
            MetricMetadataStoreConfig {
                max_durable_file_bytes: exact_file - 1,
                ..bounded_test_config()
            },
        )
        .unwrap();
        let error = file_below
            .apply_updates("tenant", std::slice::from_ref(&update))
            .expect_err("N-1 durable bytes must fail");
        assert!(error.to_string().contains("durable file byte limit"));
        assert_eq!(
            MetricMetadataStore::classify_apply_error(&error),
            Some(MetricMetadataStoreErrorCode::DurableFileBytesLimit)
        );
        let file_rejected = file_below.metrics_snapshot().unwrap();
        assert_eq!(file_rejected.entries, 0);
        assert_eq!(file_rejected.durable_file_rejections_total, 1);
        assert_eq!(file_rejected.transient_bytes, 0);
        assert!(!file_below.file_path().unwrap().exists());

        let transient_below_dir = TempDir::new().unwrap();
        let transient_below = MetricMetadataStore::open_with_config(
            Some(transient_below_dir.path()),
            MetricMetadataStoreConfig {
                max_write_transient_bytes: exact_transient - 1,
                ..bounded_test_config()
            },
        )
        .unwrap();
        let error = transient_below
            .apply_updates("tenant", &[update])
            .expect_err("N-1 transient bytes must fail");
        assert!(error.to_string().contains("write transient byte limit"));
        assert_eq!(
            MetricMetadataStore::classify_apply_error(&error),
            Some(MetricMetadataStoreErrorCode::WriteTransientBytesLimit)
        );
        let transient_rejected = transient_below.metrics_snapshot().unwrap();
        assert_eq!(transient_rejected.entries, 0);
        assert_eq!(transient_rejected.transient_rejections_total, 1);
        assert_eq!(transient_rejected.transient_bytes, 0);
        assert!(!transient_below.file_path().unwrap().exists());
    }

    #[test]
    fn startup_is_bounded_before_decode_and_reopen_reconciles_accounting() {
        let temp_dir = TempDir::new().unwrap();
        let config = bounded_test_config();
        let store = MetricMetadataStore::open_with_config(Some(temp_dir.path()), config).unwrap();
        store
            .apply_updates("tenant", &[metadata_update("metric", "persisted")])
            .unwrap();
        let before = store.metrics_snapshot().unwrap();
        drop(store);

        let reopened =
            MetricMetadataStore::open_with_config(Some(temp_dir.path()), config).unwrap();
        let after = reopened.metrics_snapshot().unwrap();
        assert_eq!(after.entries, before.entries);
        assert_eq!(after.retained_bytes, before.retained_bytes);
        assert_eq!(after.durable_file_bytes, before.durable_file_bytes);
        assert!(after.peak_transient_bytes > 0);
        assert_eq!(after.transient_bytes, 0);
        let exact_startup = usize::try_from(after.peak_transient_bytes).unwrap();
        drop(reopened);

        MetricMetadataStore::open_with_config(
            Some(temp_dir.path()),
            MetricMetadataStoreConfig {
                max_startup_transient_bytes: exact_startup,
                ..config
            },
        )
        .expect("exact startup transient bytes should fit");
        assert!(MetricMetadataStore::open_with_config(
            Some(temp_dir.path()),
            MetricMetadataStoreConfig {
                max_startup_transient_bytes: exact_startup - 1,
                ..config
            },
        )
        .expect_err("N-1 startup transient bytes must fail")
        .contains("startup transient byte limit"));

        let oversized_dir = TempDir::new().unwrap();
        std::fs::write(
            oversized_dir.path().join(METADATA_STORE_FILE_NAME),
            vec![b'x'; 33],
        )
        .unwrap();
        let oversized_config = MetricMetadataStoreConfig {
            max_durable_file_bytes: 32,
            ..bounded_test_config()
        };
        assert!(MetricMetadataStore::open_with_config(
            Some(oversized_dir.path()),
            oversized_config
        )
        .expect_err("oversized startup file must fail before reading it")
        .contains("startup durable file byte limit"));

        let record_dir = TempDir::new().unwrap();
        let record = MetricMetadataRecord {
            tenant_id: "tenant".to_string(),
            metric_family_name: "metric".to_string(),
            metric_type: MetricType::Gauge as i32,
            help: "record-too-large".to_string(),
            unit: "widgets".to_string(),
            updated_unix_ms: 1,
        };
        let raw_record = serde_json::to_string(&record).unwrap();
        let raw_store = format!(
            "{}{}{}",
            std::str::from_utf8(METADATA_STORE_PREFIX).unwrap(),
            raw_record,
            std::str::from_utf8(METADATA_STORE_SUFFIX).unwrap()
        );
        std::fs::write(record_dir.path().join(METADATA_STORE_FILE_NAME), raw_store).unwrap();
        let record_config = MetricMetadataStoreConfig {
            max_record_bytes: 8,
            max_update_batch_bytes: 8,
            ..bounded_test_config()
        };
        let error = MetricMetadataStore::open_with_config(Some(record_dir.path()), record_config)
            .expect_err("oversized decoded record must fail");
        assert!(error.contains("startup record input byte limit"), "{error}");
        assert!(!error.contains("record-too-large"));

        let corrupt_dir = TempDir::new().unwrap();
        std::fs::write(
            corrupt_dir.path().join(METADATA_STORE_FILE_NAME),
            b"{\"magic\":\"SECRET-SENTINEL\",\"entries\":[",
        )
        .unwrap();
        let error =
            MetricMetadataStore::open_with_config(Some(corrupt_dir.path()), bounded_test_config())
                .expect_err("corrupt startup JSON must fail");
        assert!(error.len() < 512);
        assert!(!error.contains("SECRET-SENTINEL"));
    }

    #[test]
    fn concurrent_final_retained_byte_updates_cannot_overcommit() {
        let candidate = MetricMetadataRecord {
            tenant_id: "tenant".to_string(),
            metric_family_name: "metric-a".to_string(),
            metric_type: MetricType::Gauge as i32,
            help: "help".to_string(),
            unit: "widgets".to_string(),
            updated_unix_ms: 1,
        };
        let exact_one_entry = modeled_empty_store_retained_bytes()
            .saturating_add(modeled_retained_entry_bytes(&candidate));
        let store = Arc::new(
            MetricMetadataStore::in_memory_with_config(MetricMetadataStoreConfig {
                max_entries: 2,
                max_update_batch_entries: 1,
                max_retained_bytes: exact_one_entry,
                ..bounded_test_config()
            })
            .unwrap(),
        );
        let barrier = Arc::new(Barrier::new(3));
        let mut writers = Vec::new();
        for metric in ["metric-a", "metric-b"] {
            let store = Arc::clone(&store);
            let barrier = Arc::clone(&barrier);
            writers.push(thread::spawn(move || {
                barrier.wait();
                store.apply_updates("tenant", &[metadata_update(metric, "help")])
            }));
        }
        barrier.wait();
        let outcomes = writers
            .into_iter()
            .map(|writer| writer.join().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(outcomes.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(outcomes.iter().filter(|result| result.is_err()).count(), 1);
        let snapshot = store.metrics_snapshot().unwrap();
        assert_eq!(snapshot.entries, 1);
        assert_eq!(snapshot.retained_bytes, exact_one_entry as u64);
        assert_eq!(snapshot.retained_rejections_total, 1);
        assert_eq!(snapshot.transient_bytes, 0);
    }

    #[test]
    fn metrics_snapshot_does_not_wait_for_store_state_lock() {
        let store = Arc::new(MetricMetadataStore::in_memory());
        store
            .apply_updates("tenant", &[metadata_update("metric", "help")])
            .unwrap();
        let expected = store.metrics_snapshot().unwrap();
        let task_store = Arc::clone(&store);
        let state_guard = store.write_entries().unwrap();
        let (sender, receiver) = mpsc::channel();
        let worker = thread::spawn(move || {
            sender
                .send(task_store.metrics_snapshot())
                .expect("snapshot result receiver should remain live");
        });

        let observed = receiver.recv_timeout(Duration::from_secs(2));
        drop(state_guard);
        worker.join().expect("snapshot worker should finish");
        let observed = observed
            .expect("metrics snapshot must not wait for the store state lock")
            .expect("atomic metrics snapshot should be available");
        assert_eq!(observed.entries, expected.entries);
        assert_eq!(observed.retained_bytes, expected.retained_bytes);
        assert_eq!(observed.peak_retained_bytes, expected.peak_retained_bytes);
    }

    #[test]
    fn guarded_query_lock_wait_honors_cancellation_and_deadline_without_residuals() {
        let store = Arc::new(MetricMetadataStore::in_memory());
        store
            .apply_updates("tenant", &[metadata_update("metric", "help")])
            .unwrap();
        let state_guard = store.write_entries().unwrap();

        let cancellation_budget = query_budget(1024 * 1024);
        let cancellation = tsink::QueryCancellationToken::new();
        let cancellation_execution = cancellation_budget
            .begin_query_with_token(cancellation.clone())
            .unwrap();
        let task_store = Arc::clone(&store);
        let task_execution = cancellation_execution.clone();
        let started = Arc::new(Barrier::new(2));
        let task_started = Arc::clone(&started);
        let (sender, receiver) = mpsc::channel();
        let worker = thread::spawn(move || {
            task_started.wait();
            let cancelled = matches!(
                task_store.query_with_execution_result(
                    "tenant",
                    Some("metric"),
                    1,
                    &task_execution
                ),
                Err(MetricMetadataQueryError::Budget(
                    QueryBudgetError::Cancelled
                ))
            );
            sender
                .send(cancelled)
                .expect("cancellation result receiver should remain live");
        });
        started.wait();
        thread::sleep(Duration::from_millis(20));
        cancellation.cancel();
        let cancelled = receiver.recv_timeout(Duration::from_secs(2));
        worker.join().expect("cancelled query worker should finish");
        assert!(cancelled.expect("cancelled lock waiter should finish promptly"));
        assert_eq!(cancellation_execution.snapshot().memory_reserved_bytes, 0);
        drop(cancellation_execution);
        let cancellation_snapshot = cancellation_budget.snapshot();
        assert_eq!(cancellation_snapshot.active_queries, 0);
        assert_eq!(cancellation_snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(cancellation_snapshot.cancellations_total, 1);

        let deadline_budget = tsink::QueryBudget::new(tsink::QueryBudgetLimits {
            per_query: tsink::QueryWorkLimits {
                max_wall_time: Some(Duration::from_millis(10)),
                ..tsink::QueryWorkLimits::default()
            },
            ..tsink::QueryBudgetLimits::default()
        })
        .expect("deadline query budget should build");
        let deadline_execution = deadline_budget.begin_query().unwrap();
        let deadline_error = store
            .query_with_execution_result("tenant", Some("metric"), 1, &deadline_execution)
            .expect_err("state-lock wait must honor the query deadline");
        assert!(matches!(
            deadline_error,
            MetricMetadataQueryError::Budget(QueryBudgetError::DeadlineExceeded)
        ));
        assert_eq!(deadline_execution.snapshot().memory_reserved_bytes, 0);
        drop(deadline_execution);
        drop(state_guard);
        let deadline_snapshot = deadline_budget.snapshot();
        assert_eq!(deadline_snapshot.active_queries, 0);
        assert_eq!(deadline_snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(deadline_snapshot.deadline_exceeded_total, 1);
        assert_eq!(store.metrics_snapshot().unwrap().query_result_bytes, 0);
    }

    #[test]
    fn guarded_query_memory_has_exact_boundary_and_zero_residual() {
        let store = MetricMetadataStore::in_memory_with_config(MetricMetadataStoreConfig {
            max_query_records: 1,
            ..bounded_test_config()
        })
        .unwrap();
        store
            .apply_updates("tenant", &[metadata_update("metric", "query-result")])
            .unwrap();

        let probe_budget = query_budget(1024 * 1024);
        let probe_execution = probe_budget.begin_query().unwrap();
        let probe = store
            .query_with_execution_result("tenant", Some("metric"), 1, &probe_execution)
            .unwrap();
        assert_eq!(probe.records().len(), 1);
        let exact = probe.reserved_memory_bytes();
        assert!(exact > 1);
        assert_eq!(probe_execution.snapshot().memory_reserved_bytes, exact);
        assert_eq!(store.metrics_snapshot().unwrap().query_result_bytes, exact);
        drop(probe);
        assert_eq!(probe_execution.snapshot().memory_reserved_bytes, 0);
        assert_eq!(store.metrics_snapshot().unwrap().query_result_bytes, 0);
        drop(probe_execution);
        assert_eq!(probe_budget.snapshot().shared_reserved_memory_bytes, 0);

        let exact_budget = query_budget(exact);
        let exact_execution = exact_budget.begin_query().unwrap();
        let exact_result = store
            .query_with_execution_result("tenant", Some("metric"), 1, &exact_execution)
            .expect("exact query result bytes should fit");
        assert_eq!(exact_result.reserved_memory_bytes(), exact);
        drop(exact_result);
        drop(exact_execution);
        assert_eq!(exact_budget.snapshot().shared_reserved_memory_bytes, 0);

        let below_budget = query_budget(exact - 1);
        let below_execution = below_budget.begin_query().unwrap();
        assert!(matches!(
            store.query_with_execution_result("tenant", Some("metric"), 1, &below_execution),
            Err(MetricMetadataQueryError::Budget(
                QueryBudgetError::LimitExceeded(_)
            ))
        ));
        assert_eq!(below_execution.snapshot().memory_reserved_bytes, 0);
        assert_eq!(store.metrics_snapshot().unwrap().query_result_bytes, 0);
        drop(below_execution);
        assert_eq!(below_budget.snapshot().shared_reserved_memory_bytes, 0);

        let result_below = MetricMetadataStore::in_memory_with_config(MetricMetadataStoreConfig {
            max_query_records: 1,
            max_query_result_bytes: usize::try_from(exact).unwrap() - 1,
            ..bounded_test_config()
        })
        .unwrap();
        result_below
            .apply_updates("tenant", &[metadata_update("metric", "query-result")])
            .unwrap();
        let budget = query_budget(1024 * 1024);
        let execution = budget.begin_query().unwrap();
        let result_limit = result_below
            .query_with_execution_result("tenant", Some("metric"), 1, &execution)
            .expect_err("N+1 result bytes must fail");
        assert!(matches!(
            &result_limit,
            MetricMetadataQueryError::ResultLimit
        ));
        assert_eq!(result_limit.code(), "query_result_bytes_limit");
        assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
        let record_limit = result_below
            .query_with_execution_result("tenant", None, 2, &execution)
            .expect_err("N+1 requested records must fail");
        assert!(matches!(
            &record_limit,
            MetricMetadataQueryError::RecordLimit
        ));
        assert_eq!(record_limit.code(), "query_records_limit");
        drop(execution);
        assert_eq!(budget.snapshot().shared_reserved_memory_bytes, 0);

        let cancelled_budget = query_budget(1024 * 1024);
        let cancellation = tsink::QueryCancellationToken::new();
        let cancelled_execution = cancelled_budget
            .begin_query_with_token(cancellation.clone())
            .unwrap();
        cancellation.cancel();
        let cancelled = store
            .query_with_execution_result("tenant", Some("metric"), 1, &cancelled_execution)
            .expect_err("a cancelled query must fail without cloning a result");
        assert!(matches!(
            &cancelled,
            MetricMetadataQueryError::Budget(QueryBudgetError::Cancelled)
        ));
        assert_eq!(cancelled.code(), "query_budget");
        assert_eq!(cancelled_execution.snapshot().memory_reserved_bytes, 0);
        assert_eq!(store.metrics_snapshot().unwrap().query_result_bytes, 0);
        drop(cancelled_execution);
        let cancelled_snapshot = cancelled_budget.snapshot();
        assert_eq!(cancelled_snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(cancelled_snapshot.active_queries, 0);
        assert_eq!(cancelled_snapshot.cancellations_total, 1);
    }

    #[test]
    fn snapshot_output_has_exact_boundary_and_no_transient_residual() {
        let update = metadata_update("metric", "snapshot");
        let probe_root = TempDir::new().unwrap();
        let probe_snapshot_dir = probe_root.path().join("probe");
        std::fs::create_dir_all(&probe_snapshot_dir).unwrap();
        let probe = MetricMetadataStore::in_memory_with_config(bounded_test_config()).unwrap();
        probe
            .apply_updates("tenant", std::slice::from_ref(&update))
            .unwrap();
        probe.snapshot_into(&probe_snapshot_dir).unwrap();
        let exact_file = usize::try_from(
            std::fs::metadata(probe_snapshot_dir.join(METADATA_STORE_FILE_NAME))
                .unwrap()
                .len(),
        )
        .unwrap();
        assert_eq!(probe.metrics_snapshot().unwrap().transient_bytes, 0);

        let exact_root = TempDir::new().unwrap();
        let exact_snapshot_dir = exact_root.path().join("exact");
        std::fs::create_dir_all(&exact_snapshot_dir).unwrap();
        let exact = MetricMetadataStore::in_memory_with_config(MetricMetadataStoreConfig {
            max_durable_file_bytes: exact_file,
            ..bounded_test_config()
        })
        .unwrap();
        exact
            .apply_updates("tenant", std::slice::from_ref(&update))
            .unwrap();
        exact
            .snapshot_into(&exact_snapshot_dir)
            .expect("exact snapshot output bytes should fit");
        assert_eq!(exact.metrics_snapshot().unwrap().transient_bytes, 0);

        let below_root = TempDir::new().unwrap();
        let below_snapshot_dir = below_root.path().join("below");
        std::fs::create_dir_all(&below_snapshot_dir).unwrap();
        let below = MetricMetadataStore::in_memory_with_config(MetricMetadataStoreConfig {
            max_durable_file_bytes: exact_file - 1,
            ..bounded_test_config()
        })
        .unwrap();
        below.apply_updates("tenant", &[update]).unwrap();
        assert!(below
            .snapshot_into(&below_snapshot_dir)
            .expect_err("N-1 snapshot bytes must fail")
            .contains("durable file byte limit"));
        assert_eq!(below.metrics_snapshot().unwrap().transient_bytes, 0);
        assert!(!below_snapshot_dir.join(METADATA_STORE_FILE_NAME).exists());
    }

    #[test]
    fn metadata_store_persists_last_write_wins_updates() {
        let temp_dir = TempDir::new().expect("temp dir should build");
        let store = MetricMetadataStore::open(Some(temp_dir.path()))
            .expect("persistent metadata store should open");

        store
            .apply_updates(
                "tenant-a",
                &[NormalizedMetricMetadataUpdate {
                    metric_family_name: "http_requests_total".to_string(),
                    metric_type: MetricType::Counter,
                    help: "original".to_string(),
                    unit: "requests".to_string(),
                }],
            )
            .expect("first metadata update should succeed");
        store
            .apply_updates(
                "tenant-a",
                &[NormalizedMetricMetadataUpdate {
                    metric_family_name: "http_requests_total".to_string(),
                    metric_type: MetricType::Counter,
                    help: "updated".to_string(),
                    unit: "requests".to_string(),
                }],
            )
            .expect("second metadata update should succeed");

        let reopened = MetricMetadataStore::open(Some(temp_dir.path()))
            .expect("reopened metadata store should load");
        let records = reopened
            .query("tenant-a", Some("http_requests_total"), 10)
            .expect("metadata query should succeed");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].help, "updated");
        assert_eq!(
            reopened
                .file_path()
                .expect("persistent store should expose file path")
                .file_name()
                .and_then(|value| value.to_str()),
            Some(METADATA_STORE_FILE_NAME)
        );
    }

    #[test]
    fn metadata_store_queries_are_tenant_scoped_and_sorted() {
        let store = MetricMetadataStore::in_memory();
        store
            .apply_updates(
                "tenant-b",
                &[NormalizedMetricMetadataUpdate {
                    metric_family_name: "zeta_metric".to_string(),
                    metric_type: MetricType::Gauge,
                    help: "z".to_string(),
                    unit: "".to_string(),
                }],
            )
            .expect("metadata update should succeed");
        store
            .apply_updates(
                "tenant-a",
                &[
                    NormalizedMetricMetadataUpdate {
                        metric_family_name: "zeta_metric".to_string(),
                        metric_type: MetricType::Gauge,
                        help: "z".to_string(),
                        unit: "".to_string(),
                    },
                    NormalizedMetricMetadataUpdate {
                        metric_family_name: "alpha_metric".to_string(),
                        metric_type: MetricType::Counter,
                        help: "a".to_string(),
                        unit: "requests".to_string(),
                    },
                ],
            )
            .expect("metadata updates should succeed");

        let records = store
            .query("tenant-a", None, 10)
            .expect("metadata query should succeed");
        assert_eq!(
            records
                .iter()
                .map(|record| record.metric_family_name.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha_metric", "zeta_metric"]
        );
        assert_eq!(
            store
                .query("tenant-a", None, 1)
                .expect("limited metadata query should succeed")
                .len(),
            1
        );
        assert_eq!(
            store
                .query("tenant-b", None, 10)
                .expect("other tenant metadata query should succeed")
                .len(),
            1
        );
    }

    #[test]
    fn metadata_store_snapshot_writes_snapshot_copy() {
        let temp_dir = TempDir::new().expect("temp dir should build");
        let snapshot_dir = temp_dir.path().join("snapshot");
        std::fs::create_dir_all(&snapshot_dir).expect("snapshot dir should build");
        let store = MetricMetadataStore::in_memory();
        store
            .apply_updates(
                "tenant-a",
                &[NormalizedMetricMetadataUpdate {
                    metric_family_name: "cpu_usage".to_string(),
                    metric_type: MetricType::Gauge,
                    help: "CPU usage".to_string(),
                    unit: "percent".to_string(),
                }],
            )
            .expect("metadata update should succeed");

        store
            .snapshot_into(&snapshot_dir)
            .expect("metadata snapshot should succeed");

        let reopened =
            MetricMetadataStore::open(Some(&snapshot_dir)).expect("snapshot copy should reopen");
        let records = reopened
            .query("tenant-a", None, 10)
            .expect("metadata query should succeed");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].metric_family_name, "cpu_usage");
    }

    #[test]
    fn metadata_store_persistence_failure_does_not_publish_updates() {
        let temp_dir = TempDir::new().expect("temp dir should build");
        let store = MetricMetadataStore::open(Some(temp_dir.path()))
            .expect("persistent metadata store should open");
        store
            .apply_updates(
                "tenant-a",
                &[NormalizedMetricMetadataUpdate {
                    metric_family_name: "http_requests_total".to_string(),
                    metric_type: MetricType::Counter,
                    help: "original".to_string(),
                    unit: "requests".to_string(),
                }],
            )
            .expect("initial metadata update should persist");

        let store_path = store
            .file_path()
            .expect("persistent store should expose file path");
        std::fs::remove_file(store_path).expect("persisted store should be removable");
        std::fs::create_dir(store_path).expect("blocking publication path should build");

        let error = store
            .apply_updates(
                "tenant-a",
                &[
                    NormalizedMetricMetadataUpdate {
                        metric_family_name: "http_requests_total".to_string(),
                        metric_type: MetricType::Counter,
                        help: "unpersisted replacement".to_string(),
                        unit: "requests".to_string(),
                    },
                    NormalizedMetricMetadataUpdate {
                        metric_family_name: "new_metric".to_string(),
                        metric_type: MetricType::Gauge,
                        help: "unpersisted insertion".to_string(),
                        unit: "widgets".to_string(),
                    },
                ],
            )
            .expect_err("publication-path collision should fail persistence");
        assert!(matches!(error, tsink::TsinkError::Io(_)), "{error}");
        assert_eq!(MetricMetadataStore::classify_apply_error(&error), None);

        let records = store
            .query("tenant-a", None, 10)
            .expect("metadata query should succeed after failed persistence");
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].metric_family_name, "http_requests_total");
        assert_eq!(records[0].help, "original");
        let metrics = store.metrics_snapshot().unwrap();
        assert_eq!(metrics.persistence_rejections_total, 1);
        assert_eq!(metrics.transient_bytes, 0);

        assert!(
            std::fs::read_dir(temp_dir.path())
                .expect("metadata directory should remain readable")
                .all(|entry| !entry
                    .expect("metadata directory entry should be readable")
                    .file_name()
                    .to_string_lossy()
                    .starts_with(".metric-metadata-store.json.tmp-")),
            "a failed publication must clean its unique temporary file"
        );
    }

    #[test]
    fn metadata_store_disk_quota_rejection_does_not_publish_updates() {
        let temp_dir = TempDir::new().expect("temp dir should build");
        let budget = tsink::LocalDiskBudget::open(
            temp_dir.path(),
            tsink::LocalDiskLimits {
                max_bytes: Some(64),
                ..tsink::LocalDiskLimits::default()
            },
        )
        .expect("disk budget should open");
        let store = MetricMetadataStore::open_with_disk_budget(
            Some(temp_dir.path()),
            Some(Arc::clone(&budget)),
        )
        .expect("persistent metadata store should open");

        let error = store
            .apply_updates(
                "tenant-a",
                &[NormalizedMetricMetadataUpdate {
                    metric_family_name: "http_requests_total".to_string(),
                    metric_type: MetricType::Counter,
                    help: "a deliberately long help string that exceeds the tiny quota".to_string(),
                    unit: "requests".to_string(),
                }],
            )
            .expect_err("tiny disk quota should reject metadata persistence");
        assert!(matches!(error, tsink::TsinkError::DiskQuotaExceeded { .. }));
        assert_eq!(MetricMetadataStore::classify_apply_error(&error), None);
        assert!(
            store
                .query("tenant-a", None, 10)
                .expect("metadata query should succeed")
                .is_empty(),
            "a rejected update must not become visible"
        );
        assert!(
            !store
                .file_path()
                .expect("persistent store should expose file path")
                .exists(),
            "a rejected update must not publish a store file"
        );
        assert_eq!(
            store
                .metrics_snapshot()
                .unwrap()
                .persistence_rejections_total,
            1
        );

        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, 0);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.rejections_total, 1);
    }

    #[test]
    fn metadata_store_observes_shared_root_usage_and_exact_category_accounting() {
        let temp_dir = TempDir::new().expect("temp dir should build");
        let shared_file = temp_dir.path().join("unrecognized-owner.bin");
        std::fs::write(&shared_file, vec![0_u8; 1_000]).expect("shared file should be written");
        let budget = tsink::LocalDiskBudget::open(
            temp_dir.path(),
            tsink::LocalDiskLimits {
                max_bytes: Some(1_024),
                ..tsink::LocalDiskLimits::default()
            },
        )
        .expect("disk budget should open");
        let store = MetricMetadataStore::open_with_disk_budget(
            Some(temp_dir.path()),
            Some(Arc::clone(&budget)),
        )
        .expect("persistent metadata store should open");
        let update = NormalizedMetricMetadataUpdate {
            metric_family_name: "cpu_usage".to_string(),
            metric_type: MetricType::Gauge,
            help: "CPU usage".to_string(),
            unit: "percent".to_string(),
        };

        let error = store
            .apply_updates("tenant-a", std::slice::from_ref(&update))
            .expect_err("other usage beneath the shared root should reject the rewrite");
        assert!(matches!(error, tsink::TsinkError::DiskQuotaExceeded { .. }));
        std::fs::remove_file(shared_file).expect("shared file should be removed");
        budget
            .reconcile()
            .expect("shared budget should reconcile after removal");
        store
            .apply_updates("tenant-a", &[update])
            .expect("metadata should persist after releasing shared capacity");

        let file_bytes = std::fs::metadata(
            store
                .file_path()
                .expect("persistent store should expose file path"),
        )
        .expect("metadata file should exist")
        .len();
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, file_bytes);
        assert_eq!(
            snapshot
                .categories
                .iter()
                .find(|usage| usage.category == tsink::DiskCategory::Metadata)
                .map(|usage| usage.bytes),
            Some(file_bytes)
        );
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
    }

    #[test]
    fn shared_budget_restart_recounts_sidecars_and_preserves_reads_at_limit() {
        let temp_dir = TempDir::new().expect("temp dir should build");
        let initial_budget =
            tsink::LocalDiskBudget::open(temp_dir.path(), tsink::LocalDiskLimits::default())
                .expect("initial disk budget should open");
        let metadata_store = MetricMetadataStore::open_with_disk_budget(
            Some(temp_dir.path()),
            Some(Arc::clone(&initial_budget)),
        )
        .expect("metadata store should open");
        let exemplar_store = crate::exemplar_store::ExemplarStore::open_with_disk_budget(
            Some(temp_dir.path()),
            Some(Arc::clone(&initial_budget)),
        )
        .expect("exemplar store should open");
        metadata_store
            .apply_updates(
                "tenant-a",
                &[NormalizedMetricMetadataUpdate {
                    metric_family_name: "restart_metric".to_string(),
                    metric_type: MetricType::Gauge,
                    help: "persisted before restart".to_string(),
                    unit: "widgets".to_string(),
                }],
            )
            .expect("metadata should persist");
        exemplar_store
            .apply_writes(&[crate::exemplar_store::ExemplarWrite {
                metric: "restart_metric".to_string(),
                series_labels: vec![tsink::Label::new("job", "restart")],
                exemplar_labels: vec![tsink::Label::new("trace_id", "persisted")],
                timestamp: 10,
                value: 1.0,
            }])
            .expect("exemplar should persist");
        let used_bytes = initial_budget.snapshot().accounted_bytes;
        assert!(used_bytes > 0);
        drop(metadata_store);
        drop(exemplar_store);
        drop(initial_budget);

        let restarted_budget = tsink::LocalDiskBudget::open(
            temp_dir.path(),
            tsink::LocalDiskLimits {
                max_bytes: Some(used_bytes),
                ..tsink::LocalDiskLimits::default()
            },
        )
        .expect("restarted disk budget should recount existing files");
        let metadata_store = MetricMetadataStore::open_with_disk_budget(
            Some(temp_dir.path()),
            Some(Arc::clone(&restarted_budget)),
        )
        .expect("metadata store should reopen");
        let exemplar_store = crate::exemplar_store::ExemplarStore::open_with_disk_budget(
            Some(temp_dir.path()),
            Some(Arc::clone(&restarted_budget)),
        )
        .expect("exemplar store should reopen");

        let metadata = metadata_store
            .query("tenant-a", Some("restart_metric"), 10)
            .expect("metadata should remain readable");
        assert_eq!(metadata.len(), 1);
        let exemplars = exemplar_store
            .query(
                &[tsink::SeriesSelection::new()
                    .with_metric("restart_metric")
                    .with_matcher(tsink::SeriesMatcher::equal("job", "restart"))],
                0,
                20,
                10,
            )
            .expect("exemplars should remain readable");
        assert_eq!(exemplars.len(), 1);
        assert_eq!(exemplars[0].exemplars.len(), 1);

        let metadata_error = metadata_store
            .apply_updates(
                "tenant-a",
                &[NormalizedMetricMetadataUpdate {
                    metric_family_name: "restart_metric".to_string(),
                    metric_type: MetricType::Gauge,
                    help: "must not replace persisted state at the limit".to_string(),
                    unit: "widgets".to_string(),
                }],
            )
            .expect_err("metadata growth at the recounted limit should fail");
        assert!(matches!(
            metadata_error,
            tsink::TsinkError::DiskQuotaExceeded { .. }
        ));
        let exemplar_error = exemplar_store
            .apply_writes(&[crate::exemplar_store::ExemplarWrite {
                metric: "restart_metric".to_string(),
                series_labels: vec![tsink::Label::new("job", "restart")],
                exemplar_labels: vec![tsink::Label::new("trace_id", "rejected")],
                timestamp: 20,
                value: 2.0,
            }])
            .expect_err("exemplar growth at the recounted limit should fail");
        assert!(matches!(
            exemplar_error.as_tsink_error(),
            tsink::TsinkError::DiskQuotaExceeded { .. }
        ));

        let snapshot = restarted_budget.snapshot();
        assert_eq!(snapshot.accounted_bytes, used_bytes);
        assert_eq!(snapshot.reserved_bytes, 0);
        assert_eq!(snapshot.active_reservations, 0);
        assert_eq!(snapshot.rejections_total, 2);
    }

    #[cfg(unix)]
    #[test]
    fn budgeted_store_rejects_a_symlinked_owned_file() {
        let temp_dir = TempDir::new().expect("temp dir should build");
        let outside = temp_dir.path().join("outside.json");
        std::fs::write(&outside, b"outside-state").expect("outside file should write");
        std::os::unix::fs::symlink(&outside, temp_dir.path().join(METADATA_STORE_FILE_NAME))
            .expect("managed-file symlink should build");
        let budget =
            tsink::LocalDiskBudget::open(temp_dir.path(), tsink::LocalDiskLimits::default())
                .expect("disk budget should open");

        let error = MetricMetadataStore::open_with_disk_budget(Some(temp_dir.path()), Some(budget))
            .expect_err("the owned store path must not be followed through a symlink");
        assert!(error.contains("must be a regular file"), "{error}");
        assert_eq!(
            std::fs::read(&outside).expect("outside file should remain readable"),
            b"outside-state"
        );
    }
}
