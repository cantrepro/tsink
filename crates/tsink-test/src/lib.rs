//! In-process fixtures for testing applications against the tsink core engine.
//!
//! This initial testkit surface is deliberately synchronous and small. It provides isolated
//! storage, canonical atomic writes, explicit-time PromQL queries and assertions, restart, and
//! bounded diagnostics without starting protocol listeners, invoking binaries, downloading
//! artifacts, or requiring Docker. Manual-clock and deterministic-maintenance APIs are not
//! implemented yet, so every PromQL helper requires an explicit timestamp.
//!
//! ```
//! use tsink::{DataPoint, Row, TimestampPrecision};
//! use tsink_test::TsinkTestDb;
//!
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let mut db = TsinkTestDb::builder()
//!     .temporary()
//!     .timestamp_precision(TimestampPrecision::Seconds)
//!     .start()?;
//! let write = db.write_atomic(&[Row::new(
//!     "requests_total",
//!     DataPoint::new(10, 3.0),
//! )])?;
//! assert_eq!(write.accepted, 1);
//! let value = db.promql_instant("requests_total", 10)?;
//! assert_eq!(value.as_instant_vector().unwrap()[0].value, 3.0);
//! db.assert_promql_scalar("scalar(sum(requests_total))", 10, 3.0, 0.0)?;
//! db.close()?;
//! # Ok(())
//! # }
//! ```

mod fixtures;
#[cfg(feature = "prometheus")]
mod prometheus;

pub use fixtures::{
    classic_histogram, counter, counter_sequence, evenly_spaced_samples, gauge, label, labels,
    metric_series, native_histogram, sample,
};
#[cfg(feature = "prometheus")]
pub use prometheus::{
    encode_prometheus_remote_write_request, prometheus_remote_write_payload,
    prometheus_remote_write_request, PrometheusRemoteWritePayload, PrometheusRemoteWriteSample,
    PrometheusRemoteWriteSeries, WriteRequest as PrometheusWriteRequest,
};

use std::cmp::Ordering as CmpOrdering;
use std::collections::VecDeque;
use std::fmt;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use tempfile::TempDir;
use tsink::promql::types::{PromqlValue, Sample, Series};
use tsink::promql::{Engine as PromqlEngine, PromqlError};
use tsink::{
    BatchWriteResult, DataPoint, Label, MetricSeries, QueryBudgetError, QueryLimitReason,
    QueryOptions, ResourceProfile, Row, RowWriteStatus, Storage, StorageBuilder,
    TimestampPrecision, TsinkError, WriteMode,
};

/// Default number of recent fixture operations retained for diagnostics.
pub const DEFAULT_DIAGNOSTIC_CAPACITY: usize = 64;

/// Hard upper bound for the diagnostic ring configured by callers.
pub const MAX_DIAGNOSTIC_CAPACITY: usize = 1_024;

/// Maximum retained UTF-8 bytes in one diagnostic message.
pub const MAX_DIAGNOSTIC_MESSAGE_BYTES: usize = 512;

/// Maximum UTF-8 bytes returned by one failed PromQL assertion.
pub const MAX_PROMQL_ASSERTION_DIAGNOSTIC_BYTES: usize = 4 * 1_024;

const MAX_PROMQL_ASSERTION_QUERY_BYTES: usize = 384;
const MAX_PROMQL_ASSERTION_VALUE_BYTES: usize = 768;
const MAX_PROMQL_ASSERTION_NEARBY_BYTES: usize = 1_536;
const MAX_TRACKED_DIAGNOSTIC_SERIES: usize = 16;
const MAX_NEARBY_DIAGNOSTIC_SERIES: usize = 4;
const MAX_NEARBY_DIAGNOSTIC_POINTS_PER_SERIES: usize = 3;
const MAX_TRACKED_SERIES_LABELS: usize = 16;
const MAX_TRACKED_SERIES_IDENTITY_BYTES: usize = 2 * 1_024;
const PROMQL_LOOKBACK_SECONDS: i64 = 5 * 60;

static NEXT_INSTANCE_SEQUENCE: AtomicU64 = AtomicU64::new(0);

/// Storage lifetime selected for a [`TsinkTestDb`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum TsinkTestDbMode {
    /// No data directory or WAL is created.
    InMemory,
    /// A uniquely named [`tempfile::TempDir`] owns the data directory.
    Temporary,
    /// The caller owns an explicit directory and its cleanup.
    PersistentDirectory,
}

/// One bounded entry from a fixture's diagnostic ring.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TsinkTestDiagnostic {
    /// Monotonic sequence local to this fixture.
    pub sequence: u64,
    /// Stable operation label such as `write_atomic`, `promql_instant`, or `restart`.
    pub operation: &'static str,
    /// Bounded outcome detail.
    pub message: String,
}

/// Typed PromQL failure expected by an assertion helper.
///
/// Matching is structural. In particular, unsupported operations compare their stable operation
/// identifier and query-limit failures compare [`QueryLimitReason`]; assertion helpers never use
/// error-message substring matching.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PromqlErrorExpectation {
    /// [`PromqlError::Parse`].
    Parse,
    /// [`PromqlError::UnexpectedToken`].
    UnexpectedToken,
    /// [`PromqlError::UnknownFunction`].
    UnknownFunction,
    /// [`PromqlError::ArgumentCount`].
    ArgumentCount,
    /// [`PromqlError::Type`].
    Type,
    /// [`PromqlError::Eval`].
    Evaluation,
    /// [`PromqlError::Regex`].
    Regex,
    /// Any [`PromqlError::Storage`] failure.
    Storage,
    /// A structured storage-level unsupported-operation failure with this exact operation code.
    UnsupportedOperation {
        /// Stable `operation` value carried by [`TsinkError::UnsupportedOperation`].
        operation: &'static str,
    },
    /// A structured query-budget limit failure with this exact reason.
    QueryLimit(QueryLimitReason),
}

impl fmt::Display for PromqlErrorExpectation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Parse => formatter.write_str("parse error"),
            Self::UnexpectedToken => formatter.write_str("unexpected-token error"),
            Self::UnknownFunction => formatter.write_str("unknown-function error"),
            Self::ArgumentCount => formatter.write_str("argument-count error"),
            Self::Type => formatter.write_str("type error"),
            Self::Evaluation => formatter.write_str("evaluation error"),
            Self::Regex => formatter.write_str("regex error"),
            Self::Storage => formatter.write_str("storage error"),
            Self::UnsupportedOperation { operation } => {
                write!(formatter, "unsupported operation {operation:?}")
            }
            Self::QueryLimit(reason) => write!(formatter, "query limit {reason}"),
        }
    }
}

impl PromqlErrorExpectation {
    fn matches(self, error: &PromqlError) -> bool {
        match (self, error) {
            (Self::Parse, PromqlError::Parse(_))
            | (Self::UnexpectedToken, PromqlError::UnexpectedToken { .. })
            | (Self::UnknownFunction, PromqlError::UnknownFunction(_))
            | (Self::ArgumentCount, PromqlError::ArgumentCount { .. })
            | (Self::Type, PromqlError::Type(_))
            | (Self::Evaluation, PromqlError::Eval(_))
            | (Self::Regex, PromqlError::Regex(_))
            | (Self::Storage, PromqlError::Storage(_)) => true,
            (
                Self::UnsupportedOperation {
                    operation: expected,
                },
                PromqlError::Storage(TsinkError::UnsupportedOperation {
                    operation: actual, ..
                }),
            ) => expected == *actual,
            (
                Self::QueryLimit(expected),
                PromqlError::Storage(TsinkError::QueryBudget(QueryBudgetError::LimitExceeded(
                    exceeded,
                ))),
            ) => expected == exceeded.reason,
            _ => false,
        }
    }
}

/// Bounded diagnostic returned by a failed PromQL assertion.
///
/// The complete message is at most [`MAX_PROMQL_ASSERTION_DIAGNOSTIC_BYTES`] UTF-8 bytes. It
/// includes a bounded query, explicit evaluation time/range, expected and actual summaries, and a
/// bounded nearby-series snapshot when the fixture can query one safely.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromqlAssertionError {
    diagnostic: String,
}

impl PromqlAssertionError {
    /// Returns the complete bounded assertion diagnostic.
    #[must_use]
    pub fn diagnostic(&self) -> &str {
        &self.diagnostic
    }
}

impl fmt::Display for PromqlAssertionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.diagnostic)
    }
}

impl std::error::Error for PromqlAssertionError {}

#[derive(Debug)]
struct DiagnosticRing {
    capacity: usize,
    next_sequence: AtomicU64,
    entries: Mutex<VecDeque<TsinkTestDiagnostic>>,
}

impl DiagnosticRing {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            next_sequence: AtomicU64::new(0),
            entries: Mutex::new(VecDeque::with_capacity(capacity)),
        }
    }

    fn record(&self, operation: &'static str, message: impl AsRef<str>) {
        let sequence = self.next_sequence.fetch_add(1, Ordering::Relaxed);
        let diagnostic = TsinkTestDiagnostic {
            sequence,
            operation,
            message: bounded_diagnostic_message(message.as_ref()),
        };
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if entries.len() == self.capacity {
            entries.pop_front();
        }
        entries.push_back(diagnostic);
    }

    fn snapshot(&self) -> Vec<TsinkTestDiagnostic> {
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .cloned()
            .collect()
    }
}

#[derive(Debug, Default)]
struct RecentSeriesRing {
    entries: Mutex<VecDeque<MetricSeries>>,
}

impl RecentSeriesRing {
    fn record_accepted_rows(&self, rows: &[Row], result: &BatchWriteResult) {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for outcome in &result.outcomes {
            if !matches!(outcome.status, RowWriteStatus::Accepted) {
                continue;
            }
            let Some(row) = rows.get(outcome.index) else {
                continue;
            };
            let Some(series) = bounded_metric_series(row) else {
                continue;
            };
            if let Some(existing) = entries.iter().position(|entry| entry == &series) {
                entries.remove(existing);
            }
            if entries.len() == MAX_TRACKED_DIAGNOSTIC_SERIES {
                entries.pop_front();
            }
            entries.push_back(series);
        }
    }

    fn newest(&self) -> Vec<MetricSeries> {
        self.entries
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .iter()
            .rev()
            .take(MAX_NEARBY_DIAGNOSTIC_SERIES)
            .cloned()
            .collect()
    }
}

#[derive(Debug, Clone, Copy)]
enum PromqlEvaluation {
    Instant { evaluation_time: i64 },
    Range { start: i64, end: i64, step: i64 },
}

impl PromqlEvaluation {
    fn nearby_storage_range(self, precision: TimestampPrecision) -> Option<(i64, i64)> {
        let (start, inclusive_end) = match self {
            Self::Instant { evaluation_time } => {
                let lookback = PROMQL_LOOKBACK_SECONDS.saturating_mul(units_per_second(precision));
                (evaluation_time.saturating_sub(lookback), evaluation_time)
            }
            Self::Range { start, end, .. } => (start.min(end), start.max(end)),
        };
        let exclusive_end = inclusive_end.checked_add(1).unwrap_or(inclusive_end);
        if start < exclusive_end {
            Some((start, exclusive_end))
        } else {
            start.checked_sub(1).map(|earlier| (earlier, start))
        }
    }
}

impl fmt::Display for PromqlEvaluation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Instant { evaluation_time } => {
                write!(formatter, "instant at {evaluation_time}")
            }
            Self::Range { start, end, step } => {
                write!(formatter, "range start={start}, end={end}, step={step}")
            }
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum ExpectedPresence {
    Empty,
    Nonempty,
}

impl ExpectedPresence {
    fn matches(self, value: &PromqlValue) -> bool {
        let is_empty = match value {
            PromqlValue::InstantVector(vector) => vector.is_empty(),
            PromqlValue::RangeVector(matrix) => matrix.is_empty(),
            PromqlValue::Scalar(_, _) | PromqlValue::String(_, _) => return false,
        };
        match self {
            Self::Empty => is_empty,
            Self::Nonempty => !is_empty,
        }
    }
}

impl fmt::Display for ExpectedPresence {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => formatter.write_str("empty"),
            Self::Nonempty => formatter.write_str("non-empty"),
        }
    }
}

#[derive(Debug, Clone)]
enum BuilderStorage {
    InMemory,
    Temporary,
    PersistentDirectory(PathBuf),
}

/// Builder for [`TsinkTestDb`].
///
/// The default is an isolated temporary database using [`ResourceProfile::Test`] and nanosecond
/// timestamps. Call [`Self::in_memory`] for a pure-memory fixture or
/// [`Self::persistent_directory`] when the caller must own the directory across fixture values.
#[derive(Debug, Clone)]
pub struct TsinkTestDbBuilder {
    storage: BuilderStorage,
    resource_profile: ResourceProfile,
    timestamp_precision: TimestampPrecision,
    diagnostic_capacity: usize,
}

impl Default for TsinkTestDbBuilder {
    fn default() -> Self {
        Self {
            storage: BuilderStorage::Temporary,
            resource_profile: ResourceProfile::Test,
            timestamp_precision: TimestampPrecision::Nanoseconds,
            diagnostic_capacity: DEFAULT_DIAGNOSTIC_CAPACITY,
        }
    }
}

impl TsinkTestDbBuilder {
    /// Creates a temporary-directory fixture builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Uses pure in-memory core storage.
    #[must_use]
    pub fn in_memory(mut self) -> Self {
        self.storage = BuilderStorage::InMemory;
        self
    }

    /// Uses an automatically cleaned, uniquely named temporary data directory.
    #[must_use]
    pub fn temporary(mut self) -> Self {
        self.storage = BuilderStorage::Temporary;
        self
    }

    /// Uses a caller-owned persistent directory.
    ///
    /// The fixture opens or creates the directory but never removes it.
    #[must_use]
    pub fn persistent_directory(mut self, path: impl AsRef<Path>) -> Self {
        self.storage = BuilderStorage::PersistentDirectory(path.as_ref().to_path_buf());
        self
    }

    /// Selects the core resource profile. The default is [`ResourceProfile::Test`].
    #[must_use]
    pub fn resource_profile(mut self, profile: ResourceProfile) -> Self {
        self.resource_profile = profile;
        self
    }

    /// Selects the timestamp unit used by writes and PromQL helpers.
    #[must_use]
    pub fn timestamp_precision(mut self, precision: TimestampPrecision) -> Self {
        self.timestamp_precision = precision;
        self
    }

    /// Sets the number of recent operations retained for diagnostics.
    ///
    /// `start` rejects zero and values above [`MAX_DIAGNOSTIC_CAPACITY`].
    #[must_use]
    pub fn diagnostic_capacity(mut self, capacity: usize) -> Self {
        self.diagnostic_capacity = capacity;
        self
    }

    /// Starts the in-process fixture.
    pub fn start(self) -> tsink::Result<TsinkTestDb> {
        if !(1..=MAX_DIAGNOSTIC_CAPACITY).contains(&self.diagnostic_capacity) {
            return Err(TsinkError::InvalidConfiguration(format!(
                "tsink-test diagnostic capacity must be within 1..={MAX_DIAGNOSTIC_CAPACITY}, got {}",
                self.diagnostic_capacity
            )));
        }

        let diagnostic_id = next_diagnostic_id()?;
        let (mode, temp_dir, data_path) = match self.storage {
            BuilderStorage::InMemory => (TsinkTestDbMode::InMemory, None, None),
            BuilderStorage::Temporary => {
                let prefix = format!("{diagnostic_id}-");
                let directory = tempfile::Builder::new().prefix(&prefix).tempdir()?;
                let path = directory.path().to_path_buf();
                (TsinkTestDbMode::Temporary, Some(directory), Some(path))
            }
            BuilderStorage::PersistentDirectory(path) => {
                (TsinkTestDbMode::PersistentDirectory, None, Some(path))
            }
        };
        let storage = open_storage(
            data_path.as_deref(),
            self.resource_profile,
            self.timestamp_precision,
        )?;
        let fixture = TsinkTestDb {
            diagnostic_id,
            mode,
            resource_profile: self.resource_profile,
            timestamp_precision: self.timestamp_precision,
            data_path,
            temp_dir,
            storage: Some(storage),
            diagnostics: DiagnosticRing::new(self.diagnostic_capacity),
            recent_series: RecentSeriesRing::default(),
        };
        fixture.record(
            "start",
            format!(
                "opened {:?} fixture with profile {:?}",
                fixture.mode, fixture.resource_profile
            ),
        );
        Ok(fixture)
    }

    /// Alias for [`Self::start`].
    pub fn build(self) -> tsink::Result<TsinkTestDb> {
        self.start()
    }
}

/// Synchronous in-process tsink test fixture.
///
/// Call [`Self::close`] explicitly so storage shutdown and temporary-directory cleanup errors are
/// observable. `Drop` performs only best-effort cleanup.
pub struct TsinkTestDb {
    diagnostic_id: String,
    mode: TsinkTestDbMode,
    resource_profile: ResourceProfile,
    timestamp_precision: TimestampPrecision,
    data_path: Option<PathBuf>,
    temp_dir: Option<TempDir>,
    storage: Option<Arc<dyn Storage>>,
    diagnostics: DiagnosticRing,
    recent_series: RecentSeriesRing,
}

impl fmt::Debug for TsinkTestDb {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TsinkTestDb")
            .field("diagnostic_id", &self.diagnostic_id)
            .field("mode", &self.mode)
            .field("resource_profile", &self.resource_profile)
            .field("timestamp_precision", &self.timestamp_precision)
            .field("data_path", &self.data_path)
            .field("closed", &self.storage.is_none())
            .finish_non_exhaustive()
    }
}

impl TsinkTestDb {
    /// Creates a builder whose default mode is a temporary directory.
    #[must_use]
    pub fn builder() -> TsinkTestDbBuilder {
        TsinkTestDbBuilder::new()
    }

    /// Returns the process-safe diagnostic identifier for this fixture.
    ///
    /// It combines the process ID with a lock-free process-local sequence, making simultaneous
    /// fixtures unique across threads and concurrently running test processes.
    #[must_use]
    pub fn diagnostic_id(&self) -> &str {
        &self.diagnostic_id
    }

    /// Returns the selected storage lifetime.
    #[must_use]
    pub const fn mode(&self) -> TsinkTestDbMode {
        self.mode
    }

    /// Returns the core resource profile used for initial open and restart.
    #[must_use]
    pub const fn resource_profile(&self) -> ResourceProfile {
        self.resource_profile
    }

    /// Returns the configured timestamp precision.
    #[must_use]
    pub const fn timestamp_precision(&self) -> TimestampPrecision {
        self.timestamp_precision
    }

    /// Returns the data directory for temporary and persistent fixtures.
    #[must_use]
    pub fn data_path(&self) -> Option<&Path> {
        self.data_path.as_deref()
    }

    /// Returns whether the active storage handle has been closed.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.storage.is_none()
    }

    /// Writes one canonical atomic core batch and returns its indexed result unchanged.
    pub fn write_atomic(&self, rows: &[Row]) -> tsink::Result<BatchWriteResult> {
        let result = self.active_storage()?.write_batch(rows, WriteMode::Atomic);
        match &result {
            Ok(result) => {
                self.recent_series.record_accepted_rows(rows, result);
                self.record(
                    "write_atomic",
                    format!(
                        "submitted={}, accepted={}, rejected={}, acknowledgement={:?}",
                        result.submitted, result.accepted, result.rejected, result.acknowledgement
                    ),
                );
            }
            Err(error) => self.record_error("write_atomic", error),
        }
        result
    }

    /// Directly selects one series from the core storage API.
    pub fn select(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
    ) -> tsink::Result<Vec<DataPoint>> {
        let result = self.active_storage()?.select(metric, labels, start, end);
        match &result {
            Ok(points) => self.record(
                "select",
                format!(
                    "metric={metric:?}, start={start}, end={end}, points={}",
                    points.len()
                ),
            ),
            Err(error) => self.record_error("select", error),
        }
        result
    }

    /// Evaluates PromQL directly through the core evaluator at an explicit timestamp.
    pub fn promql_instant(
        &self,
        query: &str,
        evaluation_time: i64,
    ) -> tsink::promql::Result<PromqlValue> {
        let storage = self
            .active_storage()
            .map(Arc::clone)
            .map_err(PromqlError::Storage)?;
        let engine = PromqlEngine::with_precision(storage, self.timestamp_precision);
        let result = engine.instant_query(query, evaluation_time);
        match &result {
            Ok(value) => self.record(
                "promql_instant",
                format!(
                    "query_bytes={}, evaluation_time={evaluation_time}, result={}",
                    query.len(),
                    promql_value_kind(value)
                ),
            ),
            Err(error) => self.record_error("promql_instant", error),
        }
        result
    }

    /// Evaluates PromQL directly through the core evaluator over an explicit range.
    pub fn promql_range(
        &self,
        query: &str,
        start: i64,
        end: i64,
        step: i64,
    ) -> tsink::promql::Result<PromqlValue> {
        let storage = self
            .active_storage()
            .map(Arc::clone)
            .map_err(PromqlError::Storage)?;
        let engine = PromqlEngine::with_precision(storage, self.timestamp_precision);
        let result = engine.range_query(query, start, end, step);
        match &result {
            Ok(value) => self.record(
                "promql_range",
                format!(
                    "query_bytes={}, start={start}, end={end}, step={step}, result={}",
                    query.len(),
                    promql_value_kind(value)
                ),
            ),
            Err(error) => self.record_error("promql_range", error),
        }
        result
    }

    /// Evaluates an instant query at `evaluation_time` and compares its complete public value.
    ///
    /// Instant-vector series and labels, and any range-vector series and samples returned by an
    /// instant expression, are normalized deterministically before comparison. The returned value
    /// is that normalized actual result, allowing a test to inspect it further without rerunning
    /// the query.
    ///
    /// ```
    /// use tsink::promql::types::{PromqlValue, Sample};
    /// use tsink::{DataPoint, Row, TimestampPrecision};
    /// use tsink_test::TsinkTestDb;
    ///
    /// # fn main() -> Result<(), Box<dyn std::error::Error>> {
    /// let mut db = TsinkTestDb::builder()
    ///     .in_memory()
    ///     .timestamp_precision(TimestampPrecision::Seconds)
    ///     .start()?;
    /// db.write_atomic(&[Row::new("ready", DataPoint::new(5, 1.0))])?;
    /// db.assert_promql_instant_eq(
    ///     "ready",
    ///     5,
    ///     PromqlValue::InstantVector(vec![Sample {
    ///         metric: "ready".to_string(),
    ///         labels: vec![],
    ///         timestamp: 5,
    ///         value: 1.0,
    ///         histogram: None,
    ///     }]),
    /// )?;
    /// db.close()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn assert_promql_instant_eq(
        &self,
        query: &str,
        evaluation_time: i64,
        expected: PromqlValue,
    ) -> Result<PromqlValue, PromqlAssertionError> {
        self.assert_promql_value_eq(
            "assert_promql_instant_eq",
            query,
            PromqlEvaluation::Instant { evaluation_time },
            self.promql_instant(query, evaluation_time),
            expected,
        )
    }

    /// Evaluates a range query at the explicit `start`, `end`, and `step` and compares its complete
    /// public value after deterministic vector/matrix normalization.
    ///
    /// The normalized actual result is returned on success.
    pub fn assert_promql_range_eq(
        &self,
        query: &str,
        start: i64,
        end: i64,
        step: i64,
        expected: PromqlValue,
    ) -> Result<PromqlValue, PromqlAssertionError> {
        self.assert_promql_value_eq(
            "assert_promql_range_eq",
            query,
            PromqlEvaluation::Range { start, end, step },
            self.promql_range(query, start, end, step),
            expected,
        )
    }

    /// Asserts that an explicit-time instant query returns a scalar within `tolerance`.
    ///
    /// `tolerance` must be finite and non-negative. The scalar's returned timestamp must equal
    /// `evaluation_time`. Equal infinities and two NaN values compare equal; otherwise finite
    /// values compare using absolute error. The actual scalar value is returned on success.
    pub fn assert_promql_scalar(
        &self,
        query: &str,
        evaluation_time: i64,
        expected: f64,
        tolerance: f64,
    ) -> Result<PromqlValue, PromqlAssertionError> {
        let evaluation = PromqlEvaluation::Instant { evaluation_time };
        let expected_summary =
            format!("scalar value {expected:?} at {evaluation_time} within {tolerance:?}");
        if !tolerance.is_finite() || tolerance < 0.0 {
            return Err(self.promql_assertion_failure(
                "assert_promql_scalar",
                query,
                evaluation,
                &expected_summary,
                "not evaluated: tolerance must be finite and non-negative",
            ));
        }

        match self.promql_instant(query, evaluation_time) {
            Ok(actual @ PromqlValue::Scalar(actual_value, actual_time))
                if actual_time == evaluation_time
                    && float_approximately_equal(actual_value, expected, tolerance) =>
            {
                self.record(
                    "assert_promql_scalar",
                    format!("matched scalar at {evaluation_time}"),
                );
                Ok(actual)
            }
            Ok(actual) => Err(self.promql_assertion_failure(
                "assert_promql_scalar",
                query,
                evaluation,
                &expected_summary,
                &bounded_debug(&actual, MAX_PROMQL_ASSERTION_VALUE_BYTES),
            )),
            Err(error) => Err(self.promql_assertion_failure(
                "assert_promql_scalar",
                query,
                evaluation,
                &expected_summary,
                &bounded_display(&error, MAX_PROMQL_ASSERTION_VALUE_BYTES),
            )),
        }
    }

    /// Asserts that an explicit-time instant query returns an empty vector or matrix.
    ///
    /// The actual value is returned on success.
    pub fn assert_promql_instant_empty(
        &self,
        query: &str,
        evaluation_time: i64,
    ) -> Result<PromqlValue, PromqlAssertionError> {
        self.assert_promql_presence(
            "assert_promql_instant_empty",
            query,
            PromqlEvaluation::Instant { evaluation_time },
            self.promql_instant(query, evaluation_time),
            ExpectedPresence::Empty,
        )
    }

    /// Asserts that an explicit-time instant query returns a non-empty vector or matrix.
    ///
    /// The actual value is returned on success.
    pub fn assert_promql_instant_nonempty(
        &self,
        query: &str,
        evaluation_time: i64,
    ) -> Result<PromqlValue, PromqlAssertionError> {
        self.assert_promql_presence(
            "assert_promql_instant_nonempty",
            query,
            PromqlEvaluation::Instant { evaluation_time },
            self.promql_instant(query, evaluation_time),
            ExpectedPresence::Nonempty,
        )
    }

    /// Asserts that an explicit-time range query returns an empty vector or matrix.
    ///
    /// The actual value is returned on success.
    pub fn assert_promql_range_empty(
        &self,
        query: &str,
        start: i64,
        end: i64,
        step: i64,
    ) -> Result<PromqlValue, PromqlAssertionError> {
        self.assert_promql_presence(
            "assert_promql_range_empty",
            query,
            PromqlEvaluation::Range { start, end, step },
            self.promql_range(query, start, end, step),
            ExpectedPresence::Empty,
        )
    }

    /// Asserts that an explicit-time range query returns a non-empty vector or matrix.
    ///
    /// The actual value is returned on success.
    pub fn assert_promql_range_nonempty(
        &self,
        query: &str,
        start: i64,
        end: i64,
        step: i64,
    ) -> Result<PromqlValue, PromqlAssertionError> {
        self.assert_promql_presence(
            "assert_promql_range_nonempty",
            query,
            PromqlEvaluation::Range { start, end, step },
            self.promql_range(query, start, end, step),
            ExpectedPresence::Nonempty,
        )
    }

    /// Asserts that an explicit-time instant query fails with `expected`.
    ///
    /// Error matching uses [`PromqlError`] and nested storage/query error variants, never rendered
    /// message text.
    pub fn assert_promql_instant_error(
        &self,
        query: &str,
        evaluation_time: i64,
        expected: PromqlErrorExpectation,
    ) -> Result<(), PromqlAssertionError> {
        self.assert_promql_error(
            "assert_promql_instant_error",
            query,
            PromqlEvaluation::Instant { evaluation_time },
            self.promql_instant(query, evaluation_time),
            expected,
        )
    }

    /// Asserts that an explicit-time range query fails with `expected`.
    ///
    /// Error matching uses [`PromqlError`] and nested storage/query error variants, never rendered
    /// message text.
    pub fn assert_promql_range_error(
        &self,
        query: &str,
        start: i64,
        end: i64,
        step: i64,
        expected: PromqlErrorExpectation,
    ) -> Result<(), PromqlAssertionError> {
        self.assert_promql_error(
            "assert_promql_range_error",
            query,
            PromqlEvaluation::Range { start, end, step },
            self.promql_range(query, start, end, step),
            expected,
        )
    }

    /// Closes and reopens a temporary or persistent fixture on the same directory.
    ///
    /// In-memory fixtures return [`TsinkError::UnsupportedOperation`]. A temporary fixture may be
    /// restarted until its final explicit [`Self::close`], which removes its temporary directory.
    pub fn restart(&mut self) -> tsink::Result<()> {
        if self.mode == TsinkTestDbMode::InMemory {
            let error = TsinkError::UnsupportedOperation {
                operation: "tsink_test_restart",
                reason: "in-memory fixtures have no persistent directory".to_string(),
            };
            self.record("restart", format!("error: {error}"));
            return Err(error);
        }
        if self.mode == TsinkTestDbMode::Temporary && self.temp_dir.is_none() {
            let error = TsinkError::InvalidConfiguration(
                "temporary fixture cannot restart after close removed its directory".to_string(),
            );
            self.record("restart", format!("error: {error}"));
            return Err(error);
        }
        let path = self
            .data_path
            .clone()
            .expect("persistent fixture modes always retain a data path");
        self.close_storage_only("restart")?;
        match open_storage(Some(&path), self.resource_profile, self.timestamp_precision) {
            Ok(storage) => {
                self.storage = Some(storage);
                self.record("restart", format!("reopened {}", path.display()));
                Ok(())
            }
            Err(error) => {
                self.record("restart", format!("reopen error: {error}"));
                Err(error)
            }
        }
    }

    /// Returns recent diagnostics in chronological order.
    #[must_use]
    pub fn diagnostics(&self) -> Vec<TsinkTestDiagnostic> {
        self.diagnostics.snapshot()
    }

    /// Returns a bounded, human-readable diagnostic dump.
    #[must_use]
    pub fn diagnostic_dump(&self) -> String {
        let diagnostics = self.diagnostics();
        let mut output = format!(
            "TsinkTestDb {} mode={:?} path={:?} closed={}\n",
            self.diagnostic_id,
            self.mode,
            self.data_path,
            self.is_closed()
        );
        for diagnostic in diagnostics {
            use std::fmt::Write as _;
            let _ = writeln!(
                output,
                "#{:04} {}: {}",
                diagnostic.sequence, diagnostic.operation, diagnostic.message
            );
        }
        output
    }

    /// Explicitly closes storage and removes an owned temporary directory.
    ///
    /// Persistent directories remain caller-owned. Repeated calls are successful no-ops. If
    /// storage shutdown fails, the directory is retained and the error is returned.
    pub fn close(&mut self) -> tsink::Result<()> {
        self.close_storage_only("close")?;
        let Some(temp_dir) = self.temp_dir.take() else {
            self.record("close", "closed");
            return Ok(());
        };
        let path = temp_dir.path().to_path_buf();
        match temp_dir.close() {
            Ok(()) => {
                self.record("close", format!("closed and removed {}", path.display()));
                Ok(())
            }
            Err(source) => {
                self.record(
                    "close",
                    format!("closed but temporary cleanup failed: {source}"),
                );
                Err(TsinkError::IoWithPath { path, source })
            }
        }
    }

    fn assert_promql_value_eq(
        &self,
        operation: &'static str,
        query: &str,
        evaluation: PromqlEvaluation,
        result: tsink::promql::Result<PromqlValue>,
        mut expected: PromqlValue,
    ) -> Result<PromqlValue, PromqlAssertionError> {
        if let Err(reason) = normalize_promql_value(&mut expected) {
            let actual = match &result {
                Ok(actual) => bounded_debug(actual, MAX_PROMQL_ASSERTION_VALUE_BYTES),
                Err(error) => bounded_display(error, MAX_PROMQL_ASSERTION_VALUE_BYTES),
            };
            return Err(self.promql_assertion_failure(
                operation,
                query,
                evaluation,
                &format!(
                    "valid normalized PromQL value; invalid expected value: {reason}; value={}",
                    bounded_debug(&expected, MAX_PROMQL_ASSERTION_VALUE_BYTES)
                ),
                &actual,
            ));
        }
        let mut actual = match result {
            Ok(actual) => actual,
            Err(error) => {
                return Err(self.promql_assertion_failure(
                    operation,
                    query,
                    evaluation,
                    &bounded_debug(&expected, MAX_PROMQL_ASSERTION_VALUE_BYTES),
                    &bounded_display(&error, MAX_PROMQL_ASSERTION_VALUE_BYTES),
                ));
            }
        };
        if let Err(reason) = normalize_promql_value(&mut actual) {
            return Err(self.promql_assertion_failure(
                operation,
                query,
                evaluation,
                &bounded_debug(&expected, MAX_PROMQL_ASSERTION_VALUE_BYTES),
                &format!(
                    "invalid actual PromQL value: {reason}; value={}",
                    bounded_debug(&actual, MAX_PROMQL_ASSERTION_VALUE_BYTES)
                ),
            ));
        }
        if promql_values_equal(&actual, &expected) {
            self.record(operation, format!("matched {evaluation}"));
            return Ok(actual);
        }
        Err(self.promql_assertion_failure(
            operation,
            query,
            evaluation,
            &bounded_debug(&expected, MAX_PROMQL_ASSERTION_VALUE_BYTES),
            &bounded_debug(&actual, MAX_PROMQL_ASSERTION_VALUE_BYTES),
        ))
    }

    fn assert_promql_presence(
        &self,
        operation: &'static str,
        query: &str,
        evaluation: PromqlEvaluation,
        result: tsink::promql::Result<PromqlValue>,
        expected: ExpectedPresence,
    ) -> Result<PromqlValue, PromqlAssertionError> {
        match result {
            Ok(actual) if expected.matches(&actual) => {
                self.record(operation, format!("matched {expected} {evaluation}"));
                Ok(actual)
            }
            Ok(actual) => Err(self.promql_assertion_failure(
                operation,
                query,
                evaluation,
                &format!("{expected} vector or matrix"),
                &bounded_debug(&actual, MAX_PROMQL_ASSERTION_VALUE_BYTES),
            )),
            Err(error) => Err(self.promql_assertion_failure(
                operation,
                query,
                evaluation,
                &format!("{expected} vector or matrix"),
                &bounded_display(&error, MAX_PROMQL_ASSERTION_VALUE_BYTES),
            )),
        }
    }

    fn assert_promql_error(
        &self,
        operation: &'static str,
        query: &str,
        evaluation: PromqlEvaluation,
        result: tsink::promql::Result<PromqlValue>,
        expected: PromqlErrorExpectation,
    ) -> Result<(), PromqlAssertionError> {
        match result {
            Err(error) if expected.matches(&error) => {
                self.record(operation, format!("matched {expected} at {evaluation}"));
                Ok(())
            }
            Err(error) => Err(self.promql_assertion_failure(
                operation,
                query,
                evaluation,
                &expected.to_string(),
                &bounded_display(&error, MAX_PROMQL_ASSERTION_VALUE_BYTES),
            )),
            Ok(actual) => Err(self.promql_assertion_failure(
                operation,
                query,
                evaluation,
                &expected.to_string(),
                &bounded_debug(&actual, MAX_PROMQL_ASSERTION_VALUE_BYTES),
            )),
        }
    }

    fn promql_assertion_failure(
        &self,
        operation: &'static str,
        query: &str,
        evaluation: PromqlEvaluation,
        expected: &str,
        actual: &str,
    ) -> PromqlAssertionError {
        let query = bounded_debug(&query, MAX_PROMQL_ASSERTION_QUERY_BYTES);
        let expected = bounded_diagnostic_text(expected, MAX_PROMQL_ASSERTION_VALUE_BYTES);
        let actual = bounded_diagnostic_text(actual, MAX_PROMQL_ASSERTION_VALUE_BYTES);
        let nearby = bounded_diagnostic_text(
            &self.nearby_series_diagnostic(evaluation),
            MAX_PROMQL_ASSERTION_NEARBY_BYTES,
        );
        let diagnostic = bounded_diagnostic_text(
            &format!(
                "PromQL assertion failed\nquery: {query}\nevaluation: {evaluation}\nexpected: {expected}\nactual: {actual}\nnearby stored series: {nearby}"
            ),
            MAX_PROMQL_ASSERTION_DIAGNOSTIC_BYTES,
        );
        self.record(operation, &diagnostic);
        PromqlAssertionError { diagnostic }
    }

    fn nearby_series_diagnostic(&self, evaluation: PromqlEvaluation) -> String {
        let series = self.recent_series.newest();
        if series.is_empty() {
            return "none tracked by this fixture".to_string();
        }
        if self.resource_profile.finite_limits().is_none() {
            return bounded_debug(&series, MAX_PROMQL_ASSERTION_NEARBY_BYTES)
                + " (points not queried without a finite resource profile)";
        }
        let Some(storage) = self.storage.as_ref() else {
            return bounded_debug(&series, MAX_PROMQL_ASSERTION_NEARBY_BYTES)
                + " (points unavailable after close)";
        };
        let Some((start, end)) = evaluation.nearby_storage_range(self.timestamp_precision) else {
            return bounded_debug(&series, MAX_PROMQL_ASSERTION_NEARBY_BYTES)
                + " (no representable nearby time range)";
        };

        let mut writer = BoundedWriter::new(MAX_PROMQL_ASSERTION_NEARBY_BYTES);
        for (index, series) in series.iter().enumerate() {
            if index > 0 && write!(&mut writer, "; ").is_err() {
                break;
            }
            if write!(&mut writer, "{series:?}").is_err() {
                break;
            }
            let options = QueryOptions::new(start, end)
                .with_labels(series.labels.clone())
                .with_pagination(0, Some(MAX_NEARBY_DIAGNOSTIC_POINTS_PER_SERIES));
            match storage.select_with_options(&series.name, options) {
                Ok(points) => {
                    if write!(&mut writer, " => {points:?}").is_err() {
                        break;
                    }
                }
                Err(TsinkError::NoDataPoints { .. }) => {
                    if write!(&mut writer, " => []").is_err() {
                        break;
                    }
                }
                Err(error) => {
                    if write!(&mut writer, " => error: {error}").is_err() {
                        break;
                    }
                }
            }
        }
        writer.finish()
    }

    fn active_storage(&self) -> tsink::Result<&Arc<dyn Storage>> {
        self.storage.as_ref().ok_or(TsinkError::StorageClosed)
    }

    fn close_storage_only(&mut self, operation: &'static str) -> tsink::Result<()> {
        let Some(storage) = self.storage.as_ref() else {
            return Ok(());
        };
        match storage.close() {
            Ok(()) => {
                self.storage = None;
                self.record(operation, "storage closed");
                Ok(())
            }
            Err(error) => {
                self.record(operation, format!("storage close error: {error}"));
                Err(error)
            }
        }
    }

    fn record(&self, operation: &'static str, message: impl AsRef<str>) {
        self.diagnostics.record(operation, message);
    }

    fn record_error(&self, operation: &'static str, error: &impl fmt::Display) {
        let mut writer = BoundedWriter::new(MAX_DIAGNOSTIC_MESSAGE_BYTES);
        let _ = write!(&mut writer, "error: {error}");
        self.record(operation, writer.finish());
    }
}

impl Drop for TsinkTestDb {
    fn drop(&mut self) {
        if let Some(storage) = self.storage.take() {
            let _ = storage.close();
        }
        // `TempDir` performs best-effort recursive cleanup after the storage handle is gone.
    }
}

struct BoundedWriter {
    output: String,
    limit: usize,
    truncated: bool,
}

impl BoundedWriter {
    fn new(limit: usize) -> Self {
        Self {
            output: String::with_capacity(limit.min(256)),
            limit,
            truncated: false,
        }
    }

    fn finish(mut self) -> String {
        if self.truncated && self.limit >= 3 {
            while self.output.len().saturating_add(3) > self.limit {
                self.output.pop();
            }
            self.output.push_str("...");
        }
        self.output
    }
}

impl fmt::Write for BoundedWriter {
    fn write_str(&mut self, value: &str) -> fmt::Result {
        if self.truncated {
            return Err(fmt::Error);
        }
        let remaining = self.limit.saturating_sub(self.output.len());
        if value.len() <= remaining {
            self.output.push_str(value);
            return Ok(());
        }
        let mut boundary = remaining;
        while !value.is_char_boundary(boundary) {
            boundary -= 1;
        }
        self.output.push_str(&value[..boundary]);
        self.truncated = true;
        Err(fmt::Error)
    }
}

fn bounded_debug(value: &impl fmt::Debug, limit: usize) -> String {
    let mut writer = BoundedWriter::new(limit);
    let _ = write!(&mut writer, "{value:?}");
    writer.finish()
}

fn bounded_display(value: &impl fmt::Display, limit: usize) -> String {
    let mut writer = BoundedWriter::new(limit);
    let _ = write!(&mut writer, "{value}");
    writer.finish()
}

fn bounded_metric_series(row: &Row) -> Option<MetricSeries> {
    if row.labels().len() > MAX_TRACKED_SERIES_LABELS {
        return None;
    }
    let mut bytes = row.metric().len();
    for label in row.labels() {
        bytes = bytes
            .checked_add(label.name.len())?
            .checked_add(label.value.len())?;
        if bytes > MAX_TRACKED_SERIES_IDENTITY_BYTES {
            return None;
        }
    }
    if bytes > MAX_TRACKED_SERIES_IDENTITY_BYTES {
        return None;
    }
    let mut labels = row.labels().to_vec();
    labels.sort_unstable();
    Some(MetricSeries {
        name: row.metric().to_string(),
        labels,
    })
}

const fn units_per_second(precision: TimestampPrecision) -> i64 {
    match precision {
        TimestampPrecision::Seconds => 1,
        TimestampPrecision::Milliseconds => 1_000,
        TimestampPrecision::Microseconds => 1_000_000,
        TimestampPrecision::Nanoseconds => 1_000_000_000,
    }
}

fn normalize_promql_value(value: &mut PromqlValue) -> Result<(), &'static str> {
    match value {
        PromqlValue::InstantVector(samples) => {
            for sample in samples.iter_mut() {
                sample.labels.sort_unstable();
            }
            samples.sort_by(compare_samples);
            if samples
                .windows(2)
                .any(|pair| pair[0].metric == pair[1].metric && pair[0].labels == pair[1].labels)
            {
                return Err("instant vector contains a duplicate series identity");
            }
        }
        PromqlValue::RangeVector(series) => {
            for item in series.iter_mut() {
                item.labels.sort_unstable();
                item.samples.sort_by(|left, right| {
                    left.0
                        .cmp(&right.0)
                        .then_with(|| left.1.total_cmp(&right.1))
                });
                item.histograms.sort_by_key(|item| item.0);
                if item.samples.windows(2).any(|pair| pair[0].0 == pair[1].0) {
                    return Err("range series contains duplicate float-sample timestamps");
                }
                if item
                    .histograms
                    .windows(2)
                    .any(|pair| pair[0].0 == pair[1].0)
                {
                    return Err("range series contains duplicate histogram timestamps");
                }
                if sorted_timestamps_intersect(&item.samples, &item.histograms) {
                    return Err(
                        "range series contains float and histogram samples at the same timestamp",
                    );
                }
            }
            series.sort_by(compare_series);
            if series
                .windows(2)
                .any(|pair| pair[0].metric == pair[1].metric && pair[0].labels == pair[1].labels)
            {
                return Err("range vector contains a duplicate series identity");
            }
        }
        PromqlValue::Scalar(_, _) | PromqlValue::String(_, _) => {}
    }
    Ok(())
}

fn sorted_timestamps_intersect(
    samples: &[(i64, f64)],
    histograms: &[(i64, Box<tsink::NativeHistogram>)],
) -> bool {
    let mut sample_index = 0;
    let mut histogram_index = 0;
    while let (Some(sample), Some(histogram)) =
        (samples.get(sample_index), histograms.get(histogram_index))
    {
        match sample.0.cmp(&histogram.0) {
            CmpOrdering::Less => sample_index += 1,
            CmpOrdering::Greater => histogram_index += 1,
            CmpOrdering::Equal => return true,
        }
    }
    false
}

fn compare_samples(left: &Sample, right: &Sample) -> CmpOrdering {
    left.metric
        .cmp(&right.metric)
        .then_with(|| left.labels.cmp(&right.labels))
        .then_with(|| left.timestamp.cmp(&right.timestamp))
        .then_with(|| left.value.total_cmp(&right.value))
        .then_with(|| left.histogram.is_some().cmp(&right.histogram.is_some()))
}

fn compare_series(left: &Series, right: &Series) -> CmpOrdering {
    left.metric
        .cmp(&right.metric)
        .then_with(|| left.labels.cmp(&right.labels))
}

fn promql_values_equal(left: &PromqlValue, right: &PromqlValue) -> bool {
    match (left, right) {
        (PromqlValue::Scalar(left, left_time), PromqlValue::Scalar(right, right_time)) => {
            left_time == right_time && float_exactly_equal(*left, *right)
        }
        (PromqlValue::InstantVector(left_samples), PromqlValue::InstantVector(right_samples)) => {
            left_samples.len() == right_samples.len()
                && left_samples.iter().zip(right_samples).all(|(left, right)| {
                    left.metric == right.metric
                        && left.labels == right.labels
                        && left.timestamp == right.timestamp
                        && float_exactly_equal(left.value, right.value)
                        && left.histogram == right.histogram
                })
        }
        (PromqlValue::RangeVector(left_series), PromqlValue::RangeVector(right_series)) => {
            left_series.len() == right_series.len()
                && left_series.iter().zip(right_series).all(|(left, right)| {
                    left.metric == right.metric
                        && left.labels == right.labels
                        && left.samples.len() == right.samples.len()
                        && left.samples.iter().zip(&right.samples).all(
                            |((left_time, left_value), (right_time, right_value))| {
                                left_time == right_time
                                    && float_exactly_equal(*left_value, *right_value)
                            },
                        )
                        && left.histograms == right.histograms
                })
        }
        (PromqlValue::String(left, left_time), PromqlValue::String(right, right_time)) => {
            left == right && left_time == right_time
        }
        _ => false,
    }
}

fn float_exactly_equal(left: f64, right: f64) -> bool {
    left == right || (left.is_nan() && right.is_nan() && left.to_bits() == right.to_bits())
}

fn float_approximately_equal(actual: f64, expected: f64, tolerance: f64) -> bool {
    actual == expected
        || (actual.is_nan() && expected.is_nan())
        || (actual.is_finite() && expected.is_finite() && (actual - expected).abs() <= tolerance)
}

fn open_storage(
    data_path: Option<&Path>,
    resource_profile: ResourceProfile,
    timestamp_precision: TimestampPrecision,
) -> tsink::Result<Arc<dyn Storage>> {
    let builder = StorageBuilder::new()
        .with_resource_profile(resource_profile)
        .with_timestamp_precision(timestamp_precision);
    match data_path {
        Some(path) => builder.with_data_path(path).build(),
        None => builder.build(),
    }
}

fn next_diagnostic_id() -> tsink::Result<String> {
    let sequence = NEXT_INSTANCE_SEQUENCE
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
            value.checked_add(1)
        })
        .map_err(|_| {
            TsinkError::InvalidConfiguration(
                "tsink-test exhausted its process-local diagnostic ID space".to_string(),
            )
        })?;
    Ok(format!(
        "tsink-test-p{}-{sequence:016x}",
        std::process::id()
    ))
}

fn bounded_diagnostic_message(message: &str) -> String {
    bounded_diagnostic_text(message, MAX_DIAGNOSTIC_MESSAGE_BYTES)
}

fn bounded_diagnostic_text(message: &str, limit: usize) -> String {
    if message.len() <= limit {
        return message.to_string();
    }
    const SUFFIX: &str = "...";
    if limit < SUFFIX.len() {
        return String::new();
    }
    let mut boundary = limit - SUFFIX.len();
    while !message.is_char_boundary(boundary) {
        boundary -= 1;
    }
    let mut bounded = String::with_capacity(limit);
    bounded.push_str(&message[..boundary]);
    bounded.push_str(SUFFIX);
    bounded
}

fn promql_value_kind(value: &PromqlValue) -> &'static str {
    match value {
        PromqlValue::Scalar(_, _) => "scalar",
        PromqlValue::InstantVector(_) => "instant_vector",
        PromqlValue::RangeVector(_) => "range_vector",
        PromqlValue::String(_, _) => "string",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tsink::QueryLimitExceeded;

    #[test]
    fn error_expectations_match_structured_unsupported_and_limit_details_exactly() {
        let unsupported = PromqlError::Storage(TsinkError::UnsupportedOperation {
            operation: "bounded PromQL metadata selection",
            reason: "test backend lacks complete accounting".to_string(),
        });
        assert!(PromqlErrorExpectation::UnsupportedOperation {
            operation: "bounded PromQL metadata selection",
        }
        .matches(&unsupported));
        assert!(!PromqlErrorExpectation::UnsupportedOperation {
            operation: "bounded PromQL point selection",
        }
        .matches(&unsupported));

        let limit = PromqlError::Storage(TsinkError::QueryBudget(QueryBudgetError::LimitExceeded(
            QueryLimitExceeded::new(QueryLimitReason::Steps, 10, 10, 1),
        )));
        assert!(PromqlErrorExpectation::QueryLimit(QueryLimitReason::Steps).matches(&limit));
        assert!(
            !PromqlErrorExpectation::QueryLimit(QueryLimitReason::SamplesScanned).matches(&limit)
        );
    }
}
