//! Main storage API and query helpers for tsink.

use crate::validation::validate_metric;
use crate::wal::{WalReplayMode, WalSyncMode};
use crate::{
    Aggregator as TypedAggregator, BytesAggregation, Codec, CodecAggregator, DataPoint, Label,
    QueryBudget, QueryBudgetLimits, QueryBudgetSnapshot, QueryCancellationToken, QueryExecution,
    QueryWorkLimits, Result, Row, TsinkError,
};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

pub(crate) const DEFAULT_CHUNK_POINTS: usize = 2048;
pub(crate) const DEFAULT_REMOTE_SEGMENT_REFRESH_INTERVAL: Duration = Duration::from_secs(5);
/// Default number of simultaneously open time-partition heads retained for each series.
pub const DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES: usize = 8;
/// Maximum regular-file and directory entries accepted from one snapshot restore tree.
///
/// The root snapshot directory counts as one entry. This bounds traversal work before staging.
pub const MAX_SNAPSHOT_RESTORE_ENTRIES: u64 = 100_000;
/// Maximum descendant-directory depth accepted from a snapshot restore tree.
///
/// The snapshot root has depth zero, so a path may contain at most this many directory components
/// beneath it. This bounds both measurement traversal and recursive copy stack use.
pub const MAX_SNAPSHOT_RESTORE_DEPTH: u32 = 128;
/// Policy floor for the temporary-admission allowance charged per snapshot restore entry.
///
/// The effective allowance is the greater of this value and the destination filesystem's reported
/// allocation unit. Restore staging adds that effective allowance for every entry to the regular
/// files' logical lengths. This is conservative admission policy, not an exact physical-byte
/// claim.
pub const SNAPSHOT_RESTORE_ENTRY_STAGING_ALLOWANCE_FLOOR_BYTES: u64 = 4 * 1024;

/// Unit used to interpret timestamps and time-based storage settings.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum TimestampPrecision {
    /// Timestamps are nanoseconds since the Unix epoch.
    Nanoseconds,
    /// Timestamps are microseconds since the Unix epoch.
    Microseconds,
    /// Timestamps are milliseconds since the Unix epoch.
    Milliseconds,
    /// Timestamps are seconds since the Unix epoch.
    Seconds,
}

/// Determines whether a storage instance owns writable local state or reads remote segments.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StorageRuntimeMode {
    /// Accept writes and maintain local persistent state when a data path is configured.
    #[default]
    ReadWrite,
    /// Serve queries from an object-store-backed segment catalog without local writes.
    ComputeOnly,
}

/// Cache policy for segments discovered in object storage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RemoteSegmentCachePolicy {
    /// Cache segment metadata while reading segment contents from the remote tier on demand.
    #[default]
    MetadataOnly,
}

#[derive(
    Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
pub struct MetricSeries {
    pub name: String,
    pub labels: Vec<Label>,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SeriesPoints {
    pub series: MetricSeries,
    pub points: Vec<DataPoint>,
}

/// Result of an execution-aware batch selection.
///
/// Built-in backends that report [`QueryExecutionAccounting::Complete`] return one
/// `matched_selectors` bit for every input selector, in input order. The bit distinguishes an
/// existing series with no points in the requested time range from a missing series. Compatibility
/// backends may return `None`.
#[derive(Debug)]
pub struct SelectManyExecutionResult {
    /// One result for every requested selector, in input order.
    pub series: Vec<SeriesPoints>,
    /// Exact series-existence bits aligned with `series`, when the backend exposes them.
    pub matched_selectors: Option<Vec<bool>>,
    memory_reservation: Option<crate::QueryMemoryReservation>,
}

impl SelectManyExecutionResult {
    /// Creates a compatibility result without exact existence metadata or a retained-memory
    /// reservation.
    #[must_use]
    pub fn unaccounted(series: Vec<SeriesPoints>) -> Self {
        Self {
            series,
            matched_selectors: None,
            memory_reservation: None,
        }
    }

    /// Creates a completely accounted result whose memory remains reserved while it is owned.
    #[must_use]
    pub fn accounted(
        series: Vec<SeriesPoints>,
        matched_selectors: Vec<bool>,
        memory_reservation: crate::QueryMemoryReservation,
    ) -> Self {
        Self {
            series,
            matched_selectors: Some(matched_selectors),
            memory_reservation: Some(memory_reservation),
        }
    }

    /// Creates a compatibility result whose returned allocation remains query-memory-accounted.
    #[must_use]
    pub fn reserved_unaccounted(
        series: Vec<SeriesPoints>,
        memory_reservation: crate::QueryMemoryReservation,
    ) -> Self {
        Self {
            series,
            matched_selectors: None,
            memory_reservation: Some(memory_reservation),
        }
    }

    /// Returns the bytes retained on behalf of this result.
    #[must_use]
    pub fn reserved_memory_bytes(&self) -> u64 {
        self.memory_reservation
            .as_ref()
            .map_or(0, crate::QueryMemoryReservation::bytes)
    }

    /// Removes and returns the reservation so a wrapper can replace it with its own accounting.
    #[must_use]
    pub fn take_memory_reservation(&mut self) -> Option<crate::QueryMemoryReservation> {
        self.memory_reservation.take()
    }

    /// Consumes the detailed result and returns the compatibility series vector.
    ///
    /// This drops the retained reservation and is only appropriate for compatibility paths or
    /// callers that already own accounting for the returned allocation. Bounded wrappers must
    /// take and transfer/adopt the reservation before extracting the series.
    #[must_use]
    pub fn into_series(self) -> Vec<SeriesPoints> {
        self.series
    }
}

/// Execution-aware metadata selection whose returned allocation remains query-memory-accounted.
#[derive(Debug)]
pub struct SelectSeriesExecutionResult {
    /// Matched series in the storage operation's deterministic order.
    pub series: Vec<MetricSeries>,
    memory_reservation: Option<crate::QueryMemoryReservation>,
}

impl SelectSeriesExecutionResult {
    /// Creates a compatibility result without a retained-memory reservation.
    #[must_use]
    pub fn unaccounted(series: Vec<MetricSeries>) -> Self {
        Self {
            series,
            memory_reservation: None,
        }
    }

    /// Creates a completely accounted result whose memory remains reserved while it is owned.
    #[must_use]
    pub fn accounted(
        series: Vec<MetricSeries>,
        memory_reservation: crate::QueryMemoryReservation,
    ) -> Self {
        Self {
            series,
            memory_reservation: Some(memory_reservation),
        }
    }

    /// Creates a compatibility metadata result whose allocation remains query-memory-accounted.
    #[must_use]
    pub fn reserved_unaccounted(
        series: Vec<MetricSeries>,
        memory_reservation: crate::QueryMemoryReservation,
    ) -> Self {
        Self {
            series,
            memory_reservation: Some(memory_reservation),
        }
    }

    /// Returns the bytes retained on behalf of this result.
    #[must_use]
    pub fn reserved_memory_bytes(&self) -> u64 {
        self.memory_reservation
            .as_ref()
            .map_or(0, crate::QueryMemoryReservation::bytes)
    }

    /// Removes and returns the reservation so a wrapper can replace it with its own accounting.
    #[must_use]
    pub fn take_memory_reservation(&mut self) -> Option<crate::QueryMemoryReservation> {
        self.memory_reservation.take()
    }

    /// Consumes the detailed result and returns the compatibility series vector.
    ///
    /// This drops the retained reservation and is only appropriate for compatibility paths or
    /// callers that already own accounting for the returned allocation. Bounded wrappers must
    /// take and transfer/adopt the reservation before extracting the series.
    #[must_use]
    pub fn into_series(self) -> Vec<MetricSeries> {
        self.series
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SeriesMatcherOp {
    Equal,
    NotEqual,
    RegexMatch,
    RegexNoMatch,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SeriesMatcher {
    pub name: String,
    pub op: SeriesMatcherOp,
    pub value: String,
}

impl SeriesMatcher {
    #[must_use]
    pub fn new(name: impl Into<String>, op: SeriesMatcherOp, value: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            op,
            value: value.into(),
        }
    }

    #[must_use]
    pub fn equal(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(name, SeriesMatcherOp::Equal, value)
    }

    #[must_use]
    pub fn not_equal(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(name, SeriesMatcherOp::NotEqual, value)
    }

    #[must_use]
    pub fn regex_match(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(name, SeriesMatcherOp::RegexMatch, value)
    }

    #[must_use]
    pub fn regex_no_match(name: impl Into<String>, value: impl Into<String>) -> Self {
        Self::new(name, SeriesMatcherOp::RegexNoMatch, value)
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct SeriesSelection {
    pub metric: Option<String>,
    pub matchers: Vec<SeriesMatcher>,
    pub start: Option<i64>,
    pub end: Option<i64>,
}

/// Stable validation failure for a structured [`SeriesSelection`].
///
/// Protocol and distributed adapters can map this type to a client-input error without
/// conflating it with storage configuration failures. Diagnostics contain only bounded indexes
/// and byte counts; submitted regex text is never echoed.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SeriesSelectionValidationError {
    MetricRequired,
    MetricTooLong {
        actual: usize,
        maximum: usize,
    },
    InvalidMetricName,
    TooManyMatchers {
        actual: usize,
        maximum: usize,
    },
    EmptyMatcherName {
        matcher_index: usize,
    },
    MatcherNameTooLong {
        matcher_index: usize,
        actual: usize,
        maximum: usize,
    },
    MatcherValueTooLong {
        matcher_index: usize,
        actual: usize,
        maximum: usize,
    },
    MatcherBytesTooLong {
        actual: usize,
        maximum: usize,
    },
    InvalidMatcherRegex {
        matcher_index: usize,
    },
    InvalidTimeRange {
        start: i64,
        end: i64,
    },
    IncompleteTimeRange,
}

impl std::fmt::Display for SeriesSelectionValidationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MetricRequired => formatter.write_str("series selection metric cannot be empty"),
            Self::MetricTooLong { actual, maximum } => write!(
                formatter,
                "series selection metric is {actual} bytes, exceeding the hard limit {maximum}"
            ),
            Self::InvalidMetricName => {
                formatter.write_str("series selection metric name is invalid")
            }
            Self::TooManyMatchers { actual, maximum } => write!(
                formatter,
                "series selection has {actual} matchers, exceeding the hard limit {maximum}"
            ),
            Self::EmptyMatcherName { matcher_index } => write!(
                formatter,
                "series matcher at index {matcher_index} has an empty label name"
            ),
            Self::MatcherNameTooLong {
                matcher_index,
                actual,
                maximum,
            } => write!(
                formatter,
                "series matcher at index {matcher_index} has a {actual}-byte label name, exceeding the hard limit {maximum}"
            ),
            Self::MatcherValueTooLong {
                matcher_index,
                actual,
                maximum,
            } => write!(
                formatter,
                "series matcher at index {matcher_index} has a {actual}-byte value, exceeding the hard limit {maximum}"
            ),
            Self::MatcherBytesTooLong { actual, maximum } => write!(
                formatter,
                "series selection matcher names and values total {actual} bytes, exceeding the hard limit {maximum}"
            ),
            Self::InvalidMatcherRegex { matcher_index } => write!(
                formatter,
                "series matcher at index {matcher_index} has an invalid or overly complex regex"
            ),
            Self::InvalidTimeRange { start, end } => write!(
                formatter,
                "invalid series selection time range: start ({start}) must be before end ({end})"
            ),
            Self::IncompleteTimeRange => formatter.write_str(
                "series selection requires both start and end when time range filtering is enabled",
            ),
        }
    }
}

impl std::error::Error for SeriesSelectionValidationError {}

impl From<SeriesSelectionValidationError> for TsinkError {
    fn from(error: SeriesSelectionValidationError) -> Self {
        match error {
            SeriesSelectionValidationError::MetricRequired => Self::MetricRequired,
            SeriesSelectionValidationError::MetricTooLong { actual, maximum } => {
                Self::InvalidMetricName(format!(
                    "metric name too long: {actual} bytes (max {maximum})"
                ))
            }
            SeriesSelectionValidationError::InvalidMetricName => {
                Self::InvalidMetricName("metric name is invalid".to_string())
            }
            SeriesSelectionValidationError::InvalidTimeRange { start, end } => {
                Self::InvalidTimeRange { start, end }
            }
            other => Self::InvalidConfiguration(other.to_string()),
        }
    }
}

/// Failure while preparing a [`SeriesSelection`] under a [`QueryExecution`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum SeriesSelectionPreparationError {
    Validation(SeriesSelectionValidationError),
    Query(crate::QueryBudgetError),
}

impl std::fmt::Display for SeriesSelectionPreparationError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Validation(error) => error.fmt(formatter),
            Self::Query(error) => error.fmt(formatter),
        }
    }
}

impl std::error::Error for SeriesSelectionPreparationError {}

impl From<SeriesSelectionPreparationError> for TsinkError {
    fn from(error: SeriesSelectionPreparationError) -> Self {
        match error {
            SeriesSelectionPreparationError::Validation(error) => error.into(),
            SeriesSelectionPreparationError::Query(error) => Self::QueryBudget(error),
        }
    }
}

/// Retained regex-program and query-memory guard for a validated series selection.
///
/// Keep this value alive through cache-key construction, request cloning, planning, and matcher
/// use. Dropping it releases all memory charged by [`SeriesSelection::prepare_with_execution`].
#[must_use = "dropping the preparation releases its query-memory reservation"]
pub struct SeriesSelectionPreparation {
    _regexes: Vec<crate::query_matcher::ExecutionBoundedRegex>,
    _slots_reservation: crate::QueryMemoryReservation,
}

impl SeriesSelection {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn with_metric(mut self, metric: impl Into<String>) -> Self {
        self.metric = Some(metric.into());
        self
    }

    #[must_use]
    pub fn with_matcher(mut self, matcher: SeriesMatcher) -> Self {
        self.matchers.push(matcher);
        self
    }

    #[must_use]
    pub fn with_matchers(mut self, matchers: Vec<SeriesMatcher>) -> Self {
        self.matchers = matchers;
        self
    }

    #[must_use]
    pub fn with_time_range(mut self, start: i64, end: i64) -> Self {
        self.start = Some(start);
        self.end = Some(end);
        self
    }

    /// Validates metric, matcher, regex, and time-range shape without accessing storage.
    ///
    /// This performs bounded regex compilation so adapters can reject malformed or overly
    /// complex patterns before cache-key construction, fanout planning, or request cloning.
    pub fn validate(&self) -> std::result::Result<(), SeriesSelectionValidationError> {
        self.validate_shape_and_time()?;
        for (matcher_index, matcher) in self.matchers.iter().enumerate() {
            if matches!(
                matcher.op,
                SeriesMatcherOp::RegexMatch | SeriesMatcherOp::RegexNoMatch
            ) && crate::query_matcher::build_bounded_regex(
                &matcher.value,
                crate::query_matcher::RegexAnchoring::Anchored,
            )
            .is_err()
            {
                return Err(SeriesSelectionValidationError::InvalidMatcherRegex { matcher_index });
            }
        }
        Ok(())
    }

    /// Performs allocation-free metric, matcher-shape, and time-range validation.
    ///
    /// This does not compile regexes. Execution-aware callers should follow it with
    /// [`Self::prepare_with_execution`] and retain the returned guard through downstream use.
    pub fn validate_shape(&self) -> std::result::Result<(), SeriesSelectionValidationError> {
        self.validate_shape_and_time().map(|_| ())
    }

    /// Validates and compiles regex matchers under `execution` memory and control limits.
    ///
    /// Compiler scratch is admitted before each compile and released afterward. Compiled
    /// programs and their bounded lazy-DFA caches remain charged until the returned guard drops.
    pub fn prepare_with_execution(
        &self,
        execution: &QueryExecution,
    ) -> std::result::Result<SeriesSelectionPreparation, SeriesSelectionPreparationError> {
        self.validate_shape_and_time()
            .map_err(SeriesSelectionPreparationError::Validation)?;
        execution
            .checkpoint()
            .map_err(SeriesSelectionPreparationError::Query)?;
        let regex_count = self
            .matchers
            .iter()
            .filter(|matcher| {
                matches!(
                    matcher.op,
                    SeriesMatcherOp::RegexMatch | SeriesMatcherOp::RegexNoMatch
                )
            })
            .count();
        let slots_reservation = execution
            .reserve_memory(crate::query_matcher::modeled_execution_regex_slots_bytes(
                regex_count,
            ))
            .map_err(SeriesSelectionPreparationError::Query)?;
        let mut regexes = Vec::with_capacity(regex_count);
        for (matcher_index, matcher) in self.matchers.iter().enumerate() {
            if !matches!(
                matcher.op,
                SeriesMatcherOp::RegexMatch | SeriesMatcherOp::RegexNoMatch
            ) {
                continue;
            }
            let regex = crate::query_matcher::prepare_bounded_regex_with_execution(
                &matcher.value,
                crate::query_matcher::RegexAnchoring::Anchored,
                execution,
            )
            .map_err(|error| match error {
                crate::query_matcher::BoundedRegexPreparationError::Regex(_) => {
                    SeriesSelectionPreparationError::Validation(
                        SeriesSelectionValidationError::InvalidMatcherRegex { matcher_index },
                    )
                }
                crate::query_matcher::BoundedRegexPreparationError::Query(error) => {
                    SeriesSelectionPreparationError::Query(error)
                }
            })?;
            regexes.push(regex);
        }
        Ok(SeriesSelectionPreparation {
            _regexes: regexes,
            _slots_reservation: slots_reservation,
        })
    }

    pub(crate) fn validate_shape_and_time(
        &self,
    ) -> std::result::Result<Option<(i64, i64)>, SeriesSelectionValidationError> {
        if let Some(metric) = self.metric.as_deref() {
            if metric.is_empty() {
                return Err(SeriesSelectionValidationError::MetricRequired);
            }
            if metric.len() > crate::label::MAX_METRIC_NAME_LEN {
                return Err(SeriesSelectionValidationError::MetricTooLong {
                    actual: metric.len(),
                    maximum: crate::label::MAX_METRIC_NAME_LEN,
                });
            }
            if crate::validation::validate_metric(metric).is_err() {
                return Err(SeriesSelectionValidationError::InvalidMetricName);
            }
        }
        crate::query_matcher::validate_matcher_shapes(
            self.matchers.len(),
            self.matchers
                .iter()
                .map(|matcher| (matcher.name.as_str(), matcher.value.as_str())),
        )
        .map_err(|error| match error {
            crate::query_matcher::MatcherShapeError::TooManyMatchers { actual } => {
                SeriesSelectionValidationError::TooManyMatchers {
                    actual,
                    maximum: crate::MAX_SERIES_SELECTION_MATCHERS,
                }
            }
            crate::query_matcher::MatcherShapeError::EmptyName { index } => {
                SeriesSelectionValidationError::EmptyMatcherName {
                    matcher_index: index,
                }
            }
            crate::query_matcher::MatcherShapeError::NameTooLong { index, actual } => {
                SeriesSelectionValidationError::MatcherNameTooLong {
                    matcher_index: index,
                    actual,
                    maximum: crate::MAX_SERIES_MATCHER_NAME_BYTES,
                }
            }
            crate::query_matcher::MatcherShapeError::ValueTooLong { index, actual } => {
                SeriesSelectionValidationError::MatcherValueTooLong {
                    matcher_index: index,
                    actual,
                    maximum: crate::MAX_SERIES_MATCHER_VALUE_BYTES,
                }
            }
            crate::query_matcher::MatcherShapeError::TotalTooLong { actual } => {
                SeriesSelectionValidationError::MatcherBytesTooLong {
                    actual,
                    maximum: crate::MAX_SERIES_SELECTION_MATCHER_BYTES,
                }
            }
        })?;

        match (self.start, self.end) {
            (None, None) => Ok(None),
            (Some(start), Some(end)) if start < end => Ok(Some((start, end))),
            (Some(start), Some(end)) => {
                Err(SeriesSelectionValidationError::InvalidTimeRange { start, end })
            }
            _ => Err(SeriesSelectionValidationError::IncompleteTimeRange),
        }
    }

    pub(crate) fn normalized_time_range(&self) -> Result<Option<(i64, i64)>> {
        self.validate_shape_and_time().map_err(Into::into)
    }
}

#[cfg(test)]
mod series_selection_validation_tests {
    use super::*;
    use crate::{
        QueryBudgetError, QueryLimitReason, MAX_QUERY_REGEX_DIAGNOSTIC_BYTES,
        MAX_SERIES_MATCHER_NAME_BYTES, MAX_SERIES_MATCHER_VALUE_BYTES,
        MAX_SERIES_SELECTION_MATCHERS, MAX_SERIES_SELECTION_MATCHER_BYTES,
    };
    use std::time::Instant;

    #[test]
    fn public_validation_accepts_exact_matcher_shape_limits_and_returns_typed_one_over_errors() {
        let exact_count = SeriesSelection::new().with_matchers(vec![
            SeriesMatcher::equal("n", "");
            MAX_SERIES_SELECTION_MATCHERS
        ]);
        assert!(exact_count.validate_shape().is_ok());
        let one_over_count =
            SeriesSelection::new().with_matchers(vec![
                SeriesMatcher::equal("n", "");
                MAX_SERIES_SELECTION_MATCHERS + 1
            ]);
        assert!(matches!(
            one_over_count.validate_shape(),
            Err(SeriesSelectionValidationError::TooManyMatchers {
                actual,
                maximum
            }) if actual == MAX_SERIES_SELECTION_MATCHERS + 1
                && maximum == MAX_SERIES_SELECTION_MATCHERS
        ));

        let exact_name = SeriesSelection::new().with_matcher(SeriesMatcher::equal(
            "n".repeat(MAX_SERIES_MATCHER_NAME_BYTES),
            "",
        ));
        assert!(exact_name.validate_shape().is_ok());
        let one_over_name = SeriesSelection::new().with_matcher(SeriesMatcher::equal(
            "n".repeat(MAX_SERIES_MATCHER_NAME_BYTES + 1),
            "",
        ));
        assert!(matches!(
            one_over_name.validate_shape(),
            Err(SeriesSelectionValidationError::MatcherNameTooLong {
                matcher_index: 0,
                actual,
                maximum
            }) if actual == MAX_SERIES_MATCHER_NAME_BYTES + 1
                && maximum == MAX_SERIES_MATCHER_NAME_BYTES
        ));

        let exact_value = SeriesSelection::new().with_matcher(SeriesMatcher::equal(
            "n",
            "v".repeat(MAX_SERIES_MATCHER_VALUE_BYTES),
        ));
        assert!(exact_value.validate_shape().is_ok());
        let one_over_value = SeriesSelection::new().with_matcher(SeriesMatcher::equal(
            "n",
            "v".repeat(MAX_SERIES_MATCHER_VALUE_BYTES + 1),
        ));
        assert!(matches!(
            one_over_value.validate_shape(),
            Err(SeriesSelectionValidationError::MatcherValueTooLong {
                matcher_index: 0,
                actual,
                maximum
            }) if actual == MAX_SERIES_MATCHER_VALUE_BYTES + 1
                && maximum == MAX_SERIES_MATCHER_VALUE_BYTES
        ));

        let exact_total_matcher_bytes = SeriesSelection::new().with_matchers(
            (0..4)
                .map(|_| {
                    SeriesMatcher::equal(
                        "n",
                        "v".repeat(MAX_SERIES_SELECTION_MATCHER_BYTES / 4 - 1),
                    )
                })
                .collect(),
        );
        assert!(exact_total_matcher_bytes.validate_shape().is_ok());
        let mut one_over_total_matchers = exact_total_matcher_bytes.matchers;
        one_over_total_matchers.push(SeriesMatcher::equal("n", ""));
        assert!(matches!(
            SeriesSelection::new()
                .with_matchers(one_over_total_matchers)
                .validate_shape(),
            Err(SeriesSelectionValidationError::MatcherBytesTooLong {
                actual,
                maximum
            }) if actual == MAX_SERIES_SELECTION_MATCHER_BYTES + 1
                && maximum == MAX_SERIES_SELECTION_MATCHER_BYTES
        ));
    }

    #[test]
    fn public_regex_validation_has_a_bounded_pattern_free_diagnostic() {
        let private_marker = "private-pattern-marker";
        let pattern = format!(
            "{}{private_marker}{}",
            "(".repeat(crate::QUERY_REGEX_NEST_LIMIT as usize + 1),
            ")".repeat(crate::QUERY_REGEX_NEST_LIMIT as usize + 1)
        );
        let selection =
            SeriesSelection::new().with_matcher(SeriesMatcher::regex_match("host", pattern));
        let error = selection
            .validate()
            .expect_err("over-nested regex must fail");
        assert!(matches!(
            error,
            SeriesSelectionValidationError::InvalidMatcherRegex { matcher_index: 0 }
        ));
        let diagnostic = error.to_string();
        assert!(diagnostic.len() <= MAX_QUERY_REGEX_DIAGNOSTIC_BYTES);
        assert!(!diagnostic.contains(private_marker));
    }

    #[test]
    fn public_execution_preparation_has_an_exact_memory_boundary_and_zero_residue() {
        let selection =
            SeriesSelection::new().with_matcher(SeriesMatcher::regex_match("host", "web-[0-9]+"));
        let calibration_budget = QueryBudget::new(QueryBudgetLimits::default()).unwrap();
        let calibration_execution = calibration_budget.begin_query().unwrap();
        let preparation = selection
            .prepare_with_execution(&calibration_execution)
            .unwrap();
        let exact_memory = calibration_budget
            .snapshot()
            .peak_shared_reserved_memory_bytes;
        assert!(exact_memory > 1);
        drop(preparation);
        assert_eq!(calibration_execution.snapshot().memory_reserved_bytes, 0);
        drop(calibration_execution);
        assert_eq!(
            calibration_budget.snapshot().shared_reserved_memory_bytes,
            0
        );

        for (limit, succeeds) in [(exact_memory, true), (exact_memory - 1, false)] {
            let budget = QueryBudget::new(QueryBudgetLimits {
                max_shared_memory_bytes: Some(limit),
                per_query: QueryWorkLimits {
                    max_memory_bytes: Some(limit),
                    ..QueryWorkLimits::default()
                },
                ..QueryBudgetLimits::default()
            })
            .unwrap();
            let execution = budget.begin_query().unwrap();
            let result = selection.prepare_with_execution(&execution);
            match (result, succeeds) {
                (Ok(preparation), true) => drop(preparation),
                (
                    Err(SeriesSelectionPreparationError::Query(QueryBudgetError::LimitExceeded(
                        exceeded,
                    ))),
                    false,
                ) => assert_eq!(exceeded.reason, QueryLimitReason::PerQueryMemoryBytes),
                (Ok(preparation), false) => {
                    drop(preparation);
                    panic!("one byte below the preparation peak must fail");
                }
                (Err(error), true) => panic!("exact preparation peak must succeed: {error}"),
                (Err(error), false) => panic!("unexpected preparation error: {error}"),
            }
            assert_eq!(execution.snapshot().memory_reserved_bytes, 0);
            drop(execution);
            let snapshot = budget.snapshot();
            assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
            assert_eq!(snapshot.accounting_invariant_violations_total, 0);
        }
    }

    #[test]
    fn public_execution_preparation_obeys_control_before_compile_and_releases_memory() {
        let selection =
            SeriesSelection::new().with_matcher(SeriesMatcher::regex_match("host", "("));

        let cancellation = QueryCancellationToken::new();
        let cancellation_budget = QueryBudget::new(QueryBudgetLimits::default()).unwrap();
        let cancellation_execution = cancellation_budget
            .begin_query_with_token(cancellation.clone())
            .unwrap();
        cancellation.cancel();
        assert!(matches!(
            selection.prepare_with_execution(&cancellation_execution),
            Err(SeriesSelectionPreparationError::Query(
                QueryBudgetError::Cancelled
            ))
        ));
        assert_eq!(cancellation_execution.snapshot().memory_reserved_bytes, 0);
        drop(cancellation_execution);
        assert_eq!(
            cancellation_budget.snapshot().shared_reserved_memory_bytes,
            0
        );

        let deadline = Instant::now() + Duration::from_millis(20);
        let deadline_budget = QueryBudget::new(QueryBudgetLimits::default()).unwrap();
        let deadline_execution = deadline_budget
            .begin_query_with_token(QueryCancellationToken::new().with_deadline(deadline))
            .unwrap();
        while Instant::now() < deadline {
            std::hint::spin_loop();
        }
        assert!(matches!(
            selection.prepare_with_execution(&deadline_execution),
            Err(SeriesSelectionPreparationError::Query(
                QueryBudgetError::DeadlineExceeded
            ))
        ));
        assert_eq!(deadline_execution.snapshot().memory_reserved_bytes, 0);
        drop(deadline_execution);
        assert_eq!(deadline_budget.snapshot().shared_reserved_memory_bytes, 0);
    }
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MetadataShardScope {
    pub shard_count: u32,
    pub shards: Vec<u32>,
}

impl MetadataShardScope {
    #[must_use]
    pub fn new(shard_count: u32, shards: Vec<u32>) -> Self {
        Self {
            shard_count,
            shards,
        }
    }

    pub fn normalized(&self) -> Result<Self> {
        if self.shard_count == 0 {
            return Err(TsinkError::InvalidConfiguration(
                "metadata shard scope requires shard_count > 0".to_string(),
            ));
        }

        let mut shards = self.shards.clone();
        shards.sort_unstable();
        shards.dedup();
        if let Some(shard) = shards
            .iter()
            .copied()
            .find(|shard| *shard >= self.shard_count)
        {
            return Err(TsinkError::InvalidConfiguration(format!(
                "metadata shard scope shard {shard} is out of range for shard_count {}",
                self.shard_count
            )));
        }

        Ok(Self {
            shard_count: self.shard_count,
            shards,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ShardWindowDigest {
    pub shard: u32,
    pub shard_count: u32,
    pub window_start: i64,
    pub window_end: i64,
    pub series_count: u64,
    pub point_count: u64,
    pub fingerprint: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ShardWindowScanOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_series: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rows: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_offset: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct ShardWindowRowsPage {
    pub shard: u32,
    pub shard_count: u32,
    pub window_start: i64,
    pub window_end: i64,
    pub series_scanned: u64,
    pub rows_scanned: u64,
    pub truncated: bool,
    pub next_row_offset: Option<u64>,
    pub rows: Vec<Row>,
}

/// Execution-aware shard-window page whose returned allocation remains query-memory-accounted.
#[derive(Debug)]
pub struct ShardWindowRowsExecutionResult {
    /// Bounded shard-window page returned by the storage backend.
    pub page: ShardWindowRowsPage,
    memory_reservation: Option<crate::QueryMemoryReservation>,
}

impl ShardWindowRowsExecutionResult {
    /// Creates a compatibility result without a retained-memory reservation.
    #[must_use]
    pub fn unaccounted(page: ShardWindowRowsPage) -> Self {
        Self {
            page,
            memory_reservation: None,
        }
    }

    /// Creates a completely accounted result whose memory remains reserved while it is owned.
    #[must_use]
    pub fn accounted(
        page: ShardWindowRowsPage,
        memory_reservation: crate::QueryMemoryReservation,
    ) -> Self {
        Self {
            page,
            memory_reservation: Some(memory_reservation),
        }
    }

    /// Returns the bytes retained on behalf of this result.
    #[must_use]
    pub fn reserved_memory_bytes(&self) -> u64 {
        self.memory_reservation
            .as_ref()
            .map_or(0, crate::QueryMemoryReservation::bytes)
    }

    /// Removes and returns the reservation so a wrapper can replace or transfer its accounting.
    #[must_use]
    pub fn take_memory_reservation(&mut self) -> Option<crate::QueryMemoryReservation> {
        self.memory_reservation.take()
    }

    /// Consumes the detailed result and returns the compatibility page.
    ///
    /// This drops the retained reservation and is only appropriate for compatibility paths.
    /// Bounded wrappers must retain or transfer the reservation while the page remains owned.
    #[must_use]
    pub fn into_page(self) -> ShardWindowRowsPage {
        self.page
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct QueryRowsScanOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rows: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub row_offset: Option<u64>,
}

#[derive(Debug, Clone)]
pub struct QueryRowsPage {
    pub rows_scanned: u64,
    pub truncated: bool,
    pub next_row_offset: Option<u64>,
    pub rows: Vec<Row>,
}

/// Execution-aware row page whose returned allocation remains query-memory-accounted.
#[derive(Debug)]
pub struct QueryRowsExecutionResult {
    /// Bounded row page returned by the storage backend.
    pub page: QueryRowsPage,
    memory_reservation: Option<crate::QueryMemoryReservation>,
}

impl QueryRowsExecutionResult {
    /// Creates a compatibility result without a retained-memory reservation.
    #[must_use]
    pub fn unaccounted(page: QueryRowsPage) -> Self {
        Self {
            page,
            memory_reservation: None,
        }
    }

    /// Creates a completely accounted result whose memory remains reserved while it is owned.
    #[must_use]
    pub fn accounted(
        page: QueryRowsPage,
        memory_reservation: crate::QueryMemoryReservation,
    ) -> Self {
        Self {
            page,
            memory_reservation: Some(memory_reservation),
        }
    }

    /// Returns the bytes retained on behalf of this result.
    #[must_use]
    pub fn reserved_memory_bytes(&self) -> u64 {
        self.memory_reservation
            .as_ref()
            .map_or(0, crate::QueryMemoryReservation::bytes)
    }

    /// Removes and returns the reservation so a wrapper can resize or transfer its accounting.
    #[must_use]
    pub fn take_memory_reservation(&mut self) -> Option<crate::QueryMemoryReservation> {
        self.memory_reservation.take()
    }

    /// Consumes the detailed result and returns the compatibility row page.
    ///
    /// This drops the retained reservation and is only appropriate for compatibility paths or
    /// callers that already own accounting for the returned allocation. Bounded wrappers must
    /// take and transfer/adopt the reservation before extracting the page.
    #[must_use]
    pub fn into_page(self) -> QueryRowsPage {
        self.page
    }
}

/// Durability guarantee established for a successful write when the call returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteAcknowledgement {
    /// The write is visible in memory without a complete crash-recovery log guarantee.
    ///
    /// This is the conservative default for backends that do not expose stronger durability
    /// metadata at the API boundary, and for storage configured without a WAL.
    Volatile,
    /// The write was appended to the WAL but may still be lost in a crash window.
    ///
    /// This is typical for [`WalSyncMode::Periodic`] before a later fsync or persistence step
    /// makes the write durable.
    Appended,
    /// The configured core WAL synchronization contract completed before the call returned.
    ///
    /// This is tsink's strongest software acknowledgement. Actual crash survival still depends on
    /// the filesystem, mount options, storage controller, and hardware honoring those operations.
    Durable,
}

impl WriteAcknowledgement {
    /// Returns the weaker of two acknowledgement guarantees.
    ///
    /// This is used for batch results that combine independently acknowledged rows. The
    /// guarantee order is [`WriteAcknowledgement::Volatile`], then
    /// [`WriteAcknowledgement::Appended`], then [`WriteAcknowledgement::Durable`].
    #[must_use]
    pub const fn weakest(self, other: Self) -> Self {
        match (self, other) {
            (Self::Volatile, _) | (_, Self::Volatile) => Self::Volatile,
            (Self::Appended, _) | (_, Self::Appended) => Self::Appended,
            (Self::Durable, Self::Durable) => Self::Durable,
        }
    }

    /// Returns whether the acknowledgement records tsink's strongest software durability level.
    #[must_use]
    pub const fn is_durable(self) -> bool {
        matches!(self, Self::Durable)
    }

    /// Returns the snake-case representation used by external adapters.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Volatile => "volatile",
            Self::Appended => "appended",
            Self::Durable => "durable",
        }
    }
}

/// Maximum UTF-8 byte length of a diagnostic message in a [`WriteRejection`].
///
/// Messages longer than this limit are truncated at a character boundary. Callers should use
/// [`WriteRejection::category`] for control flow; the bounded message is diagnostic only.
pub const MAX_WRITE_REJECTION_MESSAGE_BYTES: usize = 512;

/// Optional limits applied to one foreground write submission before the engine clones any row
/// identity or value payload.
///
/// `None` preserves the legacy unbounded behavior for that dimension. `max_modeled_input_bytes`
/// counts the logical UTF-8/byte/histogram payload plus the modeled row, label, and value storage
/// that write preparation must duplicate; it is an admission model, not serialized wire size or
/// allocator RSS.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WriteBatchLimits {
    /// Maximum rows accepted in one top-level submission. Besides bounding input traversal, this
    /// also bounds the indexed outcome vector returned by best-effort writes.
    pub max_rows: Option<usize>,
    /// Maximum modeled input bytes accepted in one top-level submission.
    pub max_modeled_input_bytes: Option<usize>,
}

fn checked_write_size_add(lhs: usize, rhs: usize) -> Result<usize> {
    lhs.checked_add(rhs)
        .ok_or(TsinkError::WriteBatchSizeOverflow)
}

fn checked_write_size_mul(lhs: usize, rhs: usize) -> Result<usize> {
    lhs.checked_mul(rhs)
        .ok_or(TsinkError::WriteBatchSizeOverflow)
}

fn modeled_histogram_payload_bytes(histogram: &crate::NativeHistogram) -> Result<usize> {
    let mut bytes = std::mem::size_of::<crate::NativeHistogram>();
    for (len, element_bytes) in [
        (
            histogram.negative_spans.len(),
            std::mem::size_of::<crate::HistogramBucketSpan>(),
        ),
        (histogram.negative_deltas.len(), std::mem::size_of::<i64>()),
        (histogram.negative_counts.len(), std::mem::size_of::<f64>()),
        (
            histogram.positive_spans.len(),
            std::mem::size_of::<crate::HistogramBucketSpan>(),
        ),
        (histogram.positive_deltas.len(), std::mem::size_of::<i64>()),
        (histogram.positive_counts.len(), std::mem::size_of::<f64>()),
        (histogram.custom_values.len(), std::mem::size_of::<f64>()),
    ] {
        bytes = checked_write_size_add(bytes, checked_write_size_mul(len, element_bytes)?)?;
    }
    Ok(bytes)
}

fn modeled_value_payload_bytes(value: &crate::Value) -> Result<usize> {
    match value {
        crate::Value::Bytes(bytes) => Ok(bytes.len()),
        crate::Value::String(text) => Ok(text.len()),
        crate::Value::Histogram(histogram) => modeled_histogram_payload_bytes(histogram),
        crate::Value::F64(_)
        | crate::Value::I64(_)
        | crate::Value::U64(_)
        | crate::Value::Bool(_) => Ok(0),
    }
}

/// Returns the checked logical-memory model used by write-batch byte admission.
///
/// The model includes each row's owned `Row` representation, metric UTF-8 bytes, label objects
/// and text, and the full logical payload of bytes, strings, and native histograms. It deliberately
/// uses lengths rather than spare allocator capacity and does not count caller-owned input twice.
/// Write preparation separately reserves every tsink-owned clone, index, response outcome, and
/// WAL encoding that can coexist with this input model.
pub fn modeled_write_batch_input_bytes(rows: &[Row]) -> Result<usize> {
    let mut total = checked_write_size_mul(rows.len(), std::mem::size_of::<Row>())?;
    for row in rows {
        total = checked_write_size_add(total, row.metric().len())?;
        total = checked_write_size_add(
            total,
            checked_write_size_mul(row.labels().len(), std::mem::size_of::<Label>())?,
        )?;
        for label in row.labels() {
            total = checked_write_size_add(total, label.name.len())?;
            total = checked_write_size_add(total, label.value.len())?;
        }
        total =
            checked_write_size_add(total, modeled_value_payload_bytes(&row.data_point().value)?)?;
    }
    Ok(total)
}

/// Admission behavior for [`Storage::write_batch`].
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteMode {
    /// Accept and commit every row as one unit, or reject every row.
    Atomic,
    /// Admit each row independently in input order and report every outcome.
    BestEffort,
}

/// Machine-readable reason that a submitted row was not accepted.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WriteRejectionCategory {
    /// The metric name is absent or invalid.
    InvalidMetric,
    /// One or more labels are invalid.
    InvalidLabels,
    /// The row's value cannot be stored for the target series.
    UnsupportedValue,
    /// The timestamp cannot be represented by the configured partition policy.
    TimestampOutOfBounds,
    /// The timestamp is older than the writable retention floor.
    BelowRetentionFloor,
    /// The timestamp exceeds the configured future-skew allowance.
    FutureSkewExceeded,
    /// Accepting the row would exceed the series-cardinality limit.
    CardinalityLimitExceeded,
    /// Creating the row's series would exceed a cardinality creation-rate limit.
    CardinalityCreationRateExceeded,
    /// The submitted batch exceeded a configured row-count or modeled-input-byte limit.
    WriteBatchLimitExceeded,
    /// The write could not be admitted within the memory budget.
    MemoryPressure,
    /// The write could not be admitted within the disk budget.
    DiskQuotaExceeded,
    /// The write could not be admitted within the WAL budget.
    WalQuotaExceeded,
    /// A database, tenant, or query policy rejected the write.
    PolicyRejected,
    /// The write could not acquire admission capacity before its deadline.
    WriteTimeout,
    /// The storage instance is closing or closed.
    StorageClosed,
    /// The storage instance is degraded or fenced against new writes.
    StorageDegraded,
    /// An internal persistence or I/O operation failed.
    InternalIo,
    /// An internal failure could not be represented more specifically.
    Internal,
}

/// Structured rejection detail for one submitted row.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WriteRejection {
    /// Machine-readable rejection category.
    pub category: WriteRejectionCategory,
    /// Input index that caused a batch-wide rejection, when the engine can identify it.
    ///
    /// In best-effort mode this is the rejected row's own index. An atomic batch may reject all
    /// rows without being able to identify which input caused the batch error.
    pub cause_index: Option<usize>,
    /// Bounded human-readable diagnostic detail.
    pub message: String,
}

impl WriteRejection {
    /// Creates a rejection and bounds its diagnostic message to
    /// [`MAX_WRITE_REJECTION_MESSAGE_BYTES`] UTF-8 bytes.
    #[must_use]
    pub fn new(
        category: WriteRejectionCategory,
        cause_index: Option<usize>,
        message: impl Into<String>,
    ) -> Self {
        let mut message = message.into();
        if message.len() > MAX_WRITE_REJECTION_MESSAGE_BYTES {
            let mut boundary = MAX_WRITE_REJECTION_MESSAGE_BYTES;
            while !message.is_char_boundary(boundary) {
                boundary -= 1;
            }
            message.truncate(boundary);
        }
        Self {
            category,
            cause_index,
            message,
        }
    }

    pub(crate) fn from_error(error: &TsinkError, cause_index: Option<usize>) -> Self {
        let category = match error {
            TsinkError::MetricRequired | TsinkError::InvalidMetricName(_) => {
                WriteRejectionCategory::InvalidMetric
            }
            TsinkError::InvalidLabel(_) => WriteRejectionCategory::InvalidLabels,
            TsinkError::UnsupportedAggregation { .. } | TsinkError::ValueTypeMismatch { .. } => {
                WriteRejectionCategory::UnsupportedValue
            }
            TsinkError::InvalidTimeRange { .. }
            | TsinkError::PartitionNotFound { .. }
            | TsinkError::InvalidPartition { .. }
            | TsinkError::LateWritePartitionFanoutExceeded { .. } => {
                WriteRejectionCategory::TimestampOutOfBounds
            }
            TsinkError::OutOfRetention { .. } => WriteRejectionCategory::BelowRetentionFloor,
            TsinkError::FutureSkewExceeded { .. } => WriteRejectionCategory::FutureSkewExceeded,
            TsinkError::CardinalityLimitExceeded { .. } => {
                WriteRejectionCategory::CardinalityLimitExceeded
            }
            TsinkError::CardinalityCreationRateExceeded { .. } => {
                WriteRejectionCategory::CardinalityCreationRateExceeded
            }
            TsinkError::WriteBatchRowLimitExceeded { .. }
            | TsinkError::WriteBatchInputLimitExceeded { .. }
            | TsinkError::WriteBatchSizeOverflow => WriteRejectionCategory::WriteBatchLimitExceeded,
            TsinkError::MemoryBudgetExceeded { .. }
            | TsinkError::AsyncQueueByteLimitExceeded { .. } => {
                WriteRejectionCategory::MemoryPressure
            }
            TsinkError::InsufficientDiskSpace { .. }
            | TsinkError::DiskQuotaExceeded { .. }
            | TsinkError::InsufficientCompactionHeadroom { .. } => {
                WriteRejectionCategory::DiskQuotaExceeded
            }
            TsinkError::WalSizeLimitExceeded { .. } => WriteRejectionCategory::WalQuotaExceeded,
            TsinkError::WriteTimeout { .. } | TsinkError::LifecycleTimeout { .. } => {
                WriteRejectionCategory::WriteTimeout
            }
            TsinkError::StorageShuttingDown => WriteRejectionCategory::StorageDegraded,
            TsinkError::StorageClosed => WriteRejectionCategory::StorageClosed,
            TsinkError::ReadOnlyPartition { .. }
            | TsinkError::MaintenanceWorkItemTooLarge { .. }
            | TsinkError::MaintenanceDependencyWindowExceeded { .. }
            | TsinkError::MaintenanceNamespaceLimitExceeded { .. }
            | TsinkError::InvalidConfiguration(_)
            | TsinkError::UnsupportedOperation { .. } => WriteRejectionCategory::PolicyRejected,
            TsinkError::DataCorruption(_)
            | TsinkError::IoWithPath { .. }
            | TsinkError::Io(_)
            | TsinkError::MemoryMap { .. }
            | TsinkError::Wal { .. }
            | TsinkError::ChecksumMismatch { .. } => WriteRejectionCategory::InternalIo,
            TsinkError::QueryBudget(_)
            | TsinkError::AsyncQueuePayloadSizeOverflow { .. }
            | TsinkError::NoDataPoints { .. }
            | TsinkError::LockPoisoned { .. }
            | TsinkError::ChannelSend { .. }
            | TsinkError::ChannelReceive { .. }
            | TsinkError::ChannelTimeout { .. }
            | TsinkError::Json(_)
            | TsinkError::Bincode(_)
            | TsinkError::Utf8(_)
            | TsinkError::InvalidOffset { .. }
            | TsinkError::Compression(_)
            | TsinkError::Codec(_)
            | TsinkError::Other(_) => WriteRejectionCategory::Internal,
        };
        Self::new(
            category,
            cause_index,
            bounded_display(error, MAX_WRITE_REJECTION_MESSAGE_BYTES),
        )
    }
}

fn bounded_display(value: &impl std::fmt::Display, limit: usize) -> String {
    struct BoundedWriter {
        output: String,
        limit: usize,
        truncated: bool,
    }

    impl std::fmt::Write for BoundedWriter {
        fn write_str(&mut self, value: &str) -> std::fmt::Result {
            if self.truncated {
                return Ok(());
            }
            let remaining = self.limit.saturating_sub(self.output.len());
            if remaining == 0 {
                self.truncated = !value.is_empty();
                return Ok(());
            }
            let mut boundary = remaining.min(value.len());
            while !value.is_char_boundary(boundary) {
                boundary -= 1;
            }
            self.output.push_str(&value[..boundary]);
            self.truncated = boundary < value.len();
            Ok(())
        }
    }

    let mut writer = BoundedWriter {
        output: String::with_capacity(limit.min(256)),
        limit,
        truncated: false,
    };
    let _ = std::fmt::write(&mut writer, format_args!("{value}"));
    writer.output
}

/// Acceptance state for one submitted row.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RowWriteStatus {
    /// The row was accepted and committed.
    Accepted,
    /// The row was not committed.
    Rejected(WriteRejection),
}

/// Indexed outcome for one row submitted to [`Storage::write_batch`].
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct RowWriteOutcome {
    /// Zero-based position of the row in the submitted slice.
    pub index: usize,
    /// Acceptance or structured rejection for this row.
    pub status: RowWriteStatus,
}

impl RowWriteOutcome {
    /// Creates an accepted outcome for `index`.
    #[must_use]
    pub const fn accepted(index: usize) -> Self {
        Self {
            index,
            status: RowWriteStatus::Accepted,
        }
    }

    /// Creates a rejected outcome for `index`.
    #[must_use]
    pub const fn rejected(index: usize, rejection: WriteRejection) -> Self {
        Self {
            index,
            status: RowWriteStatus::Rejected(rejection),
        }
    }
}

/// Complete outcome of a canonical batch write.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BatchWriteResult {
    /// Number of rows supplied by the caller.
    pub submitted: usize,
    /// Number of rows accepted and committed.
    pub accepted: usize,
    /// Number of rows rejected.
    pub rejected: usize,
    /// Weakest durability guarantee among accepted rows, or `None` when none were accepted.
    pub acknowledgement: Option<WriteAcknowledgement>,
    /// Exactly one outcome per submitted row, ordered by input index.
    pub outcomes: Vec<RowWriteOutcome>,
}

impl BatchWriteResult {
    /// Creates a batch result from ordered outcomes and their weakest acknowledgement.
    ///
    /// Counts are derived from `outcomes`. The acknowledgement is normalized to `None` when no
    /// row was accepted.
    #[must_use]
    pub fn from_outcomes(
        acknowledgement: Option<WriteAcknowledgement>,
        outcomes: Vec<RowWriteOutcome>,
    ) -> Self {
        let submitted = outcomes.len();
        let accepted = outcomes
            .iter()
            .filter(|outcome| matches!(&outcome.status, RowWriteStatus::Accepted))
            .count();
        Self {
            submitted,
            accepted,
            rejected: submitted.saturating_sub(accepted),
            acknowledgement: if accepted == 0 { None } else { acknowledgement },
            outcomes,
        }
    }

    /// Returns the canonical result for a successful empty no-op.
    #[must_use]
    pub const fn empty() -> Self {
        Self {
            submitted: 0,
            accepted: 0,
            rejected: 0,
            acknowledgement: None,
            outcomes: Vec::new(),
        }
    }
}

/// Result metadata returned by [`Storage::insert_rows_with_result`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct WriteResult {
    /// Durability established before the write call returned.
    pub acknowledgement: WriteAcknowledgement,
}

impl WriteResult {
    /// Creates a result for an explicit acknowledgement level.
    #[must_use]
    pub const fn new(acknowledgement: WriteAcknowledgement) -> Self {
        Self { acknowledgement }
    }

    /// Creates a result for a write without a complete WAL recovery guarantee.
    #[must_use]
    pub const fn volatile() -> Self {
        Self::new(WriteAcknowledgement::Volatile)
    }

    /// Creates a result for a write appended to the WAL without the complete sync contract.
    #[must_use]
    pub const fn appended() -> Self {
        Self::new(WriteAcknowledgement::Appended)
    }

    /// Creates a result for a write that completed the configured WAL synchronization contract.
    #[must_use]
    pub const fn durable() -> Self {
        Self::new(WriteAcknowledgement::Durable)
    }

    /// Returns whether this result records tsink's strongest software durability level.
    #[must_use]
    pub const fn is_durable(self) -> bool {
        self.acknowledgement.is_durable()
    }
}

/// Effective storage-side limits reported by a built backend.
///
/// This snapshot is intentionally narrower than the planned resource-profile model. It reports
/// controls that the backend currently enforces for storage writes. `None` means that no finite
/// limit is enforced for that field when `reported_by_backend` is `true`; when it is `false`, the
/// backend did not report its configuration and every optional value must be treated as unknown.
///
/// The accounted-memory limit is not a hard process-RSS cap. Inspect
/// [`Storage::observability_snapshot`] for the categories included in and excluded from that
/// accounting.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct EffectiveStorageLimits {
    /// Whether the backend supplied the values in this snapshot.
    pub reported_by_backend: bool,
    /// Whether the backend owns local persistent storage for this instance.
    pub persistent: bool,
    /// Whether a local write-ahead log is active for this instance.
    pub wal_enabled: bool,
    /// Finite budget for the engine's accounted storage memory, in bytes.
    pub accounted_memory_bytes: Option<u64>,
    /// Finite total-series cardinality limit.
    pub cardinality: Option<u64>,
    /// Maximum labels accepted in a submitted series identity.
    pub max_labels_per_series: Option<u64>,
    /// Maximum cumulative metric and label UTF-8 bytes in a submitted series identity.
    pub max_series_identity_bytes: Option<u64>,
    /// Maximum new series that may commit during one creation-rate window.
    pub max_new_series_per_window: Option<u64>,
    /// Effective creation-rate window in nanoseconds after timestamp-precision rounding.
    pub new_series_window_nanos: Option<u64>,
    /// Maximum rows accepted in one foreground write submission.
    pub max_write_batch_rows: Option<u64>,
    /// Maximum modeled logical input bytes accepted in one foreground write submission.
    pub max_write_batch_input_bytes: Option<u64>,
    /// Finite on-disk WAL byte limit. This is `None` when the WAL is inactive or unbounded.
    pub wal_bytes: Option<u64>,
    /// Finite userspace WAL writer-buffer capacity. The live buffer's actual retained capacity is
    /// charged to `accounted_memory_bytes`.
    pub wal_write_buffer_bytes: Option<u64>,
    /// Finite byte limit enforced by the local data-directory coordinator.
    ///
    /// `None` means unbounded when this is a persistent, reporting backend, and inactive or
    /// unknown otherwise.
    pub local_disk_bytes: Option<u64>,
    /// Filesystem free-space floor for the managed local data directory.
    pub filesystem_free_headroom_bytes: Option<u64>,
    /// Local-disk bytes reserved for maintenance temporary output.
    pub maintenance_temp_reserve_bytes: Option<u64>,
    /// Maximum writes admitted concurrently by the synchronous engine.
    pub max_concurrent_writers: Option<u64>,
    /// Maximum wait for a writer permit or one close-coordination acquisition, in nanoseconds.
    pub write_timeout_nanos: Option<u64>,
    /// Maximum named background worker threads owned by this storage instance.
    ///
    /// The built-in backend has one fixed slot each for flush, compaction,
    /// persisted refresh (including retention/tiering), and rollups. Inactive
    /// capabilities contribute zero to this per-instance value.
    pub max_background_threads: Option<u64>,
    /// Maximum concurrent background flush passes.
    pub max_flush_concurrency: Option<u64>,
    /// Maximum concurrent compaction passes.
    pub max_compaction_concurrency: Option<u64>,
    /// Maximum concurrent retention/tiering maintenance passes.
    pub max_retention_tiering_concurrency: Option<u64>,
    /// Maximum concurrent remote catalog refresh passes.
    pub max_remote_catalog_refresh_concurrency: Option<u64>,
    /// Maximum concurrent remote-tier payload fetches.
    ///
    /// Remote payload reads are synchronous within a query, so the built-in
    /// backend derives this from the shared concurrent-query limit. `None`
    /// therefore honestly reports an unbounded query/fetch configuration.
    pub max_remote_tier_fetch_concurrency: Option<u64>,
    /// Maximum concurrent rollup passes.
    pub max_rollup_concurrency: Option<u64>,
    /// Effective periodic flush cadence, in nanoseconds.
    pub flush_interval_nanos: Option<u64>,
    /// Effective periodic compaction cadence, in nanoseconds.
    pub compaction_interval_nanos: Option<u64>,
    /// Effective persisted-catalog worker poll cadence, in nanoseconds.
    ///
    /// Explicit pending-work notifications may wake this worker sooner. The
    /// same serialized worker owns retention/tiering and remote catalog refresh.
    pub persisted_refresh_poll_interval_nanos: Option<u64>,
    /// Effective periodic rollup cadence, in nanoseconds.
    pub rollup_interval_nanos: Option<u64>,
    /// Maximum simultaneously active partition heads for one series.
    pub max_active_partition_heads_per_series: Option<u64>,
}

/// Schema version of [`ResourceConfigurationSnapshot`].
pub const RESOURCE_CONFIGURATION_SCHEMA_VERSION: u32 = 1;

/// Finite resource settings for the runtime-independent async facade.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AsyncResourceLimits {
    /// Maximum commands waiting in each read or write queue.
    pub queue_command_capacity: usize,
    /// Maximum modeled bytes owned by commands in the write queue.
    pub write_queue_byte_capacity: usize,
    /// Maximum modeled bytes owned by commands in the read queue.
    pub read_queue_byte_capacity: usize,
    /// Dedicated synchronous reader threads owned by an async facade.
    pub read_workers: usize,
}

/// Fixed worker topology, cadence, and per-pass maintenance bounds.
///
/// The current engine owns at most one worker of each named kind. Custom profiles must retain that
/// topology and its fixed cadences; unsupported values fail validation rather than being ignored.
/// The limits are nevertheless explicit so hosts can inspect the full thread contract rather than
/// infer it from implementation details.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BackgroundResourceLimits {
    pub max_threads: usize,
    pub flush_concurrency: usize,
    pub compaction_concurrency: usize,
    pub persisted_refresh_concurrency: usize,
    pub remote_fetch_concurrency: usize,
    pub rollup_concurrency: usize,
    pub flush_interval: Duration,
    pub compaction_interval: Duration,
    pub persisted_refresh_interval: Duration,
    pub rollup_interval: Duration,
    /// Maximum logical items selected by one bounded maintenance pass.
    pub maintenance_max_items_per_pass: usize,
    /// Maximum modeled input bytes selected by one bounded maintenance pass.
    pub maintenance_max_bytes_per_pass: u64,
}

/// Complete finite limits used by a named or custom resource profile.
///
/// Standard profile constants are deliberately conservative and provisional until the complete
/// measurement matrix in `docs/resource-profile-measurements.md` is rerun on release hardware.
/// `Custom` profiles are validated at build time: every optional field nested in
/// [`WriteBatchLimits`] and [`QueryBudgetLimits`] must be finite and non-zero.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResourceLimits {
    pub accounted_memory_bytes: u64,
    pub local_disk_bytes: u64,
    pub filesystem_free_headroom_bytes: u64,
    pub maintenance_temp_reserve_bytes: u64,
    pub wal_bytes: u64,
    pub wal_write_buffer_bytes: u64,
    pub cardinality: u64,
    pub max_labels_per_series: u64,
    pub max_series_identity_bytes: u64,
    pub max_new_series_per_window: u64,
    pub new_series_window: Duration,
    pub write_batch: WriteBatchLimits,
    pub max_concurrent_writers: u64,
    pub write_timeout: Duration,
    pub max_active_partition_heads_per_series: u64,
    pub query: QueryBudgetLimits,
    pub async_runtime: AsyncResourceLimits,
    pub background: BackgroundResourceLimits,
}

/// Named base profile selected for a storage builder.
///
/// `ExpertUnlimited` preserves the legacy unbounded storage/query defaults for migrations. Hard
/// storage-format limits, bounded async channel mechanics, and the fixed worker topology still
/// apply. It should be selected deliberately, not used as a constrained-host default.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case", tag = "name", content = "limits")]
#[allow(clippy::large_enum_variant)]
pub enum ResourceProfile {
    Test,
    Embedded,
    Edge,
    Server,
    Custom(ResourceLimits),
    ExpertUnlimited,
}

/// Stable profile label included in configuration snapshots.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResourceProfileName {
    #[default]
    Unreported,
    Test,
    Embedded,
    Edge,
    Server,
    Custom,
    ExpertUnlimited,
}

impl ResourceProfile {
    #[must_use]
    pub const fn name(self) -> ResourceProfileName {
        match self {
            Self::Test => ResourceProfileName::Test,
            Self::Embedded => ResourceProfileName::Embedded,
            Self::Edge => ResourceProfileName::Edge,
            Self::Server => ResourceProfileName::Server,
            Self::Custom(_) => ResourceProfileName::Custom,
            Self::ExpertUnlimited => ResourceProfileName::ExpertUnlimited,
        }
    }

    /// Returns finite limits for a standard or custom profile.
    ///
    /// `None` is returned only for [`ResourceProfile::ExpertUnlimited`].
    #[must_use]
    pub const fn finite_limits(self) -> Option<ResourceLimits> {
        match self {
            Self::Test => Some(ResourceLimits::test()),
            Self::Embedded => Some(ResourceLimits::embedded()),
            Self::Edge => Some(ResourceLimits::edge()),
            Self::Server => Some(ResourceLimits::server()),
            Self::Custom(limits) => Some(limits),
            Self::ExpertUnlimited => None,
        }
    }
}

const MIB: u64 = 1024 * 1024;
const GIB: u64 = 1024 * MIB;

const fn finite_write_batch(rows: usize, bytes: usize) -> WriteBatchLimits {
    WriteBatchLimits {
        max_rows: Some(rows),
        max_modeled_input_bytes: Some(bytes),
    }
}

const fn finite_query_limits(
    concurrent: u64,
    shared_memory: u64,
    per_query_memory: u64,
    series: u64,
    scanned: u64,
    returned: u64,
    returned_bytes: u64,
) -> QueryBudgetLimits {
    QueryBudgetLimits {
        max_concurrent_queries: Some(concurrent),
        max_shared_memory_bytes: Some(shared_memory),
        per_query: QueryWorkLimits {
            max_series_matched: Some(series),
            max_samples_scanned: Some(scanned),
            max_samples_returned: Some(returned),
            max_returned_bytes: Some(returned_bytes),
            max_pattern_expansion: Some(series.saturating_mul(4)),
            max_steps: Some(1_000_000),
            max_intermediate_vector_size: Some(series),
            max_memory_bytes: Some(per_query_memory),
            max_wall_time: Some(Duration::from_secs(30)),
        },
    }
}

const fn background_limits(
    remote_fetch_concurrency: usize,
    maintenance_max_items_per_pass: usize,
    maintenance_max_bytes_per_pass: u64,
) -> BackgroundResourceLimits {
    BackgroundResourceLimits {
        max_threads: 4,
        flush_concurrency: 1,
        compaction_concurrency: 1,
        persisted_refresh_concurrency: 1,
        remote_fetch_concurrency,
        rollup_concurrency: 1,
        flush_interval: Duration::from_millis(250),
        compaction_interval: Duration::from_secs(5),
        persisted_refresh_interval: Duration::from_millis(250),
        rollup_interval: Duration::from_secs(5),
        maintenance_max_items_per_pass,
        maintenance_max_bytes_per_pass,
    }
}

impl ResourceLimits {
    #[must_use]
    pub const fn test() -> Self {
        Self {
            accounted_memory_bytes: 64 * MIB,
            local_disk_bytes: 256 * MIB,
            filesystem_free_headroom_bytes: 16 * MIB,
            maintenance_temp_reserve_bytes: 32 * MIB,
            wal_bytes: 32 * MIB,
            wal_write_buffer_bytes: 4 * 1024,
            cardinality: 10_000,
            max_labels_per_series: 128,
            max_series_identity_bytes: 64 * 1024,
            max_new_series_per_window: 5_000,
            new_series_window: Duration::from_secs(60),
            write_batch: finite_write_batch(10_000, 8 * MIB as usize),
            max_concurrent_writers: 2,
            write_timeout: Duration::from_secs(30),
            max_active_partition_heads_per_series: 4,
            query: finite_query_limits(2, 16 * MIB, 8 * MIB, 10_000, 1_000_000, 250_000, 16 * MIB),
            async_runtime: AsyncResourceLimits {
                queue_command_capacity: 256,
                write_queue_byte_capacity: 8 * MIB as usize,
                read_queue_byte_capacity: 4 * MIB as usize,
                read_workers: 2,
            },
            // One active chunk can conservatively grow to the complete accounted-memory ceiling
            // across repeated writes. The maintenance byte cap must therefore admit that chunk.
            background: background_limits(2, 10_000, 64 * MIB),
        }
    }

    #[must_use]
    pub const fn embedded() -> Self {
        Self {
            accounted_memory_bytes: 512 * MIB,
            local_disk_bytes: 16 * GIB,
            filesystem_free_headroom_bytes: 256 * MIB,
            maintenance_temp_reserve_bytes: GIB,
            wal_bytes: 512 * MIB,
            wal_write_buffer_bytes: 4 * 1024,
            cardinality: 1_000_000,
            max_labels_per_series: 128,
            max_series_identity_bytes: 64 * 1024,
            max_new_series_per_window: 100_000,
            new_series_window: Duration::from_secs(60),
            write_batch: finite_write_batch(100_000, 64 * MIB as usize),
            max_concurrent_writers: 4,
            write_timeout: Duration::from_secs(30),
            max_active_partition_heads_per_series: 8,
            query: finite_query_limits(
                8,
                128 * MIB,
                32 * MIB,
                250_000,
                10_000_000,
                2_000_000,
                64 * MIB,
            ),
            async_runtime: AsyncResourceLimits {
                queue_command_capacity: 1_024,
                write_queue_byte_capacity: 64 * MIB as usize,
                read_queue_byte_capacity: 16 * MIB as usize,
                read_workers: 4,
            },
            background: background_limits(8, 100_000, 512 * MIB),
        }
    }

    #[must_use]
    pub const fn edge() -> Self {
        Self {
            accounted_memory_bytes: 256 * MIB,
            local_disk_bytes: 4 * GIB,
            filesystem_free_headroom_bytes: 128 * MIB,
            maintenance_temp_reserve_bytes: 512 * MIB,
            wal_bytes: 256 * MIB,
            wal_write_buffer_bytes: 4 * 1024,
            cardinality: 250_000,
            max_labels_per_series: 128,
            max_series_identity_bytes: 64 * 1024,
            max_new_series_per_window: 25_000,
            new_series_window: Duration::from_secs(60),
            write_batch: finite_write_batch(50_000, 32 * MIB as usize),
            max_concurrent_writers: 4,
            write_timeout: Duration::from_secs(30),
            max_active_partition_heads_per_series: 8,
            query: finite_query_limits(
                4,
                64 * MIB,
                16 * MIB,
                100_000,
                5_000_000,
                1_000_000,
                32 * MIB,
            ),
            async_runtime: AsyncResourceLimits {
                queue_command_capacity: 512,
                write_queue_byte_capacity: 32 * MIB as usize,
                read_queue_byte_capacity: 8 * MIB as usize,
                read_workers: 2,
            },
            background: background_limits(4, 50_000, 256 * MIB),
        }
    }

    #[must_use]
    pub const fn server() -> Self {
        Self {
            accounted_memory_bytes: 2 * GIB,
            local_disk_bytes: 256 * GIB,
            filesystem_free_headroom_bytes: 2 * GIB,
            maintenance_temp_reserve_bytes: 16 * GIB,
            wal_bytes: 8 * GIB,
            wal_write_buffer_bytes: 64 * 1024,
            cardinality: 10_000_000,
            max_labels_per_series: 128,
            max_series_identity_bytes: 64 * 1024,
            max_new_series_per_window: 1_000_000,
            new_series_window: Duration::from_secs(60),
            write_batch: finite_write_batch(500_000, 256 * MIB as usize),
            max_concurrent_writers: 16,
            write_timeout: Duration::from_secs(30),
            max_active_partition_heads_per_series: 16,
            query: finite_query_limits(
                32,
                512 * MIB,
                128 * MIB,
                1_000_000,
                50_000_000,
                10_000_000,
                256 * MIB,
            ),
            async_runtime: AsyncResourceLimits {
                queue_command_capacity: 4_096,
                write_queue_byte_capacity: 256 * MIB as usize,
                read_queue_byte_capacity: 64 * MIB as usize,
                read_workers: 16,
            },
            background: background_limits(32, 500_000, 2 * GIB),
        }
    }

    /// Validates finite values and relationships required by the current engine topology.
    pub fn validate(self) -> Result<()> {
        fn positive(name: &str, value: u64) -> Result<()> {
            if value == 0 {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "resource limit '{name}' must be greater than zero"
                )));
            }
            Ok(())
        }

        fn finite_usize(name: &str, value: u64) -> Result<()> {
            let converted = usize::try_from(value).map_err(|_| {
                TsinkError::InvalidConfiguration(format!(
                    "resource limit '{name}' does not fit this platform's usize"
                ))
            })?;
            if converted == usize::MAX {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "resource limit '{name}' resolves to the engine's unlimited sentinel; select ResourceProfile::ExpertUnlimited explicitly or use a smaller finite value"
                )));
            }
            Ok(())
        }

        for (name, value) in [
            ("accounted_memory_bytes", self.accounted_memory_bytes),
            ("local_disk_bytes", self.local_disk_bytes),
            (
                "filesystem_free_headroom_bytes",
                self.filesystem_free_headroom_bytes,
            ),
            (
                "maintenance_temp_reserve_bytes",
                self.maintenance_temp_reserve_bytes,
            ),
            ("wal_bytes", self.wal_bytes),
            ("wal_write_buffer_bytes", self.wal_write_buffer_bytes),
            ("cardinality", self.cardinality),
            ("max_labels_per_series", self.max_labels_per_series),
            ("max_series_identity_bytes", self.max_series_identity_bytes),
            ("max_new_series_per_window", self.max_new_series_per_window),
            ("max_concurrent_writers", self.max_concurrent_writers),
            (
                "max_active_partition_heads_per_series",
                self.max_active_partition_heads_per_series,
            ),
            (
                "background.maintenance_max_bytes_per_pass",
                self.background.maintenance_max_bytes_per_pass,
            ),
        ] {
            positive(name, value)?;
        }
        for (name, value) in [
            ("accounted_memory_bytes", self.accounted_memory_bytes),
            ("wal_bytes", self.wal_bytes),
            ("wal_write_buffer_bytes", self.wal_write_buffer_bytes),
            ("cardinality", self.cardinality),
            ("max_labels_per_series", self.max_labels_per_series),
            ("max_series_identity_bytes", self.max_series_identity_bytes),
            ("max_new_series_per_window", self.max_new_series_per_window),
            ("max_concurrent_writers", self.max_concurrent_writers),
            (
                "max_active_partition_heads_per_series",
                self.max_active_partition_heads_per_series,
            ),
        ] {
            finite_usize(name, value)?;
        }
        for (name, value) in [
            (
                "async.queue_command_capacity",
                self.async_runtime.queue_command_capacity,
            ),
            (
                "async.write_queue_byte_capacity",
                self.async_runtime.write_queue_byte_capacity,
            ),
            (
                "async.read_queue_byte_capacity",
                self.async_runtime.read_queue_byte_capacity,
            ),
            ("async.read_workers", self.async_runtime.read_workers),
            ("background.max_threads", self.background.max_threads),
            (
                "background.flush_concurrency",
                self.background.flush_concurrency,
            ),
            (
                "background.compaction_concurrency",
                self.background.compaction_concurrency,
            ),
            (
                "background.persisted_refresh_concurrency",
                self.background.persisted_refresh_concurrency,
            ),
            (
                "background.remote_fetch_concurrency",
                self.background.remote_fetch_concurrency,
            ),
            (
                "background.rollup_concurrency",
                self.background.rollup_concurrency,
            ),
            (
                "background.maintenance_max_items_per_pass",
                self.background.maintenance_max_items_per_pass,
            ),
        ] {
            positive(name, u64::try_from(value).unwrap_or(u64::MAX))?;
            if value == usize::MAX {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "resource limit '{name}' resolves to the engine's unlimited sentinel; select ResourceProfile::ExpertUnlimited explicitly or use a smaller finite value"
                )));
            }
        }
        for (name, duration) in [
            ("new_series_window", self.new_series_window),
            ("write_timeout", self.write_timeout),
            ("background.flush_interval", self.background.flush_interval),
            (
                "background.compaction_interval",
                self.background.compaction_interval,
            ),
            (
                "background.persisted_refresh_interval",
                self.background.persisted_refresh_interval,
            ),
            (
                "background.rollup_interval",
                self.background.rollup_interval,
            ),
        ] {
            if duration.is_zero() {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "resource limit '{name}' must be greater than zero"
                )));
            }
        }
        let Some(max_rows) = self.write_batch.max_rows else {
            return Err(TsinkError::InvalidConfiguration(
                "finite resource profiles require write_batch.max_rows".to_string(),
            ));
        };
        let Some(max_input_bytes) = self.write_batch.max_modeled_input_bytes else {
            return Err(TsinkError::InvalidConfiguration(
                "finite resource profiles require write_batch.max_modeled_input_bytes".to_string(),
            ));
        };
        if max_rows == 0 || max_input_bytes == 0 {
            return Err(TsinkError::InvalidConfiguration(
                "finite write-batch limits must be greater than zero".to_string(),
            ));
        }
        if self.background.maintenance_max_items_per_pass < max_rows {
            return Err(TsinkError::InvalidConfiguration(
                "maintenance items per pass must be at least write_batch.max_rows so one admitted batch cannot exceed the pass cap"
                    .to_string(),
            ));
        }
        self.query
            .validate()
            .map_err(crate::QueryBudgetError::from)?;
        let query_fields_are_finite = self.query.max_concurrent_queries.is_some()
            && self.query.max_shared_memory_bytes.is_some()
            && self.query.per_query.max_series_matched.is_some()
            && self.query.per_query.max_samples_scanned.is_some()
            && self.query.per_query.max_samples_returned.is_some()
            && self.query.per_query.max_returned_bytes.is_some()
            && self.query.per_query.max_pattern_expansion.is_some()
            && self.query.per_query.max_steps.is_some()
            && self.query.per_query.max_intermediate_vector_size.is_some()
            && self.query.per_query.max_memory_bytes.is_some()
            && self.query.per_query.max_wall_time.is_some();
        if !query_fields_are_finite {
            return Err(TsinkError::InvalidConfiguration(
                "finite resource profiles require every query-budget field".to_string(),
            ));
        }
        if self.background.maintenance_max_bytes_per_pass < self.accounted_memory_bytes {
            return Err(TsinkError::InvalidConfiguration(
                "maintenance bytes per pass must be at least accounted profile memory so one admitted sealed chunk cannot exceed the pass cap"
                    .to_string(),
            ));
        }
        if self.maintenance_temp_reserve_bytes >= self.local_disk_bytes {
            return Err(TsinkError::InvalidConfiguration(
                "maintenance temporary reserve must be smaller than local disk bytes".to_string(),
            ));
        }
        if self.wal_bytes
            > self
                .local_disk_bytes
                .saturating_sub(self.maintenance_temp_reserve_bytes)
        {
            return Err(TsinkError::InvalidConfiguration(
                "WAL bytes exceed local disk growth capacity after maintenance reserve".to_string(),
            ));
        }
        if self.max_labels_per_series > crate::label::MAX_SUPPORTED_LABELS_PER_SERIES as u64 {
            return Err(TsinkError::InvalidConfiguration(format!(
                "max_labels_per_series {} exceeds the storage-format limit {}",
                self.max_labels_per_series,
                crate::label::MAX_SUPPORTED_LABELS_PER_SERIES
            )));
        }
        let fixed_worker_count = self
            .background
            .flush_concurrency
            .saturating_add(self.background.compaction_concurrency)
            .saturating_add(self.background.persisted_refresh_concurrency)
            .saturating_add(self.background.rollup_concurrency);
        if self.background.flush_concurrency != 1
            || self.background.compaction_concurrency != 1
            || self.background.persisted_refresh_concurrency != 1
            || self.background.rollup_concurrency != 1
            || self.background.max_threads != fixed_worker_count
        {
            return Err(TsinkError::InvalidConfiguration(
                "the current engine requires one flush, compaction, persisted-refresh, and rollup slot"
                    .to_string(),
            ));
        }
        let fixed_background = background_limits(
            self.background.remote_fetch_concurrency,
            self.background.maintenance_max_items_per_pass,
            self.background.maintenance_max_bytes_per_pass,
        );
        if self.background.flush_interval != fixed_background.flush_interval
            || self.background.compaction_interval != fixed_background.compaction_interval
            || self.background.persisted_refresh_interval
                != fixed_background.persisted_refresh_interval
            || self.background.rollup_interval != fixed_background.rollup_interval
        {
            return Err(TsinkError::InvalidConfiguration(
                "the current engine requires fixed background cadences: 250 ms flush, 5 s compaction, 250 ms persisted refresh, and 5 s rollup"
                    .to_string(),
            ));
        }
        let query_concurrency = self.query.max_concurrent_queries.unwrap_or(0);
        if u64::try_from(self.background.remote_fetch_concurrency).unwrap_or(u64::MAX)
            != query_concurrency
        {
            return Err(TsinkError::InvalidConfiguration(
                "remote fetch concurrency must equal shared query concurrency".to_string(),
            ));
        }
        Ok(())
    }
}

/// Low-level builder settings that differ from the selected base profile.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, serde::Serialize, serde::Deserialize,
)]
#[serde(rename_all = "snake_case")]
pub enum ResourceLimitOverride {
    AccountedMemory,
    LocalDisk,
    FilesystemFreeHeadroom,
    MaintenanceTempReserve,
    WalBytes,
    WalWriteBuffer,
    Cardinality,
    MaxLabelsPerSeries,
    MaxSeriesIdentityBytes,
    SeriesCreationRate,
    WriteBatch,
    ConcurrentWriters,
    WriteTimeout,
    PartitionHeads,
    QueryBudget,
    AsyncQueueCommands,
    AsyncWriteQueueBytes,
    AsyncReadQueueBytes,
    AsyncReadWorkers,
    MaintenanceWork,
}

/// Fully resolved limits reported by a storage or async facade.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ResolvedResourceLimits {
    pub storage: EffectiveStorageLimits,
    pub query: QueryBudgetLimits,
    /// Present only when an async facade is active.
    pub async_runtime: Option<AsyncResourceLimits>,
    pub maintenance_max_items_per_pass: Option<u64>,
    pub maintenance_max_bytes_per_pass: Option<u64>,
}

/// Versioned, serializable resource configuration and override provenance.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ResourceConfigurationSnapshot {
    pub schema_version: u32,
    pub reported_by_backend: bool,
    pub selected_profile: ResourceProfileName,
    pub resolved_limits: ResolvedResourceLimits,
    pub overrides: Vec<ResourceLimitOverride>,
}

impl Default for ResourceConfigurationSnapshot {
    fn default() -> Self {
        Self {
            schema_version: RESOURCE_CONFIGURATION_SCHEMA_VERSION,
            reported_by_backend: false,
            selected_profile: ResourceProfileName::Unreported,
            resolved_limits: ResolvedResourceLimits::default(),
            overrides: Vec::new(),
        }
    }
}

/// Cardinality and new-series admission state for the built-in backend.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CardinalityObservabilitySnapshot {
    /// Current number of registered series.
    pub series_count: u64,
    /// New-series reservations currently awaiting write publication.
    pub pending_new_series: u64,
    /// New series committed in the current fixed window.
    pub committed_in_window: u64,
    /// Start of the current fixed window in the storage timestamp precision.
    pub current_window_start: Option<i64>,
    /// New-series reservations admitted since this storage instance opened.
    pub admitted_new_series_total: u64,
    /// New series committed since this storage instance opened.
    pub committed_new_series_total: u64,
    /// Creation-rate admission rejections since this storage instance opened.
    pub creation_rate_rejections_total: u64,
}

/// Outcome metadata for a delete-series operation.
/// `matched_series` counts selector matches and `tombstones_applied` counts
/// only the matched series whose tombstone state changed.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct DeleteSeriesResult {
    pub matched_series: u64,
    pub tombstones_applied: u64,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct StorageObservabilitySnapshot {
    /// Effective storage controls reported by the backend at snapshot time.
    pub limits: EffectiveStorageLimits,
    /// Versioned selected profile, resolved limits, and low-level override provenance.
    pub resource_configuration: ResourceConfigurationSnapshot,
    /// Shared local data-directory accounting, or `None` for unsupported/non-persistent backends.
    pub local_disk: Option<crate::LocalDiskBudgetSnapshot>,
    pub memory: MemoryObservabilitySnapshot,
    /// Current total and creation-rate cardinality state.
    pub cardinality: CardinalityObservabilitySnapshot,
    pub wal: WalObservabilitySnapshot,
    pub retention: RetentionObservabilitySnapshot,
    pub flush: FlushObservabilitySnapshot,
    pub compaction: CompactionObservabilitySnapshot,
    pub query: QueryObservabilitySnapshot,
    /// Shared admission, work-limit, cancellation, and query-memory accounting.
    pub query_budget: QueryBudgetSnapshot,
    pub rollups: RollupObservabilitySnapshot,
    pub remote: RemoteStorageObservabilitySnapshot,
    /// Instance-owned background worker lifecycle, wakeup, wait, and pass state.
    pub background: BackgroundWorkObservabilitySnapshot,
    pub health: StorageHealthSnapshot,
}

/// Lifecycle and bounded-work counters for one named background worker slot.
///
/// `installed` describes whether the instance still owns a join handle;
/// `running` describes whether that thread is currently alive. They differ
/// briefly during startup and after a worker exits but before it is joined.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BackgroundWorkerObservabilitySnapshot {
    /// Whether a join handle is installed in this instance's worker slot.
    pub installed: bool,
    /// Whether the worker thread is currently alive.
    pub running: bool,
    /// The worker's periodic/poll cadence, in nanoseconds, when configured.
    pub interval_nanos: Option<u64>,
    /// Fixed maximum number of passes that this slot may execute concurrently.
    pub max_concurrency: u64,
    /// Successful thread starts since this instance opened.
    pub starts_total: u64,
    /// Thread exits since this instance opened.
    pub exits_total: u64,
    /// Coalescible explicit wakeup notifications delivered to an installed thread.
    pub notifications_total: u64,
    /// Efficient park operations entered while waiting for cadence or new work.
    pub idle_waits_total: u64,
    /// Maintenance passes that acquired their outer lifecycle gate.
    pub passes_started_total: u64,
    /// Started maintenance passes that returned or unwound.
    pub passes_completed_total: u64,
    /// Join handles successfully reaped during shutdown.
    pub shutdown_joins_total: u64,
}

/// Observability for all background worker slots owned by one storage instance.
///
/// Retention/tiering and remote catalog refresh do not create hidden threads:
/// both are serialized through `persisted_refresh`. Remote tier payload reads
/// happen on query callers and are bounded by the query-concurrency setting
/// reported in [`EffectiveStorageLimits`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct BackgroundWorkObservabilitySnapshot {
    /// Per-instance upper bound on installed background threads.
    pub max_threads: u64,
    /// Join handles currently installed across all worker slots.
    pub installed_threads: u64,
    /// Worker threads currently alive across all worker slots.
    pub running_threads: u64,
    /// Close-pipeline attempts since this instance opened, including best-effort drop.
    pub close_attempts_total: u64,
    /// Close attempts that completed the durability pipeline and joined every owned worker.
    pub close_success_total: u64,
    /// Close attempts that returned an error, including coordination timeouts and filesystem
    /// failures.
    pub close_errors_total: u64,
    /// Time spent waiting for close-critical gates and the complete writer-permit drain.
    pub close_coordination_wait_nanos_total: u64,
    /// Close coordination acquisitions that reached the configured write/lifecycle timeout.
    pub close_coordination_timeouts_total: u64,
    /// Compaction passes attempted by close across all attempts, including no-op/error passes.
    pub close_compaction_passes_total: u64,
    /// Fixed maximum compaction passes attempted by one close.
    pub close_compaction_pass_limit: u64,
    /// Wall-clock duration of all close-pipeline attempts, including durability filesystem calls.
    pub close_duration_nanos_total: u64,
    /// Wall-clock time spent joining owned worker handles across close, abrupt-test shutdown, and
    /// best-effort drop.
    pub shutdown_join_wait_nanos_total: u64,
    /// Periodic and explicitly woken flush worker.
    pub flush: BackgroundWorkerObservabilitySnapshot,
    /// Periodic and explicitly woken compaction worker.
    pub compaction: BackgroundWorkerObservabilitySnapshot,
    /// Serialized persisted refresh, retention/tiering, and remote-catalog worker.
    pub persisted_refresh: BackgroundWorkerObservabilitySnapshot,
    /// Periodic and explicitly woken rollup worker.
    pub rollup: BackgroundWorkerObservabilitySnapshot,
}

/// Current pressure on the built-in engine's modeled, admitted memory scope.
///
/// This does not describe process RSS. [`MemoryPressureLevel::Degraded`] means the storage
/// instance is degraded; it does not imply that memory pressure caused the degradation.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryPressureLevel {
    /// Accounted usage is below the configured approaching-limit threshold, or is unlimited.
    Normal,
    /// Accounted usage has reached the published approaching-limit threshold.
    ApproachingLimit,
    /// At least one writer is currently waiting for accounted memory to be reclaimed.
    Backpressured,
    /// Accounted usage is at or above the finite budget, so growth must be rejected.
    Rejecting,
    /// The storage instance is degraded and its resource state requires operator attention.
    Degraded,
}

/// Memory-pressure state and memory-specific admission counters.
#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct MemoryPressureSnapshot {
    /// Current level, or `None` when the backend does not report memory pressure.
    pub level: Option<MemoryPressureLevel>,
    /// Approaching-limit threshold in basis points of the finite budget.
    pub approaching_limit_basis_points: Option<u16>,
    /// Resolved approaching-limit threshold in bytes.
    pub approaching_limit_bytes: Option<u64>,
    /// Writers currently delayed by an accounted-memory shortfall.
    pub active_backpressured_writers: u64,
    /// Writes that entered accounted-memory backpressure.
    pub backpressure_events_total: u64,
    /// Modeled storage-memory admissions rejected with `MemoryBudgetExceeded`.
    pub rejections_total: u64,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(default)]
pub struct MemoryObservabilitySnapshot {
    /// Modeled bytes charged to the storage memory budget.
    pub accounted_bytes: usize,
    /// Accounted bytes modeled from owned allocations, excluding virtual mapping lengths.
    pub estimated_accounted_bytes: usize,
    /// Backward-compatible alias for `accounted_bytes`.
    pub budgeted_bytes: usize,
    /// Measured excluded bytes. Read this only when `excluded_bytes_known` is true.
    pub excluded_bytes: usize,
    /// Whether `excluded_bytes` is a complete measurement of the named excluded categories.
    pub excluded_bytes_known: bool,
    /// Known categories outside the admitted storage-memory calculation.
    pub excluded_categories: Vec<String>,
    pub active_and_sealed_bytes: usize,
    pub registry_bytes: usize,
    pub metadata_cache_bytes: usize,
    pub persisted_index_bytes: usize,
    /// Full virtual length of persisted file mappings; this is not resident memory or RSS.
    #[serde(default)]
    pub persisted_mmap_bytes: usize,
    pub tombstone_bytes: usize,
    /// Modeled bytes retained by finite segment-catalog work: compute-only generation
    /// reader/cursor/map staging and read-write v3/v2/pointer publication staging.
    ///
    /// This component is charged to the global storage-memory budget.
    #[serde(default)]
    pub remote_catalog_staging_bytes: usize,
    /// Actual retained capacity of the live userspace WAL writer buffer.
    ///
    /// This component is charged to the global storage-memory budget.
    #[serde(default)]
    pub wal_writer_buffer_bytes: usize,
    /// Conservative modeled bytes retained by the WAL's committed series-definition cache.
    pub wal_series_definition_cache_bytes: usize,
    /// Current conservative reservation for tsink-owned foreground-write and startup-WAL scratch.
    /// This is included in `accounted_bytes` and `estimated_accounted_bytes`.
    pub write_transient_bytes: usize,
    /// Highest concurrent write/replay scratch reservation since this instance opened.
    pub peak_write_transient_bytes: usize,
    /// Scratch leases admitted since this instance opened.
    pub write_transient_reservations_total: u64,
    /// Scratch leases rejected by the shared storage memory budget.
    pub write_transient_rejections_total: u64,
    /// True because transient bytes are conservative modeled reservations, not allocator samples.
    pub write_transient_bytes_estimated: bool,
    /// Legacy field retained for compatibility. Persisted mappings are currently budgeted above.
    pub excluded_persisted_mmap_bytes: usize,
    /// Current modeled-memory pressure and memory-specific admission counters.
    pub pressure: MemoryPressureSnapshot,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct WalObservabilitySnapshot {
    pub enabled: bool,
    /// Configured WAL sync mode (`per-append`, `periodic`, or `disabled`).
    pub sync_mode: String,
    /// Whether the configured policy synchronizes every acknowledged non-empty write immediately.
    pub acknowledged_writes_durable: bool,
    /// Finite capacity of the userspace `BufWriter`; its live capacity is storage-budget-accounted.
    pub write_buffer_capacity_bytes: u64,
    pub size_bytes: u64,
    pub segment_count: u64,
    pub active_segment: u64,
    /// Highest committed write appended to the WAL.
    pub highwater_segment: u64,
    pub highwater_frame: u64,
    /// Highest committed write known durable via WAL fsync or later segment persistence.
    pub durable_highwater_segment: u64,
    pub durable_highwater_frame: u64,
    pub replay_runs_total: u64,
    pub replay_frames_total: u64,
    pub replay_series_definitions_total: u64,
    pub replay_sample_batches_total: u64,
    pub replay_points_total: u64,
    pub replay_errors_total: u64,
    pub replay_duration_nanos_total: u64,
    pub append_series_definitions_total: u64,
    pub append_sample_batches_total: u64,
    pub append_points_total: u64,
    pub append_bytes_total: u64,
    pub append_errors_total: u64,
    pub resets_total: u64,
    pub reset_errors_total: u64,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct RetentionObservabilitySnapshot {
    pub max_observed_timestamp: Option<i64>,
    pub recency_reference_timestamp: Option<i64>,
    pub future_skew_window: i64,
    pub future_skew_points_total: u64,
    pub future_skew_max_timestamp: Option<i64>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct FlushObservabilitySnapshot {
    pub pipeline_runs_total: u64,
    pub pipeline_success_total: u64,
    pub pipeline_timeout_total: u64,
    pub pipeline_errors_total: u64,
    pub pipeline_duration_nanos_total: u64,
    #[serde(default)]
    pub admission_backpressure_delays_total: u64,
    #[serde(default)]
    pub admission_backpressure_delay_nanos_total: u64,
    #[serde(default)]
    pub admission_pressure_relief_requests_total: u64,
    #[serde(default)]
    pub admission_pressure_relief_observed_total: u64,
    pub active_flush_runs_total: u64,
    pub active_flush_errors_total: u64,
    /// Active series inspected by cursor-bounded background flush passes.
    #[serde(default)]
    pub active_flush_inspected_series_total: u64,
    /// Modeled input bytes selected by cursor-bounded background flush passes.
    #[serde(default)]
    pub active_flush_selected_input_bytes_total: u64,
    /// Background passes that consumed their complete series-inspection allowance.
    #[serde(default)]
    pub active_flush_item_limit_hits_total: u64,
    /// Candidate active heads skipped because they did not fit the pass byte allowance.
    #[serde(default)]
    pub active_flush_byte_limit_skips_total: u64,
    pub active_flushed_series_total: u64,
    pub active_flushed_chunks_total: u64,
    pub active_flushed_points_total: u64,
    pub persist_runs_total: u64,
    pub persist_success_total: u64,
    pub persist_noop_total: u64,
    pub persist_errors_total: u64,
    /// Sealed chunks inspected by bounded background persistence windows.
    #[serde(default)]
    pub persist_inspected_chunks_total: u64,
    /// Modeled sealed-chunk input bytes selected by bounded persistence windows.
    #[serde(default)]
    pub persist_selected_input_bytes_total: u64,
    /// Bounded persistence windows that exhausted their item allowance.
    #[serde(default)]
    pub persist_item_limit_hits_total: u64,
    /// Bounded persistence windows stopped by their byte allowance.
    #[serde(default)]
    pub persist_byte_limit_hits_total: u64,
    pub persisted_series_total: u64,
    pub persisted_chunks_total: u64,
    pub persisted_points_total: u64,
    pub persisted_segments_total: u64,
    pub persist_duration_nanos_total: u64,
    pub evicted_sealed_chunks_total: u64,
    pub tier_moves_total: u64,
    pub tier_move_errors_total: u64,
    pub expired_segments_total: u64,
    pub hot_segments_visible: u64,
    pub warm_segments_visible: u64,
    pub cold_segments_visible: u64,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct CompactionObservabilitySnapshot {
    pub runs_total: u64,
    pub success_total: u64,
    pub noop_total: u64,
    pub errors_total: u64,
    pub source_segments_total: u64,
    pub output_segments_total: u64,
    pub source_chunks_total: u64,
    pub output_chunks_total: u64,
    pub source_points_total: u64,
    pub output_points_total: u64,
    pub planning_directory_entries_inspected_total: u64,
    pub planning_manifests_inspected_total: u64,
    pub planning_candidates_observed_total: u64,
    pub planning_source_bytes_total: u64,
    pub planning_backlog_observed_total: u64,
    pub planning_budget_exhaustions_total: u64,
    pub duration_nanos_total: u64,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct QueryObservabilitySnapshot {
    pub select_calls_total: u64,
    pub select_errors_total: u64,
    pub select_duration_nanos_total: u64,
    pub select_points_returned_total: u64,
    pub select_with_options_calls_total: u64,
    pub select_with_options_errors_total: u64,
    pub select_with_options_duration_nanos_total: u64,
    pub select_with_options_points_returned_total: u64,
    pub select_all_calls_total: u64,
    pub select_all_errors_total: u64,
    pub select_all_duration_nanos_total: u64,
    pub select_all_series_returned_total: u64,
    pub select_all_points_returned_total: u64,
    pub select_series_calls_total: u64,
    pub select_series_errors_total: u64,
    pub select_series_duration_nanos_total: u64,
    pub select_series_returned_total: u64,
    pub merge_path_queries_total: u64,
    pub merge_path_shard_snapshots_total: u64,
    pub merge_path_shard_snapshot_wait_nanos_total: u64,
    pub merge_path_shard_snapshot_hold_nanos_total: u64,
    pub append_sort_path_queries_total: u64,
    pub hot_only_query_plans_total: u64,
    pub warm_tier_query_plans_total: u64,
    pub cold_tier_query_plans_total: u64,
    pub hot_tier_persisted_chunks_read_total: u64,
    pub warm_tier_persisted_chunks_read_total: u64,
    pub cold_tier_persisted_chunks_read_total: u64,
    pub warm_tier_fetch_duration_nanos_total: u64,
    pub cold_tier_fetch_duration_nanos_total: u64,
    pub rollup_query_plans_total: u64,
    pub partial_rollup_query_plans_total: u64,
    pub rollup_points_read_total: u64,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RemoteStorageObservabilitySnapshot {
    pub enabled: bool,
    pub runtime_mode: StorageRuntimeMode,
    pub cache_policy: RemoteSegmentCachePolicy,
    pub metadata_refresh_interval_ms: u64,
    pub mirror_hot_segments: bool,
    pub catalog_refreshes_total: u64,
    pub catalog_refresh_errors_total: u64,
    pub accessible: bool,
    pub last_refresh_attempt_unix_ms: Option<u64>,
    pub last_successful_refresh_unix_ms: Option<u64>,
    pub consecutive_refresh_failures: u64,
    pub next_refresh_retry_unix_ms: Option<u64>,
    pub backoff_active: bool,
    pub last_refresh_error: Option<String>,
}

impl Default for RemoteStorageObservabilitySnapshot {
    fn default() -> Self {
        Self {
            enabled: false,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
            metadata_refresh_interval_ms: 0,
            mirror_hot_segments: false,
            catalog_refreshes_total: 0,
            catalog_refresh_errors_total: 0,
            accessible: true,
            last_refresh_attempt_unix_ms: None,
            last_successful_refresh_unix_ms: None,
            consecutive_refresh_failures: 0,
            next_refresh_retry_unix_ms: None,
            backoff_active: false,
            last_refresh_error: None,
        }
    }
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct StorageHealthSnapshot {
    pub background_errors_total: u64,
    pub maintenance_errors_total: u64,
    pub degraded: bool,
    pub fail_fast_enabled: bool,
    pub fail_fast_triggered: bool,
    pub last_background_error: Option<String>,
    pub last_maintenance_error: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RollupPolicy {
    pub id: String,
    pub metric: String,
    #[serde(default)]
    pub match_labels: Vec<Label>,
    pub interval: i64,
    pub aggregation: Aggregation,
    #[serde(default)]
    pub bucket_origin: i64,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RollupPolicyStatus {
    pub policy: RollupPolicy,
    /// Matching sources visited in the current or most recently completed bounded traversal.
    pub matched_series: u64,
    /// Visited matching sources that currently have a checkpoint.
    pub materialized_series: u64,
    /// Minimum checkpoint after a complete traversal; `None` while coverage is partial.
    pub materialized_through: Option<i64>,
    pub lag: Option<i64>,
    /// Whether the latest bounded traversal has visited every source posting for this policy.
    #[serde(default)]
    pub source_traversal_complete: bool,
    pub last_run_started_at_ms: Option<u64>,
    pub last_run_completed_at_ms: Option<u64>,
    pub last_run_duration_nanos: u64,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RollupObservabilitySnapshot {
    pub worker_runs_total: u64,
    pub worker_success_total: u64,
    pub worker_errors_total: u64,
    pub policy_runs_total: u64,
    pub buckets_materialized_total: u64,
    pub points_materialized_total: u64,
    pub last_run_duration_nanos: u64,
    /// Whether the bounded traversal reached the end of every active policy.
    #[serde(default)]
    pub source_traversal_complete: bool,
    /// Policy whose source postings will be visited by the next bounded pass.
    #[serde(default)]
    pub continuation_policy_id: Option<String>,
    /// Exclusive source-series cursor for the next bounded pass.
    #[serde(default)]
    pub continuation_after_series_id: Option<u64>,
    #[serde(default)]
    pub policies: Vec<RollupPolicyStatus>,
}

/// Declares whether one execution-aware storage operation accounts for its returned work.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum QueryExecutionAccounting {
    /// The execution-aware operation may return work that it did not charge to the supplied
    /// execution. Callers that enforce a query budget must account for the returned result.
    #[default]
    Unaccounted,
    /// The execution-aware operation charges all work represented by its returned result.
    ///
    /// The promise is operation-specific and covers the series, sample, and returned-byte work
    /// counters. Detailed result APIs must also retain a query-memory reservation for their owned
    /// returned allocation until the result is dropped or the reservation is explicitly taken.
    /// A caller must still model its own fanout, merge, and output allocations.
    Complete,
}

/// Synchronous interface implemented by tsink storage backends.
///
/// Instances returned by [`StorageBuilder::build`] are shared trait objects and may be used from
/// multiple threads. Call [`Storage::close`] explicitly when the host shuts down so persistence
/// or worker-shutdown errors are returned to the caller.
pub trait Storage: Send + Sync {
    /// Returns the shared query budget when this backend implements core query admission.
    ///
    /// Third-party backends retain their previous behavior through the `None` default.
    fn query_budget(&self) -> Option<QueryBudget> {
        None
    }

    /// Admits one execution that nested query operations can share without acquiring more slots.
    fn begin_query_execution(
        &self,
        request_limits: QueryWorkLimits,
        cancellation: QueryCancellationToken,
    ) -> Result<Option<QueryExecution>> {
        self.query_budget()
            .map(|budget| {
                budget
                    .begin_query_with(request_limits, cancellation)
                    .map_err(Into::into)
            })
            .transpose()
    }

    /// Returns the backend's shared query-budget snapshot, or an unreported/unbounded default.
    fn query_budget_snapshot(&self) -> QueryBudgetSnapshot {
        self.query_budget()
            .map(|budget| budget.snapshot())
            .unwrap_or_default()
    }

    /// Inserts rows into the storage.
    ///
    /// This compatibility method only reports success or failure. Use
    /// [`Storage::insert_rows_with_result`] when the caller needs to distinguish between
    /// volatile, append-complete, and durable-complete acknowledgements.
    fn insert_rows(&self, rows: &[Row]) -> Result<()>;

    /// Inserts rows and returns the durability guarantee established when the call succeeds.
    ///
    /// Backends that do not override this method conservatively report
    /// [`WriteAcknowledgement::Volatile`] for non-empty writes because they do not expose a
    /// stronger durability metadata at the API boundary.
    fn insert_rows_with_result(&self, rows: &[Row]) -> Result<WriteResult> {
        self.insert_rows(rows)?;
        Ok(if rows.is_empty() {
            WriteResult::durable()
        } else {
            WriteResult::volatile()
        })
    }

    /// Writes a batch with explicit atomic or best-effort admission semantics.
    ///
    /// Implementations return one ordered [`RowWriteOutcome`] for every submitted row. Expected
    /// row rejections are represented in an `Ok` [`BatchWriteResult`]; the outer error is reserved
    /// for failures for which trustworthy row outcomes cannot be reported. A successful empty
    /// batch has no outcomes and no acknowledgement.
    ///
    /// Backends must opt in explicitly. The default does not delegate to the compatibility
    /// [`Storage::insert_rows`] method because that method cannot prove indexed outcomes or atomic
    /// rollback behavior.
    fn write_batch(&self, _rows: &[Row], _mode: WriteMode) -> Result<BatchWriteResult> {
        Err(TsinkError::UnsupportedOperation {
            operation: "write_batch",
            reason: "canonical indexed batch outcomes are not implemented by this storage backend"
                .to_string(),
        })
    }

    fn select(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
    ) -> Result<Vec<DataPoint>>;

    /// Selects under an already-admitted execution. Built-in nested callers use this to share one
    /// concurrency slot; compatibility backends safely fall back to their existing method.
    fn select_with_execution(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        _execution: &QueryExecution,
    ) -> Result<Vec<DataPoint>> {
        self.select(metric, labels, start, end)
    }

    fn select_into(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        out: &mut Vec<DataPoint>,
    ) -> Result<()> {
        *out = self.select(metric, labels, start, end)?;
        Ok(())
    }

    fn select_into_with_execution(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
        out: &mut Vec<DataPoint>,
        execution: &QueryExecution,
    ) -> Result<()> {
        *out = self.select_with_execution(metric, labels, start, end, execution)?;
        Ok(())
    }

    fn select_many(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
    ) -> Result<Vec<SeriesPoints>> {
        if start >= end {
            return Err(TsinkError::InvalidTimeRange { start, end });
        }

        let mut out = Vec::with_capacity(series.len());
        for item in series {
            let points = match self.select(&item.name, &item.labels, start, end) {
                Ok(points) => points,
                Err(TsinkError::NoDataPoints { .. }) => Vec::new(),
                Err(err) => return Err(err),
            };
            out.push(SeriesPoints {
                series: item.clone(),
                points,
            });
        }
        Ok(out)
    }

    fn select_many_with_execution(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        execution: &QueryExecution,
    ) -> Result<Vec<SeriesPoints>> {
        if start >= end {
            return Err(TsinkError::InvalidTimeRange { start, end });
        }
        let mut out = Vec::with_capacity(series.len());
        for item in series {
            let points =
                match self.select_with_execution(&item.name, &item.labels, start, end, execution) {
                    Ok(points) => points,
                    Err(TsinkError::NoDataPoints { .. }) => Vec::new(),
                    Err(err) => return Err(err),
                };
            out.push(SeriesPoints {
                series: item.clone(),
                points,
            });
        }
        Ok(out)
    }

    /// Selects a batch and, when supported, returns exact selector-existence metadata from the
    /// same operation that performed the read.
    ///
    /// The conservative default preserves compatibility for third-party backends and cannot
    /// distinguish an existing empty-range series from a missing series.
    fn select_many_with_execution_result(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        execution: &QueryExecution,
    ) -> Result<SelectManyExecutionResult> {
        self.select_many_with_execution(series, start, end, execution)
            .map(SelectManyExecutionResult::unaccounted)
    }

    /// Reports whether [`Storage::select_many_with_execution_result`] fully accounts for its
    /// returned result and supplies exact selector-existence metadata.
    ///
    /// The conservative default preserves compatibility for third-party backends.
    fn select_many_execution_accounting(&self) -> QueryExecutionAccounting {
        QueryExecutionAccounting::Unaccounted
    }

    fn select_with_options(&self, metric: &str, opts: QueryOptions) -> Result<Vec<DataPoint>>;

    fn select_with_options_with_execution(
        &self,
        metric: &str,
        opts: QueryOptions,
        _execution: &QueryExecution,
    ) -> Result<Vec<DataPoint>> {
        self.select_with_options(metric, opts)
    }

    fn select_all(
        &self,
        metric: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>>;

    fn select_all_with_execution(
        &self,
        metric: &str,
        start: i64,
        end: i64,
        _execution: &QueryExecution,
    ) -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
        self.select_all(metric, start, end)
    }

    fn list_metrics(&self) -> Result<Vec<MetricSeries>> {
        Err(TsinkError::Other(
            "list_metrics is not implemented for this storage backend".to_string(),
        ))
    }

    /// Lists known series while sharing an already-admitted query execution.
    ///
    /// Built-in and forwarding backends override this so async and nested metadata reads do not
    /// acquire a second concurrency slot. Third-party backends retain compatibility by delegating
    /// to [`Storage::list_metrics`].
    fn list_metrics_with_execution(
        &self,
        _execution: &QueryExecution,
    ) -> Result<Vec<MetricSeries>> {
        self.list_metrics()
    }

    fn list_metrics_with_wal(&self) -> Result<Vec<MetricSeries>> {
        self.list_metrics()
    }

    /// Lists known metric series within a shard scope.
    ///
    /// Backends must override this to provide a bounded shard-scoped implementation.
    fn list_metrics_in_shards(&self, scope: &MetadataShardScope) -> Result<Vec<MetricSeries>> {
        let scope = scope.normalized()?;
        if scope.shards.is_empty() {
            return Ok(Vec::new());
        }
        Err(TsinkError::UnsupportedOperation {
            operation: "list_metrics_in_shards",
            reason: "bounded shard-scoped metadata is not implemented by this storage backend"
                .to_string(),
        })
    }

    fn select_series(&self, selection: &SeriesSelection) -> Result<Vec<MetricSeries>> {
        crate::query_selection::select_series_by_scan(self, selection)
    }

    fn select_series_with_execution(
        &self,
        selection: &SeriesSelection,
        _execution: &QueryExecution,
    ) -> Result<Vec<MetricSeries>> {
        self.select_series(selection)
    }

    /// Selects metadata while retaining any query-memory reservation owned by the result.
    ///
    /// The conservative default preserves compatibility for third-party backends. A bounded
    /// caller must require [`QueryExecutionAccounting::Complete`] before relying on this result.
    fn select_series_with_execution_result(
        &self,
        selection: &SeriesSelection,
        execution: &QueryExecution,
    ) -> Result<SelectSeriesExecutionResult> {
        self.select_series_with_execution(selection, execution)
            .map(SelectSeriesExecutionResult::unaccounted)
    }

    /// Reports whether [`Storage::select_series_with_execution_result`] fully accounts for its
    /// returned result. The conservative default preserves compatibility for third-party
    /// backends.
    fn select_series_execution_accounting(&self) -> QueryExecutionAccounting {
        QueryExecutionAccounting::Unaccounted
    }

    #[cfg(test)]
    fn sync_persisted_segments_from_disk_if_dirty_for_tests(&self) -> Result<()> {
        Ok(())
    }

    /// Selects metric series by structured label matchers within a shard scope.
    ///
    /// Backends must override this to provide a bounded shard-scoped implementation.
    fn select_series_in_shards(
        &self,
        selection: &SeriesSelection,
        scope: &MetadataShardScope,
    ) -> Result<Vec<MetricSeries>> {
        let _ = crate::query_selection::prepare_series_selection(selection)?;
        let scope = scope.normalized()?;
        if scope.shards.is_empty() {
            return Ok(Vec::new());
        }
        Err(TsinkError::UnsupportedOperation {
            operation: "select_series_in_shards",
            reason: "bounded shard-scoped metadata is not implemented by this storage backend"
                .to_string(),
        })
    }

    fn select_series_in_shards_with_execution(
        &self,
        selection: &SeriesSelection,
        scope: &MetadataShardScope,
        _execution: &QueryExecution,
    ) -> Result<Vec<MetricSeries>> {
        self.select_series_in_shards(selection, scope)
    }

    /// Selects shard-scoped metadata while retaining its query-memory reservation.
    fn select_series_in_shards_with_execution_result(
        &self,
        selection: &SeriesSelection,
        scope: &MetadataShardScope,
        execution: &QueryExecution,
    ) -> Result<SelectSeriesExecutionResult> {
        self.select_series_in_shards_with_execution(selection, scope, execution)
            .map(SelectSeriesExecutionResult::unaccounted)
    }

    /// Reports whether [`Storage::select_series_in_shards_with_execution_result`] fully accounts
    /// for its returned result. The conservative default preserves compatibility for third-party
    /// backends.
    fn select_series_in_shards_execution_accounting(&self) -> QueryExecutionAccounting {
        QueryExecutionAccounting::Unaccounted
    }

    fn compute_shard_window_digest(
        &self,
        shard: u32,
        shard_count: u32,
        window_start: i64,
        window_end: i64,
    ) -> Result<ShardWindowDigest> {
        validate_shard_window_request(shard, shard_count, window_start, window_end)?;

        let scope = MetadataShardScope::new(shard_count, vec![shard]).normalized()?;
        let mut series = self.list_metrics_in_shards(&scope)?;
        series.sort_by_cached_key(|entry| {
            shard_window_series_identity_key(entry.name.as_str(), &entry.labels)
        });

        let mut points = Vec::new();
        let mut point_hashes = Vec::new();
        let mut fingerprint = SHARD_WINDOW_FNV_OFFSET_BASIS;
        let mut series_count = 0u64;
        let mut point_count = 0u64;
        for metric_series in series {
            self.select_into(
                metric_series.name.as_str(),
                &metric_series.labels,
                window_start,
                window_end,
                &mut points,
            )?;
            if points.is_empty() {
                continue;
            }

            let identity_key = shard_window_series_identity_key(
                metric_series.name.as_str(),
                &metric_series.labels,
            );
            point_hashes.clear();
            for point in &points {
                point_hashes.push(shard_window_hash_data_point(point)?);
            }
            point_hashes.sort_unstable();

            shard_window_fnv1a_update(&mut fingerprint, identity_key.as_bytes());
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

    fn compute_shard_window_digest_with_execution(
        &self,
        shard: u32,
        shard_count: u32,
        window_start: i64,
        window_end: i64,
        _execution: &QueryExecution,
    ) -> Result<ShardWindowDigest> {
        self.compute_shard_window_digest(shard, shard_count, window_start, window_end)
    }

    /// Reports whether [`Storage::compute_shard_window_digest_with_execution`] fully charges its
    /// scan work and fixed digest result to the supplied execution.
    ///
    /// The conservative default prevents bounded callers from trusting the compatibility
    /// implementation above, which does not carry execution accounting into backend work.
    fn compute_shard_window_digest_execution_accounting(&self) -> QueryExecutionAccounting {
        QueryExecutionAccounting::Unaccounted
    }

    fn scan_shard_window_rows(
        &self,
        shard: u32,
        shard_count: u32,
        window_start: i64,
        window_end: i64,
        options: ShardWindowScanOptions,
    ) -> Result<ShardWindowRowsPage> {
        validate_shard_window_request(shard, shard_count, window_start, window_end)?;
        validate_shard_window_scan_options(options)?;

        let scope = MetadataShardScope::new(shard_count, vec![shard]).normalized()?;
        let mut series = self.list_metrics_in_shards(&scope)?;
        series.sort_by_cached_key(|entry| {
            shard_window_series_identity_key(entry.name.as_str(), &entry.labels)
        });

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
        'series_scan: for metric_series in series {
            self.select_into(
                metric_series.name.as_str(),
                &metric_series.labels,
                window_start,
                window_end,
                &mut points,
            )?;
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
                    metric_series.name.clone(),
                    metric_series.labels.clone(),
                    point.clone(),
                ));
                stream_row_offset = stream_row_offset.saturating_add(1);
            }
        }

        Ok(response)
    }

    fn scan_shard_window_rows_with_execution(
        &self,
        shard: u32,
        shard_count: u32,
        window_start: i64,
        window_end: i64,
        options: ShardWindowScanOptions,
        _execution: &QueryExecution,
    ) -> Result<ShardWindowRowsPage> {
        self.scan_shard_window_rows(shard, shard_count, window_start, window_end, options)
    }

    /// Scans a shard-window page while retaining the result's query-memory reservation.
    ///
    /// The conservative default preserves third-party compatibility but is not sufficient for a
    /// bounded caller.
    fn scan_shard_window_rows_with_execution_result(
        &self,
        shard: u32,
        shard_count: u32,
        window_start: i64,
        window_end: i64,
        options: ShardWindowScanOptions,
        execution: &QueryExecution,
    ) -> Result<ShardWindowRowsExecutionResult> {
        self.scan_shard_window_rows_with_execution(
            shard,
            shard_count,
            window_start,
            window_end,
            options,
            execution,
        )
        .map(ShardWindowRowsExecutionResult::unaccounted)
    }

    /// Reports whether the execution-aware shard-window scan accounts all returned work and
    /// retains a memory reservation for the returned page.
    ///
    /// The conservative default prevents bounded repair callers from silently delegating to the
    /// legacy scan above.
    fn scan_shard_window_rows_execution_accounting(&self) -> QueryExecutionAccounting {
        QueryExecutionAccounting::Unaccounted
    }

    fn scan_series_rows(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        options: QueryRowsScanOptions,
    ) -> Result<QueryRowsPage> {
        if start >= end {
            return Err(TsinkError::InvalidTimeRange { start, end });
        }
        validate_query_rows_scan_options(options)?;

        let max_rows = options.max_rows.unwrap_or(usize::MAX);
        let row_offset = options.row_offset.unwrap_or(0);

        let mut response = QueryRowsPage {
            rows_scanned: 0,
            truncated: false,
            next_row_offset: None,
            rows: Vec::new(),
        };

        let mut points = Vec::new();
        let mut stream_row_offset = 0u64;
        'series_scan: for metric_series in series {
            self.select_into(
                metric_series.name.as_str(),
                &metric_series.labels,
                start,
                end,
                &mut points,
            )?;
            if points.is_empty() {
                continue;
            }

            for point in points.iter() {
                if stream_row_offset < row_offset {
                    stream_row_offset = stream_row_offset.saturating_add(1);
                    continue;
                }

                if response.rows.len() >= max_rows {
                    response.truncated = true;
                    response.next_row_offset = Some(stream_row_offset);
                    break 'series_scan;
                }

                response.rows_scanned = response.rows_scanned.saturating_add(1);
                response.rows.push(Row::with_labels(
                    metric_series.name.clone(),
                    metric_series.labels.clone(),
                    point.clone(),
                ));
                stream_row_offset = stream_row_offset.saturating_add(1);
            }
        }

        Ok(response)
    }

    fn scan_series_rows_with_execution(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        options: QueryRowsScanOptions,
        _execution: &QueryExecution,
    ) -> Result<QueryRowsPage> {
        self.scan_series_rows(series, start, end, options)
    }

    /// Scans a row page while retaining any query-memory reservation owned by the result.
    ///
    /// The conservative default preserves compatibility for third-party backends. A bounded
    /// caller must require [`QueryExecutionAccounting::Complete`] before relying on this result.
    fn scan_series_rows_with_execution_result(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        options: QueryRowsScanOptions,
        execution: &QueryExecution,
    ) -> Result<QueryRowsExecutionResult> {
        self.scan_series_rows_with_execution(series, start, end, options, execution)
            .map(QueryRowsExecutionResult::unaccounted)
    }

    /// Reports whether [`Storage::scan_series_rows_with_execution_result`] fully accounts for its
    /// returned work and retains a query-memory reservation for the returned row page.
    ///
    /// The conservative default preserves compatibility for third-party backends.
    fn scan_series_rows_execution_accounting(&self) -> QueryExecutionAccounting {
        QueryExecutionAccounting::Unaccounted
    }

    fn scan_metric_rows(
        &self,
        metric: &str,
        start: i64,
        end: i64,
        options: QueryRowsScanOptions,
    ) -> Result<QueryRowsPage> {
        validate_metric(metric)?;
        if start >= end {
            return Err(TsinkError::InvalidTimeRange { start, end });
        }
        validate_query_rows_scan_options(options)?;

        let series = self
            .list_metrics()?
            .into_iter()
            .filter(|entry| entry.name == metric)
            .collect::<Vec<_>>();
        self.scan_series_rows(&series, start, end, options)
    }

    fn scan_metric_rows_with_execution(
        &self,
        metric: &str,
        start: i64,
        end: i64,
        options: QueryRowsScanOptions,
        _execution: &QueryExecution,
    ) -> Result<QueryRowsPage> {
        self.scan_metric_rows(metric, start, end, options)
    }

    /// Adds deletion tombstones for series selected by matchers and optional time range.
    ///
    /// Implementations that cannot durably persist tombstones, such as compute-only
    /// query nodes, must reject the request instead of reporting an ephemeral success.
    fn delete_series(&self, _selection: &SeriesSelection) -> Result<DeleteSeriesResult> {
        Err(TsinkError::InvalidConfiguration(
            "delete_series is not implemented for this storage backend".to_string(),
        ))
    }

    fn memory_used(&self) -> usize {
        0
    }

    /// Reports the storage-side limits enforced by this backend.
    ///
    /// Third-party backends receive an unreported default. They should override this method only
    /// for limits they actually enforce, not for advisory targets.
    fn effective_storage_limits(&self) -> EffectiveStorageLimits {
        EffectiveStorageLimits::default()
    }

    /// Reports the selected resource profile, resolved limits, and override provenance.
    ///
    /// Third-party backends receive an unreported versioned default until they opt in.
    fn resource_configuration_snapshot(&self) -> ResourceConfigurationSnapshot {
        ResourceConfigurationSnapshot {
            schema_version: RESOURCE_CONFIGURATION_SCHEMA_VERSION,
            ..ResourceConfigurationSnapshot::default()
        }
    }

    /// Returns configured in-memory byte budget for the storage engine.
    ///
    /// `usize::MAX` means "no explicit budget configured".
    fn memory_budget(&self) -> usize {
        usize::MAX
    }

    fn observability_snapshot(&self) -> StorageObservabilitySnapshot {
        StorageObservabilitySnapshot {
            limits: self.effective_storage_limits(),
            resource_configuration: self.resource_configuration_snapshot(),
            ..StorageObservabilitySnapshot::default()
        }
    }

    fn apply_rollup_policies(
        &self,
        _policies: Vec<RollupPolicy>,
    ) -> Result<RollupObservabilitySnapshot> {
        Err(TsinkError::InvalidConfiguration(
            "rollup policies are not implemented for this storage backend".to_string(),
        ))
    }

    /// Advances synchronous rollup materialization and returns bounded traversal progress.
    ///
    /// Finite built-in profiles process at most one configured item/byte page. Callers that need a
    /// complete traversal repeat this method until
    /// [`RollupObservabilitySnapshot::source_traversal_complete`] is true. The explicit
    /// [`ResourceProfile::ExpertUnlimited`] profile drains one complete cycle per call.
    fn trigger_rollup_run(&self) -> Result<RollupObservabilitySnapshot> {
        Err(TsinkError::InvalidConfiguration(
            "rollup runtime is not implemented for this storage backend".to_string(),
        ))
    }

    /// Writes an atomic on-disk snapshot to `destination`.
    ///
    /// The built-in persistent backend requires a destination that does not already exist and
    /// creates its sibling staging directory with create-exclusive semantics before publishing the
    /// completed snapshot with an atomic no-replace directory rename. It bounds each copied source
    /// tree and the aggregate staged namespace by [`MAX_SNAPSHOT_RESTORE_ENTRIES`], bounds depth by
    /// [`MAX_SNAPSHOT_RESTORE_DEPTH`], and synchronizes every copied regular file. Error cleanup
    /// verifies the staging directory's captured filesystem identity before bounded deletion, so a
    /// foreign replacement at the same path is preserved. Snapshot support is backend-specific and
    /// may not be available for all storage implementations. Restore built-in snapshots with
    /// [`StorageBuilder::restore_from_snapshot`].
    fn snapshot(&self, _destination: &Path) -> Result<()> {
        Err(TsinkError::InvalidConfiguration(
            "snapshot is not implemented for this storage backend".to_string(),
        ))
    }

    /// Flushes pending state and shuts down resources owned by this storage instance.
    ///
    /// A successful close ends the instance's lifecycle; subsequent operations return
    /// [`TsinkError::StorageClosed`] for the built-in backend. Explicit close is preferred over
    /// relying on drop because it lets the embedder handle shutdown failures. The built-in
    /// backend applies the configured write timeout to each outer maintenance, writer-drain, and
    /// compaction coordination acquisition. Once durability filesystem I/O begins, portable
    /// blocking file APIs cannot be safely preempted; close waits for that call and reports its
    /// actual result.
    fn close(&self) -> Result<()>;

    /// Test-only abrupt-shutdown simulation. Implementations that support it must stop workers
    /// and release leases without flushing or recovering durable state.
    #[cfg(test)]
    fn abandon_without_close_for_tests(&self) -> Result<()> {
        Err(TsinkError::UnsupportedOperation {
            operation: "abandon_without_close_for_tests",
            reason: "abrupt-shutdown simulation is not implemented by this storage backend"
                .to_string(),
        })
    }
}

/// Configures and opens the built-in storage engine.
///
/// The default builder creates read-write storage and uses nanosecond timestamps. Without a
/// [`StorageBuilder::with_data_path`], storage is in-memory and no on-disk WAL is opened. The
/// default base profile is [`ResourceProfile::Embedded`], which supplies finite memory,
/// cardinality, write, query, WAL, and persistent-disk limits. Profile disk controls are dormant
/// for in-memory storage. Select [`ResourceProfile::ExpertUnlimited`] explicitly to preserve the
/// legacy unbounded storage/query behavior during migration.
pub struct StorageBuilder {
    resource_profile: ResourceProfile,
    resource_overrides: BTreeSet<ResourceLimitOverride>,
    data_path: Option<PathBuf>,
    object_store_path: Option<PathBuf>,
    retention: Duration,
    retention_enforced: bool,
    max_future_skew: Option<Duration>,
    hot_tier_retention: Option<Duration>,
    warm_tier_retention: Option<Duration>,
    runtime_mode: StorageRuntimeMode,
    remote_segment_cache_policy: RemoteSegmentCachePolicy,
    remote_segment_refresh_interval: Duration,
    mirror_hot_segments_to_object_store: bool,
    timestamp_precision: TimestampPrecision,
    chunk_points: usize,
    max_writers: usize,
    write_timeout: Duration,
    partition_duration: Duration,
    max_active_partition_heads_per_series: usize,
    memory_limit_bytes: usize,
    cardinality_limit: usize,
    max_labels_per_series: usize,
    max_series_identity_bytes: usize,
    max_new_series_per_window: Option<usize>,
    new_series_window: Duration,
    write_batch_limits: WriteBatchLimits,
    wal_enabled: bool,
    wal_size_limit_bytes: usize,
    local_disk_limit_bytes: Option<u64>,
    filesystem_free_headroom_bytes: u64,
    maintenance_temp_reserve_bytes: u64,
    maintenance_max_items_per_pass: usize,
    maintenance_max_bytes_per_pass: u64,
    shared_local_disk_budget: Option<std::sync::Arc<crate::LocalDiskBudget>>,
    wal_buffer_size: usize,
    wal_sync_mode: WalSyncMode,
    wal_replay_mode: WalReplayMode,
    background_fail_fast: bool,
    metadata_shard_count: Option<u32>,
    query_budget_limits: QueryBudgetLimits,
    background_threads_enabled_override: Option<bool>,
    #[cfg(test)]
    current_time_override: Option<i64>,
}

impl Default for StorageBuilder {
    fn default() -> Self {
        let mut builder = Self {
            resource_profile: ResourceProfile::Embedded,
            resource_overrides: BTreeSet::new(),
            data_path: None,
            object_store_path: None,
            retention: Duration::from_secs(14 * 24 * 3600),
            retention_enforced: false,
            max_future_skew: None,
            hot_tier_retention: None,
            warm_tier_retention: None,
            runtime_mode: StorageRuntimeMode::ReadWrite,
            remote_segment_cache_policy: RemoteSegmentCachePolicy::MetadataOnly,
            remote_segment_refresh_interval: DEFAULT_REMOTE_SEGMENT_REFRESH_INTERVAL,
            mirror_hot_segments_to_object_store: false,
            timestamp_precision: TimestampPrecision::Nanoseconds,
            chunk_points: DEFAULT_CHUNK_POINTS,
            max_writers: crate::cgroup::default_workers_limit(),
            write_timeout: Duration::from_secs(30),
            partition_duration: Duration::from_secs(3600),
            max_active_partition_heads_per_series: DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES,
            memory_limit_bytes: usize::MAX,
            cardinality_limit: usize::MAX,
            max_labels_per_series: crate::label::DEFAULT_MAX_LABELS_PER_SERIES,
            max_series_identity_bytes: crate::label::DEFAULT_MAX_SERIES_IDENTITY_BYTES,
            max_new_series_per_window: None,
            new_series_window: Duration::from_secs(60),
            write_batch_limits: WriteBatchLimits::default(),
            wal_enabled: true,
            wal_size_limit_bytes: usize::MAX,
            local_disk_limit_bytes: None,
            filesystem_free_headroom_bytes: 0,
            maintenance_temp_reserve_bytes: 0,
            maintenance_max_items_per_pass: 1_024,
            maintenance_max_bytes_per_pass: 256 * 1024 * 1024,
            shared_local_disk_budget: None,
            wal_buffer_size: 4096,
            wal_sync_mode: WalSyncMode::default(),
            wal_replay_mode: WalReplayMode::Strict,
            background_fail_fast: true,
            metadata_shard_count: None,
            query_budget_limits: QueryBudgetLimits::default(),
            background_threads_enabled_override: None,
            #[cfg(test)]
            current_time_override: None,
        };
        builder.apply_selected_resource_profile();
        builder
    }
}

impl StorageBuilder {
    fn has_resource_override(&self, field: ResourceLimitOverride) -> bool {
        self.resource_overrides.contains(&field)
    }

    fn apply_selected_resource_profile(&mut self) {
        let finite = self.resource_profile.finite_limits();
        let as_usize = |value: u64| usize::try_from(value).unwrap_or(usize::MAX);

        if !self.has_resource_override(ResourceLimitOverride::AccountedMemory) {
            self.memory_limit_bytes = finite
                .map(|limits| as_usize(limits.accounted_memory_bytes))
                .unwrap_or(usize::MAX);
        }
        if !self.has_resource_override(ResourceLimitOverride::LocalDisk) {
            self.local_disk_limit_bytes = finite.map(|limits| limits.local_disk_bytes);
        }
        if !self.has_resource_override(ResourceLimitOverride::FilesystemFreeHeadroom) {
            self.filesystem_free_headroom_bytes = finite
                .map(|limits| limits.filesystem_free_headroom_bytes)
                .unwrap_or(0);
        }
        if !self.has_resource_override(ResourceLimitOverride::MaintenanceTempReserve) {
            self.maintenance_temp_reserve_bytes = finite
                .map(|limits| limits.maintenance_temp_reserve_bytes)
                .unwrap_or(0);
        }
        if !self.has_resource_override(ResourceLimitOverride::WalBytes) {
            self.wal_size_limit_bytes = finite
                .map(|limits| as_usize(limits.wal_bytes))
                .unwrap_or(usize::MAX);
        }
        if !self.has_resource_override(ResourceLimitOverride::WalWriteBuffer) {
            self.wal_buffer_size = finite
                .map(|limits| as_usize(limits.wal_write_buffer_bytes))
                .unwrap_or(4096);
        }
        if !self.has_resource_override(ResourceLimitOverride::Cardinality) {
            self.cardinality_limit = finite
                .map(|limits| as_usize(limits.cardinality))
                .unwrap_or(usize::MAX);
        }
        if !self.has_resource_override(ResourceLimitOverride::MaxLabelsPerSeries) {
            self.max_labels_per_series = finite
                .map(|limits| as_usize(limits.max_labels_per_series))
                .unwrap_or(crate::label::DEFAULT_MAX_LABELS_PER_SERIES);
        }
        if !self.has_resource_override(ResourceLimitOverride::MaxSeriesIdentityBytes) {
            self.max_series_identity_bytes = finite
                .map(|limits| as_usize(limits.max_series_identity_bytes))
                .unwrap_or(crate::label::DEFAULT_MAX_SERIES_IDENTITY_BYTES);
        }
        if !self.has_resource_override(ResourceLimitOverride::SeriesCreationRate) {
            self.max_new_series_per_window =
                finite.map(|limits| as_usize(limits.max_new_series_per_window));
            self.new_series_window = finite
                .map(|limits| limits.new_series_window)
                .unwrap_or(Duration::from_secs(60));
        }
        if !self.has_resource_override(ResourceLimitOverride::WriteBatch) {
            self.write_batch_limits = finite.map(|limits| limits.write_batch).unwrap_or_default();
        }
        if !self.has_resource_override(ResourceLimitOverride::ConcurrentWriters) {
            self.max_writers = finite
                .map(|limits| as_usize(limits.max_concurrent_writers))
                .unwrap_or_else(crate::cgroup::default_workers_limit)
                .max(1);
        }
        if !self.has_resource_override(ResourceLimitOverride::WriteTimeout) {
            self.write_timeout = finite
                .map(|limits| limits.write_timeout)
                .unwrap_or(Duration::from_secs(30));
        }
        if !self.has_resource_override(ResourceLimitOverride::PartitionHeads) {
            self.max_active_partition_heads_per_series = finite
                .map(|limits| as_usize(limits.max_active_partition_heads_per_series))
                .unwrap_or(DEFAULT_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES)
                .max(1);
        }
        if !self.has_resource_override(ResourceLimitOverride::QueryBudget) {
            self.query_budget_limits = finite.map(|limits| limits.query).unwrap_or_default();
        }
        if !self.has_resource_override(ResourceLimitOverride::MaintenanceWork) {
            self.maintenance_max_items_per_pass = finite
                .map(|limits| limits.background.maintenance_max_items_per_pass)
                .unwrap_or(usize::MAX);
            self.maintenance_max_bytes_per_pass = finite
                .map(|limits| limits.background.maintenance_max_bytes_per_pass)
                .unwrap_or(u64::MAX);
        }
    }

    /// Selects a named or custom base profile.
    ///
    /// Low-level settings already applied to this builder remain overrides. This rule is
    /// deliberately independent of call order: selecting another profile never erases them.
    #[must_use]
    pub fn with_resource_profile(mut self, profile: ResourceProfile) -> Self {
        self.resource_profile = profile;
        self.apply_selected_resource_profile();
        self
    }

    /// Clears one low-level override and restores that field from the selected base profile.
    #[must_use]
    pub fn clear_resource_limit_override(mut self, field: ResourceLimitOverride) -> Self {
        self.resource_overrides.remove(&field);
        self.apply_selected_resource_profile();
        self
    }

    /// Clears every low-level override and restores the complete selected base profile.
    #[must_use]
    pub fn clear_resource_limit_overrides(mut self) -> Self {
        self.resource_overrides.clear();
        self.shared_local_disk_budget = None;
        self.apply_selected_resource_profile();
        self
    }

    /// Returns the selected base profile, including custom finite limits when applicable.
    #[must_use]
    pub const fn resource_profile(&self) -> ResourceProfile {
        self.resource_profile
    }

    /// Returns the currently resolved, versioned resource configuration without opening storage.
    #[must_use]
    pub fn resource_configuration_snapshot(&self) -> ResourceConfigurationSnapshot {
        let persistent =
            self.runtime_mode == StorageRuntimeMode::ReadWrite && self.data_path.is_some();
        let wal_enabled = persistent && self.wal_enabled;
        let finite_usize =
            |value: usize| (value != usize::MAX).then(|| u64::try_from(value).unwrap_or(u64::MAX));
        let duration_nanos =
            |duration: Duration| u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX);
        let has_tiered_storage = self.object_store_path.is_some();
        let flush_concurrency = u64::from(persistent);
        let compaction_concurrency = u64::from(persistent);
        let persisted_refresh_concurrency = u64::from(
            persistent
                || (self.runtime_mode == StorageRuntimeMode::ComputeOnly && has_tiered_storage),
        );
        let rollup_concurrency = u64::from(persistent);
        ResourceConfigurationSnapshot {
            schema_version: RESOURCE_CONFIGURATION_SCHEMA_VERSION,
            reported_by_backend: true,
            selected_profile: self.resource_profile.name(),
            resolved_limits: ResolvedResourceLimits {
                storage: EffectiveStorageLimits {
                    reported_by_backend: true,
                    persistent,
                    wal_enabled,
                    accounted_memory_bytes: finite_usize(self.memory_limit_bytes),
                    cardinality: finite_usize(self.cardinality_limit),
                    max_labels_per_series: finite_usize(self.max_labels_per_series),
                    max_series_identity_bytes: finite_usize(self.max_series_identity_bytes),
                    max_new_series_per_window: self
                        .max_new_series_per_window
                        .map(|value| u64::try_from(value).unwrap_or(u64::MAX)),
                    new_series_window_nanos: self
                        .max_new_series_per_window
                        .map(|_| duration_nanos(self.new_series_window)),
                    max_write_batch_rows: self
                        .write_batch_limits
                        .max_rows
                        .map(|value| u64::try_from(value).unwrap_or(u64::MAX)),
                    max_write_batch_input_bytes: self
                        .write_batch_limits
                        .max_modeled_input_bytes
                        .map(|value| u64::try_from(value).unwrap_or(u64::MAX)),
                    wal_bytes: wal_enabled
                        .then(|| finite_usize(self.wal_size_limit_bytes))
                        .flatten(),
                    wal_write_buffer_bytes: wal_enabled
                        .then(|| u64::try_from(self.wal_buffer_size.max(1)).unwrap_or(u64::MAX)),
                    local_disk_bytes: persistent.then_some(self.local_disk_limit_bytes).flatten(),
                    filesystem_free_headroom_bytes: persistent
                        .then_some(self.filesystem_free_headroom_bytes),
                    maintenance_temp_reserve_bytes: persistent
                        .then_some(self.maintenance_temp_reserve_bytes),
                    max_concurrent_writers: Some(
                        u64::try_from(self.max_writers.max(1)).unwrap_or(u64::MAX),
                    ),
                    write_timeout_nanos: Some(duration_nanos(self.write_timeout)),
                    max_background_threads: Some(
                        flush_concurrency
                            .saturating_add(compaction_concurrency)
                            .saturating_add(persisted_refresh_concurrency)
                            .saturating_add(rollup_concurrency),
                    ),
                    max_flush_concurrency: Some(flush_concurrency),
                    max_compaction_concurrency: Some(compaction_concurrency),
                    max_retention_tiering_concurrency: Some(u64::from(
                        persistent && self.retention_enforced,
                    )),
                    max_remote_catalog_refresh_concurrency: Some(u64::from(
                        self.runtime_mode == StorageRuntimeMode::ComputeOnly && has_tiered_storage,
                    )),
                    max_remote_tier_fetch_concurrency: if has_tiered_storage {
                        self.query_budget_limits.max_concurrent_queries
                    } else {
                        Some(0)
                    },
                    max_rollup_concurrency: Some(rollup_concurrency),
                    flush_interval_nanos: (flush_concurrency > 0)
                        .then(|| duration_nanos(Duration::from_millis(250))),
                    compaction_interval_nanos: (compaction_concurrency > 0)
                        .then(|| duration_nanos(Duration::from_secs(5))),
                    persisted_refresh_poll_interval_nanos: (persisted_refresh_concurrency > 0)
                        .then(|| duration_nanos(Duration::from_millis(250))),
                    rollup_interval_nanos: (rollup_concurrency > 0)
                        .then(|| duration_nanos(Duration::from_secs(5))),
                    max_active_partition_heads_per_series: Some(
                        u64::try_from(self.max_active_partition_heads_per_series)
                            .unwrap_or(u64::MAX),
                    ),
                },
                query: self.query_budget_limits,
                async_runtime: None,
                maintenance_max_items_per_pass: (self.maintenance_max_items_per_pass != usize::MAX)
                    .then(|| {
                        u64::try_from(self.maintenance_max_items_per_pass).unwrap_or(u64::MAX)
                    }),
                maintenance_max_bytes_per_pass: (self.maintenance_max_bytes_per_pass != u64::MAX)
                    .then_some(self.maintenance_max_bytes_per_pass),
            },
            overrides: self.resource_overrides.iter().copied().collect(),
        }
    }

    fn validate_resource_configuration(&self) -> Result<()> {
        if let Some(limits) = self.resource_profile.finite_limits() {
            limits.validate()?;
        }
        if self
            .write_batch_limits
            .max_rows
            .is_some_and(|value| value == 0)
            || self
                .write_batch_limits
                .max_modeled_input_bytes
                .is_some_and(|value| value == 0)
        {
            return Err(TsinkError::InvalidConfiguration(
                "configured write-batch limits must be greater than zero".to_string(),
            ));
        }
        if self.maintenance_max_items_per_pass == 0 || self.maintenance_max_bytes_per_pass == 0 {
            return Err(TsinkError::InvalidConfiguration(
                "maintenance per-pass limits must be greater than zero".to_string(),
            ));
        }
        if let Some(max_rows) = self.write_batch_limits.max_rows {
            if self.maintenance_max_items_per_pass != usize::MAX
                && self.maintenance_max_items_per_pass < max_rows
            {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "maintenance item cap {} is smaller than write-batch row limit {}; increase the maintenance cap or lower the row limit so one admitted batch cannot be stranded",
                    self.maintenance_max_items_per_pass, max_rows
                )));
            }
        }
        if self.maintenance_max_bytes_per_pass != u64::MAX {
            if self.memory_limit_bytes == usize::MAX {
                return Err(TsinkError::InvalidConfiguration(
                    "a finite maintenance byte cap requires a finite accounted-memory limit so one admitted sealed chunk cannot exceed the pass cap"
                        .to_string(),
                ));
            }
            let memory_limit_bytes = u64::try_from(self.memory_limit_bytes).unwrap_or(u64::MAX);
            if self.maintenance_max_bytes_per_pass < memory_limit_bytes {
                return Err(TsinkError::InvalidConfiguration(format!(
                    "maintenance byte cap {} is smaller than accounted-memory limit {}; increase the maintenance cap or lower the memory limit so one admitted sealed chunk cannot be stranded",
                    self.maintenance_max_bytes_per_pass, memory_limit_bytes
                )));
            }
        }
        self.query_budget_limits
            .validate()
            .map_err(crate::QueryBudgetError::from)?;
        let persistent_wal_enabled = self.runtime_mode == StorageRuntimeMode::ReadWrite
            && self.data_path.is_some()
            && self.wal_enabled;
        if persistent_wal_enabled
            && self.memory_limit_bytes != usize::MAX
            && self.memory_limit_bytes < self.wal_buffer_size.max(1)
        {
            return Err(TsinkError::InvalidConfiguration(format!(
                "accounted-memory limit {} is smaller than the configured WAL writer-buffer capacity {}; increase the memory limit or lower the WAL buffer size",
                self.memory_limit_bytes,
                self.wal_buffer_size.max(1)
            )));
        }
        if let Some(local_disk_bytes) = self.local_disk_limit_bytes {
            if local_disk_bytes == 0 {
                return Err(TsinkError::InvalidConfiguration(
                    "local disk limit must be greater than zero".to_string(),
                ));
            }
            if self.maintenance_temp_reserve_bytes >= local_disk_bytes {
                return Err(TsinkError::InvalidConfiguration(
                    "maintenance temporary reserve must be smaller than local disk limit"
                        .to_string(),
                ));
            }
            if self.wal_enabled && self.wal_size_limit_bytes != usize::MAX {
                let wal_bytes = u64::try_from(self.wal_size_limit_bytes).unwrap_or(u64::MAX);
                if wal_bytes > local_disk_bytes.saturating_sub(self.maintenance_temp_reserve_bytes)
                {
                    return Err(TsinkError::InvalidConfiguration(
                        "WAL limit exceeds local disk growth capacity after maintenance reserve"
                            .to_string(),
                    ));
                }
            }
        }
        Ok(())
    }

    /// Creates a builder with the defaults described on [`StorageBuilder`].
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the directory used for local segments, metadata, and the WAL.
    ///
    /// Building opens or creates this directory, recovers compatible existing state, and acquires
    /// the built-in backend's process lock for the storage lifecycle.
    #[must_use]
    pub fn with_data_path(mut self, path: impl AsRef<Path>) -> Self {
        self.data_path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Sets the root path for object-storage-backed warm/cold segment tiers.
    ///
    /// The path should typically reference storage outside the local `data_path`
    /// volume, such as an object-store mount or remote-backed filesystem.
    #[must_use]
    pub fn with_object_store_path(mut self, path: impl AsRef<Path>) -> Self {
        self.object_store_path = Some(path.as_ref().to_path_buf());
        self
    }

    /// Sets the retention window and enables retention enforcement.
    ///
    /// Use [`StorageBuilder::with_retention_enforced`] afterwards to configure a window without
    /// enforcing it.
    #[must_use]
    pub fn with_retention(mut self, retention: Duration) -> Self {
        self.retention = retention;
        self.retention_enforced = true;
        self
    }

    /// Enables or disables retention enforcement.
    ///
    /// Builders default to allowing historical/backfill timestamps. When enforcement is
    /// disabled, points are never rejected or filtered due to retention.
    #[must_use]
    pub fn with_retention_enforced(mut self, enforced: bool) -> Self {
        self.retention_enforced = enforced;
        self
    }

    /// Rejects samples farther than `max_future_skew` ahead of the storage clock.
    ///
    /// This policy is opt in. By default, future timestamps remain accepted and are only tracked
    /// by the engine's future-skew observability and bounded-recency logic. The duration is
    /// converted using the configured [`TimestampPrecision`] when the storage is built.
    #[must_use]
    pub fn with_max_future_skew(mut self, max_future_skew: Duration) -> Self {
        self.max_future_skew = Some(max_future_skew);
        self
    }

    /// Configures hot, warm, and cold tier cutoffs within the global retention window.
    ///
    /// Calling this also enables retention enforcement.
    ///
    /// Data newer than `hot_retention` stays on local storage. Data older than
    /// `hot_retention` moves to the warm object-store tier. Data older than
    /// `warm_retention` moves to the cold object-store tier until global retention
    /// expires it entirely.
    #[must_use]
    pub fn with_tiered_retention_policy(
        mut self,
        hot_retention: Duration,
        warm_retention: Duration,
    ) -> Self {
        self.retention_enforced = true;
        self.hot_tier_retention = Some(hot_retention);
        self.warm_tier_retention = Some(warm_retention);
        self
    }

    /// Selects the read-write or compute-only runtime mode.
    #[must_use]
    pub fn with_runtime_mode(mut self, mode: StorageRuntimeMode) -> Self {
        self.runtime_mode = mode;
        self
    }

    /// Selects how segments discovered in remote storage are cached locally.
    #[must_use]
    pub fn with_remote_segment_cache_policy(mut self, policy: RemoteSegmentCachePolicy) -> Self {
        self.remote_segment_cache_policy = policy;
        self
    }

    /// Sets how often storage refreshes its remote segment catalog.
    ///
    /// Intervals below one millisecond are normalized to one millisecond.
    #[must_use]
    pub fn with_remote_segment_refresh_interval(mut self, interval: Duration) -> Self {
        self.remote_segment_refresh_interval = interval.max(Duration::from_millis(1));
        self
    }

    /// Controls whether newly persisted hot segments are mirrored to object storage.
    #[must_use]
    pub fn with_mirror_hot_segments_to_object_store(mut self, enabled: bool) -> Self {
        self.mirror_hot_segments_to_object_store = enabled;
        self
    }

    /// Sets the unit used for sample timestamps and duration-based engine settings.
    #[must_use]
    pub fn with_timestamp_precision(mut self, precision: TimestampPrecision) -> Self {
        self.timestamp_precision = precision;
        self
    }

    /// Sets the target number of points per encoded chunk.
    ///
    /// The value is clamped to the range `1..=u16::MAX`.
    #[must_use]
    pub fn with_chunk_points(mut self, points: usize) -> Self {
        self.chunk_points = points.clamp(1, u16::MAX as usize);
        self
    }

    /// Sets the maximum number of writes admitted concurrently.
    ///
    /// Passing zero selects the cgroup-aware worker default.
    #[must_use]
    pub fn with_max_writers(mut self, max_writers: usize) -> Self {
        self.resource_overrides
            .insert(ResourceLimitOverride::ConcurrentWriters);
        self.max_writers = if max_writers == 0 {
            crate::cgroup::default_workers_limit().max(1)
        } else {
            max_writers
        };
        self
    }

    /// Sets the per-acquisition wait for writer permits and close maintenance/compaction gates.
    #[must_use]
    pub fn with_write_timeout(mut self, timeout: Duration) -> Self {
        self.resource_overrides
            .insert(ResourceLimitOverride::WriteTimeout);
        self.write_timeout = timeout;
        self
    }

    /// Sets the width of the active time partitions used for ingestion.
    #[must_use]
    pub fn with_partition_duration(mut self, duration: Duration) -> Self {
        self.partition_duration = duration;
        self
    }

    /// Sets the maximum number of simultaneously open partition heads per series.
    ///
    /// The engine keeps a bounded set of open partition heads per series. When a write
    /// advances into a newer partition and the bound is full, the oldest open partition
    /// head is sealed to make room. When a write would open a new older partition after
    /// the bound is already full, the write is rejected instead of force-sealing another
    /// active head.
    #[must_use]
    pub fn with_max_active_partition_heads_per_series(mut self, max_heads: usize) -> Self {
        self.resource_overrides
            .insert(ResourceLimitOverride::PartitionHeads);
        self.max_active_partition_heads_per_series = max_heads.max(1);
        self
    }

    /// Sets the modeled storage-memory budget.
    ///
    /// The budget charges active and sealed chunks, registry and metadata state, persisted indexes
    /// and virtual mapping lengths, tombstones, and conservative transient reservations for
    /// foreground write preparation, startup WAL replay, and pre-live registry, segment-inventory,
    /// and persisted-index hydration. It excludes caller-owned input, query working sets, the
    /// finite WAL `BufWriter`, thread stacks, allocator overhead, and adapter state, so it is not a
    /// process-RSS cap. [`Storage::observability_snapshot`] exposes the exact current category
    /// inventory and pressure state; conservative startup hydration admission is released before
    /// the built instance becomes live.
    ///
    /// When a write would exceed the modeled budget, the engine applies backpressure by persisting
    /// sealed chunks to L0 and evicting the oldest sealed chunks before rejecting the write.
    /// The `Embedded` builder default is 512 MiB. `usize::MAX` remains an explicit low-level
    /// opt-out and is also selected by `ExpertUnlimited`.
    #[must_use]
    pub fn with_memory_limit(mut self, bytes: usize) -> Self {
        self.resource_overrides
            .insert(ResourceLimitOverride::AccountedMemory);
        self.memory_limit_bytes = bytes;
        self
    }

    /// Sets a hard upper bound for total series cardinality.
    ///
    /// New metric+label combinations are rejected once the limit is reached.
    /// The `Embedded` builder default is 1,000,000 series. `usize::MAX` remains an explicit
    /// low-level opt-out and is also selected by `ExpertUnlimited`.
    #[must_use]
    pub fn with_cardinality_limit(mut self, series: usize) -> Self {
        self.resource_overrides
            .insert(ResourceLimitOverride::Cardinality);
        self.cardinality_limit = series;
        self
    }

    /// Sets the maximum label count accepted in a submitted series identity.
    ///
    /// The default is [`crate::DEFAULT_MAX_LABELS_PER_SERIES`]. Values above
    /// [`crate::MAX_SUPPORTED_LABELS_PER_SERIES`] are rejected during build because the current
    /// WAL and segment formats cannot represent them.
    #[must_use]
    pub fn with_max_labels_per_series(mut self, labels: usize) -> Self {
        self.resource_overrides
            .insert(ResourceLimitOverride::MaxLabelsPerSeries);
        self.max_labels_per_series = labels;
        self
    }

    /// Sets the maximum cumulative UTF-8 bytes in a submitted series identity.
    ///
    /// The calculation includes the metric name and every label name and value. The default is
    /// [`crate::DEFAULT_MAX_SERIES_IDENTITY_BYTES`]. `usize::MAX` is an explicit expert opt-out.
    #[must_use]
    pub fn with_max_series_identity_bytes(mut self, bytes: usize) -> Self {
        self.resource_overrides
            .insert(ResourceLimitOverride::MaxSeriesIdentityBytes);
        self.max_series_identity_bytes = bytes;
        self
    }

    /// Limits successful creation of new series during a fixed storage-clock window.
    ///
    /// Admission includes concurrent in-flight writes. A failed write releases its reservation;
    /// only a published write counts as committed. The window is rounded up to one configured
    /// timestamp unit when it is finer than [`StorageBuilder::with_timestamp_precision`].
    #[must_use]
    pub fn with_series_creation_rate_limit(
        mut self,
        max_new_series: usize,
        window: Duration,
    ) -> Self {
        self.resource_overrides
            .insert(ResourceLimitOverride::SeriesCreationRate);
        self.max_new_series_per_window = Some(max_new_series);
        self.new_series_window = window;
        self
    }

    /// Configures pre-allocation row-count and modeled-input-byte admission for writes.
    ///
    /// Each `None` field preserves legacy unbounded behavior. Finite limits apply to atomic,
    /// best-effort, async, internal rollup, and WAL-replay ingestion paths.
    #[must_use]
    pub fn with_write_batch_limits(mut self, limits: WriteBatchLimits) -> Self {
        self.resource_overrides
            .insert(ResourceLimitOverride::WriteBatch);
        self.write_batch_limits = limits;
        self
    }

    /// Enables or disables the WAL for persistent read-write storage.
    ///
    /// The WAL is enabled by default, but is only opened when a data path is configured.
    #[must_use]
    pub fn with_wal_enabled(mut self, enabled: bool) -> Self {
        self.wal_enabled = enabled;
        self
    }

    /// Sets a hard upper bound for on-disk WAL bytes across all WAL segments.
    ///
    /// The `Embedded` builder default is 512 MiB. `usize::MAX` disables the limit and is selected
    /// by `ExpertUnlimited`.
    #[must_use]
    pub fn with_wal_size_limit(mut self, bytes: usize) -> Self {
        self.resource_overrides
            .insert(ResourceLimitOverride::WalBytes);
        self.wal_size_limit_bytes = bytes;
        self
    }

    /// Sets a hard upper bound for local data-directory growth admitted by tsink's coordinator.
    ///
    /// Existing directories above a newly configured limit may still be opened for recovery and
    /// deletion, but new growth is rejected. The bound is enforced only for persistent read-write
    /// storage and requires [`StorageBuilder::with_data_path`]. Object-store roots and snapshot
    /// destinations outside the data directory are not included.
    #[must_use]
    pub fn with_local_disk_limit(mut self, bytes: u64) -> Self {
        self.resource_overrides
            .insert(ResourceLimitOverride::LocalDisk);
        self.local_disk_limit_bytes = Some(bytes);
        self
    }

    /// Sets filesystem free space that tsink must leave available for the host.
    ///
    /// This physical-space floor is enforced in addition to the logical local-disk limit. Normal
    /// writes must also leave the maintenance temporary reserve available.
    #[must_use]
    pub fn with_filesystem_free_headroom(mut self, bytes: u64) -> Self {
        self.resource_overrides
            .insert(ResourceLimitOverride::FilesystemFreeHeadroom);
        self.filesystem_free_headroom_bytes = bytes;
        self
    }

    /// Reserves local-disk bytes for compaction and other maintenance temporary output.
    ///
    /// Foreground growth cannot consume this reserve. Maintenance may use it while still honoring
    /// the global local-disk limit and filesystem free-space headroom.
    #[must_use]
    pub fn with_maintenance_temp_reserve(mut self, bytes: u64) -> Self {
        self.resource_overrides
            .insert(ResourceLimitOverride::MaintenanceTempReserve);
        self.maintenance_temp_reserve_bytes = bytes;
        self
    }

    /// Sets the maximum logical items selected by one background maintenance pass.
    #[must_use]
    pub fn with_maintenance_max_items_per_pass(mut self, max_items: usize) -> Self {
        self.resource_overrides
            .insert(ResourceLimitOverride::MaintenanceWork);
        self.maintenance_max_items_per_pass = max_items;
        self
    }

    /// Sets the maximum modeled input bytes selected by one background maintenance pass.
    #[must_use]
    pub fn with_maintenance_max_bytes_per_pass(mut self, max_bytes: u64) -> Self {
        self.resource_overrides
            .insert(ResourceLimitOverride::MaintenanceWork);
        self.maintenance_max_bytes_per_pass = max_bytes;
        self
    }

    /// Installs a pre-created local-disk coordinator for this core data path.
    ///
    /// This lets an adapter create and retain the coordinator used by core storage. It does not
    /// automatically govern independent adapter files or filesystem writes that bypass the
    /// coordinator. The coordinator root must exactly match the configured data path. Its limits
    /// become the builder's effective disk settings.
    #[must_use]
    pub fn with_shared_local_disk_budget(
        mut self,
        budget: std::sync::Arc<crate::LocalDiskBudget>,
    ) -> Self {
        self.resource_overrides
            .insert(ResourceLimitOverride::LocalDisk);
        self.resource_overrides
            .insert(ResourceLimitOverride::FilesystemFreeHeadroom);
        self.resource_overrides
            .insert(ResourceLimitOverride::MaintenanceTempReserve);
        let limits = budget.limits();
        self.local_disk_limit_bytes = limits.max_bytes;
        self.filesystem_free_headroom_bytes = limits.filesystem_free_headroom_bytes;
        self.maintenance_temp_reserve_bytes = limits.maintenance_temp_reserve_bytes;
        self.shared_local_disk_budget = Some(budget);
        self
    }

    /// Sets the byte capacity of the userspace WAL writer buffer.
    ///
    /// A zero value is normalized to a one-byte buffer when the WAL is opened.
    #[must_use]
    pub fn with_wal_buffer_size(mut self, size: usize) -> Self {
        self.resource_overrides
            .insert(ResourceLimitOverride::WalWriteBuffer);
        self.wal_buffer_size = size;
        self
    }

    /// Selects when successful WAL appends are synchronized to durable storage.
    #[must_use]
    pub fn with_wal_sync_mode(mut self, mode: WalSyncMode) -> Self {
        self.wal_sync_mode = mode;
        self
    }

    /// Sets WAL replay policy when corruption is encountered mid-log.
    ///
    /// Builders default to [`WalReplayMode::Strict`] so durable startup never silently drops
    /// corrupted WAL history unless salvage is opted into explicitly.
    #[must_use]
    pub fn with_wal_replay_mode(mut self, mode: WalReplayMode) -> Self {
        self.wal_replay_mode = mode;
        self
    }

    /// Controls whether background durability worker failures fence service.
    ///
    /// Builders default to `true` so flush, compaction, and persisted-refresh failures stop
    /// admitting new work unless callers opt out explicitly.
    #[must_use]
    pub fn with_background_fail_fast(mut self, enabled: bool) -> Self {
        self.background_fail_fast = enabled;
        self
    }

    /// Enables the metadata shard index used by bounded shard-scoped discovery APIs.
    ///
    /// A shard count of zero disables the index.
    #[must_use]
    pub fn with_metadata_shard_count(mut self, shard_count: u32) -> Self {
        self.metadata_shard_count = Some(shard_count);
        self
    }

    /// Configures shared query concurrency/memory admission and per-query work limits.
    ///
    /// Every `None` field remains unbounded for backward compatibility. Finite values are
    /// validated when [`StorageBuilder::build`] constructs the storage instance.
    #[must_use]
    pub fn with_query_budget_limits(mut self, limits: QueryBudgetLimits) -> Self {
        self.resource_overrides
            .insert(ResourceLimitOverride::QueryBudget);
        self.query_budget_limits = limits;
        self
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn with_current_time_override_for_tests(mut self, timestamp: i64) -> Self {
        self.current_time_override = Some(timestamp);
        self
    }

    #[cfg(test)]
    #[must_use]
    pub(crate) fn with_background_threads_enabled_for_tests(mut self, enabled: bool) -> Self {
        self.background_threads_enabled_override = Some(enabled);
        self
    }

    /// Internal override used by snapshot restore's production-open validation copy.
    ///
    /// This is intentionally crate-private: embedders should not create a storage lifecycle
    /// without its configured workers, while restore needs a deterministic strict open that owns
    /// no concurrent mutator before exact validation-copy cleanup.
    #[must_use]
    pub(crate) fn with_background_threads_enabled_for_validation(mut self, enabled: bool) -> Self {
        self.background_threads_enabled_override = Some(enabled);
        self
    }

    /// Opens the configured storage and starts any background workers it owns.
    ///
    /// The returned [`Arc`] may be shared between host threads. Call [`Storage::close`] once the
    /// host has stopped submitting work and before discarding its storage handles.
    pub fn build(self) -> Result<Arc<dyn Storage>> {
        self.validate_before_build()?;
        crate::engine::build_storage(self)
    }

    pub(crate) fn build_for_snapshot_validation(
        self,
    ) -> Result<crate::engine::engine::SnapshotValidationStorage> {
        self.validate_before_build()?;
        crate::engine::build_storage_for_snapshot_validation(self)
    }

    fn validate_before_build(&self) -> Result<()> {
        self.validate_resource_configuration()?;
        if self.max_labels_per_series > crate::label::MAX_SUPPORTED_LABELS_PER_SERIES {
            return Err(TsinkError::InvalidConfiguration(format!(
                "max_labels_per_series {} exceeds the storage-format limit {}",
                self.max_labels_per_series,
                crate::label::MAX_SUPPORTED_LABELS_PER_SERIES
            )));
        }
        if self.max_new_series_per_window.is_some() && self.new_series_window.is_zero() {
            return Err(TsinkError::InvalidConfiguration(
                "new-series creation-rate window must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }

    /// Restores a snapshot directory into `data_path`.
    ///
    /// Perform restoration before opening storage at the target path. If `data_path` already
    /// exists, a successful restore replaces it; activation uses staging and attempts rollback if
    /// the replacement fails. Both the snapshot and an existing replacement target must fit
    /// [`MAX_SNAPSHOT_RESTORE_ENTRIES`] entries and [`MAX_SNAPSHOT_RESTORE_DEPTH`] depth. Staging
    /// creation, backup publication, and target activation use create/no-replace semantics, so a
    /// path installed during activation is preserved rather than overwritten. Owned staging and
    /// backup cleanup is filesystem-identity checked, exact, bounded, and no-follow; an identity
    /// capture failure retains the reported staging path instead of deleting by pathname alone.
    ///
    /// Before target capture or publication, restore copies the snapshot to a private validation
    /// sibling and opens it through strict production discovery, registry recovery, segment and
    /// catalog validation, tombstone hydration, WAL replay, and rollup loading. Validation uses the
    /// finite [`ResourceProfile::Server`] memory/cardinality/WAL/disk limits, requires
    /// non-degraded health, disables workers, and exits without normal close/flush persistence.
    ///
    /// The snapshot and its containing namespace must remain offline and immutable for the
    /// duration of the call. Traversal is anchored to retained no-follow directory handles and
    /// closed entry identities; the offline contract excludes a hostile same-identity actor
    /// racing the platform's final namespace operations.
    pub fn restore_from_snapshot(
        snapshot_path: impl AsRef<Path>,
        data_path: impl AsRef<Path>,
    ) -> Result<()> {
        crate::engine::restore_storage_from_snapshot(snapshot_path.as_ref(), data_path.as_ref())
    }

    /// Restores an external snapshot under a caller-owned local disk budget.
    ///
    /// The destination must be a strict descendant of `disk_budget.root()`, while the snapshot
    /// must not overlap that root. Restoration rejects link-like entries through handle-anchored
    /// traversal and measures logical file bytes plus a finite entry count before mutation. It
    /// accepts at most
    /// [`MAX_SNAPSHOT_RESTORE_ENTRIES`] entries and depth
    /// [`MAX_SNAPSHOT_RESTORE_DEPTH`]. Its staging term reserves:
    /// `2 * logical_file_bytes + (snapshot_entry_count + 2) *
    /// max(policy_floor, filesystem_allocation_unit)`, where the two extra entries cover the
    /// validation lock and one possible atomic recovery scratch path. The coordinator separately
    /// adds `missing_target_parent_directories * entry_allowance`. The floor is
    /// [`SNAPSHOT_RESTORE_ENTRY_STAGING_ALLOWANCE_FLOOR_BYTES`]. This admission is deliberately
    /// conservative and is not presented as an exact physical filesystem footprint. Activation is
    /// serialized with other managed budget mutations and uses create/no-replace staging, backup,
    /// and target operations. An existing replacement target is also preflighted against the same
    /// entry/depth envelope so backup cleanup remains finite. Owned staging and backup cleanup
    /// verifies its captured filesystem identity and builds an exact no-follow plan before
    /// deletion. A successful post-operation scan installs exact logical accounting before
    /// release; if that scan fails, the API returns an explicit committed-but-accounting error and
    /// conservatively charges the full reservation.
    ///
    /// Perform restoration before opening storage at the target path. The supplied budget is an
    /// offline restore envelope rooted above the target; it must not concurrently coordinate an
    /// open storage instance. After restore, open storage with its normal data-path budget rooted
    /// at the restored target. Before target capture or publication, a private copy must pass the
    /// same strict production-open validation described by
    /// [`StorageBuilder::restore_from_snapshot`]. The snapshot and its containing namespace must
    /// remain offline and immutable throughout the call.
    pub fn restore_from_snapshot_with_disk_budget(
        snapshot_path: impl AsRef<Path>,
        data_path: impl AsRef<Path>,
        disk_budget: Arc<crate::LocalDiskBudget>,
    ) -> Result<()> {
        crate::engine::restore_storage_from_snapshot_with_disk_budget(
            snapshot_path.as_ref(),
            data_path.as_ref(),
            disk_budget,
        )
    }

    pub(crate) fn chunk_points(&self) -> usize {
        self.chunk_points
    }

    pub(crate) fn data_path(&self) -> Option<&Path> {
        self.data_path.as_deref()
    }

    pub(crate) fn retention(&self) -> Duration {
        self.retention
    }

    pub(crate) fn retention_enforced(&self) -> bool {
        self.retention_enforced
    }

    pub(crate) fn max_future_skew(&self) -> Option<Duration> {
        self.max_future_skew
    }

    pub(crate) fn object_store_path(&self) -> Option<&Path> {
        self.object_store_path.as_deref()
    }

    pub(crate) fn hot_tier_retention(&self) -> Option<Duration> {
        self.hot_tier_retention
    }

    pub(crate) fn warm_tier_retention(&self) -> Option<Duration> {
        self.warm_tier_retention
    }

    pub(crate) fn timestamp_precision(&self) -> TimestampPrecision {
        self.timestamp_precision
    }

    pub(crate) fn runtime_mode(&self) -> StorageRuntimeMode {
        self.runtime_mode
    }

    pub(crate) fn remote_segment_cache_policy(&self) -> RemoteSegmentCachePolicy {
        self.remote_segment_cache_policy
    }

    pub(crate) fn remote_segment_refresh_interval(&self) -> Duration {
        self.remote_segment_refresh_interval
    }

    pub(crate) fn mirror_hot_segments_to_object_store(&self) -> bool {
        self.mirror_hot_segments_to_object_store
    }

    pub(crate) fn max_writers(&self) -> usize {
        self.max_writers
    }

    pub(crate) fn write_timeout(&self) -> Duration {
        self.write_timeout
    }

    pub(crate) fn partition_duration(&self) -> Duration {
        self.partition_duration
    }

    pub(crate) fn memory_limit_bytes(&self) -> usize {
        self.memory_limit_bytes
    }

    pub(crate) fn max_active_partition_heads_per_series(&self) -> usize {
        self.max_active_partition_heads_per_series.max(1)
    }

    pub(crate) fn cardinality_limit(&self) -> usize {
        self.cardinality_limit
    }

    pub(crate) fn max_labels_per_series(&self) -> usize {
        self.max_labels_per_series
    }

    pub(crate) fn max_series_identity_bytes(&self) -> usize {
        self.max_series_identity_bytes
    }

    pub(crate) fn max_new_series_per_window(&self) -> Option<usize> {
        self.max_new_series_per_window
    }

    pub(crate) fn new_series_window(&self) -> Duration {
        self.new_series_window
    }

    pub(crate) fn write_batch_limits(&self) -> WriteBatchLimits {
        self.write_batch_limits
    }

    pub(crate) fn wal_enabled(&self) -> bool {
        self.wal_enabled
    }

    pub(crate) fn wal_size_limit_bytes(&self) -> usize {
        self.wal_size_limit_bytes
    }

    pub(crate) fn local_disk_limits(&self) -> crate::LocalDiskLimits {
        crate::LocalDiskLimits {
            max_bytes: self.local_disk_limit_bytes,
            filesystem_free_headroom_bytes: self.filesystem_free_headroom_bytes,
            maintenance_temp_reserve_bytes: self.maintenance_temp_reserve_bytes,
        }
    }

    pub(crate) fn has_explicit_local_disk_settings(&self) -> bool {
        self.has_resource_override(ResourceLimitOverride::LocalDisk)
            || self.has_resource_override(ResourceLimitOverride::FilesystemFreeHeadroom)
            || self.has_resource_override(ResourceLimitOverride::MaintenanceTempReserve)
            || self.shared_local_disk_budget.is_some()
    }

    pub(crate) fn shared_local_disk_budget(
        &self,
    ) -> Option<&std::sync::Arc<crate::LocalDiskBudget>> {
        self.shared_local_disk_budget.as_ref()
    }

    pub(crate) fn maintenance_max_items_per_pass(&self) -> usize {
        self.maintenance_max_items_per_pass
    }

    pub(crate) fn maintenance_max_bytes_per_pass(&self) -> u64 {
        self.maintenance_max_bytes_per_pass
    }

    pub(crate) fn wal_buffer_size(&self) -> usize {
        self.wal_buffer_size
    }

    pub(crate) fn wal_sync_mode(&self) -> WalSyncMode {
        self.wal_sync_mode
    }

    pub(crate) fn wal_replay_mode(&self) -> WalReplayMode {
        self.wal_replay_mode
    }

    pub(crate) fn background_fail_fast(&self) -> bool {
        self.background_fail_fast
    }

    pub(crate) fn metadata_shard_count(&self) -> Option<u32> {
        self.metadata_shard_count
    }

    pub(crate) fn query_budget_limits(&self) -> QueryBudgetLimits {
        self.query_budget_limits
    }

    pub(crate) fn background_threads_enabled_override(&self) -> Option<bool> {
        self.background_threads_enabled_override
    }

    #[cfg(test)]
    pub(crate) fn current_time_override_for_tests(&self) -> Option<i64> {
        self.current_time_override
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(crate) fn metric_series_matches_shard_scope(
    series: &MetricSeries,
    scope: &MetadataShardScope,
) -> bool {
    if scope.shard_count == 0 {
        return false;
    }

    let shard = (crate::label::stable_series_identity_hash(series.name.as_str(), &series.labels)
        % u64::from(scope.shard_count)) as u32;
    scope.shards.contains(&shard)
}

pub(crate) fn validate_shard_window_request(
    shard: u32,
    shard_count: u32,
    window_start: i64,
    window_end: i64,
) -> Result<()> {
    if shard_count == 0 {
        return Err(TsinkError::InvalidConfiguration(
            "shard_count must be greater than zero".to_string(),
        ));
    }
    if shard >= shard_count {
        return Err(TsinkError::InvalidConfiguration(format!(
            "shard {shard} is out of range for shard_count {shard_count}"
        )));
    }
    if window_start >= window_end {
        return Err(TsinkError::InvalidTimeRange {
            start: window_start,
            end: window_end,
        });
    }
    Ok(())
}

pub(crate) fn validate_shard_window_scan_options(options: ShardWindowScanOptions) -> Result<()> {
    if options.max_series.is_some_and(|value| value == 0) {
        return Err(TsinkError::InvalidConfiguration(
            "max_series must be greater than zero when set".to_string(),
        ));
    }
    if options.max_rows.is_some_and(|value| value == 0) {
        return Err(TsinkError::InvalidConfiguration(
            "max_rows must be greater than zero when set".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn validate_query_rows_scan_options(options: QueryRowsScanOptions) -> Result<()> {
    if options.max_rows.is_some_and(|value| value == 0) {
        return Err(TsinkError::InvalidConfiguration(
            "max_rows must be greater than zero when set".to_string(),
        ));
    }
    Ok(())
}

pub(crate) fn shard_window_series_identity_key(metric: &str, labels: &[Label]) -> String {
    crate::label::canonical_series_identity_key(metric, labels)
}

pub(crate) const SHARD_WINDOW_FNV_OFFSET_BASIS: u64 = 0xcbf29ce484222325;
const SHARD_WINDOW_FNV_PRIME: u64 = 0x100000001b3;

struct ShardWindowFnvWriter<'a> {
    hash: &'a mut u64,
}

impl std::io::Write for ShardWindowFnvWriter<'_> {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        shard_window_fnv1a_update(self.hash, buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

pub(crate) fn shard_window_hash_data_point(point: &DataPoint) -> Result<u64> {
    let mut hash = SHARD_WINDOW_FNV_OFFSET_BASIS;
    shard_window_fnv1a_update(&mut hash, &point.timestamp.to_le_bytes());
    serde_json::to_writer(ShardWindowFnvWriter { hash: &mut hash }, &point.value)?;
    Ok(hash)
}

pub(crate) fn shard_window_fnv1a_update(hash: &mut u64, bytes: &[u8]) {
    for byte in bytes {
        *hash ^= u64::from(*byte);
        *hash = hash.wrapping_mul(SHARD_WINDOW_FNV_PRIME);
    }
}

pub(crate) fn sort_data_points_for_shard_window(points: &mut [DataPoint]) {
    points.sort_by_cached_key(|point| {
        (
            point.timestamp,
            serde_json::to_vec(&point.value).unwrap_or_default(),
        )
    });
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Aggregation {
    #[default]
    None,
    Sum,
    Min,
    Max,
    Avg,
    First,
    Last,
    Count,
    Median,
    Range,
    Variance,
    StdDev,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DownsampleOptions {
    pub interval: i64,
}

#[derive(Clone)]
pub struct QueryOptions {
    pub labels: Vec<Label>,
    pub start: i64,
    pub end: i64,
    pub aggregation: Aggregation,
    pub downsample: Option<DownsampleOptions>,
    pub custom_aggregation: Option<Arc<dyn BytesAggregation>>,
    pub limit: Option<usize>,
    pub offset: usize,
}

impl QueryOptions {
    #[must_use]
    pub fn new(start: i64, end: i64) -> Self {
        Self {
            labels: Vec::new(),
            start,
            end,
            aggregation: Aggregation::None,
            downsample: None,
            custom_aggregation: None,
            limit: None,
            offset: 0,
        }
    }

    #[must_use]
    pub fn with_labels(mut self, labels: Vec<Label>) -> Self {
        self.labels = labels;
        self
    }

    #[must_use]
    pub fn with_pagination(mut self, offset: usize, limit: Option<usize>) -> Self {
        self.offset = offset;
        self.limit = limit;
        self
    }

    /// Apply downsampling using the given interval and aggregation.
    #[must_use]
    pub fn with_downsample(mut self, interval: i64, aggregation: Aggregation) -> Self {
        self.downsample = Some(DownsampleOptions { interval });
        self.aggregation = aggregation;
        self
    }

    /// Apply aggregation without downsampling (reduces the whole series to one point).
    #[must_use]
    pub fn with_aggregation(mut self, aggregation: Aggregation) -> Self {
        self.aggregation = aggregation;
        self
    }

    /// Apply a custom bytes aggregation by providing a codec and typed aggregator.
    #[must_use]
    pub fn with_custom_bytes_aggregation<C, A>(mut self, codec: C, aggregator: A) -> Self
    where
        C: Codec + 'static,
        A: TypedAggregator<C::Item> + 'static,
    {
        self.custom_aggregation = Some(Arc::new(CodecAggregator::new(codec, aggregator)));
        self
    }
}
