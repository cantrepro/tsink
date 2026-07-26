//! Runtime-independent query admission, cancellation, and accounting primitives.
//!
//! This module provides the shared mechanics needed to enforce query limits in the embedded
//! engine and in adapters without depending on Tokio or another async runtime. It does not, by
//! itself, make an engine query bounded: query entrypoints and scan/evaluation loops must carry a
//! [`QueryExecution`] and charge the work they perform.
//!
//! Every field in [`QueryBudgetLimits`] and [`QueryWorkLimits`] is optional. `None` means that this
//! layer does not enforce that particular limit. In particular, the all-`None` default is the
//! explicit `ExpertUnlimited` legacy configuration; standard resource profiles populate finite
//! values.

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use thiserror::Error;

/// Per-query work limits enforced by a [`QueryExecution`].
///
/// A request-specific value can tighten an instance value with [`Self::tightened_by`]. An
/// all-`None` value adds no request-specific restriction.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct QueryWorkLimits {
    /// Maximum distinct series admitted as matches during one query.
    pub max_series_matched: Option<u64>,
    /// Maximum raw samples visited or decoded during one query.
    pub max_samples_scanned: Option<u64>,
    /// Maximum samples returned by one query.
    pub max_samples_returned: Option<u64>,
    /// Maximum encoded or canonically modeled logical result bytes returned by one query.
    ///
    /// Modeled in-process results use content lengths, not allocator capacity or slack, so
    /// logically equal values receive the same charge. Protocol adapters may additionally charge
    /// the exact bytes they encode.
    pub max_returned_bytes: Option<u64>,
    /// Maximum regex/pattern candidate expansion performed by one query.
    pub max_pattern_expansion: Option<u64>,
    /// Maximum range or subquery evaluation steps performed by one query.
    pub max_steps: Option<u64>,
    /// Maximum simultaneously materialized intermediate vector length.
    pub max_intermediate_vector_size: Option<u64>,
    /// Maximum modeled bytes reserved for tsink-owned intermediate memory by one query.
    ///
    /// The portable model charges observed collection capacities and value payloads plus a named
    /// per-allocation allowance; it does not claim to measure private global-allocator metadata.
    /// It covers storage-engine decode buffers, snapshots, candidate sets, and built-in
    /// aggregation working sets. Memory allocated internally by caller-provided
    /// [`crate::Aggregator`] or [`crate::CodecAggregator`] implementations is outside tsink's
    /// allocator control and is therefore excluded from this limit; their input and returned
    /// tsink values remain accounted.
    pub max_memory_bytes: Option<u64>,
    /// Maximum wall-clock duration of one query.
    pub max_wall_time: Option<Duration>,
}

impl QueryWorkLimits {
    /// Validates that every configured finite limit is non-zero.
    pub fn validate(self) -> Result<Self, QueryBudgetConfigError> {
        validate_positive_u64("max_series_matched", self.max_series_matched)?;
        validate_positive_u64("max_samples_scanned", self.max_samples_scanned)?;
        validate_positive_u64("max_samples_returned", self.max_samples_returned)?;
        validate_positive_u64("max_returned_bytes", self.max_returned_bytes)?;
        validate_positive_u64("max_pattern_expansion", self.max_pattern_expansion)?;
        validate_positive_u64("max_steps", self.max_steps)?;
        validate_positive_u64(
            "max_intermediate_vector_size",
            self.max_intermediate_vector_size,
        )?;
        validate_positive_u64("max_memory_bytes", self.max_memory_bytes)?;
        if self.max_wall_time.is_some_and(|value| value.is_zero()) {
            return Err(QueryBudgetConfigError::ZeroLimit {
                field: "max_wall_time",
            });
        }
        Ok(self)
    }

    /// Returns the fieldwise tighter combination of instance and request limits.
    ///
    /// A request cannot loosen an existing finite limit: two finite values use their minimum,
    /// while a finite value always wins over `None`.
    #[must_use]
    pub fn tightened_by(self, request: Self) -> Self {
        Self {
            max_series_matched: min_optional(self.max_series_matched, request.max_series_matched),
            max_samples_scanned: min_optional(
                self.max_samples_scanned,
                request.max_samples_scanned,
            ),
            max_samples_returned: min_optional(
                self.max_samples_returned,
                request.max_samples_returned,
            ),
            max_returned_bytes: min_optional(self.max_returned_bytes, request.max_returned_bytes),
            max_pattern_expansion: min_optional(
                self.max_pattern_expansion,
                request.max_pattern_expansion,
            ),
            max_steps: min_optional(self.max_steps, request.max_steps),
            max_intermediate_vector_size: min_optional(
                self.max_intermediate_vector_size,
                request.max_intermediate_vector_size,
            ),
            max_memory_bytes: min_optional(self.max_memory_bytes, request.max_memory_bytes),
            max_wall_time: min_optional(self.max_wall_time, request.max_wall_time),
        }
    }
}

/// Shared and per-query limits owned by one storage instance.
///
/// The default has no enforced limits. Named finite resource profiles are intentionally outside
/// this module and must not be inferred from this default.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct QueryBudgetLimits {
    /// Maximum queries that may hold a slot concurrently.
    pub max_concurrent_queries: Option<u64>,
    /// Maximum query-intermediate bytes reserved across all live queries.
    pub max_shared_memory_bytes: Option<u64>,
    /// Base limits inherited by each admitted query.
    pub per_query: QueryWorkLimits,
}

impl QueryBudgetLimits {
    /// Validates finite values and cross-field memory relationships.
    pub fn validate(self) -> Result<Self, QueryBudgetConfigError> {
        validate_positive_u64("max_concurrent_queries", self.max_concurrent_queries)?;
        validate_positive_u64("max_shared_memory_bytes", self.max_shared_memory_bytes)?;
        self.per_query.validate()?;
        if let (Some(per_query), Some(shared)) = (
            self.per_query.max_memory_bytes,
            self.max_shared_memory_bytes,
        ) {
            if per_query > shared {
                return Err(QueryBudgetConfigError::PerQueryMemoryExceedsShared {
                    per_query,
                    shared,
                });
            }
        }
        Ok(self)
    }
}

fn validate_positive_u64(
    field: &'static str,
    value: Option<u64>,
) -> Result<(), QueryBudgetConfigError> {
    if value == Some(0) {
        return Err(QueryBudgetConfigError::ZeroLimit { field });
    }
    Ok(())
}

fn min_optional<T: Ord>(left: Option<T>, right: Option<T>) -> Option<T> {
    match (left, right) {
        (Some(left), Some(right)) => Some(left.min(right)),
        (Some(value), None) | (None, Some(value)) => Some(value),
        (None, None) => None,
    }
}

/// Invalid query-budget configuration.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum QueryBudgetConfigError {
    /// A configured finite limit was zero.
    #[error("query limit '{field}' must be greater than zero when configured")]
    ZeroLimit { field: &'static str },
    /// One query was allowed to reserve more than the shared query-memory budget.
    #[error(
        "per-query memory limit {per_query} bytes exceeds shared query-memory limit {shared} bytes"
    )]
    PerQueryMemoryExceedsShared { per_query: u64, shared: u64 },
}

/// Stable machine-readable reason for a query resource rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
#[non_exhaustive]
pub enum QueryLimitReason {
    ConcurrentQueries,
    SharedMemoryBytes,
    PerQueryMemoryBytes,
    SeriesMatched,
    SamplesScanned,
    SamplesReturned,
    ReturnedBytes,
    PatternExpansion,
    Steps,
    IntermediateVectorSize,
}

impl QueryLimitReason {
    /// Stable snake-case reason used by adapters and metrics labels.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ConcurrentQueries => "concurrent_queries",
            Self::SharedMemoryBytes => "shared_memory_bytes",
            Self::PerQueryMemoryBytes => "per_query_memory_bytes",
            Self::SeriesMatched => "series_matched",
            Self::SamplesScanned => "samples_scanned",
            Self::SamplesReturned => "samples_returned",
            Self::ReturnedBytes => "returned_bytes",
            Self::PatternExpansion => "pattern_expansion",
            Self::Steps => "steps",
            Self::IntermediateVectorSize => "intermediate_vector_size",
        }
    }
}

impl fmt::Display for QueryLimitReason {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// Structured details for one query limit rejection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Error)]
#[error("query limit '{reason}' exceeded: limit {limit}, current {current}, requested {requested}")]
pub struct QueryLimitExceeded {
    pub reason: QueryLimitReason,
    pub limit: u64,
    pub current: u64,
    pub requested: u64,
}

impl QueryLimitExceeded {
    #[must_use]
    pub const fn new(reason: QueryLimitReason, limit: u64, current: u64, requested: u64) -> Self {
        Self {
            reason,
            limit,
            current,
            requested,
        }
    }
}

/// Error returned by query admission or accounting checkpoints.
#[derive(Debug, Clone, PartialEq, Eq, Error)]
#[non_exhaustive]
pub enum QueryBudgetError {
    /// Request-specific limit overrides were invalid.
    #[error(transparent)]
    InvalidLimits(#[from] QueryBudgetConfigError),
    /// A finite work, concurrency, or memory limit was exceeded.
    #[error(transparent)]
    LimitExceeded(#[from] QueryLimitExceeded),
    /// Cooperative cancellation was requested.
    #[error("query cancelled")]
    Cancelled,
    /// The query's earliest caller or budget deadline elapsed.
    #[error("query deadline exceeded")]
    DeadlineExceeded,
}

impl QueryBudgetError {
    /// Stable reason code suitable for adapter mapping.
    #[must_use]
    pub const fn reason_code(&self) -> &'static str {
        match self {
            Self::InvalidLimits(_) => "invalid_query_limits",
            Self::LimitExceeded(exceeded) => exceeded.reason.as_str(),
            Self::Cancelled => "cancelled",
            Self::DeadlineExceeded => "deadline_exceeded",
        }
    }
}

/// Cloneable synchronous cancellation and deadline token.
///
/// Cancellation is shared across clones. A deadline is local to the derived token, which lets the
/// core tighten a caller token with an instance wall-time limit without mutating the caller's
/// other clones. [`Self::with_deadline`] always retains the earlier deadline.
#[derive(Clone)]
pub struct QueryCancellationToken {
    cancelled: Arc<AtomicBool>,
    deadline: Option<Instant>,
}

impl Default for QueryCancellationToken {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for QueryCancellationToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryCancellationToken")
            .field("cancelled", &self.is_cancelled())
            .field("deadline", &self.deadline)
            .finish()
    }
}

impl QueryCancellationToken {
    /// Creates a token without cancellation or a deadline.
    #[must_use]
    pub fn new() -> Self {
        Self {
            cancelled: Arc::new(AtomicBool::new(false)),
            deadline: None,
        }
    }

    /// Returns a derived token with the earlier of its current deadline and `deadline`.
    #[must_use]
    pub fn with_deadline(mut self, deadline: Instant) -> Self {
        self.deadline = min_optional(self.deadline, Some(deadline));
        self
    }

    /// Returns a derived token with a deadline measured from now.
    ///
    /// If the platform cannot represent `now + timeout`, the returned token is immediately
    /// expired instead of silently losing the requested bound.
    #[must_use]
    pub fn with_timeout(self, timeout: Duration) -> Self {
        let now = Instant::now();
        self.with_deadline(now.checked_add(timeout).unwrap_or(now))
    }

    /// Requests cooperative cancellation for this token and all clones.
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::Release);
    }

    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    #[must_use]
    pub fn deadline(&self) -> Option<Instant> {
        self.deadline
    }

    /// Checks cancellation and deadline state synchronously.
    ///
    /// Cancellation wins deterministically when both conditions are true.
    pub fn checkpoint(&self) -> Result<(), QueryBudgetError> {
        if self.is_cancelled() {
            return Err(QueryBudgetError::Cancelled);
        }
        if self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline)
        {
            return Err(QueryBudgetError::DeadlineExceeded);
        }
        Ok(())
    }
}

/// Internally consistent query-budget observability snapshot.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryBudgetSnapshot {
    pub limits: QueryBudgetLimits,
    pub active_queries: u64,
    pub peak_active_queries: u64,
    pub shared_reserved_memory_bytes: u64,
    pub peak_shared_reserved_memory_bytes: u64,
    pub queries_started_total: u64,
    pub queries_completed_total: u64,
    /// Limit errors returned to callers. A query that retries and hits another limit increments
    /// this counter again.
    pub limit_rejections_total: u64,
    pub concurrency_rejections_total: u64,
    pub shared_memory_rejections_total: u64,
    pub per_query_memory_rejections_total: u64,
    #[serde(default)]
    pub series_matched_rejections_total: u64,
    #[serde(default)]
    pub samples_scanned_rejections_total: u64,
    #[serde(default)]
    pub samples_returned_rejections_total: u64,
    #[serde(default)]
    pub returned_bytes_rejections_total: u64,
    #[serde(default)]
    pub pattern_expansion_rejections_total: u64,
    #[serde(default)]
    pub steps_rejections_total: u64,
    #[serde(default)]
    pub intermediate_vector_size_rejections_total: u64,
    pub cancellations_total: u64,
    pub deadline_exceeded_total: u64,
    /// Internal release inconsistencies detected without allowing a gauge to wrap.
    pub accounting_invariant_violations_total: u64,
}

#[derive(Debug, Default)]
struct QueryBudgetState {
    active_queries: u64,
    peak_active_queries: u64,
    shared_reserved_memory_bytes: u64,
    peak_shared_reserved_memory_bytes: u64,
    queries_started_total: u64,
    queries_completed_total: u64,
    limit_rejections_total: u64,
    concurrency_rejections_total: u64,
    shared_memory_rejections_total: u64,
    per_query_memory_rejections_total: u64,
    series_matched_rejections_total: u64,
    samples_scanned_rejections_total: u64,
    samples_returned_rejections_total: u64,
    returned_bytes_rejections_total: u64,
    pattern_expansion_rejections_total: u64,
    steps_rejections_total: u64,
    intermediate_vector_size_rejections_total: u64,
    cancellations_total: u64,
    deadline_exceeded_total: u64,
    accounting_invariant_violations_total: u64,
}

impl QueryBudgetState {
    fn increment(value: &mut u64) {
        *value = value.saturating_add(1);
    }

    fn record_limit(&mut self, reason: QueryLimitReason) {
        Self::increment(&mut self.limit_rejections_total);
        match reason {
            QueryLimitReason::ConcurrentQueries => {
                Self::increment(&mut self.concurrency_rejections_total);
            }
            QueryLimitReason::SharedMemoryBytes => {
                Self::increment(&mut self.shared_memory_rejections_total);
            }
            QueryLimitReason::PerQueryMemoryBytes => {
                Self::increment(&mut self.per_query_memory_rejections_total);
            }
            QueryLimitReason::SeriesMatched => {
                Self::increment(&mut self.series_matched_rejections_total);
            }
            QueryLimitReason::SamplesScanned => {
                Self::increment(&mut self.samples_scanned_rejections_total);
            }
            QueryLimitReason::SamplesReturned => {
                Self::increment(&mut self.samples_returned_rejections_total);
            }
            QueryLimitReason::ReturnedBytes => {
                Self::increment(&mut self.returned_bytes_rejections_total);
            }
            QueryLimitReason::PatternExpansion => {
                Self::increment(&mut self.pattern_expansion_rejections_total);
            }
            QueryLimitReason::Steps => Self::increment(&mut self.steps_rejections_total),
            QueryLimitReason::IntermediateVectorSize => {
                Self::increment(&mut self.intermediate_vector_size_rejections_total);
            }
        }
    }

    fn snapshot(&self, limits: QueryBudgetLimits) -> QueryBudgetSnapshot {
        QueryBudgetSnapshot {
            limits,
            active_queries: self.active_queries,
            peak_active_queries: self.peak_active_queries,
            shared_reserved_memory_bytes: self.shared_reserved_memory_bytes,
            peak_shared_reserved_memory_bytes: self.peak_shared_reserved_memory_bytes,
            queries_started_total: self.queries_started_total,
            queries_completed_total: self.queries_completed_total,
            limit_rejections_total: self.limit_rejections_total,
            concurrency_rejections_total: self.concurrency_rejections_total,
            shared_memory_rejections_total: self.shared_memory_rejections_total,
            per_query_memory_rejections_total: self.per_query_memory_rejections_total,
            series_matched_rejections_total: self.series_matched_rejections_total,
            samples_scanned_rejections_total: self.samples_scanned_rejections_total,
            samples_returned_rejections_total: self.samples_returned_rejections_total,
            returned_bytes_rejections_total: self.returned_bytes_rejections_total,
            pattern_expansion_rejections_total: self.pattern_expansion_rejections_total,
            steps_rejections_total: self.steps_rejections_total,
            intermediate_vector_size_rejections_total: self
                .intermediate_vector_size_rejections_total,
            cancellations_total: self.cancellations_total,
            deadline_exceeded_total: self.deadline_exceeded_total,
            accounting_invariant_violations_total: self.accounting_invariant_violations_total,
        }
    }
}

#[derive(Debug)]
struct QueryBudgetInner {
    limits: QueryBudgetLimits,
    state: Mutex<QueryBudgetState>,
}

/// Shared query-slot and intermediate-memory budget for one storage instance.
#[derive(Clone, Debug)]
pub struct QueryBudget {
    inner: Arc<QueryBudgetInner>,
}

impl QueryBudget {
    /// Builds a budget after validating every configured finite limit.
    pub fn new(limits: QueryBudgetLimits) -> Result<Self, QueryBudgetConfigError> {
        let limits = limits.validate()?;
        Ok(Self {
            inner: Arc::new(QueryBudgetInner {
                limits,
                state: Mutex::new(QueryBudgetState::default()),
            }),
        })
    }

    /// Returns the limits owned by this budget.
    #[must_use]
    pub fn limits(&self) -> QueryBudgetLimits {
        self.inner.limits
    }

    /// Tries to admit a query with the instance's base work limits.
    pub fn begin_query(&self) -> Result<QueryExecution, QueryBudgetError> {
        self.begin_query_with(QueryWorkLimits::default(), QueryCancellationToken::new())
    }

    /// Tries to admit a query with a caller cancellation/deadline token.
    pub fn begin_query_with_token(
        &self,
        token: QueryCancellationToken,
    ) -> Result<QueryExecution, QueryBudgetError> {
        self.begin_query_with(QueryWorkLimits::default(), token)
    }

    /// Tries to admit a query with request-specific tightening and cooperative control.
    ///
    /// Admission is non-blocking. A full concurrency budget returns a structured rejection so an
    /// adapter may decide whether and how long to wait before retrying. `request_limits` can only
    /// tighten the instance limits.
    pub fn begin_query_with(
        &self,
        request_limits: QueryWorkLimits,
        token: QueryCancellationToken,
    ) -> Result<QueryExecution, QueryBudgetError> {
        request_limits.validate()?;
        let effective_limits = self.inner.limits.per_query.tightened_by(request_limits);
        if let (Some(per_query), Some(shared)) = (
            effective_limits.max_memory_bytes,
            self.inner.limits.max_shared_memory_bytes,
        ) {
            if per_query > shared {
                return Err(QueryBudgetConfigError::PerQueryMemoryExceedsShared {
                    per_query,
                    shared,
                }
                .into());
            }
        }
        let token = apply_wall_time_limit(token, effective_limits.max_wall_time);

        if let Err(error) = token.checkpoint() {
            self.record_pre_admission_control_error(&error);
            return Err(error);
        }

        let mut state = self.inner.state.lock();
        if let Err(error) = token.checkpoint() {
            record_control_error_in_state(&mut state, &error);
            return Err(error);
        }
        let current = state.active_queries;
        let next = current.checked_add(1).ok_or_else(|| {
            let exceeded =
                QueryLimitExceeded::new(QueryLimitReason::ConcurrentQueries, u64::MAX, current, 1);
            state.record_limit(exceeded.reason);
            QueryBudgetError::LimitExceeded(exceeded)
        })?;
        if let Some(limit) = self.inner.limits.max_concurrent_queries {
            if next > limit {
                let exceeded =
                    QueryLimitExceeded::new(QueryLimitReason::ConcurrentQueries, limit, current, 1);
                state.record_limit(exceeded.reason);
                return Err(QueryBudgetError::LimitExceeded(exceeded));
            }
        }

        state.active_queries = next;
        state.peak_active_queries = state.peak_active_queries.max(next);
        QueryBudgetState::increment(&mut state.queries_started_total);
        drop(state);

        Ok(QueryExecution {
            lease: Arc::new(QueryLease {
                budget: Arc::clone(&self.inner),
                limits: effective_limits,
                token,
                cancellation_recorded: AtomicBool::new(false),
                deadline_recorded: AtomicBool::new(false),
                memory_reserved_bytes: AtomicU64::new(0),
                series_matched: AtomicU64::new(0),
                samples_scanned: AtomicU64::new(0),
                samples_returned: AtomicU64::new(0),
                returned_bytes: AtomicU64::new(0),
                pattern_expansion: AtomicU64::new(0),
                steps: AtomicU64::new(0),
                intermediate_vector_size: AtomicU64::new(0),
            }),
        })
    }

    fn record_pre_admission_control_error(&self, error: &QueryBudgetError) {
        let mut state = self.inner.state.lock();
        record_control_error_in_state(&mut state, error);
    }

    /// Returns one internally consistent snapshot of gauges and counters.
    #[must_use]
    pub fn snapshot(&self) -> QueryBudgetSnapshot {
        self.inner.state.lock().snapshot(self.inner.limits)
    }
}

fn record_control_error_in_state(state: &mut QueryBudgetState, error: &QueryBudgetError) {
    match error {
        QueryBudgetError::Cancelled => {
            QueryBudgetState::increment(&mut state.cancellations_total);
        }
        QueryBudgetError::DeadlineExceeded => {
            QueryBudgetState::increment(&mut state.deadline_exceeded_total);
        }
        _ => {}
    }
}

fn apply_wall_time_limit(
    token: QueryCancellationToken,
    max_wall_time: Option<Duration>,
) -> QueryCancellationToken {
    let Some(max_wall_time) = max_wall_time else {
        return token;
    };
    let now = Instant::now();
    token.with_deadline(now.checked_add(max_wall_time).unwrap_or(now))
}

struct QueryLease {
    budget: Arc<QueryBudgetInner>,
    limits: QueryWorkLimits,
    token: QueryCancellationToken,
    cancellation_recorded: AtomicBool,
    deadline_recorded: AtomicBool,
    memory_reserved_bytes: AtomicU64,
    series_matched: AtomicU64,
    samples_scanned: AtomicU64,
    samples_returned: AtomicU64,
    returned_bytes: AtomicU64,
    pattern_expansion: AtomicU64,
    steps: AtomicU64,
    intermediate_vector_size: AtomicU64,
}

impl QueryLease {
    fn checkpoint(&self) -> Result<(), QueryBudgetError> {
        match self.token.checkpoint() {
            Ok(()) => Ok(()),
            Err(error) => {
                self.record_control_error(&error);
                Err(error)
            }
        }
    }

    fn record_control_error(&self, error: &QueryBudgetError) {
        let recorded = match error {
            QueryBudgetError::Cancelled => &self.cancellation_recorded,
            QueryBudgetError::DeadlineExceeded => &self.deadline_recorded,
            _ => return,
        };
        if recorded
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let mut state = self.budget.state.lock();
        record_control_error_in_state(&mut state, error);
    }

    fn record_control_error_locked(&self, state: &mut QueryBudgetState, error: &QueryBudgetError) {
        let recorded = match error {
            QueryBudgetError::Cancelled => &self.cancellation_recorded,
            QueryBudgetError::DeadlineExceeded => &self.deadline_recorded,
            _ => return,
        };
        if recorded
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            record_control_error_in_state(state, error);
        }
    }

    fn record_limit(&self, reason: QueryLimitReason) {
        self.budget.state.lock().record_limit(reason);
    }

    fn record_limit_locked(&self, state: &mut QueryBudgetState, reason: QueryLimitReason) {
        state.record_limit(reason);
    }

    fn charge(
        &self,
        counter: &AtomicU64,
        requested: u64,
        limit: Option<u64>,
        reason: QueryLimitReason,
    ) -> Result<(), QueryBudgetError> {
        self.checkpoint()?;
        let mut current = counter.load(Ordering::Relaxed);
        loop {
            let Some(next) = current.checked_add(requested) else {
                let exceeded = QueryLimitExceeded::new(reason, u64::MAX, current, requested);
                self.record_limit(reason);
                return Err(QueryBudgetError::LimitExceeded(exceeded));
            };
            if let Some(limit) = limit {
                if next > limit {
                    let exceeded = QueryLimitExceeded::new(reason, limit, current, requested);
                    self.record_limit(reason);
                    return Err(QueryBudgetError::LimitExceeded(exceeded));
                }
            }
            match counter.compare_exchange_weak(current, next, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => return Ok(()),
                Err(observed) => current = observed,
            }
        }
    }

    fn ensure_charge(
        &self,
        counter: &AtomicU64,
        requested: u64,
        limit: Option<u64>,
        reason: QueryLimitReason,
    ) -> Result<(), QueryBudgetError> {
        self.checkpoint()?;
        let current = counter.load(Ordering::Relaxed);
        let Some(next) = current.checked_add(requested) else {
            let exceeded = QueryLimitExceeded::new(reason, u64::MAX, current, requested);
            self.record_limit(reason);
            return Err(QueryBudgetError::LimitExceeded(exceeded));
        };
        if let Some(limit) = limit {
            if next > limit {
                let exceeded = QueryLimitExceeded::new(reason, limit, current, requested);
                self.record_limit(reason);
                return Err(QueryBudgetError::LimitExceeded(exceeded));
            }
        }
        Ok(())
    }

    fn observe_max(
        &self,
        counter: &AtomicU64,
        observed: u64,
        limit: Option<u64>,
        reason: QueryLimitReason,
    ) -> Result<(), QueryBudgetError> {
        self.checkpoint()?;
        let mut current = counter.load(Ordering::Relaxed);
        loop {
            if let Some(limit) = limit {
                if observed > limit {
                    let exceeded = QueryLimitExceeded::new(reason, limit, current, observed);
                    self.record_limit(reason);
                    return Err(QueryBudgetError::LimitExceeded(exceeded));
                }
            }
            if observed <= current {
                return Ok(());
            }
            match counter.compare_exchange_weak(
                current,
                observed,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return Ok(()),
                Err(actual) => current = actual,
            }
        }
    }

    fn reserve_memory(
        self: &Arc<Self>,
        bytes: u64,
    ) -> Result<QueryMemoryReservation, QueryBudgetError> {
        self.checkpoint()?;
        let mut state = self.budget.state.lock();
        if let Err(error) = self.token.checkpoint() {
            self.record_control_error_locked(&mut state, &error);
            return Err(error);
        }
        let current_query = self.memory_reserved_bytes.load(Ordering::Relaxed);
        let Some(next_query) = current_query.checked_add(bytes) else {
            let exceeded = QueryLimitExceeded::new(
                QueryLimitReason::PerQueryMemoryBytes,
                u64::MAX,
                current_query,
                bytes,
            );
            self.record_limit_locked(&mut state, exceeded.reason);
            return Err(QueryBudgetError::LimitExceeded(exceeded));
        };
        if let Some(limit) = self.limits.max_memory_bytes {
            if next_query > limit {
                let exceeded = QueryLimitExceeded::new(
                    QueryLimitReason::PerQueryMemoryBytes,
                    limit,
                    current_query,
                    bytes,
                );
                self.record_limit_locked(&mut state, exceeded.reason);
                return Err(QueryBudgetError::LimitExceeded(exceeded));
            }
        }

        let current_shared = state.shared_reserved_memory_bytes;
        let Some(next_shared) = current_shared.checked_add(bytes) else {
            let exceeded = QueryLimitExceeded::new(
                QueryLimitReason::SharedMemoryBytes,
                u64::MAX,
                current_shared,
                bytes,
            );
            self.record_limit_locked(&mut state, exceeded.reason);
            return Err(QueryBudgetError::LimitExceeded(exceeded));
        };
        if let Some(limit) = self.budget.limits.max_shared_memory_bytes {
            if next_shared > limit {
                let exceeded = QueryLimitExceeded::new(
                    QueryLimitReason::SharedMemoryBytes,
                    limit,
                    current_shared,
                    bytes,
                );
                self.record_limit_locked(&mut state, exceeded.reason);
                return Err(QueryBudgetError::LimitExceeded(exceeded));
            }
        }

        self.memory_reserved_bytes
            .store(next_query, Ordering::Relaxed);
        state.shared_reserved_memory_bytes = next_shared;
        state.peak_shared_reserved_memory_bytes =
            state.peak_shared_reserved_memory_bytes.max(next_shared);
        drop(state);

        Ok(QueryMemoryReservation {
            lease: Arc::clone(self),
            bytes,
            released: false,
        })
    }

    fn release_memory(&self, requested_release: u64) {
        let mut state = self.budget.state.lock();
        let current_query = self.memory_reserved_bytes.load(Ordering::Relaxed);
        let query_release = requested_release.min(current_query);
        if query_release != requested_release {
            QueryBudgetState::increment(&mut state.accounting_invariant_violations_total);
        }
        self.memory_reserved_bytes
            .store(current_query - query_release, Ordering::Relaxed);

        let shared_release = query_release.min(state.shared_reserved_memory_bytes);
        if shared_release != query_release {
            QueryBudgetState::increment(&mut state.accounting_invariant_violations_total);
        }
        state.shared_reserved_memory_bytes -= shared_release;
    }
}

impl Drop for QueryLease {
    fn drop(&mut self) {
        let mut state = self.budget.state.lock();

        let leaked_memory = self.memory_reserved_bytes.swap(0, Ordering::Relaxed);
        if leaked_memory > 0 {
            QueryBudgetState::increment(&mut state.accounting_invariant_violations_total);
            let release = leaked_memory.min(state.shared_reserved_memory_bytes);
            if release != leaked_memory {
                QueryBudgetState::increment(&mut state.accounting_invariant_violations_total);
            }
            state.shared_reserved_memory_bytes -= release;
        }

        if state.active_queries == 0 {
            QueryBudgetState::increment(&mut state.accounting_invariant_violations_total);
        } else {
            state.active_queries -= 1;
        }
        QueryBudgetState::increment(&mut state.queries_completed_total);
    }
}

/// Snapshot of counters owned by one admitted query.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueryExecutionSnapshot {
    pub memory_reserved_bytes: u64,
    pub series_matched: u64,
    pub samples_scanned: u64,
    pub samples_returned: u64,
    pub returned_bytes: u64,
    pub pattern_expansion: u64,
    pub steps: u64,
    pub intermediate_vector_size: u64,
}

/// One admitted query slot shared by all nested work for that query.
///
/// Cloning this value does not acquire another slot. The slot is released only after the last
/// clone and the last [`QueryMemoryReservation`] are dropped, so a memory reservation cannot
/// outlive the query slot it is charged to.
#[derive(Clone)]
pub struct QueryExecution {
    lease: Arc<QueryLease>,
}

#[derive(Debug)]
pub(crate) enum QueryMemoryCoalesceError {
    Budget(QueryBudgetError),
    InvalidReservations,
}

impl From<QueryBudgetError> for QueryMemoryCoalesceError {
    fn from(error: QueryBudgetError) -> Self {
        Self::Budget(error)
    }
}

impl fmt::Debug for QueryExecution {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryExecution")
            .field("limits", &self.lease.limits)
            .field("snapshot", &self.snapshot())
            .finish_non_exhaustive()
    }
}

impl QueryExecution {
    /// Effective instance/request limits for this query.
    #[must_use]
    pub fn limits(&self) -> QueryWorkLimits {
        self.lease.limits
    }

    /// Returns the cancellation token carrying this query's effective deadline.
    #[must_use]
    pub fn cancellation_token(&self) -> QueryCancellationToken {
        self.lease.token.clone()
    }

    /// Checks cooperative cancellation and the effective deadline.
    pub fn checkpoint(&self) -> Result<(), QueryBudgetError> {
        self.lease.checkpoint()
    }

    pub fn charge_series_matched(&self, count: u64) -> Result<(), QueryBudgetError> {
        self.lease.charge(
            &self.lease.series_matched,
            count,
            self.lease.limits.max_series_matched,
            QueryLimitReason::SeriesMatched,
        )
    }

    /// Checks whether `count` additional matched series would fit without changing counters.
    pub fn ensure_series_matched(&self, count: u64) -> Result<(), QueryBudgetError> {
        self.lease.ensure_charge(
            &self.lease.series_matched,
            count,
            self.lease.limits.max_series_matched,
            QueryLimitReason::SeriesMatched,
        )
    }

    pub fn charge_samples_scanned(&self, count: u64) -> Result<(), QueryBudgetError> {
        self.lease.charge(
            &self.lease.samples_scanned,
            count,
            self.lease.limits.max_samples_scanned,
            QueryLimitReason::SamplesScanned,
        )
    }

    /// Checks whether `count` additional scanned samples would fit without changing counters.
    ///
    /// Callers use this before allocating a conservatively sized decode/materialization buffer;
    /// the subsequent incremental charge remains authoritative.
    pub fn ensure_samples_scanned(&self, count: u64) -> Result<(), QueryBudgetError> {
        self.lease.ensure_charge(
            &self.lease.samples_scanned,
            count,
            self.lease.limits.max_samples_scanned,
            QueryLimitReason::SamplesScanned,
        )
    }

    pub fn charge_samples_returned(&self, count: u64) -> Result<(), QueryBudgetError> {
        self.lease.charge(
            &self.lease.samples_returned,
            count,
            self.lease.limits.max_samples_returned,
            QueryLimitReason::SamplesReturned,
        )
    }

    /// Checks whether `count` additional returned samples would fit without changing counters.
    pub fn ensure_samples_returned(&self, count: u64) -> Result<(), QueryBudgetError> {
        self.lease.ensure_charge(
            &self.lease.samples_returned,
            count,
            self.lease.limits.max_samples_returned,
            QueryLimitReason::SamplesReturned,
        )
    }

    pub fn charge_returned_bytes(&self, bytes: u64) -> Result<(), QueryBudgetError> {
        self.lease.charge(
            &self.lease.returned_bytes,
            bytes,
            self.lease.limits.max_returned_bytes,
            QueryLimitReason::ReturnedBytes,
        )
    }

    /// Checks whether `bytes` additional returned bytes would fit without changing counters.
    pub fn ensure_returned_bytes(&self, bytes: u64) -> Result<(), QueryBudgetError> {
        self.lease.ensure_charge(
            &self.lease.returned_bytes,
            bytes,
            self.lease.limits.max_returned_bytes,
            QueryLimitReason::ReturnedBytes,
        )
    }

    pub fn charge_pattern_expansion(&self, count: u64) -> Result<(), QueryBudgetError> {
        self.lease.charge(
            &self.lease.pattern_expansion,
            count,
            self.lease.limits.max_pattern_expansion,
            QueryLimitReason::PatternExpansion,
        )
    }

    /// Checks whether `count` additional pattern candidates would fit without changing counters.
    ///
    /// Planners use this before constructing a candidate bitmap, then apply the authoritative
    /// incremental charge while candidates are visited.
    pub fn ensure_pattern_expansion(&self, count: u64) -> Result<(), QueryBudgetError> {
        self.lease.ensure_charge(
            &self.lease.pattern_expansion,
            count,
            self.lease.limits.max_pattern_expansion,
            QueryLimitReason::PatternExpansion,
        )
    }

    pub fn charge_steps(&self, count: u64) -> Result<(), QueryBudgetError> {
        self.lease.charge(
            &self.lease.steps,
            count,
            self.lease.limits.max_steps,
            QueryLimitReason::Steps,
        )
    }

    /// Checks whether `count` additional range/subquery steps would fit without changing
    /// counters. Evaluators use this before prefetch or output allocation, then charge each step
    /// incrementally at its cancellation checkpoint.
    pub fn ensure_steps(&self, count: u64) -> Result<(), QueryBudgetError> {
        self.lease.ensure_charge(
            &self.lease.steps,
            count,
            self.lease.limits.max_steps,
            QueryLimitReason::Steps,
        )
    }

    /// Observes the current size of an intermediate vector.
    ///
    /// This is a high-water measurement, not a cumulative charge. Intermediate allocation bytes
    /// are accounted separately with [`Self::reserve_memory`].
    pub fn observe_intermediate_vector_size(&self, size: u64) -> Result<(), QueryBudgetError> {
        self.lease.observe_max(
            &self.lease.intermediate_vector_size,
            size,
            self.lease.limits.max_intermediate_vector_size,
            QueryLimitReason::IntermediateVectorSize,
        )
    }

    /// Reserves intermediate bytes against both per-query and shared limits.
    ///
    /// Dropping the returned value releases the bytes. Forgetting it intentionally leaks both the
    /// reservation and its query slot conservatively instead of making the bytes appear free.
    pub fn reserve_memory(&self, bytes: u64) -> Result<QueryMemoryReservation, QueryBudgetError> {
        self.lease.reserve_memory(bytes)
    }

    /// Coalesces reservations from this execution into one exact retained-memory guard.
    ///
    /// Existing bytes stay charged while any required growth is admitted. Consumed guards are
    /// then disarmed and only excess bytes are released, so callers can hand an allocation from
    /// intermediate accounting to result accounting without a zero-accounting gap or a
    /// duplicate full-size reservation.
    pub(crate) fn coalesce_memory_reservations(
        &self,
        mut reservations: Vec<QueryMemoryReservation>,
        retained_bytes: u64,
    ) -> std::result::Result<QueryMemoryReservation, QueryMemoryCoalesceError> {
        self.checkpoint()?;
        if reservations.iter().any(|reservation| {
            reservation.released || !Arc::ptr_eq(&reservation.lease, &self.lease)
        }) {
            return Err(QueryMemoryCoalesceError::InvalidReservations);
        }
        let Some(reserved_bytes) = reservations.iter().try_fold(0u64, |bytes, reservation| {
            bytes.checked_add(reservation.bytes)
        }) else {
            return Err(QueryMemoryCoalesceError::InvalidReservations);
        };

        let mut additional = if retained_bytes > reserved_bytes {
            Some(self.reserve_memory(retained_bytes - reserved_bytes)?)
        } else {
            None
        };
        for reservation in &mut reservations {
            reservation.released = true;
        }
        if let Some(additional) = additional.as_mut() {
            additional.released = true;
        }
        if reserved_bytes > retained_bytes {
            self.lease.release_memory(reserved_bytes - retained_bytes);
        }

        Ok(QueryMemoryReservation {
            lease: Arc::clone(&self.lease),
            bytes: retained_bytes,
            released: false,
        })
    }

    #[must_use]
    pub fn snapshot(&self) -> QueryExecutionSnapshot {
        QueryExecutionSnapshot {
            memory_reserved_bytes: self.lease.memory_reserved_bytes.load(Ordering::Relaxed),
            series_matched: self.lease.series_matched.load(Ordering::Relaxed),
            samples_scanned: self.lease.samples_scanned.load(Ordering::Relaxed),
            samples_returned: self.lease.samples_returned.load(Ordering::Relaxed),
            returned_bytes: self.lease.returned_bytes.load(Ordering::Relaxed),
            pattern_expansion: self.lease.pattern_expansion.load(Ordering::Relaxed),
            steps: self.lease.steps.load(Ordering::Relaxed),
            intermediate_vector_size: self.lease.intermediate_vector_size.load(Ordering::Relaxed),
        }
    }
}

/// RAII reservation for query-intermediate memory.
#[must_use = "dropping the reservation releases its query-memory charge"]
pub struct QueryMemoryReservation {
    lease: Arc<QueryLease>,
    bytes: u64,
    released: bool,
}

impl fmt::Debug for QueryMemoryReservation {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("QueryMemoryReservation")
            .field("bytes", &self.bytes())
            .finish_non_exhaustive()
    }
}

impl QueryMemoryReservation {
    #[must_use]
    pub fn bytes(&self) -> u64 {
        if self.released {
            0
        } else {
            self.bytes
        }
    }

    /// Resizes this reservation while preserving its association with the same query.
    ///
    /// Growing performs normal per-query and shared-memory admission before changing the
    /// reservation. Shrinking releases the difference immediately. This lets callers admit a
    /// conservative upper bound before allocation and reconcile it to retained memory afterward.
    pub fn resize(&mut self, bytes: u64) -> Result<(), QueryBudgetError> {
        if self.released || bytes == self.bytes {
            return Ok(());
        }
        if bytes < self.bytes {
            let release = self.bytes - bytes;
            self.lease.release_memory(release);
            self.bytes = bytes;
            return Ok(());
        }

        let additional_bytes = bytes - self.bytes;
        let mut additional = self.lease.reserve_memory(additional_bytes)?;
        self.bytes = bytes;
        additional.released = true;
        Ok(())
    }

    /// Releases the reservation before the end of its lexical scope.
    pub fn release(mut self) {
        self.release_inner();
    }

    fn release_inner(&mut self) {
        if !self.released {
            self.lease.release_memory(self.bytes);
            self.released = true;
        }
    }
}

impl Drop for QueryMemoryReservation {
    fn drop(&mut self) {
        self.release_inner();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::{catch_unwind, AssertUnwindSafe};
    use std::sync::{Barrier, Mutex as StdMutex};
    use std::thread;

    fn finite_limits() -> QueryBudgetLimits {
        QueryBudgetLimits {
            max_concurrent_queries: Some(2),
            max_shared_memory_bytes: Some(10),
            per_query: QueryWorkLimits {
                max_series_matched: Some(3),
                max_samples_scanned: Some(5),
                max_samples_returned: Some(4),
                max_returned_bytes: Some(64),
                max_pattern_expansion: Some(6),
                max_steps: Some(7),
                max_intermediate_vector_size: Some(5),
                max_memory_bytes: Some(7),
                max_wall_time: Some(Duration::from_secs(1)),
            },
        }
    }

    #[test]
    fn default_is_explicitly_unenforced() {
        let limits = QueryBudgetLimits::default().validate().unwrap();
        assert_eq!(limits, QueryBudgetLimits::default());

        let budget = QueryBudget::new(limits).unwrap();
        let query = budget.begin_query().unwrap();
        query.charge_samples_scanned(u64::MAX).unwrap();
        assert_eq!(query.snapshot().samples_scanned, u64::MAX);
    }

    #[test]
    fn coalescing_same_query_reservations_releases_only_the_excess_without_a_gap() {
        let budget = QueryBudget::new(QueryBudgetLimits {
            max_shared_memory_bytes: Some(16),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(16),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        })
        .unwrap();
        let query = budget.begin_query().unwrap();
        let first = query.reserve_memory(4).unwrap();
        let second = query.reserve_memory(5).unwrap();
        assert_eq!(query.snapshot().memory_reserved_bytes, 9);

        let retained = query
            .coalesce_memory_reservations(vec![first, second], 6)
            .unwrap();
        assert_eq!(retained.bytes(), 6);
        assert_eq!(query.snapshot().memory_reserved_bytes, 6);
        assert_eq!(budget.snapshot().shared_reserved_memory_bytes, 6);

        drop(retained);
        assert_eq!(query.snapshot().memory_reserved_bytes, 0);
        drop(query);
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn coalescing_growth_is_admitted_while_source_guards_remain_live() {
        for (limit, succeeds) in [(10, true), (9, false)] {
            let budget = QueryBudget::new(QueryBudgetLimits {
                max_shared_memory_bytes: Some(limit),
                per_query: QueryWorkLimits {
                    max_memory_bytes: Some(limit),
                    ..QueryWorkLimits::default()
                },
                ..QueryBudgetLimits::default()
            })
            .unwrap();
            let query = budget.begin_query().unwrap();
            let first = query.reserve_memory(4).unwrap();
            let second = query.reserve_memory(5).unwrap();
            let result = query.coalesce_memory_reservations(vec![first, second], 10);
            if succeeds {
                let retained = result.unwrap();
                assert_eq!(query.snapshot().memory_reserved_bytes, 10);
                drop(retained);
            } else {
                assert!(matches!(
                    result,
                    Err(QueryMemoryCoalesceError::Budget(
                        QueryBudgetError::LimitExceeded(exceeded)
                    )) if exceeded.reason == QueryLimitReason::PerQueryMemoryBytes
                ));
            }
            assert_eq!(query.snapshot().memory_reserved_bytes, 0);
            drop(query);
            let snapshot = budget.snapshot();
            assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
            assert_eq!(snapshot.accounting_invariant_violations_total, 0);
        }
    }

    #[test]
    fn every_finite_zero_limit_is_rejected() {
        let mut cases = Vec::new();
        cases.push(QueryBudgetLimits {
            max_concurrent_queries: Some(0),
            ..QueryBudgetLimits::default()
        });
        cases.push(QueryBudgetLimits {
            max_shared_memory_bytes: Some(0),
            ..QueryBudgetLimits::default()
        });

        let mut push_work = |work: QueryWorkLimits| {
            cases.push(QueryBudgetLimits {
                per_query: work,
                ..QueryBudgetLimits::default()
            });
        };
        push_work(QueryWorkLimits {
            max_series_matched: Some(0),
            ..QueryWorkLimits::default()
        });
        push_work(QueryWorkLimits {
            max_samples_scanned: Some(0),
            ..QueryWorkLimits::default()
        });
        push_work(QueryWorkLimits {
            max_samples_returned: Some(0),
            ..QueryWorkLimits::default()
        });
        push_work(QueryWorkLimits {
            max_returned_bytes: Some(0),
            ..QueryWorkLimits::default()
        });
        push_work(QueryWorkLimits {
            max_pattern_expansion: Some(0),
            ..QueryWorkLimits::default()
        });
        push_work(QueryWorkLimits {
            max_steps: Some(0),
            ..QueryWorkLimits::default()
        });
        push_work(QueryWorkLimits {
            max_intermediate_vector_size: Some(0),
            ..QueryWorkLimits::default()
        });
        push_work(QueryWorkLimits {
            max_memory_bytes: Some(0),
            ..QueryWorkLimits::default()
        });
        push_work(QueryWorkLimits {
            max_wall_time: Some(Duration::ZERO),
            ..QueryWorkLimits::default()
        });

        for limits in cases {
            assert!(matches!(
                QueryBudget::new(limits),
                Err(QueryBudgetConfigError::ZeroLimit { .. })
            ));
        }
    }

    #[test]
    fn per_query_memory_cannot_exceed_shared_memory() {
        let err = QueryBudget::new(QueryBudgetLimits {
            max_shared_memory_bytes: Some(4),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(5),
                ..QueryWorkLimits::default()
            },
            ..QueryBudgetLimits::default()
        })
        .unwrap_err();
        assert_eq!(
            err,
            QueryBudgetConfigError::PerQueryMemoryExceedsShared {
                per_query: 5,
                shared: 4
            }
        );
    }

    #[test]
    fn request_limits_tighten_but_never_loosen_instance_limits() {
        let base = QueryWorkLimits {
            max_series_matched: Some(10),
            max_samples_scanned: None,
            max_wall_time: Some(Duration::from_secs(5)),
            ..QueryWorkLimits::default()
        };
        let request = QueryWorkLimits {
            max_series_matched: Some(20),
            max_samples_scanned: Some(7),
            max_wall_time: Some(Duration::from_secs(2)),
            ..QueryWorkLimits::default()
        };
        let effective = base.tightened_by(request);
        assert_eq!(effective.max_series_matched, Some(10));
        assert_eq!(effective.max_samples_scanned, Some(7));
        assert_eq!(effective.max_wall_time, Some(Duration::from_secs(2)));

        let budget = QueryBudget::new(QueryBudgetLimits {
            per_query: base,
            ..QueryBudgetLimits::default()
        })
        .unwrap();
        let execution = budget
            .begin_query_with(request, QueryCancellationToken::new())
            .unwrap();
        assert_eq!(execution.limits(), effective);
    }

    #[test]
    fn request_memory_limit_must_fit_the_shared_budget() {
        let budget = QueryBudget::new(QueryBudgetLimits {
            max_shared_memory_bytes: Some(4),
            ..QueryBudgetLimits::default()
        })
        .unwrap();

        let err = budget
            .begin_query_with(
                QueryWorkLimits {
                    max_memory_bytes: Some(5),
                    ..QueryWorkLimits::default()
                },
                QueryCancellationToken::new(),
            )
            .unwrap_err();
        assert_eq!(
            err,
            QueryBudgetError::InvalidLimits(QueryBudgetConfigError::PerQueryMemoryExceedsShared {
                per_query: 5,
                shared: 4,
            })
        );
        assert_eq!(budget.snapshot().queries_started_total, 0);
    }

    #[test]
    fn query_slot_outlives_execution_clones_and_memory_reservations() {
        let budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(10),
            per_query: QueryWorkLimits {
                max_memory_bytes: Some(10),
                ..QueryWorkLimits::default()
            },
        })
        .unwrap();

        let query = budget.begin_query().unwrap();
        let clone = query.clone();
        let reservation = query.reserve_memory(4).unwrap();
        drop(query);
        drop(clone);

        assert!(matches!(
            budget.begin_query(),
            Err(QueryBudgetError::LimitExceeded(QueryLimitExceeded {
                reason: QueryLimitReason::ConcurrentQueries,
                ..
            }))
        ));
        assert_eq!(budget.snapshot().shared_reserved_memory_bytes, 4);

        drop(reservation);
        assert_eq!(budget.snapshot().active_queries, 0);
        assert!(budget.begin_query().is_ok());
    }

    #[test]
    fn per_query_and_shared_memory_are_both_enforced_and_released() {
        let budget = QueryBudget::new(finite_limits()).unwrap();
        let first = budget.begin_query().unwrap();
        let second = budget.begin_query().unwrap();
        let first_memory = first.reserve_memory(7).unwrap();
        let second_memory = second.reserve_memory(3).unwrap();

        let shared_err = second.reserve_memory(1).unwrap_err();
        assert!(matches!(
            shared_err,
            QueryBudgetError::LimitExceeded(QueryLimitExceeded {
                reason: QueryLimitReason::SharedMemoryBytes,
                limit: 10,
                current: 10,
                requested: 1,
            })
        ));

        drop(first_memory);
        let second_more = second.reserve_memory(4).unwrap();
        assert_eq!(second.snapshot().memory_reserved_bytes, 7);
        let per_query_err = second.reserve_memory(1).unwrap_err();
        assert!(matches!(
            per_query_err,
            QueryBudgetError::LimitExceeded(QueryLimitExceeded {
                reason: QueryLimitReason::PerQueryMemoryBytes,
                limit: 7,
                current: 7,
                requested: 1,
            })
        ));

        drop(second_memory);
        drop(second_more);
        assert_eq!(budget.snapshot().shared_reserved_memory_bytes, 0);
    }

    #[test]
    fn retryable_limit_errors_do_not_suppress_later_limit_or_cancellation_metrics() {
        let budget = QueryBudget::new(finite_limits()).unwrap();
        let first = budget.begin_query().unwrap();
        let second = budget.begin_query().unwrap();
        let first_memory = first.reserve_memory(7).unwrap();

        assert!(matches!(
            second.reserve_memory(4),
            Err(QueryBudgetError::LimitExceeded(QueryLimitExceeded {
                reason: QueryLimitReason::SharedMemoryBytes,
                ..
            }))
        ));
        drop(first_memory);

        let second_memory = second.reserve_memory(3).unwrap();
        second.charge_samples_scanned(5).unwrap();
        assert!(matches!(
            second.charge_samples_scanned(1),
            Err(QueryBudgetError::LimitExceeded(QueryLimitExceeded {
                reason: QueryLimitReason::SamplesScanned,
                ..
            }))
        ));

        second.cancellation_token().cancel();
        assert_eq!(second.checkpoint(), Err(QueryBudgetError::Cancelled));
        assert_eq!(second.checkpoint(), Err(QueryBudgetError::Cancelled));

        let snapshot = budget.snapshot();
        assert_eq!(snapshot.limit_rejections_total, 2);
        assert_eq!(snapshot.shared_memory_rejections_total, 1);
        assert_eq!(snapshot.cancellations_total, 1);
        drop(second_memory);
    }

    #[test]
    fn cancellation_is_shared_and_counted_once_per_query() {
        let budget = QueryBudget::new(QueryBudgetLimits::default()).unwrap();
        let token = QueryCancellationToken::new();
        let query = budget.begin_query_with_token(token.clone()).unwrap();
        token.cancel();

        assert_eq!(query.checkpoint(), Err(QueryBudgetError::Cancelled));
        assert_eq!(query.checkpoint(), Err(QueryBudgetError::Cancelled));
        assert_eq!(budget.snapshot().cancellations_total, 1);
        drop(query);
        assert_eq!(budget.snapshot().active_queries, 0);
    }

    #[test]
    fn an_expired_deadline_is_deterministic_and_cancellation_wins() {
        let expired = QueryCancellationToken::new().with_deadline(Instant::now());
        assert_eq!(
            expired.checkpoint(),
            Err(QueryBudgetError::DeadlineExceeded)
        );

        expired.cancel();
        assert_eq!(expired.checkpoint(), Err(QueryBudgetError::Cancelled));
    }

    #[test]
    fn pre_admission_deadline_does_not_consume_a_slot() {
        let budget = QueryBudget::new(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            ..QueryBudgetLimits::default()
        })
        .unwrap();
        let token = QueryCancellationToken::new().with_deadline(Instant::now());
        assert!(matches!(
            budget.begin_query_with_token(token),
            Err(QueryBudgetError::DeadlineExceeded)
        ));
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.queries_started_total, 0);
        assert_eq!(snapshot.deadline_exceeded_total, 1);
    }

    #[test]
    fn cumulative_work_charges_accept_exact_boundary_then_reject() {
        let budget = QueryBudget::new(finite_limits()).unwrap();
        let query = budget.begin_query().unwrap();
        query.charge_series_matched(2).unwrap();
        query.charge_series_matched(1).unwrap();
        let err = query.charge_series_matched(1).unwrap_err();
        assert_eq!(
            err,
            QueryBudgetError::LimitExceeded(QueryLimitExceeded::new(
                QueryLimitReason::SeriesMatched,
                3,
                3,
                1,
            ))
        );
        assert_eq!(query.snapshot().series_matched, 3);
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.limit_rejections_total, 1);
        assert_eq!(snapshot.series_matched_rejections_total, 1);
    }

    #[test]
    fn unlimited_counter_overflow_is_rejected_instead_of_wrapping() {
        let budget = QueryBudget::new(QueryBudgetLimits::default()).unwrap();
        let query = budget.begin_query().unwrap();
        query.charge_samples_scanned(u64::MAX).unwrap();
        let err = query.charge_samples_scanned(1).unwrap_err();
        assert_eq!(
            err,
            QueryBudgetError::LimitExceeded(QueryLimitExceeded::new(
                QueryLimitReason::SamplesScanned,
                u64::MAX,
                u64::MAX,
                1,
            ))
        );
        assert_eq!(query.snapshot().samples_scanned, u64::MAX);
        assert_eq!(budget.snapshot().samples_scanned_rejections_total, 1);
    }

    #[test]
    fn intermediate_vector_limit_is_a_high_water_mark() {
        let budget = QueryBudget::new(finite_limits()).unwrap();
        let query = budget.begin_query().unwrap();
        query.observe_intermediate_vector_size(4).unwrap();
        query.observe_intermediate_vector_size(3).unwrap();
        query.observe_intermediate_vector_size(5).unwrap();
        assert_eq!(query.snapshot().intermediate_vector_size, 5);

        let err = query.observe_intermediate_vector_size(6).unwrap_err();
        assert!(matches!(
            err,
            QueryBudgetError::LimitExceeded(QueryLimitExceeded {
                reason: QueryLimitReason::IntermediateVectorSize,
                limit: 5,
                requested: 6,
                ..
            })
        ));
        assert_eq!(
            budget.snapshot().intermediate_vector_size_rejections_total,
            1
        );
    }

    #[test]
    fn cancellation_prevents_new_memory_reservations() {
        let budget = QueryBudget::new(finite_limits()).unwrap();
        let query = budget.begin_query().unwrap();
        query.cancellation_token().cancel();
        assert!(matches!(
            query.reserve_memory(1),
            Err(QueryBudgetError::Cancelled)
        ));
        assert_eq!(budget.snapshot().shared_reserved_memory_bytes, 0);
    }

    #[test]
    fn concurrent_queries_cannot_overbook_shared_memory() {
        let budget = Arc::new(
            QueryBudget::new(QueryBudgetLimits {
                max_concurrent_queries: Some(2),
                max_shared_memory_bytes: Some(8),
                per_query: QueryWorkLimits {
                    max_memory_bytes: Some(8),
                    ..QueryWorkLimits::default()
                },
            })
            .unwrap(),
        );
        let barrier = Arc::new(Barrier::new(3));
        let results = Arc::new(StdMutex::new(Vec::new()));
        let mut handles = Vec::new();

        for _ in 0..2 {
            let budget = Arc::clone(&budget);
            let barrier = Arc::clone(&barrier);
            let results = Arc::clone(&results);
            handles.push(thread::spawn(move || {
                let query = budget.begin_query().unwrap();
                barrier.wait();
                let reservation = query.reserve_memory(8);
                results
                    .lock()
                    .unwrap()
                    .push(reservation.as_ref().map(|_| ()).map_err(Clone::clone));
                barrier.wait();
                drop(reservation);
            }));
        }

        barrier.wait();
        barrier.wait();
        for handle in handles {
            handle.join().unwrap();
        }

        let results = results.lock().unwrap();
        assert_eq!(results.iter().filter(|result| result.is_ok()).count(), 1);
        assert_eq!(
            results
                .iter()
                .filter(|result| matches!(
                    result,
                    Err(QueryBudgetError::LimitExceeded(QueryLimitExceeded {
                        reason: QueryLimitReason::SharedMemoryBytes,
                        ..
                    }))
                ))
                .count(),
            1
        );
        drop(results);
        assert_eq!(budget.snapshot().shared_reserved_memory_bytes, 0);
    }

    #[test]
    fn panic_unwind_releases_memory_and_slot() {
        let budget = QueryBudget::new(finite_limits()).unwrap();
        let result = catch_unwind(AssertUnwindSafe({
            let budget = budget.clone();
            move || {
                let query = budget.begin_query().unwrap();
                let _memory = query.reserve_memory(5).unwrap();
                panic!("intentional query panic");
            }
        }));
        assert!(result.is_err());
        let snapshot = budget.snapshot();
        assert_eq!(snapshot.active_queries, 0);
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 0);
    }

    #[test]
    fn erroneous_release_never_wraps_observability_gauges() {
        let budget = QueryBudget::new(QueryBudgetLimits::default()).unwrap();
        let query = budget.begin_query().unwrap();

        query.lease.release_memory(1);

        let snapshot = budget.snapshot();
        assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
        assert_eq!(query.snapshot().memory_reserved_bytes, 0);
        assert_eq!(snapshot.accounting_invariant_violations_total, 1);
    }
}
