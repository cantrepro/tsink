use std::fmt;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

pub const WRITE_MAX_INFLIGHT_REQUESTS_ENV: &str = "TSINK_SERVER_WRITE_MAX_INFLIGHT_REQUESTS";
pub const WRITE_MAX_INFLIGHT_ROWS_ENV: &str = "TSINK_SERVER_WRITE_MAX_INFLIGHT_ROWS";
pub const WRITE_RESOURCE_ACQUIRE_TIMEOUT_MS_ENV: &str =
    "TSINK_SERVER_WRITE_RESOURCE_ACQUIRE_TIMEOUT_MS";
pub const READ_MAX_INFLIGHT_REQUESTS_ENV: &str = "TSINK_SERVER_READ_MAX_INFLIGHT_REQUESTS";
pub const READ_MAX_INFLIGHT_QUERIES_ENV: &str = "TSINK_SERVER_READ_MAX_INFLIGHT_QUERIES";
pub const READ_RESOURCE_ACQUIRE_TIMEOUT_MS_ENV: &str =
    "TSINK_SERVER_READ_RESOURCE_ACQUIRE_TIMEOUT_MS";

pub const DEFAULT_WRITE_MAX_INFLIGHT_REQUESTS: usize = 64;
pub const DEFAULT_WRITE_MAX_INFLIGHT_ROWS: usize = 200_000;
pub const DEFAULT_WRITE_RESOURCE_ACQUIRE_TIMEOUT_MS: u64 = 25;
pub const DEFAULT_READ_MAX_INFLIGHT_REQUESTS: usize = 64;
pub const DEFAULT_READ_MAX_INFLIGHT_QUERIES: usize = 128;
pub const DEFAULT_READ_RESOURCE_ACQUIRE_TIMEOUT_MS: u64 = 25;

const WRITE_RESOURCE_GLOBAL_INFLIGHT_REQUESTS: &str = "global_inflight_write_requests";
const WRITE_RESOURCE_GLOBAL_INFLIGHT_ROWS: &str = "global_inflight_write_rows";
const READ_RESOURCE_GLOBAL_INFLIGHT_REQUESTS: &str = "global_inflight_read_requests";
const READ_RESOURCE_GLOBAL_INFLIGHT_QUERIES: &str = "global_inflight_read_queries";

static WRITE_ADMISSION_REJECTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
static WRITE_ADMISSION_REQUEST_SLOT_REJECTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
static WRITE_ADMISSION_ROW_BUDGET_REJECTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
static WRITE_ADMISSION_OVERSIZE_ROWS_REJECTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
static WRITE_ADMISSION_ACQUIRE_WAIT_NANOS_TOTAL: AtomicU64 = AtomicU64::new(0);
static WRITE_ADMISSION_ACTIVE_REQUESTS: AtomicU64 = AtomicU64::new(0);
static WRITE_ADMISSION_ACTIVE_ROWS: AtomicU64 = AtomicU64::new(0);
static READ_ADMISSION_REJECTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
static READ_ADMISSION_REQUEST_SLOT_REJECTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
static READ_ADMISSION_QUERY_BUDGET_REJECTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
static READ_ADMISSION_OVERSIZE_QUERIES_REJECTIONS_TOTAL: AtomicU64 = AtomicU64::new(0);
static READ_ADMISSION_ACQUIRE_WAIT_NANOS_TOTAL: AtomicU64 = AtomicU64::new(0);
static READ_ADMISSION_ACTIVE_REQUESTS: AtomicU64 = AtomicU64::new(0);
static READ_ADMISSION_ACTIVE_QUERIES: AtomicU64 = AtomicU64::new(0);

static GLOBAL_PUBLIC_WRITE_ADMISSION: OnceLock<Result<WriteAdmissionController, String>> =
    OnceLock::new();
static GLOBAL_PUBLIC_READ_ADMISSION: OnceLock<Result<ReadAdmissionController, String>> =
    OnceLock::new();

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteAdmissionGuardrails {
    pub max_inflight_requests: usize,
    pub max_inflight_rows: usize,
    pub acquire_timeout: Duration,
}

impl WriteAdmissionGuardrails {
    pub fn validate(self) -> Result<(), String> {
        if self.max_inflight_requests == 0 {
            return Err("write max in-flight requests must be greater than zero".to_string());
        }
        if self.max_inflight_rows == 0 {
            return Err("write max in-flight rows must be greater than zero".to_string());
        }
        if self.max_inflight_rows > u32::MAX as usize {
            return Err(format!(
                "write max in-flight rows must be <= {}, got {}",
                u32::MAX,
                self.max_inflight_rows
            ));
        }
        Ok(())
    }
}

impl Default for WriteAdmissionGuardrails {
    fn default() -> Self {
        Self {
            max_inflight_requests: DEFAULT_WRITE_MAX_INFLIGHT_REQUESTS,
            max_inflight_rows: DEFAULT_WRITE_MAX_INFLIGHT_ROWS,
            acquire_timeout: Duration::from_millis(DEFAULT_WRITE_RESOURCE_ACQUIRE_TIMEOUT_MS),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadAdmissionGuardrails {
    pub max_inflight_requests: usize,
    pub max_inflight_queries: usize,
    pub acquire_timeout: Duration,
}

impl ReadAdmissionGuardrails {
    pub fn validate(self) -> Result<(), String> {
        if self.max_inflight_requests == 0 {
            return Err("read max in-flight requests must be greater than zero".to_string());
        }
        if self.max_inflight_queries == 0 {
            return Err("read max in-flight queries must be greater than zero".to_string());
        }
        if self.max_inflight_queries > u32::MAX as usize {
            return Err(format!(
                "read max in-flight queries must be <= {}, got {}",
                u32::MAX,
                self.max_inflight_queries
            ));
        }
        Ok(())
    }
}

impl Default for ReadAdmissionGuardrails {
    fn default() -> Self {
        Self {
            max_inflight_requests: DEFAULT_READ_MAX_INFLIGHT_REQUESTS,
            max_inflight_queries: DEFAULT_READ_MAX_INFLIGHT_QUERIES,
            acquire_timeout: Duration::from_millis(DEFAULT_READ_RESOURCE_ACQUIRE_TIMEOUT_MS),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteAdmissionMetricsSnapshot {
    pub rejections_total: u64,
    pub request_slot_rejections_total: u64,
    pub row_budget_rejections_total: u64,
    pub oversize_rows_rejections_total: u64,
    pub acquire_wait_nanos_total: u64,
    pub active_requests: u64,
    pub active_rows: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadAdmissionMetricsSnapshot {
    pub rejections_total: u64,
    pub request_slot_rejections_total: u64,
    pub query_budget_rejections_total: u64,
    pub oversize_queries_rejections_total: u64,
    pub acquire_wait_nanos_total: u64,
    pub active_requests: u64,
    pub active_queries: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteAdmissionError {
    ResourceLimitExceeded {
        resource: &'static str,
        requested: usize,
        limit: usize,
        retryable: bool,
    },
}

impl WriteAdmissionError {
    pub fn retryable(&self) -> bool {
        match self {
            Self::ResourceLimitExceeded { retryable, .. } => *retryable,
        }
    }
}

impl fmt::Display for WriteAdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ResourceLimitExceeded {
                resource,
                requested,
                limit,
                retryable,
            } => write!(
                f,
                "write resource limit exceeded for {resource}: requested {requested}, limit {limit}, retryable={retryable}"
            ),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReadAdmissionError {
    ResourceLimitExceeded {
        resource: &'static str,
        requested: usize,
        limit: usize,
        retryable: bool,
    },
}

impl ReadAdmissionError {
    pub fn retryable(&self) -> bool {
        match self {
            Self::ResourceLimitExceeded { retryable, .. } => *retryable,
        }
    }
}

impl fmt::Display for ReadAdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ResourceLimitExceeded {
                resource,
                requested,
                limit,
                retryable,
            } => write!(
                f,
                "read resource limit exceeded for {resource}: requested {requested}, limit {limit}, retryable={retryable}"
            ),
        }
    }
}

#[derive(Debug)]
pub struct WriteAdmissionController {
    request_slots: Arc<Semaphore>,
    row_budget: Arc<Semaphore>,
    acquire_timeout: Duration,
    max_inflight_requests: usize,
    max_inflight_rows: usize,
}

#[derive(Debug)]
pub struct ReadAdmissionController {
    request_slots: Arc<Semaphore>,
    query_budget: Arc<Semaphore>,
    acquire_timeout: Duration,
    max_inflight_requests: usize,
    max_inflight_queries: usize,
}

#[derive(Debug)]
pub struct WriteRequestSlotLease {
    _permit: OwnedSemaphorePermit,
    wait_nanos: u64,
}

impl Drop for WriteRequestSlotLease {
    fn drop(&mut self) {
        WRITE_ADMISSION_ACTIVE_REQUESTS.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub struct WriteAdmissionLease {
    _request_slot: WriteRequestSlotLease,
    _rows: OwnedSemaphorePermit,
    reserved_rows: u64,
}

impl Drop for WriteAdmissionLease {
    fn drop(&mut self) {
        WRITE_ADMISSION_ACTIVE_ROWS.fetch_sub(self.reserved_rows, Ordering::Relaxed);
    }
}

#[derive(Debug)]
struct ReadRequestSlotLease {
    _permit: OwnedSemaphorePermit,
    wait_nanos: u64,
}

impl Drop for ReadRequestSlotLease {
    fn drop(&mut self) {
        READ_ADMISSION_ACTIVE_REQUESTS.fetch_sub(1, Ordering::Relaxed);
    }
}

#[derive(Debug)]
pub struct ReadAdmissionLease {
    _request_slot: ReadRequestSlotLease,
    _queries: Option<OwnedSemaphorePermit>,
    reserved_queries: u64,
}

impl Drop for ReadAdmissionLease {
    fn drop(&mut self) {
        READ_ADMISSION_ACTIVE_QUERIES.fetch_sub(self.reserved_queries, Ordering::Relaxed);
    }
}

impl WriteAdmissionController {
    pub fn new(guardrails: WriteAdmissionGuardrails) -> Result<Self, String> {
        guardrails.validate()?;
        Ok(Self {
            request_slots: Arc::new(Semaphore::new(guardrails.max_inflight_requests)),
            row_budget: Arc::new(Semaphore::new(guardrails.max_inflight_rows)),
            acquire_timeout: guardrails.acquire_timeout,
            max_inflight_requests: guardrails.max_inflight_requests,
            max_inflight_rows: guardrails.max_inflight_rows,
        })
    }

    pub fn from_env() -> Result<Self, String> {
        Self::new(read_write_admission_guardrails_from_env()?)
    }

    pub fn guardrails(&self) -> WriteAdmissionGuardrails {
        WriteAdmissionGuardrails {
            max_inflight_requests: self.max_inflight_requests,
            max_inflight_rows: self.max_inflight_rows,
            acquire_timeout: self.acquire_timeout,
        }
    }

    pub async fn acquire_request_slot(&self) -> Result<WriteRequestSlotLease, WriteAdmissionError> {
        let started = Instant::now();
        if let Ok(permit) = Arc::clone(&self.request_slots).try_acquire_owned() {
            WRITE_ADMISSION_ACTIVE_REQUESTS.fetch_add(1, Ordering::Relaxed);
            return Ok(WriteRequestSlotLease {
                _permit: permit,
                wait_nanos: 0,
            });
        }

        let acquire = Arc::clone(&self.request_slots).acquire_owned();
        match tokio::time::timeout(self.acquire_timeout, acquire).await {
            Ok(Ok(permit)) => {
                WRITE_ADMISSION_ACTIVE_REQUESTS.fetch_add(1, Ordering::Relaxed);
                Ok(WriteRequestSlotLease {
                    _permit: permit,
                    wait_nanos: saturating_elapsed_nanos(started.elapsed()),
                })
            }
            Ok(Err(_)) => Err(WriteAdmissionError::ResourceLimitExceeded {
                resource: WRITE_RESOURCE_GLOBAL_INFLIGHT_REQUESTS,
                requested: 1,
                limit: self.max_inflight_requests,
                retryable: false,
            }),
            Err(_) => {
                track_write_admission_rejection(WRITE_RESOURCE_GLOBAL_INFLIGHT_REQUESTS, true);
                Err(WriteAdmissionError::ResourceLimitExceeded {
                    resource: WRITE_RESOURCE_GLOBAL_INFLIGHT_REQUESTS,
                    requested: 1,
                    limit: self.max_inflight_requests,
                    retryable: true,
                })
            }
        }
    }

    pub async fn reserve_rows(
        &self,
        request_slot: WriteRequestSlotLease,
        requested_rows: usize,
    ) -> Result<WriteAdmissionLease, WriteAdmissionError> {
        if requested_rows > self.max_inflight_rows {
            track_write_admission_rejection(WRITE_RESOURCE_GLOBAL_INFLIGHT_ROWS, false);
            return Err(WriteAdmissionError::ResourceLimitExceeded {
                resource: WRITE_RESOURCE_GLOBAL_INFLIGHT_ROWS,
                requested: requested_rows,
                limit: self.max_inflight_rows,
                retryable: false,
            });
        }

        let permits = u32::try_from(requested_rows).expect("requested rows validated to u32 range");
        let started = Instant::now();
        let row_budget =
            if let Ok(permit) = Arc::clone(&self.row_budget).try_acquire_many_owned(permits) {
                permit
            } else {
                let acquire = Arc::clone(&self.row_budget).acquire_many_owned(permits);
                match tokio::time::timeout(self.acquire_timeout, acquire).await {
                    Ok(Ok(permit)) => permit,
                    Ok(Err(_)) => {
                        return Err(WriteAdmissionError::ResourceLimitExceeded {
                            resource: WRITE_RESOURCE_GLOBAL_INFLIGHT_ROWS,
                            requested: requested_rows,
                            limit: self.max_inflight_rows,
                            retryable: false,
                        })
                    }
                    Err(_) => {
                        track_write_admission_rejection(WRITE_RESOURCE_GLOBAL_INFLIGHT_ROWS, true);
                        return Err(WriteAdmissionError::ResourceLimitExceeded {
                            resource: WRITE_RESOURCE_GLOBAL_INFLIGHT_ROWS,
                            requested: requested_rows,
                            limit: self.max_inflight_rows,
                            retryable: true,
                        });
                    }
                }
            };

        let total_wait_nanos = request_slot
            .wait_nanos
            .saturating_add(saturating_elapsed_nanos(started.elapsed()));
        WRITE_ADMISSION_ACQUIRE_WAIT_NANOS_TOTAL.fetch_add(total_wait_nanos, Ordering::Relaxed);
        let reserved_rows = u64::try_from(requested_rows).unwrap_or(u64::MAX);
        WRITE_ADMISSION_ACTIVE_ROWS.fetch_add(reserved_rows, Ordering::Relaxed);
        Ok(WriteAdmissionLease {
            _request_slot: request_slot,
            _rows: row_budget,
            reserved_rows,
        })
    }
}

impl ReadAdmissionController {
    pub fn new(guardrails: ReadAdmissionGuardrails) -> Result<Self, String> {
        guardrails.validate()?;
        Ok(Self {
            request_slots: Arc::new(Semaphore::new(guardrails.max_inflight_requests)),
            query_budget: Arc::new(Semaphore::new(guardrails.max_inflight_queries)),
            acquire_timeout: guardrails.acquire_timeout,
            max_inflight_requests: guardrails.max_inflight_requests,
            max_inflight_queries: guardrails.max_inflight_queries,
        })
    }

    pub fn from_env() -> Result<Self, String> {
        Self::new(read_public_read_admission_guardrails_from_env()?)
    }

    pub fn guardrails(&self) -> ReadAdmissionGuardrails {
        ReadAdmissionGuardrails {
            max_inflight_requests: self.max_inflight_requests,
            max_inflight_queries: self.max_inflight_queries,
            acquire_timeout: self.acquire_timeout,
        }
    }

    pub async fn admit_request(
        &self,
        requested_queries: usize,
    ) -> Result<ReadAdmissionLease, ReadAdmissionError> {
        let request_slot = self.acquire_request_slot().await?;
        self.reserve_queries(request_slot, requested_queries).await
    }

    async fn acquire_request_slot(&self) -> Result<ReadRequestSlotLease, ReadAdmissionError> {
        let started = Instant::now();
        if let Ok(permit) = Arc::clone(&self.request_slots).try_acquire_owned() {
            READ_ADMISSION_ACTIVE_REQUESTS.fetch_add(1, Ordering::Relaxed);
            return Ok(ReadRequestSlotLease {
                _permit: permit,
                wait_nanos: 0,
            });
        }

        let acquire = Arc::clone(&self.request_slots).acquire_owned();
        match tokio::time::timeout(self.acquire_timeout, acquire).await {
            Ok(Ok(permit)) => {
                READ_ADMISSION_ACTIVE_REQUESTS.fetch_add(1, Ordering::Relaxed);
                Ok(ReadRequestSlotLease {
                    _permit: permit,
                    wait_nanos: saturating_elapsed_nanos(started.elapsed()),
                })
            }
            Ok(Err(_)) => Err(ReadAdmissionError::ResourceLimitExceeded {
                resource: READ_RESOURCE_GLOBAL_INFLIGHT_REQUESTS,
                requested: 1,
                limit: self.max_inflight_requests,
                retryable: false,
            }),
            Err(_) => {
                track_read_admission_rejection(READ_RESOURCE_GLOBAL_INFLIGHT_REQUESTS, true);
                Err(ReadAdmissionError::ResourceLimitExceeded {
                    resource: READ_RESOURCE_GLOBAL_INFLIGHT_REQUESTS,
                    requested: 1,
                    limit: self.max_inflight_requests,
                    retryable: true,
                })
            }
        }
    }

    async fn reserve_queries(
        &self,
        request_slot: ReadRequestSlotLease,
        requested_queries: usize,
    ) -> Result<ReadAdmissionLease, ReadAdmissionError> {
        if requested_queries > self.max_inflight_queries {
            track_read_admission_rejection(READ_RESOURCE_GLOBAL_INFLIGHT_QUERIES, false);
            return Err(ReadAdmissionError::ResourceLimitExceeded {
                resource: READ_RESOURCE_GLOBAL_INFLIGHT_QUERIES,
                requested: requested_queries,
                limit: self.max_inflight_queries,
                retryable: false,
            });
        }

        let started = Instant::now();
        let (query_budget, reserved_queries) = if requested_queries == 0 {
            (None, 0usize)
        } else {
            let permits =
                u32::try_from(requested_queries).expect("requested queries validated to u32 range");
            let permit = if let Ok(permit) =
                Arc::clone(&self.query_budget).try_acquire_many_owned(permits)
            {
                permit
            } else {
                let acquire = Arc::clone(&self.query_budget).acquire_many_owned(permits);
                match tokio::time::timeout(self.acquire_timeout, acquire).await {
                    Ok(Ok(permit)) => permit,
                    Ok(Err(_)) => {
                        return Err(ReadAdmissionError::ResourceLimitExceeded {
                            resource: READ_RESOURCE_GLOBAL_INFLIGHT_QUERIES,
                            requested: requested_queries,
                            limit: self.max_inflight_queries,
                            retryable: false,
                        })
                    }
                    Err(_) => {
                        track_read_admission_rejection(READ_RESOURCE_GLOBAL_INFLIGHT_QUERIES, true);
                        return Err(ReadAdmissionError::ResourceLimitExceeded {
                            resource: READ_RESOURCE_GLOBAL_INFLIGHT_QUERIES,
                            requested: requested_queries,
                            limit: self.max_inflight_queries,
                            retryable: true,
                        });
                    }
                }
            };
            (Some(permit), requested_queries)
        };

        let total_wait_nanos = request_slot
            .wait_nanos
            .saturating_add(saturating_elapsed_nanos(started.elapsed()));
        READ_ADMISSION_ACQUIRE_WAIT_NANOS_TOTAL.fetch_add(total_wait_nanos, Ordering::Relaxed);
        let reserved_queries = u64::try_from(reserved_queries).unwrap_or(u64::MAX);
        READ_ADMISSION_ACTIVE_QUERIES.fetch_add(reserved_queries, Ordering::Relaxed);
        Ok(ReadAdmissionLease {
            _request_slot: request_slot,
            _queries: query_budget,
            reserved_queries,
        })
    }
}

pub fn validate_public_write_admission_config() -> Result<WriteAdmissionGuardrails, String> {
    read_write_admission_guardrails_from_env()
}

pub fn validate_public_read_admission_config() -> Result<ReadAdmissionGuardrails, String> {
    read_public_read_admission_guardrails_from_env()
}

pub fn global_public_write_admission() -> Result<&'static WriteAdmissionController, String> {
    match GLOBAL_PUBLIC_WRITE_ADMISSION.get_or_init(WriteAdmissionController::from_env) {
        Ok(controller) => Ok(controller),
        Err(err) => Err(err.clone()),
    }
}

pub fn global_public_read_admission() -> Result<&'static ReadAdmissionController, String> {
    match GLOBAL_PUBLIC_READ_ADMISSION.get_or_init(ReadAdmissionController::from_env) {
        Ok(controller) => Ok(controller),
        Err(err) => Err(err.clone()),
    }
}

pub fn write_admission_metrics_snapshot() -> WriteAdmissionMetricsSnapshot {
    WriteAdmissionMetricsSnapshot {
        rejections_total: WRITE_ADMISSION_REJECTIONS_TOTAL.load(Ordering::Relaxed),
        request_slot_rejections_total: WRITE_ADMISSION_REQUEST_SLOT_REJECTIONS_TOTAL
            .load(Ordering::Relaxed),
        row_budget_rejections_total: WRITE_ADMISSION_ROW_BUDGET_REJECTIONS_TOTAL
            .load(Ordering::Relaxed),
        oversize_rows_rejections_total: WRITE_ADMISSION_OVERSIZE_ROWS_REJECTIONS_TOTAL
            .load(Ordering::Relaxed),
        acquire_wait_nanos_total: WRITE_ADMISSION_ACQUIRE_WAIT_NANOS_TOTAL.load(Ordering::Relaxed),
        active_requests: WRITE_ADMISSION_ACTIVE_REQUESTS.load(Ordering::Relaxed),
        active_rows: WRITE_ADMISSION_ACTIVE_ROWS.load(Ordering::Relaxed),
    }
}

pub fn read_admission_metrics_snapshot() -> ReadAdmissionMetricsSnapshot {
    ReadAdmissionMetricsSnapshot {
        rejections_total: READ_ADMISSION_REJECTIONS_TOTAL.load(Ordering::Relaxed),
        request_slot_rejections_total: READ_ADMISSION_REQUEST_SLOT_REJECTIONS_TOTAL
            .load(Ordering::Relaxed),
        query_budget_rejections_total: READ_ADMISSION_QUERY_BUDGET_REJECTIONS_TOTAL
            .load(Ordering::Relaxed),
        oversize_queries_rejections_total: READ_ADMISSION_OVERSIZE_QUERIES_REJECTIONS_TOTAL
            .load(Ordering::Relaxed),
        acquire_wait_nanos_total: READ_ADMISSION_ACQUIRE_WAIT_NANOS_TOTAL.load(Ordering::Relaxed),
        active_requests: READ_ADMISSION_ACTIVE_REQUESTS.load(Ordering::Relaxed),
        active_queries: READ_ADMISSION_ACTIVE_QUERIES.load(Ordering::Relaxed),
    }
}

fn read_write_admission_guardrails_from_env() -> Result<WriteAdmissionGuardrails, String> {
    let guardrails = WriteAdmissionGuardrails {
        max_inflight_requests: parse_env_usize(
            WRITE_MAX_INFLIGHT_REQUESTS_ENV,
            DEFAULT_WRITE_MAX_INFLIGHT_REQUESTS,
            true,
        )?,
        max_inflight_rows: parse_env_usize(
            WRITE_MAX_INFLIGHT_ROWS_ENV,
            DEFAULT_WRITE_MAX_INFLIGHT_ROWS,
            true,
        )?,
        acquire_timeout: Duration::from_millis(parse_env_u64(
            WRITE_RESOURCE_ACQUIRE_TIMEOUT_MS_ENV,
            DEFAULT_WRITE_RESOURCE_ACQUIRE_TIMEOUT_MS,
            false,
        )?),
    };
    guardrails.validate()?;
    Ok(guardrails)
}

fn read_public_read_admission_guardrails_from_env() -> Result<ReadAdmissionGuardrails, String> {
    let guardrails = ReadAdmissionGuardrails {
        max_inflight_requests: parse_env_usize(
            READ_MAX_INFLIGHT_REQUESTS_ENV,
            DEFAULT_READ_MAX_INFLIGHT_REQUESTS,
            true,
        )?,
        max_inflight_queries: parse_env_usize(
            READ_MAX_INFLIGHT_QUERIES_ENV,
            DEFAULT_READ_MAX_INFLIGHT_QUERIES,
            true,
        )?,
        acquire_timeout: Duration::from_millis(parse_env_u64(
            READ_RESOURCE_ACQUIRE_TIMEOUT_MS_ENV,
            DEFAULT_READ_RESOURCE_ACQUIRE_TIMEOUT_MS,
            false,
        )?),
    };
    guardrails.validate()?;
    Ok(guardrails)
}

fn parse_env_u64(var: &str, default: u64, enforce_positive: bool) -> Result<u64, String> {
    let value = match std::env::var(var) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(default),
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(format!("{var} must be valid UTF-8 when set"));
        }
    };
    let parsed = value
        .trim()
        .parse::<u64>()
        .map_err(|_| format!("{var} must be an integer, got '{value}'"))?;
    if enforce_positive && parsed == 0 {
        return Err(format!("{var} must be greater than zero"));
    }
    Ok(parsed)
}

fn parse_env_usize(var: &str, default: usize, enforce_positive: bool) -> Result<usize, String> {
    let value = match std::env::var(var) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(default),
        Err(std::env::VarError::NotUnicode(_)) => {
            return Err(format!("{var} must be valid UTF-8 when set"));
        }
    };
    let parsed = value
        .trim()
        .parse::<usize>()
        .map_err(|_| format!("{var} must be an integer, got '{value}'"))?;
    if enforce_positive && parsed == 0 {
        return Err(format!("{var} must be greater than zero"));
    }
    Ok(parsed)
}

fn saturating_elapsed_nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn track_write_admission_rejection(resource: &'static str, retryable: bool) {
    WRITE_ADMISSION_REJECTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
    match (resource, retryable) {
        (WRITE_RESOURCE_GLOBAL_INFLIGHT_REQUESTS, true) => {
            WRITE_ADMISSION_REQUEST_SLOT_REJECTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        (WRITE_RESOURCE_GLOBAL_INFLIGHT_ROWS, true) => {
            WRITE_ADMISSION_ROW_BUDGET_REJECTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        (WRITE_RESOURCE_GLOBAL_INFLIGHT_ROWS, false) => {
            WRITE_ADMISSION_OVERSIZE_ROWS_REJECTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        _ => {}
    }
}

fn track_read_admission_rejection(resource: &'static str, retryable: bool) {
    READ_ADMISSION_REJECTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
    match (resource, retryable) {
        (READ_RESOURCE_GLOBAL_INFLIGHT_REQUESTS, true) => {
            READ_ADMISSION_REQUEST_SLOT_REJECTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        (READ_RESOURCE_GLOBAL_INFLIGHT_QUERIES, true) => {
            READ_ADMISSION_QUERY_BUDGET_REJECTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        (READ_RESOURCE_GLOBAL_INFLIGHT_QUERIES, false) => {
            READ_ADMISSION_OVERSIZE_QUERIES_REJECTIONS_TOTAL.fetch_add(1, Ordering::Relaxed);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_invalid_guardrails() {
        let err = WriteAdmissionGuardrails {
            max_inflight_requests: 0,
            max_inflight_rows: 1,
            acquire_timeout: Duration::from_millis(1),
        }
        .validate()
        .expect_err("zero request limit must be rejected");
        assert!(err.contains("greater than zero"));

        let err = WriteAdmissionGuardrails {
            max_inflight_requests: 1,
            max_inflight_rows: 0,
            acquire_timeout: Duration::from_millis(1),
        }
        .validate()
        .expect_err("zero row limit must be rejected");
        assert!(err.contains("greater than zero"));
    }

    #[test]
    fn rejects_invalid_read_guardrails() {
        let err = ReadAdmissionGuardrails {
            max_inflight_requests: 0,
            max_inflight_queries: 1,
            acquire_timeout: Duration::from_millis(1),
        }
        .validate()
        .expect_err("zero request limit must be rejected");
        assert!(err.contains("greater than zero"));

        let err = ReadAdmissionGuardrails {
            max_inflight_requests: 1,
            max_inflight_queries: 0,
            acquire_timeout: Duration::from_millis(1),
        }
        .validate()
        .expect_err("zero query limit must be rejected");
        assert!(err.contains("greater than zero"));
    }

    #[tokio::test]
    async fn request_slot_timeout_returns_retryable_resource_limit_error() {
        let controller = WriteAdmissionController::new(WriteAdmissionGuardrails {
            max_inflight_requests: 1,
            max_inflight_rows: 8,
            acquire_timeout: Duration::from_millis(1),
        })
        .expect("controller should build");
        let first = controller
            .acquire_request_slot()
            .await
            .expect("first request slot should be admitted");

        let err = controller
            .acquire_request_slot()
            .await
            .expect_err("second request slot should time out");
        assert!(matches!(
            err,
            WriteAdmissionError::ResourceLimitExceeded {
                resource: WRITE_RESOURCE_GLOBAL_INFLIGHT_REQUESTS,
                requested: 1,
                limit: 1,
                retryable: true,
            }
        ));

        drop(first);
    }

    #[tokio::test]
    async fn read_request_slot_timeout_returns_retryable_resource_limit_error() {
        let controller = ReadAdmissionController::new(ReadAdmissionGuardrails {
            max_inflight_requests: 1,
            max_inflight_queries: 8,
            acquire_timeout: Duration::from_millis(1),
        })
        .expect("controller should build");
        let first = controller
            .acquire_request_slot()
            .await
            .expect("first request slot should be admitted");

        let err = controller
            .acquire_request_slot()
            .await
            .expect_err("second request slot should time out");
        assert!(matches!(
            err,
            ReadAdmissionError::ResourceLimitExceeded {
                resource: READ_RESOURCE_GLOBAL_INFLIGHT_REQUESTS,
                requested: 1,
                limit: 1,
                retryable: true,
            }
        ));

        drop(first);
    }

    #[tokio::test]
    async fn reserve_rows_rejects_requests_larger_than_budget() {
        let controller = WriteAdmissionController::new(WriteAdmissionGuardrails {
            max_inflight_requests: 2,
            max_inflight_rows: 4,
            acquire_timeout: Duration::from_millis(1),
        })
        .expect("controller should build");
        let request_slot = controller
            .acquire_request_slot()
            .await
            .expect("request slot should be admitted");

        let err = controller
            .reserve_rows(request_slot, 5)
            .await
            .expect_err("oversized request should be rejected");
        assert!(matches!(
            err,
            WriteAdmissionError::ResourceLimitExceeded {
                resource: WRITE_RESOURCE_GLOBAL_INFLIGHT_ROWS,
                requested: 5,
                limit: 4,
                retryable: false,
            }
        ));
    }

    #[tokio::test]
    async fn admit_request_rejects_requests_larger_than_query_budget() {
        let controller = ReadAdmissionController::new(ReadAdmissionGuardrails {
            max_inflight_requests: 2,
            max_inflight_queries: 4,
            acquire_timeout: Duration::from_millis(1),
        })
        .expect("controller should build");

        let err = controller
            .admit_request(5)
            .await
            .expect_err("oversized request should be rejected");
        assert!(matches!(
            err,
            ReadAdmissionError::ResourceLimitExceeded {
                resource: READ_RESOURCE_GLOBAL_INFLIGHT_QUERIES,
                requested: 5,
                limit: 4,
                retryable: false,
            }
        ));
    }

    #[tokio::test]
    async fn reserve_rows_tracks_active_resources_while_lease_is_held() {
        let controller = WriteAdmissionController::new(WriteAdmissionGuardrails {
            max_inflight_requests: 2,
            max_inflight_rows: 8,
            acquire_timeout: Duration::from_millis(1),
        })
        .expect("controller should build");
        let request_slot = controller
            .acquire_request_slot()
            .await
            .expect("request slot should be admitted");
        let lease = controller
            .reserve_rows(request_slot, 3)
            .await
            .expect("row budget should be admitted");

        assert_eq!(controller.request_slots.available_permits(), 1);
        assert_eq!(controller.row_budget.available_permits(), 5);

        drop(lease);
        assert_eq!(controller.request_slots.available_permits(), 2);
        assert_eq!(controller.row_budget.available_permits(), 8);
    }

    #[tokio::test]
    async fn read_admit_request_tracks_active_resources_while_lease_is_held() {
        let controller = ReadAdmissionController::new(ReadAdmissionGuardrails {
            max_inflight_requests: 2,
            max_inflight_queries: 8,
            acquire_timeout: Duration::from_millis(1),
        })
        .expect("controller should build");
        let lease = controller
            .admit_request(3)
            .await
            .expect("query budget should be admitted");

        assert_eq!(controller.request_slots.available_permits(), 1);
        assert_eq!(controller.query_budget.available_permits(), 5);

        drop(lease);
        assert_eq!(controller.request_slots.available_permits(), 2);
        assert_eq!(controller.query_budget.available_permits(), 8);
    }
}
