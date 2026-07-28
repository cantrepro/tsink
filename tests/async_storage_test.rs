use parking_lot::{Condvar, Mutex};
use std::future::{poll_fn, Future};
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::Poll;
use std::time::Duration;
use tokio::sync::Notify;

use tempfile::TempDir;
use tsink::{
    Aggregation, AsyncRuntimeOptions, AsyncStorage, AsyncStorageBuilder, BatchWriteResult,
    DataPoint, Label, MetricSeries, QueryBudget, QueryBudgetError, QueryBudgetLimits,
    QueryExecution, QueryExecutionAccounting, QueryLimitReason, QueryOptions,
    QueryRowsExecutionResult, QueryRowsPage, QueryRowsScanOptions, QueryWorkLimits, ResourceLimits,
    Result, RollupPolicy, Row, RowWriteOutcome, RowWriteStatus, SelectSeriesExecutionResult,
    SeriesSelection, Storage, StorageBuilder, TimestampPrecision, TsinkError, WalSyncMode,
    WriteAcknowledgement, WriteBatchLimits, WriteMode, WriteRejectionCategory,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn basic_insert_and_select_roundtrip() -> Result<()> {
    let storage = AsyncStorageBuilder::new().build()?;

    storage
        .insert_rows(vec![
            Row::new("cpu", DataPoint::new(1, 1.0)),
            Row::new("cpu", DataPoint::new(2, 2.0)),
        ])
        .await?;

    let points = storage.select("cpu", vec![], 0, 10).await?;
    assert_eq!(points.len(), 2);
    assert_eq!(points[0].value_as_f64(), Some(1.0));
    assert_eq!(points[1].value_as_f64(), Some(2.0));

    storage.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_list_metrics_shares_one_query_slot_and_releases_all_resources() -> Result<()> {
    let storage = AsyncStorageBuilder::new()
        .with_query_budget_limits(QueryBudgetLimits {
            max_concurrent_queries: Some(1),
            max_shared_memory_bytes: Some(1024 * 1024),
            per_query: QueryWorkLimits {
                max_series_matched: Some(8),
                max_returned_bytes: Some(1024 * 1024),
                max_intermediate_vector_size: Some(8),
                max_memory_bytes: Some(1024 * 1024),
                ..QueryWorkLimits::default()
            },
        })
        .build()?;
    storage
        .insert_rows(vec![Row::with_labels(
            "async_metadata",
            vec![Label::new("host", "a")],
            DataPoint::new(1, 1.0),
        )])
        .await?;

    assert_eq!(storage.list_metrics().await?.len(), 1);
    let snapshot = storage.inner().query_budget_snapshot();
    assert_eq!(snapshot.queries_started_total, 1);
    assert_eq!(snapshot.queries_completed_total, 1);
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.shared_reserved_memory_bytes, 0);

    storage.close().await?;
    Ok(())
}

struct MetadataReplyTestStorage {
    inner: Arc<dyn Storage>,
    expose_query_budget: bool,
    accounting: QueryExecutionAccounting,
    block_detailed_metadata: bool,
    omit_detailed_metadata_reservation: bool,
    undersize_detailed_metadata_reservation: bool,
    compatibility_list_calls: AtomicUsize,
    compatibility_select_calls: AtomicUsize,
    detailed_list_calls: AtomicUsize,
    detailed_select_calls: AtomicUsize,
    detailed_metadata_started: Notify,
    release_detailed_metadata: Condvar,
    release_flag: Mutex<bool>,
}

impl MetadataReplyTestStorage {
    fn new(
        inner: Arc<dyn Storage>,
        expose_query_budget: bool,
        accounting: QueryExecutionAccounting,
        block_detailed_metadata: bool,
    ) -> Self {
        Self {
            inner,
            expose_query_budget,
            accounting,
            block_detailed_metadata,
            omit_detailed_metadata_reservation: false,
            undersize_detailed_metadata_reservation: false,
            compatibility_list_calls: AtomicUsize::new(0),
            compatibility_select_calls: AtomicUsize::new(0),
            detailed_list_calls: AtomicUsize::new(0),
            detailed_select_calls: AtomicUsize::new(0),
            detailed_metadata_started: Notify::new(),
            release_detailed_metadata: Condvar::new(),
            release_flag: Mutex::new(false),
        }
    }

    fn release_detailed_metadata(&self) {
        *self.release_flag.lock() = true;
        self.release_detailed_metadata.notify_all();
    }

    fn with_missing_detailed_metadata_reservation(mut self) -> Self {
        self.omit_detailed_metadata_reservation = true;
        self
    }

    fn with_undersized_detailed_metadata_reservation(mut self) -> Self {
        self.undersize_detailed_metadata_reservation = true;
        self
    }

    fn wait_for_detailed_metadata_release(&self) {
        self.detailed_metadata_started.notify_one();
        if self.block_detailed_metadata {
            let mut released = self.release_flag.lock();
            while !*released {
                self.release_detailed_metadata.wait(&mut released);
            }
        }
    }

    fn maybe_omit_detailed_metadata_reservation(
        &self,
        detailed: SelectSeriesExecutionResult,
        execution: &QueryExecution,
    ) -> Result<SelectSeriesExecutionResult> {
        if self.omit_detailed_metadata_reservation {
            Ok(SelectSeriesExecutionResult::unaccounted(
                detailed.into_series(),
            ))
        } else if self.undersize_detailed_metadata_reservation {
            let series = detailed.into_series();
            let reservation = execution.reserve_memory(1)?;
            Ok(SelectSeriesExecutionResult::accounted(series, reservation))
        } else {
            Ok(detailed)
        }
    }

    fn total_detailed_calls(&self) -> usize {
        self.detailed_list_calls.load(Ordering::SeqCst)
            + self.detailed_select_calls.load(Ordering::SeqCst)
    }
}

struct DetailedMetadataReleaseGuard {
    storage: Arc<MetadataReplyTestStorage>,
    armed: bool,
}

impl DetailedMetadataReleaseGuard {
    fn new(storage: Arc<MetadataReplyTestStorage>) -> Self {
        Self {
            storage,
            armed: true,
        }
    }

    fn release(&mut self) {
        if self.armed {
            self.storage.release_detailed_metadata();
            self.armed = false;
        }
    }
}

impl Drop for DetailedMetadataReleaseGuard {
    fn drop(&mut self) {
        self.release();
    }
}

impl Storage for MetadataReplyTestStorage {
    fn query_budget(&self) -> Option<QueryBudget> {
        self.expose_query_budget
            .then(|| self.inner.query_budget())
            .flatten()
    }

    fn insert_rows(&self, rows: &[Row]) -> Result<()> {
        self.inner.insert_rows(rows)
    }

    fn select(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
    ) -> Result<Vec<DataPoint>> {
        self.inner.select(metric, labels, start, end)
    }

    fn select_with_options(&self, metric: &str, options: QueryOptions) -> Result<Vec<DataPoint>> {
        self.inner.select_with_options(metric, options)
    }

    fn select_all(
        &self,
        metric: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
        self.inner.select_all(metric, start, end)
    }

    fn list_metrics(&self) -> Result<Vec<MetricSeries>> {
        self.compatibility_list_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.list_metrics()
    }

    fn list_metrics_with_execution_result(
        &self,
        execution: &QueryExecution,
    ) -> Result<SelectSeriesExecutionResult> {
        self.detailed_list_calls.fetch_add(1, Ordering::SeqCst);
        self.wait_for_detailed_metadata_release();
        self.inner
            .list_metrics_with_execution_result(execution)
            .and_then(|detailed| self.maybe_omit_detailed_metadata_reservation(detailed, execution))
    }

    fn list_metrics_execution_accounting(&self) -> QueryExecutionAccounting {
        self.accounting
    }

    fn select_series(&self, selection: &SeriesSelection) -> Result<Vec<MetricSeries>> {
        self.compatibility_select_calls
            .fetch_add(1, Ordering::SeqCst);
        self.inner.select_series(selection)
    }

    fn select_series_with_execution_result(
        &self,
        selection: &SeriesSelection,
        execution: &QueryExecution,
    ) -> Result<SelectSeriesExecutionResult> {
        self.detailed_select_calls.fetch_add(1, Ordering::SeqCst);
        self.wait_for_detailed_metadata_release();
        self.inner
            .select_series_with_execution_result(selection, execution)
            .and_then(|detailed| self.maybe_omit_detailed_metadata_reservation(detailed, execution))
    }

    fn select_series_execution_accounting(&self) -> QueryExecutionAccounting {
        self.accounting
    }

    fn close(&self) -> Result<()> {
        self.inner.close()
    }
}

#[derive(Clone, Copy)]
enum AsyncMetadataOperation {
    ListMetrics,
    SelectSeries,
}

impl AsyncMetadataOperation {
    async fn run(self, storage: &AsyncStorage) -> Result<Vec<MetricSeries>> {
        match self {
            Self::ListMetrics => storage.list_metrics().await,
            Self::SelectSeries => {
                storage
                    .select_series(SeriesSelection::new().with_metric("async_metadata"))
                    .await
            }
        }
    }

    fn operation_name(self) -> &'static str {
        match self {
            Self::ListMetrics => "async_list_metrics",
            Self::SelectSeries => "async_select_series",
        }
    }

    fn compatibility_calls(self, storage: &MetadataReplyTestStorage) -> usize {
        match self {
            Self::ListMetrics => storage.compatibility_list_calls.load(Ordering::SeqCst),
            Self::SelectSeries => storage.compatibility_select_calls.load(Ordering::SeqCst),
        }
    }

    fn detailed_calls(self, storage: &MetadataReplyTestStorage) -> usize {
        match self {
            Self::ListMetrics => storage.detailed_list_calls.load(Ordering::SeqCst),
            Self::SelectSeries => storage.detailed_select_calls.load(Ordering::SeqCst),
        }
    }
}

struct RowScanTestStorage {
    inner: Arc<dyn Storage>,
    expose_query_budget: bool,
    accounting: QueryExecutionAccounting,
    block_detailed_scan: bool,
    omit_detailed_scan_reservation: bool,
    compatibility_scan_calls: AtomicUsize,
    detailed_scan_calls: AtomicUsize,
    detailed_scan_started: Notify,
    release_detailed_scan: Condvar,
    release_flag: Mutex<bool>,
}

impl RowScanTestStorage {
    fn new(
        inner: Arc<dyn Storage>,
        expose_query_budget: bool,
        accounting: QueryExecutionAccounting,
        block_detailed_scan: bool,
    ) -> Self {
        Self {
            inner,
            expose_query_budget,
            accounting,
            block_detailed_scan,
            omit_detailed_scan_reservation: false,
            compatibility_scan_calls: AtomicUsize::new(0),
            detailed_scan_calls: AtomicUsize::new(0),
            detailed_scan_started: Notify::new(),
            release_detailed_scan: Condvar::new(),
            release_flag: Mutex::new(false),
        }
    }

    fn release_detailed_scan(&self) {
        *self.release_flag.lock() = true;
        self.release_detailed_scan.notify_all();
    }

    fn with_missing_detailed_scan_reservation(mut self) -> Self {
        self.omit_detailed_scan_reservation = true;
        self
    }
}

struct DetailedScanReleaseGuard {
    storage: Arc<RowScanTestStorage>,
    armed: bool,
}

impl DetailedScanReleaseGuard {
    fn new(storage: Arc<RowScanTestStorage>) -> Self {
        Self {
            storage,
            armed: true,
        }
    }

    fn release(&mut self) {
        if self.armed {
            self.storage.release_detailed_scan();
            self.armed = false;
        }
    }
}

impl Drop for DetailedScanReleaseGuard {
    fn drop(&mut self) {
        self.release();
    }
}

impl Storage for RowScanTestStorage {
    fn query_budget(&self) -> Option<QueryBudget> {
        self.expose_query_budget
            .then(|| self.inner.query_budget())
            .flatten()
    }

    fn insert_rows(&self, rows: &[Row]) -> Result<()> {
        self.inner.insert_rows(rows)
    }

    fn select(
        &self,
        metric: &str,
        labels: &[Label],
        start: i64,
        end: i64,
    ) -> Result<Vec<DataPoint>> {
        self.inner.select(metric, labels, start, end)
    }

    fn select_with_options(&self, metric: &str, options: QueryOptions) -> Result<Vec<DataPoint>> {
        self.inner.select_with_options(metric, options)
    }

    fn select_all(
        &self,
        metric: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
        self.inner.select_all(metric, start, end)
    }

    fn list_metrics(&self) -> Result<Vec<MetricSeries>> {
        self.inner.list_metrics()
    }

    fn scan_series_rows(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        options: QueryRowsScanOptions,
    ) -> Result<QueryRowsPage> {
        self.compatibility_scan_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.scan_series_rows(series, start, end, options)
    }

    fn scan_series_rows_with_execution_result(
        &self,
        series: &[MetricSeries],
        start: i64,
        end: i64,
        options: QueryRowsScanOptions,
        execution: &QueryExecution,
    ) -> Result<QueryRowsExecutionResult> {
        self.detailed_scan_calls.fetch_add(1, Ordering::SeqCst);
        self.detailed_scan_started.notify_one();

        if self.block_detailed_scan {
            let mut released = self.release_flag.lock();
            while !*released {
                self.release_detailed_scan.wait(&mut released);
            }
        }

        if self.accounting == QueryExecutionAccounting::Complete {
            let detailed = self
                .inner
                .scan_series_rows_with_execution_result(series, start, end, options, execution)?;
            if self.omit_detailed_scan_reservation {
                Ok(QueryRowsExecutionResult::unaccounted(detailed.into_page()))
            } else {
                Ok(detailed)
            }
        } else {
            self.scan_series_rows(series, start, end, options)
                .map(QueryRowsExecutionResult::unaccounted)
        }
    }

    fn scan_series_rows_execution_accounting(&self) -> QueryExecutionAccounting {
        self.accounting
    }

    fn scan_metric_rows(
        &self,
        metric: &str,
        start: i64,
        end: i64,
        options: QueryRowsScanOptions,
    ) -> Result<QueryRowsPage> {
        self.compatibility_scan_calls.fetch_add(1, Ordering::SeqCst);
        self.inner.scan_metric_rows(metric, start, end, options)
    }

    fn scan_metric_rows_with_execution_result(
        &self,
        metric: &str,
        start: i64,
        end: i64,
        options: QueryRowsScanOptions,
        execution: &QueryExecution,
    ) -> Result<QueryRowsExecutionResult> {
        self.detailed_scan_calls.fetch_add(1, Ordering::SeqCst);
        self.detailed_scan_started.notify_one();

        if self.block_detailed_scan {
            let mut released = self.release_flag.lock();
            while !*released {
                self.release_detailed_scan.wait(&mut released);
            }
        }

        if self.accounting == QueryExecutionAccounting::Complete {
            let detailed = self
                .inner
                .scan_metric_rows_with_execution_result(metric, start, end, options, execution)?;
            if self.omit_detailed_scan_reservation {
                Ok(QueryRowsExecutionResult::unaccounted(detailed.into_page()))
            } else {
                Ok(detailed)
            }
        } else {
            self.scan_metric_rows(metric, start, end, options)
                .map(QueryRowsExecutionResult::unaccounted)
        }
    }

    fn scan_metric_rows_execution_accounting(&self) -> QueryExecutionAccounting {
        self.accounting
    }

    fn close(&self) -> Result<()> {
        self.inner.close()
    }
}

fn guarded_row_scan_limits() -> QueryBudgetLimits {
    QueryBudgetLimits {
        max_concurrent_queries: Some(1),
        max_shared_memory_bytes: Some(1024 * 1024),
        per_query: QueryWorkLimits {
            max_memory_bytes: Some(1024 * 1024),
            ..QueryWorkLimits::default()
        },
    }
}

fn build_row_scan_storage(limits: QueryBudgetLimits) -> Result<Arc<dyn Storage>> {
    let storage = StorageBuilder::new()
        .with_query_budget_limits(limits)
        .build()?;
    storage.insert_rows(&[Row::with_labels(
        "async_rows",
        vec![Label::new("host", "a")],
        DataPoint::new(1, 1.0),
    )])?;
    Ok(storage)
}

fn row_scan_series() -> Vec<MetricSeries> {
    vec![MetricSeries {
        name: "async_rows".to_string(),
        labels: vec![Label::new("host", "a")],
    }]
}

fn row_scan_options() -> QueryRowsScanOptions {
    QueryRowsScanOptions {
        max_rows: Some(1),
        row_offset: None,
    }
}

async fn poll_once_pending<F>(mut future: Pin<&mut F>)
where
    F: Future + ?Sized,
{
    poll_fn(|context| match future.as_mut().poll(context) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(_) => panic!("row scan unexpectedly completed during its enqueue poll"),
    })
    .await;
}

fn assert_concurrent_query_rejection(error: TsinkError) {
    match error {
        TsinkError::QueryBudget(QueryBudgetError::LimitExceeded(exceeded)) => {
            assert_eq!(exceeded.reason, QueryLimitReason::ConcurrentQueries);
        }
        other => panic!("expected concurrent-query rejection, got {other:?}"),
    }
}

async fn wait_for_query_resources_to_release(storage: &Arc<dyn Storage>) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = storage.query_budget_snapshot();
            if snapshot.active_queries == 0 && snapshot.shared_reserved_memory_bytes == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("query slot and retained result memory must be released");
}

fn build_metadata_reply_storage(limits: QueryBudgetLimits) -> Result<Arc<dyn Storage>> {
    let storage = StorageBuilder::new()
        .with_query_budget_limits(limits)
        .build()?;
    storage.insert_rows(&[Row::with_labels(
        "async_metadata",
        vec![Label::new("host", "a")],
        DataPoint::new(1, 1.0),
    )])?;
    Ok(storage)
}

async fn assert_async_metadata_reply_holds_resources(
    operation: AsyncMetadataOperation,
) -> Result<()> {
    let inner = build_metadata_reply_storage(guarded_row_scan_limits())?;
    let storage = Arc::new(MetadataReplyTestStorage::new(
        Arc::clone(&inner),
        true,
        QueryExecutionAccounting::Complete,
        true,
    ));
    let async_storage = AsyncStorage::from_storage_with_options(
        storage.clone() as Arc<dyn Storage>,
        AsyncRuntimeOptions {
            read_workers: 1,
            ..AsyncRuntimeOptions::default()
        },
    )?;
    let mut release_guard = DetailedMetadataReleaseGuard::new(Arc::clone(&storage));

    let mut held_reply = Box::pin(operation.run(&async_storage));
    poll_once_pending(held_reply.as_mut()).await;
    tokio::time::timeout(
        Duration::from_secs(2),
        storage.detailed_metadata_started.notified(),
    )
    .await
    .expect("the guarded metadata read must reach the backend");
    release_guard.release();

    let error = tokio::time::timeout(Duration::from_secs(2), operation.run(&async_storage))
        .await
        .expect("the worker must process a metadata read behind the completed held reply")
        .expect_err("the held metadata reply must continue owning the only query slot");
    assert_concurrent_query_rejection(error);

    let held_snapshot = inner.query_budget_snapshot();
    assert_eq!(held_snapshot.active_queries, 1);
    assert!(held_snapshot.shared_reserved_memory_bytes > 0);
    assert_eq!(held_snapshot.queries_started_total, 1);
    assert_eq!(held_snapshot.queries_completed_total, 0);
    assert_eq!(held_snapshot.concurrency_rejections_total, 1);
    assert_eq!(operation.detailed_calls(&storage), 1);
    assert_eq!(storage.total_detailed_calls(), 1);
    assert_eq!(storage.compatibility_list_calls.load(Ordering::SeqCst), 0);
    assert_eq!(storage.compatibility_select_calls.load(Ordering::SeqCst), 0);

    let series = held_reply.await?;
    assert_eq!(series.len(), 1);
    wait_for_query_resources_to_release(&inner).await;
    let released_snapshot = inner.query_budget_snapshot();
    assert_eq!(released_snapshot.queries_started_total, 1);
    assert_eq!(released_snapshot.queries_completed_total, 1);
    assert_eq!(released_snapshot.active_queries, 0);
    assert_eq!(released_snapshot.shared_reserved_memory_bytes, 0);

    async_storage.close().await?;
    Ok(())
}

async fn assert_dropping_async_metadata_reply_releases_resources(
    operation: AsyncMetadataOperation,
) -> Result<()> {
    let inner = build_metadata_reply_storage(guarded_row_scan_limits())?;
    let storage = Arc::new(MetadataReplyTestStorage::new(
        Arc::clone(&inner),
        true,
        QueryExecutionAccounting::Complete,
        true,
    ));
    let async_storage = AsyncStorage::from_storage_with_options(
        storage.clone() as Arc<dyn Storage>,
        AsyncRuntimeOptions {
            read_workers: 1,
            ..AsyncRuntimeOptions::default()
        },
    )?;
    let mut release_guard = DetailedMetadataReleaseGuard::new(Arc::clone(&storage));

    let mut held_reply = Box::pin(operation.run(&async_storage));
    poll_once_pending(held_reply.as_mut()).await;
    tokio::time::timeout(
        Duration::from_secs(2),
        storage.detailed_metadata_started.notified(),
    )
    .await
    .expect("the guarded metadata read must reach the backend");
    release_guard.release();

    let error = tokio::time::timeout(Duration::from_secs(2), operation.run(&async_storage))
        .await
        .expect("the worker must buffer the first reply and process the second metadata read")
        .expect_err("the buffered metadata reply must continue owning the only query slot");
    assert_concurrent_query_rejection(error);
    let held_snapshot = inner.query_budget_snapshot();
    assert_eq!(held_snapshot.active_queries, 1);
    assert!(held_snapshot.shared_reserved_memory_bytes > 0);

    drop(held_reply);
    wait_for_query_resources_to_release(&inner).await;
    let released_snapshot = inner.query_budget_snapshot();
    assert_eq!(released_snapshot.queries_started_total, 1);
    assert_eq!(released_snapshot.queries_completed_total, 1);
    assert_eq!(released_snapshot.active_queries, 0);
    assert_eq!(released_snapshot.shared_reserved_memory_bytes, 0);

    async_storage.close().await?;
    Ok(())
}

async fn assert_budgeted_async_metadata_rejects_unaccounted_backend(
    operation: AsyncMetadataOperation,
) -> Result<()> {
    let inner = build_metadata_reply_storage(guarded_row_scan_limits())?;
    let storage = Arc::new(MetadataReplyTestStorage::new(
        Arc::clone(&inner),
        true,
        QueryExecutionAccounting::Unaccounted,
        false,
    ));
    let async_storage = AsyncStorage::from_storage(storage.clone() as Arc<dyn Storage>)?;

    let error = operation
        .run(&async_storage)
        .await
        .expect_err("a budgeted unaccounted metadata backend must fail closed");
    assert!(matches!(
        error,
        TsinkError::UnsupportedOperation {
            operation: actual_operation,
            reason,
        } if actual_operation == operation.operation_name()
            && reason == "bounded async metadata reads require complete execution accounting"
    ));
    assert_eq!(operation.detailed_calls(&storage), 0);
    assert_eq!(storage.total_detailed_calls(), 0);
    assert_eq!(operation.compatibility_calls(&storage), 0);

    wait_for_query_resources_to_release(&inner).await;
    let snapshot = inner.query_budget_snapshot();
    assert_eq!(snapshot.queries_started_total, 1);
    assert_eq!(snapshot.queries_completed_total, 1);
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.shared_reserved_memory_bytes, 0);

    async_storage.close().await?;
    Ok(())
}

async fn assert_budgeted_async_metadata_rejects_missing_result_guard(
    operation: AsyncMetadataOperation,
) -> Result<()> {
    let inner = build_metadata_reply_storage(guarded_row_scan_limits())?;
    let storage = Arc::new(
        MetadataReplyTestStorage::new(
            Arc::clone(&inner),
            true,
            QueryExecutionAccounting::Complete,
            false,
        )
        .with_missing_detailed_metadata_reservation(),
    );
    let async_storage = AsyncStorage::from_storage(storage.clone() as Arc<dyn Storage>)?;

    let error = operation
        .run(&async_storage)
        .await
        .expect_err("a false-complete metadata result without a guard must fail closed");
    assert!(matches!(
        error,
        TsinkError::UnsupportedOperation {
            operation: actual_operation,
            reason,
        } if actual_operation == operation.operation_name()
            && reason == "bounded async metadata reads require complete execution accounting"
    ));
    assert_eq!(operation.detailed_calls(&storage), 1);
    assert_eq!(storage.total_detailed_calls(), 1);
    assert_eq!(operation.compatibility_calls(&storage), 0);

    wait_for_query_resources_to_release(&inner).await;
    let snapshot = inner.query_budget_snapshot();
    assert_eq!(snapshot.queries_started_total, 1);
    assert_eq!(snapshot.queries_completed_total, 1);
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
    assert_eq!(snapshot.accounting_invariant_violations_total, 0);

    async_storage.close().await?;
    Ok(())
}

async fn assert_budgeted_async_metadata_rejects_undersized_result_guard(
    operation: AsyncMetadataOperation,
) -> Result<()> {
    let inner = build_metadata_reply_storage(guarded_row_scan_limits())?;
    let storage = Arc::new(
        MetadataReplyTestStorage::new(
            Arc::clone(&inner),
            true,
            QueryExecutionAccounting::Complete,
            false,
        )
        .with_undersized_detailed_metadata_reservation(),
    );
    let async_storage = AsyncStorage::from_storage(storage.clone() as Arc<dyn Storage>)?;

    let error = operation
        .run(&async_storage)
        .await
        .expect_err("a false-complete metadata result with an undersized guard must fail closed");
    assert!(matches!(
        error,
        TsinkError::UnsupportedOperation {
            operation: actual_operation,
            reason,
        } if actual_operation == operation.operation_name()
            && reason == "bounded async metadata reads require complete execution accounting"
    ));
    assert_eq!(operation.detailed_calls(&storage), 1);
    assert_eq!(storage.total_detailed_calls(), 1);
    assert_eq!(operation.compatibility_calls(&storage), 0);

    wait_for_query_resources_to_release(&inner).await;
    let snapshot = inner.query_budget_snapshot();
    assert_eq!(snapshot.queries_started_total, 1);
    assert_eq!(snapshot.queries_completed_total, 1);
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.shared_reserved_memory_bytes, 0);
    assert_eq!(snapshot.accounting_invariant_violations_total, 0);

    async_storage.close().await?;
    Ok(())
}

async fn assert_unlimited_async_metadata_preserves_compatibility(
    operation: AsyncMetadataOperation,
) -> Result<()> {
    let inner = build_metadata_reply_storage(QueryBudgetLimits::default())?;
    let storage = Arc::new(MetadataReplyTestStorage::new(
        Arc::clone(&inner),
        false,
        QueryExecutionAccounting::Unaccounted,
        false,
    ));
    let async_storage = AsyncStorage::from_storage(storage.clone() as Arc<dyn Storage>)?;

    let series = operation.run(&async_storage).await?;
    assert_eq!(series.len(), 1);
    assert_eq!(operation.detailed_calls(&storage), 0);
    assert_eq!(storage.total_detailed_calls(), 0);
    assert_eq!(operation.compatibility_calls(&storage), 1);

    async_storage.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_metadata_list_metrics_holds_query_resources_until_reply_receipt() -> Result<()> {
    assert_async_metadata_reply_holds_resources(AsyncMetadataOperation::ListMetrics).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_metadata_select_series_holds_query_resources_until_reply_receipt() -> Result<()> {
    assert_async_metadata_reply_holds_resources(AsyncMetadataOperation::SelectSeries).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_metadata_dropping_completed_list_metrics_future_releases_resources() -> Result<()> {
    assert_dropping_async_metadata_reply_releases_resources(AsyncMetadataOperation::ListMetrics)
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_metadata_dropping_completed_select_series_future_releases_resources() -> Result<()> {
    assert_dropping_async_metadata_reply_releases_resources(AsyncMetadataOperation::SelectSeries)
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_metadata_budgeted_list_metrics_rejects_unaccounted_backend() -> Result<()> {
    assert_budgeted_async_metadata_rejects_unaccounted_backend(AsyncMetadataOperation::ListMetrics)
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_metadata_budgeted_select_series_rejects_unaccounted_backend() -> Result<()> {
    assert_budgeted_async_metadata_rejects_unaccounted_backend(AsyncMetadataOperation::SelectSeries)
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_metadata_budgeted_list_metrics_rejects_missing_result_guard() -> Result<()> {
    assert_budgeted_async_metadata_rejects_missing_result_guard(AsyncMetadataOperation::ListMetrics)
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_metadata_budgeted_select_series_rejects_missing_result_guard() -> Result<()> {
    assert_budgeted_async_metadata_rejects_missing_result_guard(
        AsyncMetadataOperation::SelectSeries,
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_metadata_budgeted_list_metrics_rejects_undersized_result_guard() -> Result<()> {
    assert_budgeted_async_metadata_rejects_undersized_result_guard(
        AsyncMetadataOperation::ListMetrics,
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_metadata_budgeted_select_series_rejects_undersized_result_guard() -> Result<()> {
    assert_budgeted_async_metadata_rejects_undersized_result_guard(
        AsyncMetadataOperation::SelectSeries,
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_metadata_unlimited_list_metrics_preserves_compatibility() -> Result<()> {
    assert_unlimited_async_metadata_preserves_compatibility(AsyncMetadataOperation::ListMetrics)
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_metadata_unlimited_select_series_preserves_compatibility() -> Result<()> {
    assert_unlimited_async_metadata_preserves_compatibility(AsyncMetadataOperation::SelectSeries)
        .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_scan_series_rows_holds_query_resources_until_reply_receipt() -> Result<()> {
    let inner = build_row_scan_storage(guarded_row_scan_limits())?;
    let storage = Arc::new(RowScanTestStorage::new(
        Arc::clone(&inner),
        true,
        QueryExecutionAccounting::Complete,
        true,
    ));
    let async_storage = AsyncStorage::from_storage_with_options(
        storage.clone() as Arc<dyn Storage>,
        AsyncRuntimeOptions {
            read_workers: 1,
            ..AsyncRuntimeOptions::default()
        },
    )?;
    let mut release_guard = DetailedScanReleaseGuard::new(Arc::clone(&storage));

    let mut held_reply =
        Box::pin(async_storage.scan_series_rows(row_scan_series(), 0, 2, row_scan_options()));
    poll_once_pending(held_reply.as_mut()).await;
    tokio::time::timeout(
        Duration::from_secs(2),
        storage.detailed_scan_started.notified(),
    )
    .await
    .expect("the guarded scan must reach the backend");
    release_guard.release();

    let error = tokio::time::timeout(
        Duration::from_secs(2),
        async_storage.scan_series_rows(row_scan_series(), 0, 2, row_scan_options()),
    )
    .await
    .expect("the worker must process a scan behind the completed held reply")
    .expect_err("the held reply must continue owning the only query slot");
    assert_concurrent_query_rejection(error);

    let held_snapshot = inner.query_budget_snapshot();
    assert_eq!(held_snapshot.active_queries, 1);
    assert!(held_snapshot.shared_reserved_memory_bytes > 0);
    assert_eq!(held_snapshot.queries_started_total, 1);
    assert_eq!(held_snapshot.queries_completed_total, 0);
    assert_eq!(held_snapshot.concurrency_rejections_total, 1);
    assert_eq!(storage.detailed_scan_calls.load(Ordering::SeqCst), 1);
    assert_eq!(storage.compatibility_scan_calls.load(Ordering::SeqCst), 0);

    let page = held_reply.await?;
    assert_eq!(page.rows.len(), 1);
    wait_for_query_resources_to_release(&inner).await;
    let released_snapshot = inner.query_budget_snapshot();
    assert_eq!(released_snapshot.queries_started_total, 1);
    assert_eq!(released_snapshot.queries_completed_total, 1);
    assert_eq!(released_snapshot.active_queries, 0);
    assert_eq!(released_snapshot.shared_reserved_memory_bytes, 0);

    async_storage.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_completed_async_scan_series_rows_future_releases_query_resources() -> Result<()> {
    let inner = build_row_scan_storage(guarded_row_scan_limits())?;
    let storage = Arc::new(RowScanTestStorage::new(
        Arc::clone(&inner),
        true,
        QueryExecutionAccounting::Complete,
        true,
    ));
    let async_storage = AsyncStorage::from_storage_with_options(
        storage.clone() as Arc<dyn Storage>,
        AsyncRuntimeOptions {
            read_workers: 1,
            ..AsyncRuntimeOptions::default()
        },
    )?;
    let mut release_guard = DetailedScanReleaseGuard::new(Arc::clone(&storage));

    let mut held_reply =
        Box::pin(async_storage.scan_series_rows(row_scan_series(), 0, 2, row_scan_options()));
    poll_once_pending(held_reply.as_mut()).await;
    tokio::time::timeout(
        Duration::from_secs(2),
        storage.detailed_scan_started.notified(),
    )
    .await
    .expect("the guarded scan must reach the backend");
    release_guard.release();

    let error = tokio::time::timeout(
        Duration::from_secs(2),
        async_storage.scan_series_rows(row_scan_series(), 0, 2, row_scan_options()),
    )
    .await
    .expect("the worker must buffer the first reply and process the second scan")
    .expect_err("the buffered reply must continue owning the only query slot");
    assert_concurrent_query_rejection(error);
    let held_snapshot = inner.query_budget_snapshot();
    assert_eq!(held_snapshot.active_queries, 1);
    assert!(held_snapshot.shared_reserved_memory_bytes > 0);

    drop(held_reply);
    wait_for_query_resources_to_release(&inner).await;
    let released_snapshot = inner.query_budget_snapshot();
    assert_eq!(released_snapshot.queries_started_total, 1);
    assert_eq!(released_snapshot.queries_completed_total, 1);
    assert_eq!(released_snapshot.active_queries, 0);
    assert_eq!(released_snapshot.shared_reserved_memory_bytes, 0);

    async_storage.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn budgeted_async_scan_series_rows_rejects_unaccounted_backends() -> Result<()> {
    let inner = build_row_scan_storage(guarded_row_scan_limits())?;
    let storage = Arc::new(RowScanTestStorage::new(
        Arc::clone(&inner),
        true,
        QueryExecutionAccounting::Unaccounted,
        false,
    ));
    let async_storage = AsyncStorage::from_storage(storage.clone() as Arc<dyn Storage>)?;

    let error = async_storage
        .scan_series_rows(row_scan_series(), 0, 2, row_scan_options())
        .await
        .expect_err("a budgeted unaccounted backend must fail closed");
    assert!(matches!(
        error,
        TsinkError::UnsupportedOperation {
            operation: "async_scan_series_rows",
            reason,
        } if reason == "bounded async row scans require complete execution accounting"
    ));
    assert_eq!(storage.detailed_scan_calls.load(Ordering::SeqCst), 0);
    assert_eq!(storage.compatibility_scan_calls.load(Ordering::SeqCst), 0);

    wait_for_query_resources_to_release(&inner).await;
    let snapshot = inner.query_budget_snapshot();
    assert_eq!(snapshot.queries_started_total, 1);
    assert_eq!(snapshot.queries_completed_total, 1);
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.shared_reserved_memory_bytes, 0);

    async_storage.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unlimited_async_scan_series_rows_preserves_unaccounted_backend_compatibility() -> Result<()>
{
    let inner = build_row_scan_storage(QueryBudgetLimits::default())?;
    let storage = Arc::new(RowScanTestStorage::new(
        Arc::clone(&inner),
        false,
        QueryExecutionAccounting::Unaccounted,
        false,
    ));
    let async_storage = AsyncStorage::from_storage(storage.clone() as Arc<dyn Storage>)?;

    let page = async_storage
        .scan_series_rows(row_scan_series(), 0, 2, row_scan_options())
        .await?;
    assert_eq!(page.rows.len(), 1);
    assert_eq!(storage.detailed_scan_calls.load(Ordering::SeqCst), 0);
    assert_eq!(storage.compatibility_scan_calls.load(Ordering::SeqCst), 1);

    async_storage.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_scan_metric_rows_holds_query_resources_until_reply_receipt() -> Result<()> {
    let inner = build_row_scan_storage(guarded_row_scan_limits())?;
    let storage = Arc::new(RowScanTestStorage::new(
        Arc::clone(&inner),
        true,
        QueryExecutionAccounting::Complete,
        true,
    ));
    let async_storage = AsyncStorage::from_storage_with_options(
        storage.clone() as Arc<dyn Storage>,
        AsyncRuntimeOptions {
            read_workers: 1,
            ..AsyncRuntimeOptions::default()
        },
    )?;
    let mut release_guard = DetailedScanReleaseGuard::new(Arc::clone(&storage));

    let mut held_reply =
        Box::pin(async_storage.scan_metric_rows("async_rows", 0, 2, row_scan_options()));
    poll_once_pending(held_reply.as_mut()).await;
    tokio::time::timeout(
        Duration::from_secs(2),
        storage.detailed_scan_started.notified(),
    )
    .await
    .expect("the guarded metric scan must reach the backend");
    release_guard.release();

    let error = tokio::time::timeout(
        Duration::from_secs(2),
        async_storage.scan_metric_rows("async_rows", 0, 2, row_scan_options()),
    )
    .await
    .expect("the worker must process a metric scan behind the completed held reply")
    .expect_err("the held metric reply must continue owning the only query slot");
    assert_concurrent_query_rejection(error);

    let held_snapshot = inner.query_budget_snapshot();
    assert_eq!(held_snapshot.active_queries, 1);
    assert!(held_snapshot.shared_reserved_memory_bytes > 0);
    assert_eq!(held_snapshot.queries_started_total, 1);
    assert_eq!(held_snapshot.queries_completed_total, 0);
    assert_eq!(held_snapshot.concurrency_rejections_total, 1);
    assert_eq!(storage.detailed_scan_calls.load(Ordering::SeqCst), 1);
    assert_eq!(storage.compatibility_scan_calls.load(Ordering::SeqCst), 0);

    let page = held_reply.await?;
    assert_eq!(page.rows.len(), 1);
    wait_for_query_resources_to_release(&inner).await;
    let released_snapshot = inner.query_budget_snapshot();
    assert_eq!(released_snapshot.queries_started_total, 1);
    assert_eq!(released_snapshot.queries_completed_total, 1);
    assert_eq!(released_snapshot.active_queries, 0);
    assert_eq!(released_snapshot.shared_reserved_memory_bytes, 0);

    async_storage.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_completed_async_scan_metric_rows_future_releases_query_resources() -> Result<()> {
    let inner = build_row_scan_storage(guarded_row_scan_limits())?;
    let storage = Arc::new(RowScanTestStorage::new(
        Arc::clone(&inner),
        true,
        QueryExecutionAccounting::Complete,
        true,
    ));
    let async_storage = AsyncStorage::from_storage_with_options(
        storage.clone() as Arc<dyn Storage>,
        AsyncRuntimeOptions {
            read_workers: 1,
            ..AsyncRuntimeOptions::default()
        },
    )?;
    let mut release_guard = DetailedScanReleaseGuard::new(Arc::clone(&storage));

    let mut held_reply =
        Box::pin(async_storage.scan_metric_rows("async_rows", 0, 2, row_scan_options()));
    poll_once_pending(held_reply.as_mut()).await;
    tokio::time::timeout(
        Duration::from_secs(2),
        storage.detailed_scan_started.notified(),
    )
    .await
    .expect("the guarded metric scan must reach the backend");
    release_guard.release();

    let error = tokio::time::timeout(
        Duration::from_secs(2),
        async_storage.scan_metric_rows("async_rows", 0, 2, row_scan_options()),
    )
    .await
    .expect("the worker must buffer the first metric reply and process the second scan")
    .expect_err("the buffered metric reply must continue owning the only query slot");
    assert_concurrent_query_rejection(error);
    let held_snapshot = inner.query_budget_snapshot();
    assert_eq!(held_snapshot.active_queries, 1);
    assert!(held_snapshot.shared_reserved_memory_bytes > 0);

    drop(held_reply);
    wait_for_query_resources_to_release(&inner).await;
    let released_snapshot = inner.query_budget_snapshot();
    assert_eq!(released_snapshot.queries_started_total, 1);
    assert_eq!(released_snapshot.queries_completed_total, 1);
    assert_eq!(released_snapshot.active_queries, 0);
    assert_eq!(released_snapshot.shared_reserved_memory_bytes, 0);

    async_storage.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn budgeted_async_scan_metric_rows_rejects_unaccounted_backends() -> Result<()> {
    let inner = build_row_scan_storage(guarded_row_scan_limits())?;
    let storage = Arc::new(RowScanTestStorage::new(
        Arc::clone(&inner),
        true,
        QueryExecutionAccounting::Unaccounted,
        false,
    ));
    let async_storage = AsyncStorage::from_storage(storage.clone() as Arc<dyn Storage>)?;

    let error = async_storage
        .scan_metric_rows("async_rows", 0, 2, row_scan_options())
        .await
        .expect_err("a budgeted unaccounted metric backend must fail closed");
    assert!(matches!(
        error,
        TsinkError::UnsupportedOperation {
            operation: "async_scan_metric_rows",
            reason,
        } if reason == "bounded async row scans require complete execution accounting"
    ));
    assert_eq!(storage.detailed_scan_calls.load(Ordering::SeqCst), 0);
    assert_eq!(storage.compatibility_scan_calls.load(Ordering::SeqCst), 0);

    wait_for_query_resources_to_release(&inner).await;
    let snapshot = inner.query_budget_snapshot();
    assert_eq!(snapshot.queries_started_total, 1);
    assert_eq!(snapshot.queries_completed_total, 1);
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.shared_reserved_memory_bytes, 0);

    async_storage.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn budgeted_async_scan_metric_rows_rejects_a_missing_result_reservation() -> Result<()> {
    let inner = build_row_scan_storage(guarded_row_scan_limits())?;
    let storage = Arc::new(
        RowScanTestStorage::new(
            Arc::clone(&inner),
            true,
            QueryExecutionAccounting::Complete,
            false,
        )
        .with_missing_detailed_scan_reservation(),
    );
    let async_storage = AsyncStorage::from_storage(storage.clone() as Arc<dyn Storage>)?;

    let error = async_storage
        .scan_metric_rows("async_rows", 0, 2, row_scan_options())
        .await
        .expect_err("a falsely complete metric backend must fail closed");
    assert!(matches!(
        error,
        TsinkError::UnsupportedOperation {
            operation: "async_scan_metric_rows",
            reason,
        } if reason == "bounded async row scans require complete execution accounting"
    ));
    assert_eq!(storage.detailed_scan_calls.load(Ordering::SeqCst), 1);
    assert_eq!(storage.compatibility_scan_calls.load(Ordering::SeqCst), 0);

    wait_for_query_resources_to_release(&inner).await;
    let snapshot = inner.query_budget_snapshot();
    assert_eq!(snapshot.queries_started_total, 1);
    assert_eq!(snapshot.queries_completed_total, 1);
    assert_eq!(snapshot.active_queries, 0);
    assert_eq!(snapshot.shared_reserved_memory_bytes, 0);

    async_storage.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unlimited_async_scan_metric_rows_preserves_unaccounted_backend_compatibility() -> Result<()>
{
    let inner = build_row_scan_storage(QueryBudgetLimits::default())?;
    let storage = Arc::new(RowScanTestStorage::new(
        Arc::clone(&inner),
        false,
        QueryExecutionAccounting::Unaccounted,
        false,
    ));
    let async_storage = AsyncStorage::from_storage(storage.clone() as Arc<dyn Storage>)?;

    let page = async_storage
        .scan_metric_rows("async_rows", 0, 2, row_scan_options())
        .await?;
    assert_eq!(page.rows.len(), 1);
    assert_eq!(storage.detailed_scan_calls.load(Ordering::SeqCst), 0);
    assert_eq!(storage.compatibility_scan_calls.load(Ordering::SeqCst), 1);

    async_storage.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn effective_storage_limits_match_the_built_backend() -> Result<()> {
    let storage = AsyncStorageBuilder::new()
        .with_wal_enabled(false)
        .with_memory_limit(8 * 1024 * 1024)
        .with_cardinality_limit(512)
        .with_max_labels_per_series(9)
        .with_max_series_identity_bytes(2_048)
        .with_series_creation_rate_limit(13, Duration::from_secs(2))
        .with_write_batch_limits(WriteBatchLimits {
            max_rows: Some(17),
            max_modeled_input_bytes: Some(4_096),
        })
        .with_max_writers(2)
        .with_write_timeout(Duration::from_millis(23))
        .with_max_active_partition_heads_per_series(3)
        .build()?;

    let limits = storage.effective_storage_limits();
    assert!(limits.reported_by_backend);
    assert!(!limits.persistent);
    assert!(!limits.wal_enabled);
    assert_eq!(limits.accounted_memory_bytes, Some(8 * 1024 * 1024));
    assert_eq!(limits.cardinality, Some(512));
    assert_eq!(limits.max_labels_per_series, Some(9));
    assert_eq!(limits.max_series_identity_bytes, Some(2_048));
    assert_eq!(limits.max_new_series_per_window, Some(13));
    assert_eq!(limits.new_series_window_nanos, Some(2_000_000_000));
    assert_eq!(limits.max_write_batch_rows, Some(17));
    assert_eq!(limits.max_write_batch_input_bytes, Some(4_096));
    assert_eq!(limits.wal_bytes, None);
    assert_eq!(limits.local_disk_bytes, None);
    assert!(storage.observability_snapshot().local_disk.is_none());
    assert_eq!(limits.max_concurrent_writers, Some(2));
    assert_eq!(limits.write_timeout_nanos, Some(23_000_000));
    assert_eq!(limits.max_active_partition_heads_per_series, Some(3));

    storage.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_best_effort_cannot_bypass_top_level_write_batch_limit() -> Result<()> {
    let storage = AsyncStorageBuilder::new()
        .with_write_batch_limits(WriteBatchLimits {
            max_rows: Some(2),
            max_modeled_input_bytes: None,
        })
        .build()?;

    let err = storage
        .write_batch(
            vec![
                Row::new("async_batch_limit", DataPoint::new(1, 1.0)),
                Row::new("async_batch_limit", DataPoint::new(2, 2.0)),
                Row::new("async_batch_limit", DataPoint::new(3, 3.0)),
            ],
            WriteMode::BestEffort,
        )
        .await
        .unwrap_err();
    assert!(matches!(
        err,
        TsinkError::WriteBatchRowLimitExceeded {
            limit: 2,
            submitted: 3
        }
    ));
    assert!(storage
        .select("async_batch_limit", vec![], 0, 4)
        .await?
        .is_empty());

    storage.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_builder_reports_configured_local_disk_budget() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let storage = AsyncStorageBuilder::new()
        .with_data_path(dir.path())
        .with_wal_enabled(false)
        .with_local_disk_limit(8 * 1024 * 1024)
        .with_filesystem_free_headroom(1024)
        .with_maintenance_temp_reserve(4096)
        .build()?;

    let limits = storage.effective_storage_limits();
    assert_eq!(limits.local_disk_bytes, Some(8 * 1024 * 1024));
    assert_eq!(limits.filesystem_free_headroom_bytes, Some(1024));
    assert_eq!(limits.maintenance_temp_reserve_bytes, Some(4096));
    let disk = storage
        .observability_snapshot()
        .local_disk
        .expect("persistent async storage should report the core disk coordinator");
    assert_eq!(disk.limits.max_bytes, Some(8 * 1024 * 1024));
    assert_eq!(disk.active_reservations, 0);

    storage.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn insert_rows_with_result_reports_periodic_acknowledgement() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let storage = AsyncStorageBuilder::new()
        .with_data_path(dir.path())
        .with_wal_sync_mode(WalSyncMode::Periodic(Duration::from_secs(3600)))
        .build()?;

    let result = storage
        .insert_rows_with_result(vec![Row::new("periodic_async_ack", DataPoint::new(1, 1.0))])
        .await?;

    assert_eq!(result.acknowledgement, WriteAcknowledgement::Appended);
    assert!(!result.is_durable());

    storage.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_batch_atomic_invalid_middle_reports_no_acceptance() -> Result<()> {
    let storage = AsyncStorageBuilder::new().build()?;

    let result = storage
        .write_batch(
            vec![
                Row::new("async_atomic_first", DataPoint::new(1, 1.0)),
                Row::new("", DataPoint::new(2, 2.0)),
                Row::new("async_atomic_last", DataPoint::new(3, 3.0)),
            ],
            WriteMode::Atomic,
        )
        .await?;

    assert_eq!(result.submitted, 3);
    assert_eq!(result.accepted, 0);
    assert_eq!(result.rejected, 3);
    assert_eq!(result.acknowledgement, None);
    assert_eq!(
        result
            .outcomes
            .iter()
            .map(|outcome| outcome.index)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert!(result
        .outcomes
        .iter()
        .all(|outcome| matches!(outcome.status, RowWriteStatus::Rejected(_))));
    assert!(storage
        .select("async_atomic_first", vec![], 0, 10)
        .await?
        .is_empty());
    assert!(storage
        .select("async_atomic_last", vec![], 0, 10)
        .await?
        .is_empty());

    storage.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_batch_reports_future_skew_rejections_through_async_facade() -> Result<()> {
    let storage = AsyncStorageBuilder::new()
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_max_future_skew(Duration::ZERO)
        .build()?;

    let result = storage
        .write_batch(
            vec![Row::new(
                "async_future_skew_rejection",
                DataPoint::new(i64::MAX, 1.0),
            )],
            WriteMode::Atomic,
        )
        .await?;

    assert_eq!(result.accepted, 0);
    assert_eq!(result.rejected, 1);
    assert_eq!(result.acknowledgement, None);
    assert!(matches!(
        &result.outcomes[0].status,
        RowWriteStatus::Rejected(rejection)
            if rejection.category == WriteRejectionCategory::FutureSkewExceeded
    ));

    storage.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_batch_best_effort_preserves_indexed_outcomes_and_acknowledgement() -> Result<()> {
    let storage = AsyncStorageBuilder::new().build()?;

    let result = storage
        .write_batch(
            vec![
                Row::new("async_best_effort_first", DataPoint::new(1, 1.0)),
                Row::new("", DataPoint::new(2, 2.0)),
                Row::new("async_best_effort_last", DataPoint::new(3, 3.0)),
            ],
            WriteMode::BestEffort,
        )
        .await?;

    assert_eq!(result.submitted, 3);
    assert_eq!(result.accepted, 2);
    assert_eq!(result.rejected, 1);
    assert_eq!(result.acknowledgement, Some(WriteAcknowledgement::Volatile));
    assert_eq!(result.outcomes.len(), 3);
    assert_eq!(result.outcomes[0].index, 0);
    assert!(matches!(
        result.outcomes[0].status,
        RowWriteStatus::Accepted
    ));
    assert_eq!(result.outcomes[1].index, 1);
    assert!(matches!(
        result.outcomes[1].status,
        RowWriteStatus::Rejected(_)
    ));
    assert_eq!(result.outcomes[2].index, 2);
    assert!(matches!(
        result.outcomes[2].status,
        RowWriteStatus::Accepted
    ));
    assert_eq!(
        storage
            .select("async_best_effort_first", vec![], 0, 10)
            .await?,
        vec![DataPoint::new(1, 1.0)]
    );
    assert_eq!(
        storage
            .select("async_best_effort_last", vec![], 0, 10)
            .await?,
        vec![DataPoint::new(3, 3.0)]
    );

    storage.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn labeled_queries_and_options_work() -> Result<()> {
    let storage = AsyncStorageBuilder::new().build()?;

    storage
        .insert_rows(vec![
            Row::with_labels(
                "http_requests",
                vec![Label::new("method", "GET"), Label::new("status", "200")],
                DataPoint::new(10, 100u64),
            ),
            Row::with_labels(
                "http_requests",
                vec![Label::new("method", "GET"), Label::new("status", "200")],
                DataPoint::new(11, 120u64),
            ),
            Row::with_labels(
                "http_requests",
                vec![Label::new("method", "POST"), Label::new("status", "500")],
                DataPoint::new(10, 5u64),
            ),
        ])
        .await?;

    let opts = QueryOptions::new(0, 100)
        .with_labels(vec![
            Label::new("method", "GET"),
            Label::new("status", "200"),
        ])
        .with_aggregation(Aggregation::Count);

    let count = storage.select_with_options("http_requests", opts).await?;
    assert_eq!(count.len(), 1);
    assert_eq!(count[0].value.as_u64(), Some(2));

    let all = storage.select_all("http_requests", 0, 100).await?;
    assert_eq!(all.len(), 2);

    let metrics = storage.list_metrics().await?;
    assert_eq!(metrics.len(), 2);

    storage.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn close_then_operations_error_with_storage_closed() -> Result<()> {
    let storage = AsyncStorageBuilder::new().build()?;
    storage
        .insert_rows(vec![Row::new("closed_metric", DataPoint::new(1, 1.0))])
        .await?;

    storage.close().await?;

    assert!(matches!(
        storage
            .insert_rows(vec![Row::new("closed_metric", DataPoint::new(2, 2.0))])
            .await,
        Err(TsinkError::StorageClosed)
    ));
    assert!(matches!(
        storage.write_batch(Vec::new(), WriteMode::Atomic).await,
        Err(TsinkError::StorageClosed)
    ));
    for mode in [WriteMode::Atomic, WriteMode::BestEffort] {
        let result = storage
            .write_batch(
                vec![Row::new("closed_canonical_write", DataPoint::new(1, 1.0))],
                mode,
            )
            .await?;
        assert_eq!(result.accepted, 0);
        assert_eq!(result.rejected, 1);
        assert_eq!(result.acknowledgement, None);
        assert!(matches!(
            &result.outcomes[0].status,
            RowWriteStatus::Rejected(rejection)
                if rejection.category == WriteRejectionCategory::StorageClosed
        ));
    }
    assert!(matches!(
        storage.select("closed_metric", vec![], 0, 10).await,
        Err(TsinkError::StorageClosed)
    ));
    assert!(matches!(
        storage.list_metrics().await,
        Err(TsinkError::StorageClosed)
    ));
    assert!(matches!(
        storage
            .apply_rollup_policies(vec![RollupPolicy {
                id: "closed_policy".to_string(),
                metric: "closed_metric".to_string(),
                match_labels: Vec::new(),
                interval: 60,
                aggregation: Aggregation::Avg,
                bucket_origin: 0,
            }])
            .await,
        Err(TsinkError::StorageClosed)
    ));
    assert!(matches!(
        storage.trigger_rollup_run().await,
        Err(TsinkError::StorageClosed)
    ));

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn persistent_storage_reopen_roundtrip() -> Result<()> {
    let dir = TempDir::new().unwrap();

    {
        let storage = AsyncStorageBuilder::new()
            .with_data_path(dir.path())
            .build()?;
        storage
            .insert_rows(vec![Row::new("persisted", DataPoint::new(1, 42.0))])
            .await?;
        storage.close().await?;
    }

    {
        let reopened = AsyncStorageBuilder::new()
            .with_data_path(dir.path())
            .build()?;
        let points = reopened.select("persisted", vec![], 0, 10).await?;
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].value_as_f64(), Some(42.0));
        reopened.close().await?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn wal_backed_async_pre_apply_batch_rejection_commits_none_across_reopen() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let rows = vec![
        Row::new("async_atomic_batch_first", DataPoint::new(1, 1.0)),
        Row::new("", DataPoint::new(2, 2.0)),
        Row::new("async_atomic_batch_last", DataPoint::new(3, 3.0)),
    ];

    {
        let storage = AsyncStorageBuilder::new()
            .with_data_path(dir.path())
            .with_wal_enabled(true)
            .with_wal_sync_mode(WalSyncMode::PerAppend)
            .build()?;

        let err = storage.insert_rows(rows).await.unwrap_err();
        assert!(matches!(err, TsinkError::MetricRequired));
        assert!(storage
            .select("async_atomic_batch_first", vec![], 0, 10)
            .await?
            .is_empty());
        assert!(storage
            .select("async_atomic_batch_last", vec![], 0, 10)
            .await?
            .is_empty());
        assert!(storage.list_metrics().await?.is_empty());
        assert!(storage.inner().list_metrics_with_wal()?.is_empty());

        storage.close().await?;
    }

    let reopened = AsyncStorageBuilder::new()
        .with_data_path(dir.path())
        .with_wal_enabled(true)
        .with_wal_sync_mode(WalSyncMode::PerAppend)
        .build()?;

    assert!(reopened
        .select("async_atomic_batch_first", vec![], 0, 10)
        .await?
        .is_empty());
    assert!(reopened
        .select("async_atomic_batch_last", vec![], 0, 10)
        .await?
        .is_empty());
    assert!(reopened.list_metrics().await?.is_empty());
    assert!(reopened.inner().list_metrics_with_wal()?.is_empty());
    reopened.close().await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn async_snapshot_restore_roundtrip() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let source_path = dir.path().join("source");
    let snapshot_path = dir.path().join("snapshot");
    let restored_path = dir.path().join("restored");

    {
        let storage = AsyncStorageBuilder::new()
            .with_data_path(&source_path)
            .build()?;
        storage
            .insert_rows(vec![Row::new("snapshot_async", DataPoint::new(1, 42.0))])
            .await?;
        storage.snapshot(&snapshot_path).await?;
        storage.close().await?;
    }

    StorageBuilder::restore_from_snapshot(&snapshot_path, &restored_path)?;

    {
        let restored = AsyncStorageBuilder::new()
            .with_data_path(&restored_path)
            .build()?;
        let points = restored.select("snapshot_async", vec![], 0, 10).await?;
        assert_eq!(points, vec![DataPoint::new(1, 42.0)]);
        restored.close().await?;
    }

    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn async_rollup_policy_management_roundtrip() -> Result<()> {
    let dir = TempDir::new().unwrap();
    let labels = vec![Label::new("host", "a")];
    let storage = AsyncStorageBuilder::new()
        .with_data_path(dir.path())
        .with_timestamp_precision(TimestampPrecision::Milliseconds)
        .build()?;

    storage
        .insert_rows(vec![
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(0, 1.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(1_000, 2.0)),
            Row::with_labels("cpu_usage", labels.clone(), DataPoint::new(2_000, 3.0)),
        ])
        .await?;

    let snapshot = storage
        .apply_rollup_policies(vec![RollupPolicy {
            id: "cpu_1s_avg".to_string(),
            metric: "cpu_usage".to_string(),
            match_labels: Vec::new(),
            interval: 1_000,
            aggregation: Aggregation::Avg,
            bucket_origin: 0,
        }])
        .await?;
    assert_eq!(snapshot.policies.len(), 1);
    assert_eq!(snapshot.policies[0].policy.id, "cpu_1s_avg");
    assert_eq!(snapshot.policies[0].matched_series, 1);
    assert_eq!(snapshot.policies[0].materialized_series, 1);
    assert_eq!(snapshot.policies[0].materialized_through, Some(2_000));

    storage
        .insert_rows(vec![Row::with_labels(
            "cpu_usage",
            labels.clone(),
            DataPoint::new(3_000, 4.0),
        )])
        .await?;

    let rerun = storage.trigger_rollup_run().await?;
    assert_eq!(rerun.policies.len(), 1);
    assert_eq!(rerun.policies[0].materialized_through, Some(3_000));

    let points = storage
        .select_with_options(
            "cpu_usage",
            QueryOptions::new(0, 4_000)
                .with_labels(labels)
                .with_downsample(1_000, Aggregation::Avg),
        )
        .await?;
    assert_eq!(
        points,
        vec![
            DataPoint::new(0, 1.0),
            DataPoint::new(1_000, 2.0),
            DataPoint::new(2_000, 3.0),
            DataPoint::new(3_000, 4.0),
        ]
    );

    storage.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writes_from_tokio_tasks() -> Result<()> {
    let storage = AsyncStorageBuilder::new().with_read_workers(2).build()?;

    let mut tasks = Vec::new();
    for i in 0..32_i64 {
        let storage = storage.clone();
        tasks.push(tokio::spawn(async move {
            storage
                .insert_rows(vec![Row::new("concurrent", DataPoint::new(i, i))])
                .await
        }));
    }

    for task in tasks {
        task.await.expect("join should succeed")?;
    }

    let points = storage.select("concurrent", vec![], 0, 100).await?;
    assert_eq!(points.len(), 32);

    storage.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn can_wrap_existing_storage_arc() -> Result<()> {
    let sync_storage = StorageBuilder::new().build()?;
    let async_storage = AsyncStorage::from_storage(Arc::clone(&sync_storage))?;

    async_storage
        .insert_rows(vec![Row::new("wrapped", DataPoint::new(1, 7.0))])
        .await?;

    let points = async_storage.select("wrapped", vec![], 0, 10).await?;
    assert_eq!(points.len(), 1);
    assert_eq!(points[0].value_as_f64(), Some(7.0));

    async_storage.close().await?;
    assert!(matches!(
        sync_storage.list_metrics(),
        Err(TsinkError::StorageClosed)
    ));

    Ok(())
}

struct BlockingInsertStorage {
    inserted: Mutex<Vec<Row>>,
    insert_calls: AtomicUsize,
    block_inserts: AtomicBool,
    insert_started: Notify,
    release_insert: Condvar,
    release_flag: Mutex<bool>,
}

impl BlockingInsertStorage {
    fn new() -> Self {
        Self {
            inserted: Mutex::new(Vec::new()),
            insert_calls: AtomicUsize::new(0),
            block_inserts: AtomicBool::new(false),
            insert_started: Notify::new(),
            release_insert: Condvar::new(),
            release_flag: Mutex::new(false),
        }
    }
}

impl Storage for BlockingInsertStorage {
    fn insert_rows(&self, rows: &[Row]) -> Result<()> {
        self.insert_calls.fetch_add(1, Ordering::SeqCst);
        self.insert_started.notify_one();

        if self.block_inserts.load(Ordering::SeqCst) {
            let mut released = self.release_flag.lock();
            while !*released {
                self.release_insert.wait(&mut released);
            }
        }

        self.inserted.lock().extend_from_slice(rows);
        Ok(())
    }

    fn write_batch(&self, rows: &[Row], _mode: WriteMode) -> Result<BatchWriteResult> {
        self.insert_rows(rows)?;
        Ok(BatchWriteResult::from_outcomes(
            (!rows.is_empty()).then_some(WriteAcknowledgement::Volatile),
            (0..rows.len()).map(RowWriteOutcome::accepted).collect(),
        ))
    }

    fn select(
        &self,
        _metric: &str,
        _labels: &[Label],
        _start: i64,
        _end: i64,
    ) -> Result<Vec<DataPoint>> {
        Ok(Vec::new())
    }

    fn select_with_options(&self, _metric: &str, _opts: QueryOptions) -> Result<Vec<DataPoint>> {
        Ok(Vec::new())
    }

    fn select_all(
        &self,
        _metric: &str,
        _start: i64,
        _end: i64,
    ) -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
        Ok(Vec::new())
    }

    fn list_metrics(&self) -> Result<Vec<tsink::MetricSeries>> {
        Ok(Vec::new())
    }

    fn close(&self) -> Result<()> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canceled_insert_still_commits_after_queue_accept() -> Result<()> {
    let storage = Arc::new(BlockingInsertStorage::new());
    storage.block_inserts.store(true, Ordering::SeqCst);

    let async_storage = AsyncStorage::from_storage(storage.clone() as Arc<dyn Storage>)?;
    let write_task = tokio::spawn({
        let async_storage = async_storage.clone();
        async move {
            async_storage
                .insert_rows(vec![Row::new("cancelled_write", DataPoint::new(1, 10u64))])
                .await
        }
    });

    storage.insert_started.notified().await;

    write_task.abort();
    let _ = write_task.await;

    storage.block_inserts.store(false, Ordering::SeqCst);
    {
        let mut released = storage.release_flag.lock();
        *released = true;
    }
    storage.release_insert.notify_all();

    tokio::time::sleep(Duration::from_millis(25)).await;

    assert_eq!(storage.insert_calls.load(Ordering::SeqCst), 1);
    assert_eq!(storage.inserted.lock().len(), 1);

    async_storage.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn canceled_write_batch_still_commits_after_queue_accept() -> Result<()> {
    let storage = Arc::new(BlockingInsertStorage::new());
    storage.block_inserts.store(true, Ordering::SeqCst);

    let async_storage = AsyncStorage::from_storage(storage.clone() as Arc<dyn Storage>)?;
    let write_task = tokio::spawn({
        let async_storage = async_storage.clone();
        async move {
            async_storage
                .write_batch(
                    vec![
                        Row::new("cancelled_batch_write", DataPoint::new(1, 10u64)),
                        Row::new("cancelled_batch_write", DataPoint::new(2, 20u64)),
                    ],
                    WriteMode::Atomic,
                )
                .await
        }
    });

    storage.insert_started.notified().await;

    write_task.abort();
    let _ = write_task.await;

    storage.block_inserts.store(false, Ordering::SeqCst);
    {
        let mut released = storage.release_flag.lock();
        *released = true;
    }
    storage.release_insert.notify_all();

    tokio::time::sleep(Duration::from_millis(25)).await;

    assert_eq!(storage.insert_calls.load(Ordering::SeqCst), 1);
    assert_eq!(storage.inserted.lock().len(), 2);

    async_storage.close().await?;
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_queue_bytes_admit_n_reject_n_plus_one_and_drain_through_close() -> Result<()> {
    let storage = Arc::new(BlockingInsertStorage::new());
    storage.block_inserts.store(true, Ordering::SeqCst);

    let exact_rows = vec![Row::new("q", DataPoint::new(2, 2.0))];
    let exact_bytes = tsink::modeled_write_batch_input_bytes(&exact_rows)?;
    let oversized_rows = vec![Row::new("qq", DataPoint::new(3, 3.0))];
    assert_eq!(
        tsink::modeled_write_batch_input_bytes(&oversized_rows)?,
        exact_bytes + 1
    );

    let async_storage = AsyncStorage::from_storage_with_options(
        storage.clone() as Arc<dyn Storage>,
        AsyncRuntimeOptions {
            queue_capacity: 8,
            write_queue_byte_capacity: exact_bytes,
            ..AsyncRuntimeOptions::default()
        },
    )?;

    let first_task = tokio::spawn({
        let async_storage = async_storage.clone();
        async move {
            async_storage
                .insert_rows(vec![Row::new("q", DataPoint::new(1, 1.0))])
                .await
        }
    });
    storage.insert_started.notified().await;

    let exact_task = tokio::spawn({
        let async_storage = async_storage.clone();
        async move { async_storage.insert_rows(exact_rows).await }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if async_storage
                .async_runtime_snapshot()
                .current_write_queue_bytes
                == exact_bytes
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the exact-size write must wait in the queue");

    let error = async_storage
        .insert_rows(oversized_rows)
        .await
        .expect_err("N+1 queued bytes must be rejected before enqueue");
    assert!(matches!(
        error,
        TsinkError::AsyncQueueByteLimitExceeded {
            queue: "write",
            limit,
            current,
            requested,
        } if limit == exact_bytes && current == exact_bytes && requested == exact_bytes + 1
    ));

    let close_task = tokio::spawn({
        let async_storage = async_storage.clone();
        async move { async_storage.close().await }
    });

    storage.block_inserts.store(false, Ordering::SeqCst);
    *storage.release_flag.lock() = true;
    storage.release_insert.notify_all();

    first_task.await.expect("first write task must join")?;
    exact_task.await.expect("exact-size write task must join")?;
    close_task.await.expect("close task must join")?;

    let snapshot = async_storage.async_runtime_snapshot();
    assert_eq!(snapshot.write_queue_byte_capacity, exact_bytes);
    assert_eq!(snapshot.peak_write_queue_bytes, exact_bytes);
    assert_eq!(snapshot.current_write_queue_bytes, 0);
    assert_eq!(snapshot.write_queue_depth, 0);
    assert_eq!(snapshot.write_queue_byte_rejections_total, 1);
    assert_eq!(snapshot.write_workers, 1);
    Ok(())
}

struct CancellableReadStorage {
    query_budget: QueryBudget,
    select_calls: AtomicUsize,
    read_started: Notify,
    read_finished: Notify,
}

impl CancellableReadStorage {
    fn new() -> Self {
        Self {
            query_budget: QueryBudget::new(ResourceLimits::test().query).unwrap(),
            select_calls: AtomicUsize::new(0),
            read_started: Notify::new(),
            read_finished: Notify::new(),
        }
    }
}

impl Storage for CancellableReadStorage {
    fn query_budget(&self) -> Option<QueryBudget> {
        Some(self.query_budget.clone())
    }

    fn insert_rows(&self, _rows: &[Row]) -> Result<()> {
        Ok(())
    }

    fn select(
        &self,
        _metric: &str,
        _labels: &[Label],
        _start: i64,
        _end: i64,
    ) -> Result<Vec<DataPoint>> {
        Err(TsinkError::Other(
            "async reader did not propagate its query execution".to_string(),
        ))
    }

    fn select_with_execution(
        &self,
        _metric: &str,
        _labels: &[Label],
        _start: i64,
        _end: i64,
        execution: &QueryExecution,
    ) -> Result<Vec<DataPoint>> {
        self.select_calls.fetch_add(1, Ordering::SeqCst);
        self.read_started.notify_one();
        loop {
            if let Err(error) = execution.checkpoint() {
                self.read_finished.notify_one();
                return Err(error.into());
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    fn select_with_options(&self, _metric: &str, _opts: QueryOptions) -> Result<Vec<DataPoint>> {
        Ok(Vec::new())
    }

    fn select_all(
        &self,
        _metric: &str,
        _start: i64,
        _end: i64,
    ) -> Result<Vec<(Vec<Label>, Vec<DataPoint>)>> {
        Ok(Vec::new())
    }

    fn list_metrics(&self) -> Result<Vec<tsink::MetricSeries>> {
        Ok(Vec::new())
    }

    fn close(&self) -> Result<()> {
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropped_read_futures_cancel_running_work_and_release_queued_bytes() -> Result<()> {
    const EXACT_BYTES: usize = 4;
    let storage = Arc::new(CancellableReadStorage::new());
    let async_storage = AsyncStorage::from_storage_with_options(
        storage.clone() as Arc<dyn Storage>,
        AsyncRuntimeOptions {
            queue_capacity: 8,
            read_queue_byte_capacity: EXACT_BYTES,
            read_workers: 1,
            ..AsyncRuntimeOptions::default()
        },
    )?;

    let running_task = tokio::spawn({
        let async_storage = async_storage.clone();
        async move { async_storage.select("hold", vec![], 0, 1).await }
    });
    storage.read_started.notified().await;

    let queued_task = tokio::spawn({
        let async_storage = async_storage.clone();
        async move { async_storage.select("read", vec![], 0, 1).await }
    });
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if async_storage
                .async_runtime_snapshot()
                .current_read_queue_bytes
                == EXACT_BYTES
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the exact-size read must wait behind the running read");

    let error = async_storage
        .select("reads", vec![], 0, 1)
        .await
        .expect_err("N+1 queued read bytes must be rejected");
    assert!(matches!(
        error,
        TsinkError::AsyncQueueByteLimitExceeded {
            queue: "read",
            limit: EXACT_BYTES,
            current: EXACT_BYTES,
            requested: 5,
        }
    ));

    queued_task.abort();
    let _ = queued_task.await;
    running_task.abort();
    let _ = running_task.await;

    tokio::time::timeout(Duration::from_secs(2), storage.read_finished.notified())
        .await
        .expect("dropping the running future must cancel its query execution");
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let snapshot = async_storage.async_runtime_snapshot();
            if snapshot.current_read_queue_bytes == 0 && snapshot.read_queue_depth == 0 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("the canceled queued command must be drained and released");

    let query_snapshot = storage.query_budget.snapshot();
    assert_eq!(query_snapshot.limits, ResourceLimits::test().query);
    assert_eq!(query_snapshot.active_queries, 0);
    assert_eq!(query_snapshot.shared_reserved_memory_bytes, 0);
    assert_eq!(query_snapshot.queries_started_total, 1);
    assert_eq!(query_snapshot.queries_completed_total, 1);
    assert_eq!(query_snapshot.cancellations_total, 1);
    assert_eq!(query_snapshot.accounting_invariant_violations_total, 0);
    assert_eq!(storage.select_calls.load(Ordering::SeqCst), 1);

    let snapshot = async_storage.async_runtime_snapshot();
    assert_eq!(snapshot.read_queue_byte_capacity, EXACT_BYTES);
    assert_eq!(snapshot.peak_read_queue_bytes, EXACT_BYTES);
    assert_eq!(snapshot.current_read_queue_bytes, 0);
    assert_eq!(snapshot.read_queue_byte_rejections_total, 1);
    assert_eq!(snapshot.read_workers, 1);

    async_storage.close().await?;
    Ok(())
}
