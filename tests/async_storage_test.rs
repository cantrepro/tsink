use parking_lot::{Condvar, Mutex};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Notify;

use tempfile::TempDir;
use tsink::{
    Aggregation, AsyncStorage, AsyncStorageBuilder, DataPoint, Label, QueryOptions, Result, Row,
    Storage, StorageBuilder, TsinkError, WalSyncMode, WriteAcknowledgement,
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
        storage.select("closed_metric", vec![], 0, 10).await,
        Err(TsinkError::StorageClosed)
    ));
    assert!(matches!(
        storage.list_metrics().await,
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
