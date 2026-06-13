use std::fmt;
use std::sync::Arc;

use tsink::Storage;

use crate::error::{Result, TsinkUniFFIError};
use crate::query::{UQueryOptions, USeriesSelection};
use crate::types::{
    UDataPoint, UDeleteSeriesResult, ULabel, ULabeledDataPoints, UMetadataShardScope,
    UMetricSeries, UQueryRowsPage, UQueryRowsScanOptions, URollupObservabilitySnapshot,
    URollupPolicy, URow, USeriesPoints, UShardWindowDigest, UShardWindowRowsPage,
    UShardWindowScanOptions, UStorageObservabilitySnapshot, UWriteResult,
};

#[derive(uniffi::Object)]
pub struct TsinkDB {
    storage: Arc<dyn Storage>,
}

impl fmt::Debug for TsinkDB {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TsinkDB").finish_non_exhaustive()
    }
}

impl TsinkDB {
    pub fn from_storage(storage: Arc<dyn Storage>) -> Self {
        Self { storage }
    }
}

#[uniffi::export]
impl TsinkDB {
    pub fn insert_rows(&self, rows: Vec<URow>) -> Result<()> {
        let rows: Vec<tsink::Row> = rows.into_iter().map(Into::into).collect();
        self.storage
            .insert_rows(&rows)
            .map_err(TsinkUniFFIError::from)
    }

    pub fn insert_rows_with_result(&self, rows: Vec<URow>) -> Result<UWriteResult> {
        let rows: Vec<tsink::Row> = rows.into_iter().map(Into::into).collect();
        self.storage
            .insert_rows_with_result(&rows)
            .map(Into::into)
            .map_err(TsinkUniFFIError::from)
    }

    pub fn select(
        &self,
        metric: String,
        labels: Vec<ULabel>,
        start: i64,
        end: i64,
    ) -> Result<Vec<UDataPoint>> {
        let labels: Vec<tsink::Label> = labels.into_iter().map(Into::into).collect();
        self.storage
            .select(&metric, &labels, start, end)
            .map(|dps| dps.into_iter().map(Into::into).collect())
            .map_err(TsinkUniFFIError::from)
    }

    pub fn select_with_options(
        &self,
        metric: String,
        options: UQueryOptions,
    ) -> Result<Vec<UDataPoint>> {
        self.storage
            .select_with_options(&metric, options.into())
            .map(|dps| dps.into_iter().map(Into::into).collect())
            .map_err(TsinkUniFFIError::from)
    }

    pub fn select_all(
        &self,
        metric: String,
        start: i64,
        end: i64,
    ) -> Result<Vec<ULabeledDataPoints>> {
        self.storage
            .select_all(&metric, start, end)
            .map(|series| {
                series
                    .into_iter()
                    .map(|(labels, data_points)| ULabeledDataPoints {
                        labels: labels.into_iter().map(Into::into).collect(),
                        data_points: data_points.into_iter().map(Into::into).collect(),
                    })
                    .collect()
            })
            .map_err(TsinkUniFFIError::from)
    }

    pub fn select_many(
        &self,
        series: Vec<UMetricSeries>,
        start: i64,
        end: i64,
    ) -> Result<Vec<USeriesPoints>> {
        let series: Vec<tsink::MetricSeries> = series.into_iter().map(Into::into).collect();
        self.storage
            .select_many(&series, start, end)
            .map(|series_points| series_points.into_iter().map(Into::into).collect())
            .map_err(TsinkUniFFIError::from)
    }

    pub fn list_metrics(&self) -> Result<Vec<UMetricSeries>> {
        self.storage
            .list_metrics()
            .map(|ms| ms.into_iter().map(Into::into).collect())
            .map_err(TsinkUniFFIError::from)
    }

    pub fn list_metrics_with_wal(&self) -> Result<Vec<UMetricSeries>> {
        self.storage
            .list_metrics_with_wal()
            .map(|ms| ms.into_iter().map(Into::into).collect())
            .map_err(TsinkUniFFIError::from)
    }

    pub fn list_metrics_in_shards(&self, scope: UMetadataShardScope) -> Result<Vec<UMetricSeries>> {
        self.storage
            .list_metrics_in_shards(&scope.into())
            .map(|ms| ms.into_iter().map(Into::into).collect())
            .map_err(TsinkUniFFIError::from)
    }

    pub fn select_series(&self, selection: USeriesSelection) -> Result<Vec<UMetricSeries>> {
        self.storage
            .select_series(&selection.into())
            .map(|ms| ms.into_iter().map(Into::into).collect())
            .map_err(TsinkUniFFIError::from)
    }

    pub fn select_series_in_shards(
        &self,
        selection: USeriesSelection,
        scope: UMetadataShardScope,
    ) -> Result<Vec<UMetricSeries>> {
        self.storage
            .select_series_in_shards(&selection.into(), &scope.into())
            .map(|ms| ms.into_iter().map(Into::into).collect())
            .map_err(TsinkUniFFIError::from)
    }

    pub fn compute_shard_window_digest(
        &self,
        shard: u32,
        shard_count: u32,
        window_start: i64,
        window_end: i64,
    ) -> Result<UShardWindowDigest> {
        self.storage
            .compute_shard_window_digest(shard, shard_count, window_start, window_end)
            .map(Into::into)
            .map_err(TsinkUniFFIError::from)
    }

    pub fn scan_shard_window_rows(
        &self,
        shard: u32,
        shard_count: u32,
        window_start: i64,
        window_end: i64,
        options: UShardWindowScanOptions,
    ) -> Result<UShardWindowRowsPage> {
        self.storage
            .scan_shard_window_rows(shard, shard_count, window_start, window_end, options.into())
            .map(Into::into)
            .map_err(TsinkUniFFIError::from)
    }

    pub fn scan_series_rows(
        &self,
        series: Vec<UMetricSeries>,
        start: i64,
        end: i64,
        options: UQueryRowsScanOptions,
    ) -> Result<UQueryRowsPage> {
        let series: Vec<tsink::MetricSeries> = series.into_iter().map(Into::into).collect();
        self.storage
            .scan_series_rows(&series, start, end, options.into())
            .map(Into::into)
            .map_err(TsinkUniFFIError::from)
    }

    pub fn scan_metric_rows(
        &self,
        metric: String,
        start: i64,
        end: i64,
        options: UQueryRowsScanOptions,
    ) -> Result<UQueryRowsPage> {
        self.storage
            .scan_metric_rows(&metric, start, end, options.into())
            .map(Into::into)
            .map_err(TsinkUniFFIError::from)
    }

    pub fn delete_series(&self, selection: USeriesSelection) -> Result<UDeleteSeriesResult> {
        self.storage
            .delete_series(&selection.into())
            .map(Into::into)
            .map_err(TsinkUniFFIError::from)
    }

    pub fn memory_used(&self) -> u64 {
        self.storage.memory_used() as u64
    }

    pub fn memory_budget(&self) -> u64 {
        self.storage.memory_budget() as u64
    }

    pub fn observability_snapshot(&self) -> UStorageObservabilitySnapshot {
        self.storage.observability_snapshot().into()
    }

    pub fn apply_rollup_policies(
        &self,
        policies: Vec<URollupPolicy>,
    ) -> Result<URollupObservabilitySnapshot> {
        let policies: Vec<tsink::RollupPolicy> = policies.into_iter().map(Into::into).collect();
        self.storage
            .apply_rollup_policies(policies)
            .map(Into::into)
            .map_err(TsinkUniFFIError::from)
    }

    pub fn trigger_rollup_run(&self) -> Result<URollupObservabilitySnapshot> {
        self.storage
            .trigger_rollup_run()
            .map(Into::into)
            .map_err(TsinkUniFFIError::from)
    }

    pub fn snapshot(&self, path: String) -> Result<()> {
        self.storage
            .snapshot(std::path::Path::new(path.as_str()))
            .map_err(TsinkUniFFIError::from)
    }

    pub fn close(&self) -> Result<()> {
        self.storage.close().map_err(TsinkUniFFIError::from)
    }
}
