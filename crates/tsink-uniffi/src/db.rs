use std::fmt;
use std::sync::Arc;

use tsink::Storage;

use crate::error::{Result, TsinkUniFFIError};
use crate::query::{UQueryOptions, USeriesSelection};
use crate::types::{UDataPoint, ULabel, ULabeledDataPoints, UMetricSeries, URow};

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

    pub fn select_series(&self, selection: USeriesSelection) -> Result<Vec<UMetricSeries>> {
        self.storage
            .select_series(&selection.into())
            .map(|ms| ms.into_iter().map(Into::into).collect())
            .map_err(TsinkUniFFIError::from)
    }

    pub fn memory_used(&self) -> u64 {
        self.storage.memory_used() as u64
    }

    pub fn memory_budget(&self) -> u64 {
        self.storage.memory_budget() as u64
    }

    pub fn close(&self) -> Result<()> {
        self.storage.close().map_err(TsinkUniFFIError::from)
    }
}
