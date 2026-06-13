use crate::query_matcher::{compile_series_matchers, CompiledSeriesMatcher};
use crate::storage::Storage;
use crate::validation::validate_metric;
use crate::{MetricSeries, Result, SeriesSelection};

pub(crate) struct PreparedSeriesSelection {
    pub(crate) time_range: Option<(i64, i64)>,
    pub(crate) compiled_matchers: Vec<CompiledSeriesMatcher>,
}

pub(crate) trait SeriesSelectionBackend {
    type Candidate;

    fn candidate_items(
        &self,
        selection: &SeriesSelection,
        prepared: &PreparedSeriesSelection,
    ) -> Result<Vec<Self::Candidate>>;

    fn retain_items_in_time_range(
        &self,
        items: &mut Vec<Self::Candidate>,
        start: i64,
        end: i64,
    ) -> Result<()>;

    fn materialize_items(&self, items: Vec<Self::Candidate>) -> Result<Vec<MetricSeries>>;
}

pub(crate) fn validate_series_selection(selection: &SeriesSelection) -> Result<Option<(i64, i64)>> {
    if let Some(metric) = selection.metric.as_deref() {
        validate_metric(metric)?;
    }
    selection.normalized_time_range()
}

pub(crate) fn prepare_series_selection(
    selection: &SeriesSelection,
) -> Result<PreparedSeriesSelection> {
    let time_range = validate_series_selection(selection)?;
    let compiled_matchers = compile_series_matchers(&selection.matchers)?;
    Ok(PreparedSeriesSelection {
        time_range,
        compiled_matchers,
    })
}

fn series_matches_selection(
    entry: &MetricSeries,
    selection: &SeriesSelection,
    prepared: &PreparedSeriesSelection,
) -> bool {
    if selection
        .metric
        .as_ref()
        .is_some_and(|metric| entry.name != *metric)
    {
        return false;
    }

    prepared
        .compiled_matchers
        .iter()
        .all(|matcher| matcher.matches(&entry.name, &entry.labels))
}

fn retain_series_matching_selection(
    series: &mut Vec<MetricSeries>,
    selection: &SeriesSelection,
    prepared: &PreparedSeriesSelection,
) {
    series.retain(|entry| series_matches_selection(entry, selection, prepared));
}

pub(crate) fn execute_series_selection<B: SeriesSelectionBackend>(
    backend: &B,
    selection: &SeriesSelection,
) -> Result<Vec<MetricSeries>> {
    let prepared = prepare_series_selection(selection)?;
    let mut candidates = backend.candidate_items(selection, &prepared)?;

    if let Some((start, end)) = prepared.time_range {
        backend.retain_items_in_time_range(&mut candidates, start, end)?;
    }

    let mut series = backend.materialize_items(candidates)?;
    retain_series_matching_selection(&mut series, selection, &prepared);
    series.sort();
    Ok(series)
}

struct ScanSeriesSelectionBackend<'a, S: Storage + ?Sized> {
    storage: &'a S,
}

impl<S: Storage + ?Sized> SeriesSelectionBackend for ScanSeriesSelectionBackend<'_, S> {
    type Candidate = MetricSeries;

    fn candidate_items(
        &self,
        selection: &SeriesSelection,
        prepared: &PreparedSeriesSelection,
    ) -> Result<Vec<Self::Candidate>> {
        let mut series = self.storage.list_metrics()?;
        retain_series_matching_selection(&mut series, selection, prepared);
        Ok(series)
    }

    fn retain_items_in_time_range(
        &self,
        items: &mut Vec<Self::Candidate>,
        start: i64,
        end: i64,
    ) -> Result<()> {
        if items.is_empty() {
            return Ok(());
        }

        let mut filtered = Vec::with_capacity(items.len());
        for entry in items.drain(..) {
            let points = self
                .storage
                .select(&entry.name, &entry.labels, start, end)?;
            if !points.is_empty() {
                filtered.push(entry);
            }
        }
        *items = filtered;
        Ok(())
    }

    fn materialize_items(&self, items: Vec<Self::Candidate>) -> Result<Vec<MetricSeries>> {
        Ok(items)
    }
}

pub(crate) fn select_series_by_scan<S: Storage + ?Sized>(
    storage: &S,
    selection: &SeriesSelection,
) -> Result<Vec<MetricSeries>> {
    execute_series_selection(&ScanSeriesSelectionBackend { storage }, selection)
}
