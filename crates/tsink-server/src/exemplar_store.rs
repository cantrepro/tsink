use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{RwLock, RwLockReadGuard, RwLockWriteGuard};
use tsink::{Label, SeriesMatcher, SeriesMatcherOp, SeriesSelection};

const EXEMPLAR_STORE_FILE_NAME: &str = "exemplar-store.json";
const EXEMPLAR_STORE_MAGIC: &str = "tsink-exemplar-store";
const EXEMPLAR_STORE_SCHEMA_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExemplarStoreConfig {
    pub max_total_exemplars: usize,
    pub max_exemplars_per_series: usize,
    pub max_exemplars_per_request: usize,
    pub max_query_results: usize,
    pub max_query_selectors: usize,
}

impl Default for ExemplarStoreConfig {
    fn default() -> Self {
        Self {
            max_total_exemplars: 50_000,
            max_exemplars_per_series: 128,
            max_exemplars_per_request: 512,
            max_query_results: 1_000,
            max_query_selectors: 32,
        }
    }
}

impl ExemplarStoreConfig {
    pub fn validate(self) -> Result<Self, String> {
        if self.max_total_exemplars == 0 {
            return Err("exemplar store max_total_exemplars must be greater than zero".to_string());
        }
        if self.max_exemplars_per_series == 0 {
            return Err(
                "exemplar store max_exemplars_per_series must be greater than zero".to_string(),
            );
        }
        if self.max_exemplars_per_request == 0 {
            return Err(
                "exemplar store max_exemplars_per_request must be greater than zero".to_string(),
            );
        }
        if self.max_query_results == 0 {
            return Err("exemplar store max_query_results must be greater than zero".to_string());
        }
        if self.max_query_selectors == 0 {
            return Err("exemplar store max_query_selectors must be greater than zero".to_string());
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExemplarStoreMetricsSnapshot {
    pub accepted_total: u64,
    pub rejected_total: u64,
    pub dropped_total: u64,
    pub query_requests_total: u64,
    pub query_series_total: u64,
    pub query_exemplars_total: u64,
    pub stored_series: u64,
    pub stored_exemplars: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ExemplarApplyOutcome {
    pub accepted: usize,
    pub dropped: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExemplarSample {
    #[serde(default)]
    pub labels: Vec<Label>,
    pub value: f64,
    pub timestamp: i64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExemplarSeries {
    pub metric: String,
    #[serde(default)]
    pub labels: Vec<Label>,
    #[serde(default)]
    pub exemplars: Vec<ExemplarSample>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExemplarWrite {
    pub metric: String,
    #[serde(default)]
    pub series_labels: Vec<Label>,
    #[serde(default)]
    pub exemplar_labels: Vec<Label>,
    pub timestamp: i64,
    pub value: f64,
}

#[derive(Debug, Serialize, Deserialize)]
struct PersistedExemplarStore {
    magic: String,
    schema_version: u16,
    entries: Vec<ExemplarWrite>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct SeriesKey {
    metric: String,
    labels: Vec<Label>,
}

#[derive(Debug, Default)]
struct ExemplarStoreState {
    series: BTreeMap<SeriesKey, BTreeMap<i64, ExemplarSample>>,
}

impl ExemplarStoreState {
    fn exemplar_count(&self) -> usize {
        self.series.values().map(BTreeMap::len).sum()
    }
}

#[derive(Debug, Default)]
struct ExemplarStoreMetrics {
    accepted_total: AtomicU64,
    rejected_total: AtomicU64,
    dropped_total: AtomicU64,
    query_requests_total: AtomicU64,
    query_series_total: AtomicU64,
    query_exemplars_total: AtomicU64,
}

#[derive(Debug)]
pub struct ExemplarStore {
    path: Option<PathBuf>,
    config: ExemplarStoreConfig,
    state: RwLock<ExemplarStoreState>,
    metrics: ExemplarStoreMetrics,
}

impl ExemplarStore {
    #[allow(dead_code)]
    pub fn in_memory() -> Self {
        Self::in_memory_with_config(ExemplarStoreConfig::default())
    }

    #[allow(dead_code)]
    pub fn in_memory_with_config(config: ExemplarStoreConfig) -> Self {
        Self {
            path: None,
            config: config.validate().expect("valid exemplar store config"),
            state: RwLock::new(ExemplarStoreState::default()),
            metrics: ExemplarStoreMetrics::default(),
        }
    }

    pub fn open(data_path: Option<&Path>) -> Result<Self, String> {
        Self::open_with_config(data_path, ExemplarStoreConfig::default())
    }

    pub fn open_with_config(
        data_path: Option<&Path>,
        config: ExemplarStoreConfig,
    ) -> Result<Self, String> {
        let config = config.validate()?;
        let path = data_path.map(|path| path.join(EXEMPLAR_STORE_FILE_NAME));
        let state = if let Some(path) = path.as_ref() {
            load_state(path)?
        } else {
            ExemplarStoreState::default()
        };

        Ok(Self {
            path,
            config,
            state: RwLock::new(state),
            metrics: ExemplarStoreMetrics::default(),
        })
    }

    pub fn config(&self) -> ExemplarStoreConfig {
        self.config
    }

    pub fn apply_writes(
        &self,
        exemplars: &[ExemplarWrite],
    ) -> Result<ExemplarApplyOutcome, String> {
        if exemplars.is_empty() {
            return Ok(ExemplarApplyOutcome {
                accepted: 0,
                dropped: 0,
            });
        }

        let mut state = self.write_state()?;
        let mut accepted = 0usize;
        let mut dropped = 0usize;
        let mut changed = false;

        for exemplar in exemplars {
            let key = SeriesKey {
                metric: exemplar.metric.clone(),
                labels: exemplar.series_labels.clone(),
            };
            let series = state.series.entry(key).or_default();
            let replaced = series.insert(
                exemplar.timestamp,
                ExemplarSample {
                    labels: exemplar.exemplar_labels.clone(),
                    value: exemplar.value,
                    timestamp: exemplar.timestamp,
                },
            );
            accepted = accepted.saturating_add(1);
            changed = true;

            if replaced.is_none() {
                while series.len() > self.config.max_exemplars_per_series {
                    if series.pop_first().is_some() {
                        dropped = dropped.saturating_add(1);
                    } else {
                        break;
                    }
                }
            }
        }

        while state.exemplar_count() > self.config.max_total_exemplars {
            if !drop_oldest_exemplar(&mut state) {
                break;
            }
            dropped = dropped.saturating_add(1);
            changed = true;
        }

        state.series.retain(|_, exemplars| !exemplars.is_empty());
        if changed {
            self.persist_state(&state)?;
        }

        self.metrics
            .accepted_total
            .fetch_add(accepted as u64, Ordering::Relaxed);
        self.metrics
            .dropped_total
            .fetch_add(dropped as u64, Ordering::Relaxed);

        Ok(ExemplarApplyOutcome { accepted, dropped })
    }

    pub fn query(
        &self,
        selections: &[SeriesSelection],
        start: i64,
        end: i64,
        limit: usize,
    ) -> Result<Vec<ExemplarSeries>, String> {
        let limit = limit.min(self.config.max_query_results).max(1);
        let compiled = selections
            .iter()
            .map(CompiledSelection::new)
            .collect::<Result<Vec<_>, _>>()?;
        let state = self.read_state()?;
        let mut out = Vec::new();
        let mut total_exemplars = 0usize;

        for (series_key, exemplars) in &state.series {
            if !compiled
                .iter()
                .any(|selection| selection.matches(&series_key.metric, &series_key.labels))
            {
                continue;
            }

            let remaining = limit.saturating_sub(total_exemplars);
            if remaining == 0 {
                break;
            }
            let matched = exemplars
                .range(start..=end)
                .take(remaining)
                .map(|(_, exemplar)| exemplar.clone())
                .collect::<Vec<_>>();
            if matched.is_empty() {
                continue;
            }

            total_exemplars = total_exemplars.saturating_add(matched.len());
            out.push(ExemplarSeries {
                metric: series_key.metric.clone(),
                labels: series_key.labels.clone(),
                exemplars: matched,
            });
            if total_exemplars >= limit {
                break;
            }
        }

        self.metrics
            .query_requests_total
            .fetch_add(1, Ordering::Relaxed);
        self.metrics
            .query_series_total
            .fetch_add(out.len() as u64, Ordering::Relaxed);
        self.metrics
            .query_exemplars_total
            .fetch_add(total_exemplars as u64, Ordering::Relaxed);

        Ok(out)
    }

    pub fn record_rejected(&self, count: usize) {
        self.metrics
            .rejected_total
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    pub fn snapshot_into(&self, snapshot_path: &Path) -> Result<(), String> {
        let snapshot_file = snapshot_path.join(EXEMPLAR_STORE_FILE_NAME);
        let state = self.read_state()?;
        write_store_file(&snapshot_file, &state)
    }

    pub fn metrics_snapshot(&self) -> Result<ExemplarStoreMetricsSnapshot, String> {
        let state = self.read_state()?;
        Ok(ExemplarStoreMetricsSnapshot {
            accepted_total: self.metrics.accepted_total.load(Ordering::Relaxed),
            rejected_total: self.metrics.rejected_total.load(Ordering::Relaxed),
            dropped_total: self.metrics.dropped_total.load(Ordering::Relaxed),
            query_requests_total: self.metrics.query_requests_total.load(Ordering::Relaxed),
            query_series_total: self.metrics.query_series_total.load(Ordering::Relaxed),
            query_exemplars_total: self.metrics.query_exemplars_total.load(Ordering::Relaxed),
            stored_series: state.series.len() as u64,
            stored_exemplars: state.exemplar_count() as u64,
        })
    }

    fn read_state(&self) -> Result<RwLockReadGuard<'_, ExemplarStoreState>, String> {
        self.state
            .read()
            .map_err(|_| "exemplar store read lock poisoned".to_string())
    }

    fn write_state(&self) -> Result<RwLockWriteGuard<'_, ExemplarStoreState>, String> {
        self.state
            .write()
            .map_err(|_| "exemplar store write lock poisoned".to_string())
    }

    fn persist_state(&self, state: &ExemplarStoreState) -> Result<(), String> {
        let Some(path) = self.path.as_ref() else {
            return Ok(());
        };
        write_store_file(path, state)
    }
}

fn drop_oldest_exemplar(state: &mut ExemplarStoreState) -> bool {
    let Some(oldest_series_key) = state
        .series
        .iter()
        .filter_map(|(series_key, exemplars)| {
            exemplars.first_key_value().map(|(ts, _)| (series_key, *ts))
        })
        .min_by(|(series_a, ts_a), (series_b, ts_b)| {
            ts_a.cmp(ts_b).then_with(|| series_a.cmp(series_b))
        })
        .map(|(series_key, _)| series_key.clone())
    else {
        return false;
    };

    let Some(series) = state.series.get_mut(&oldest_series_key) else {
        return false;
    };
    let removed = series.pop_first().is_some();
    if series.is_empty() {
        state.series.remove(&oldest_series_key);
    }
    removed
}

fn load_state(path: &Path) -> Result<ExemplarStoreState, String> {
    if !path.exists() {
        return Ok(ExemplarStoreState::default());
    }

    let raw = std::fs::read(path)
        .map_err(|err| format!("failed to read exemplar store {}: {err}", path.display()))?;
    let persisted: PersistedExemplarStore = serde_json::from_slice(&raw)
        .map_err(|err| format!("failed to parse exemplar store {}: {err}", path.display()))?;
    if persisted.magic != EXEMPLAR_STORE_MAGIC {
        return Err(format!(
            "exemplar store {} has unsupported magic '{}'",
            path.display(),
            persisted.magic
        ));
    }
    if persisted.schema_version != EXEMPLAR_STORE_SCHEMA_VERSION {
        return Err(format!(
            "exemplar store {} has unsupported schema version {}",
            path.display(),
            persisted.schema_version
        ));
    }

    let mut state = ExemplarStoreState::default();
    for exemplar in persisted.entries {
        state
            .series
            .entry(SeriesKey {
                metric: exemplar.metric,
                labels: exemplar.series_labels,
            })
            .or_default()
            .insert(
                exemplar.timestamp,
                ExemplarSample {
                    labels: exemplar.exemplar_labels,
                    value: exemplar.value,
                    timestamp: exemplar.timestamp,
                },
            );
    }
    Ok(state)
}

fn write_store_file(path: &Path, state: &ExemplarStoreState) -> Result<(), String> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).map_err(|err| {
            format!(
                "failed to create exemplar store directory {}: {err}",
                parent.display()
            )
        })?;
    }

    let persisted = PersistedExemplarStore {
        magic: EXEMPLAR_STORE_MAGIC.to_string(),
        schema_version: EXEMPLAR_STORE_SCHEMA_VERSION,
        entries: state
            .series
            .iter()
            .flat_map(|(series, exemplars)| {
                exemplars.values().map(|exemplar| ExemplarWrite {
                    metric: series.metric.clone(),
                    series_labels: series.labels.clone(),
                    exemplar_labels: exemplar.labels.clone(),
                    timestamp: exemplar.timestamp,
                    value: exemplar.value,
                })
            })
            .collect(),
    };
    let mut encoded = serde_json::to_vec_pretty(&persisted)
        .map_err(|err| format!("failed to serialize exemplar store: {err}"))?;
    encoded.push(b'\n');

    let tmp_path = path.with_extension("tmp");
    let mut file = OpenOptions::new()
        .create(true)
        .truncate(true)
        .write(true)
        .open(&tmp_path)
        .map_err(|err| {
            format!(
                "failed to open temporary exemplar store {}: {err}",
                tmp_path.display()
            )
        })?;
    file.write_all(&encoded).map_err(|err| {
        format!(
            "failed to write temporary exemplar store {}: {err}",
            tmp_path.display()
        )
    })?;
    file.sync_all().map_err(|err| {
        format!(
            "failed to fsync temporary exemplar store {}: {err}",
            tmp_path.display()
        )
    })?;
    std::fs::rename(&tmp_path, path).map_err(|err| {
        format!(
            "failed to install exemplar store {} from {}: {err}",
            path.display(),
            tmp_path.display()
        )
    })?;
    Ok(())
}

#[derive(Debug)]
struct CompiledSelection {
    metric: Option<String>,
    matchers: Vec<CompiledMatcher>,
}

impl CompiledSelection {
    fn new(selection: &SeriesSelection) -> Result<Self, String> {
        let matchers = selection
            .matchers
            .iter()
            .map(CompiledMatcher::new)
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self {
            metric: selection.metric.clone(),
            matchers,
        })
    }

    fn matches(&self, metric: &str, labels: &[Label]) -> bool {
        if self
            .metric
            .as_deref()
            .is_some_and(|expected| expected != metric)
        {
            return false;
        }
        self.matchers
            .iter()
            .all(|matcher| matcher.matches(metric, labels))
    }
}

#[derive(Debug)]
struct CompiledMatcher {
    name: String,
    op: SeriesMatcherOp,
    value: String,
    regex: Option<Regex>,
}

impl CompiledMatcher {
    fn new(matcher: &SeriesMatcher) -> Result<Self, String> {
        if matcher.name.trim().is_empty() {
            return Err("series matcher label name cannot be empty".to_string());
        }
        let regex =
            match matcher.op {
                SeriesMatcherOp::RegexMatch | SeriesMatcherOp::RegexNoMatch => {
                    let anchored = format!("^(?:{})$", matcher.value);
                    Some(Regex::new(&anchored).map_err(|err| {
                        format!("invalid matcher regex '{}': {err}", matcher.value)
                    })?)
                }
                SeriesMatcherOp::Equal | SeriesMatcherOp::NotEqual => None,
            };
        Ok(Self {
            name: matcher.name.clone(),
            op: matcher.op,
            value: matcher.value.clone(),
            regex,
        })
    }

    fn matches(&self, metric: &str, labels: &[Label]) -> bool {
        let actual = if self.name == "__name__" {
            Some(metric)
        } else {
            labels
                .iter()
                .find(|label| label.name == self.name)
                .map(|label| label.value.as_str())
        };

        match self.op {
            SeriesMatcherOp::Equal => actual.is_some_and(|value| value == self.value),
            SeriesMatcherOp::NotEqual => actual.is_none_or(|value| value != self.value),
            SeriesMatcherOp::RegexMatch => actual.is_some_and(|value| {
                self.regex
                    .as_ref()
                    .is_some_and(|regex| regex.is_match(value))
            }),
            SeriesMatcherOp::RegexNoMatch => !actual.is_some_and(|value| {
                self.regex
                    .as_ref()
                    .is_some_and(|regex| regex.is_match(value))
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn exemplar_store_persists_and_queries_series() {
        let temp_dir = TempDir::new().expect("tempdir");
        let store = ExemplarStore::open(Some(temp_dir.path())).expect("store should open");
        store
            .apply_writes(&[ExemplarWrite {
                metric: "http_request_duration_seconds".to_string(),
                series_labels: vec![
                    Label::new("job", "api"),
                    Label::new("__tsink_tenant__", "a"),
                ],
                exemplar_labels: vec![Label::new("trace_id", "abc")],
                timestamp: 10,
                value: 0.42,
            }])
            .expect("write should persist");

        let reopened = ExemplarStore::open(Some(temp_dir.path())).expect("store should reopen");
        let queried = reopened
            .query(
                &[SeriesSelection::new()
                    .with_metric("http_request_duration_seconds")
                    .with_matcher(SeriesMatcher::equal("job", "api"))
                    .with_matcher(SeriesMatcher::equal("__tsink_tenant__", "a"))],
                0,
                20,
                10,
            )
            .expect("query should succeed");
        assert_eq!(queried.len(), 1);
        assert_eq!(queried[0].exemplars.len(), 1);
        assert_eq!(queried[0].exemplars[0].labels[0].name, "trace_id");
    }

    #[test]
    fn exemplar_store_enforces_global_and_series_bounds() {
        let store = ExemplarStore::in_memory_with_config(ExemplarStoreConfig {
            max_total_exemplars: 2,
            max_exemplars_per_series: 1,
            max_exemplars_per_request: 8,
            max_query_results: 8,
            max_query_selectors: 4,
        });
        store
            .apply_writes(&[
                ExemplarWrite {
                    metric: "metric_a".to_string(),
                    series_labels: vec![Label::new("job", "a")],
                    exemplar_labels: vec![Label::new("trace_id", "1")],
                    timestamp: 10,
                    value: 1.0,
                },
                ExemplarWrite {
                    metric: "metric_a".to_string(),
                    series_labels: vec![Label::new("job", "a")],
                    exemplar_labels: vec![Label::new("trace_id", "2")],
                    timestamp: 20,
                    value: 2.0,
                },
                ExemplarWrite {
                    metric: "metric_b".to_string(),
                    series_labels: vec![Label::new("job", "b")],
                    exemplar_labels: vec![Label::new("trace_id", "3")],
                    timestamp: 30,
                    value: 3.0,
                },
            ])
            .expect("writes should succeed");

        let metrics = store.metrics_snapshot().expect("metrics should load");
        assert_eq!(metrics.stored_exemplars, 2);
        assert_eq!(metrics.dropped_total, 1);
    }

    #[test]
    fn exemplar_store_query_supports_regex_matchers() {
        let store = ExemplarStore::in_memory();
        store
            .apply_writes(&[
                ExemplarWrite {
                    metric: "metric_a".to_string(),
                    series_labels: vec![Label::new("job", "api-a")],
                    exemplar_labels: vec![Label::new("trace_id", "1")],
                    timestamp: 10,
                    value: 1.0,
                },
                ExemplarWrite {
                    metric: "metric_b".to_string(),
                    series_labels: vec![Label::new("job", "worker-b")],
                    exemplar_labels: vec![Label::new("trace_id", "2")],
                    timestamp: 20,
                    value: 2.0,
                },
            ])
            .expect("writes should succeed");

        let queried =
            store
                .query(
                    &[SeriesSelection::new()
                        .with_matcher(SeriesMatcher::regex_match("job", "api-.*"))],
                    0,
                    30,
                    10,
                )
                .expect("query should succeed");
        assert_eq!(queried.len(), 1);
        assert_eq!(queried[0].metric, "metric_a");
    }
}
