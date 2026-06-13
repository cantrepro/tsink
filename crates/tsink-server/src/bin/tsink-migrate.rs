use base64::Engine;
use prost::Message;
use regex::Regex;
use reqwest::blocking::Client;
use reqwest::Method;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use snap::raw::{Decoder as SnappyDecoder, Encoder as SnappyEncoder};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::fs;
use std::io::{BufRead, BufReader, Cursor};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tsink::promql::ast::{Expr, MatchOp, VectorSelector};
use tsink::{SeriesMatcherOp, TimestampPrecision};

mod tenant {
    #[allow(dead_code)]
    pub const DEFAULT_TENANT_ID: &str = "default";
    pub const TENANT_LABEL: &str = "__tsink_tenant__";
}

#[allow(unused)]
#[path = "../legacy_ingest.rs"]
mod legacy_ingest;
#[allow(unused)]
#[path = "../otlp.rs"]
mod otlp;
#[path = "../prom_remote.rs"]
mod prom_remote;
#[allow(unused)]
#[path = "../prom_write.rs"]
mod prom_write;

use legacy_ingest::{
    graphite_config, influx_line_protocol_config, normalize_graphite_plaintext_line,
    normalize_influx_line_protocol, statsd_config, StatsdAdapter,
};
use otlp::{normalize_metrics_export_request, ExportMetricsServiceRequest};

type MetricMetadataDescriptor = (String, String, String);
type MetricMetadataMap = BTreeMap<String, BTreeSet<MetricMetadataDescriptor>>;
use prom_remote::{
    histogram, BucketSpan, Exemplar, Histogram, Label, LabelMatcher, MatcherType, MetricMetadata,
    MetricType, Query, QueryResult, ReadRequest, ReadResponse, ReadResponseType, Sample,
    TimeSeries, WriteRequest,
};
use prom_write::{
    NormalizedHistogramCount, NormalizedHistogramSample, NormalizedMetricMetadataUpdate,
    NormalizedSeriesIdentity, NormalizedWriteEnvelope,
};

const TSINK_TENANT_HEADER: &str = "X-Tsink-Tenant";
const MAX_ISSUES_PER_CHECK: usize = 8;

fn main() {
    if let Err(err) = real_main() {
        eprintln!("error: {err}");
        std::process::exit(1);
    }
}

fn real_main() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let Some(command_name) = args.next() else {
        println!("{}", usage());
        return Ok(());
    };
    if command_name == "--help" || command_name == "-h" {
        println!("{}", usage());
        return Ok(());
    }

    let command = Command::from_parts(command_name, args)?;
    let plan = load_plan(command.args().config_path.as_path())?;
    let report = match &command {
        Command::Backfill(args) => CommandReport::Backfill(run_backfill(&plan, args)?),
        Command::Verify(args) => CommandReport::Verify(run_verify(&plan, args)?),
        Command::CutoverCheck(args) => CommandReport::Cutover(run_cutover_check(&plan, args)?),
    };

    if let Some(dir) = &command.args().artifact_dir {
        write_report_artifacts(dir, &report)?;
    }

    println!("{}", report.console_summary());
    if report.is_failure() {
        std::process::exit(1);
    }
    Ok(())
}

#[derive(Debug, Clone)]
enum Command {
    Backfill(RunArgs),
    Verify(RunArgs),
    CutoverCheck(RunArgs),
}

#[derive(Debug, Clone)]
struct RunArgs {
    config_path: PathBuf,
    start_ms: i64,
    end_ms: i64,
    artifact_dir: Option<PathBuf>,
}

impl Command {
    fn from_parts(command: String, mut args: impl Iterator<Item = String>) -> Result<Self, String> {
        let mut config_path = None;
        let mut start_ms = None;
        let mut end_ms = None;
        let mut artifact_dir = None;

        while let Some(flag) = args.next() {
            match flag.as_str() {
                "--config" => {
                    let Some(value) = args.next() else {
                        return Err("missing value for --config".to_string());
                    };
                    config_path = Some(PathBuf::from(value));
                }
                "--start-ms" => {
                    let Some(value) = args.next() else {
                        return Err("missing value for --start-ms".to_string());
                    };
                    start_ms = Some(
                        value
                            .parse::<i64>()
                            .map_err(|_| format!("invalid --start-ms value: {value}"))?,
                    );
                }
                "--end-ms" => {
                    let Some(value) = args.next() else {
                        return Err("missing value for --end-ms".to_string());
                    };
                    end_ms = Some(
                        value
                            .parse::<i64>()
                            .map_err(|_| format!("invalid --end-ms value: {value}"))?,
                    );
                }
                "--artifact-dir" => {
                    let Some(value) = args.next() else {
                        return Err("missing value for --artifact-dir".to_string());
                    };
                    artifact_dir = Some(PathBuf::from(value));
                }
                "--help" | "-h" => return Err(usage()),
                other => return Err(format!("unknown argument: {other}\n\n{}", usage())),
            }
        }

        let args = RunArgs {
            config_path: config_path.ok_or_else(|| "missing required --config".to_string())?,
            start_ms: start_ms.ok_or_else(|| "missing required --start-ms".to_string())?,
            end_ms: end_ms.ok_or_else(|| "missing required --end-ms".to_string())?,
            artifact_dir,
        };
        if args.end_ms < args.start_ms {
            return Err("--end-ms must be greater than or equal to --start-ms".to_string());
        }

        match command.as_str() {
            "backfill" => Ok(Self::Backfill(args)),
            "verify" => Ok(Self::Verify(args)),
            "cutover-check" => Ok(Self::CutoverCheck(args)),
            _ => Err(format!("unknown command: {command}\n\n{}", usage())),
        }
    }

    fn args(&self) -> &RunArgs {
        match self {
            Self::Backfill(args) | Self::Verify(args) | Self::CutoverCheck(args) => args,
        }
    }
}

fn usage() -> String {
    "\
Usage:
  cargo run -p tsink-server --bin tsink-migrate -- <backfill|verify|cutover-check> \\
    --config <plan.json> --start-ms <unix_ms> --end-ms <unix_ms> [--artifact-dir <dir>]

Commands:
  backfill       Copy raw data from Prometheus, VictoriaMetrics, OTLP, Influx, StatsD, or Graphite into tsink.
  verify         Compare raw series, row counts, metadata, and exemplars between the source capture/window and tsink.
  cutover-check  Run target capability checks plus shared raw verification and optional PromQL parity verification.
"
    .to_string()
}

#[derive(Debug, Clone, Deserialize)]
struct MigrationPlan {
    source: SourceConfig,
    target: TargetConfig,
    selectors: Vec<String>,
    #[serde(default)]
    metadata_metrics: Vec<String>,
    #[serde(default)]
    exemplar_checks: Vec<ExemplarCheckPlan>,
    #[serde(default)]
    promql_checks: Vec<PromqlCheckPlan>,
    #[serde(default)]
    batch: BatchConfig,
    #[serde(default)]
    compare: CompareConfig,
    #[serde(skip)]
    plan_dir: PathBuf,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum SourceKind {
    Prometheus,
    Victoriametrics,
    Otlp,
    InfluxLineProtocol,
    Statsd,
    GraphitePlaintext,
}

#[derive(Debug, Clone, Deserialize)]
struct SourceConfig {
    kind: SourceKind,
    #[serde(default)]
    headers: BTreeMap<String, String>,
    remote_read_url: Option<String>,
    export_url: Option<String>,
    query_range_url: Option<String>,
    metadata_url: Option<String>,
    exemplar_url: Option<String>,
    capture_manifest_path: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct TargetConfig {
    #[serde(default)]
    headers: BTreeMap<String, String>,
    tenant: Option<String>,
    write_url: String,
    read_url: String,
    query_range_url: Option<String>,
    metadata_url: Option<String>,
    exemplar_url: Option<String>,
    status_url: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
struct BatchConfig {
    max_series_per_write: usize,
    max_points_per_write: usize,
    http_timeout_secs: u64,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_series_per_write: 250,
            max_points_per_write: 25_000,
            http_timeout_secs: 30,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(default)]
struct CompareConfig {
    max_absolute_value_delta: f64,
    max_relative_value_delta: f64,
}

impl Default for CompareConfig {
    fn default() -> Self {
        Self {
            max_absolute_value_delta: 1e-12,
            max_relative_value_delta: 1e-9,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
struct ExemplarCheckPlan {
    query: String,
    #[serde(default = "default_exemplar_limit")]
    limit: usize,
}

fn default_exemplar_limit() -> usize {
    200
}

#[derive(Debug, Clone, Deserialize)]
struct PromqlCheckPlan {
    query: String,
    #[serde(default = "default_promql_step")]
    step: String,
}

fn default_promql_step() -> String {
    "30s".to_string()
}

#[derive(Debug)]
struct HttpClient {
    inner: Client,
}

impl HttpClient {
    fn new(timeout_secs: u64) -> Result<Self, String> {
        let inner = Client::builder()
            .timeout(Duration::from_secs(timeout_secs.max(1)))
            .build()
            .map_err(|err| format!("failed to build HTTP client: {err}"))?;
        Ok(Self { inner })
    }

    fn get_json(
        &self,
        url: &str,
        headers: &BTreeMap<String, String>,
        tenant: Option<&str>,
        query: &[(String, String)],
    ) -> Result<JsonValue, String> {
        let body = self.request_bytes(Method::GET, url, headers, tenant, query, None, None)?;
        serde_json::from_slice(&body).map_err(|err| format!("invalid JSON from {url}: {err}"))
    }

    fn get_bytes(
        &self,
        url: &str,
        headers: &BTreeMap<String, String>,
        tenant: Option<&str>,
        query: &[(String, String)],
    ) -> Result<Vec<u8>, String> {
        self.request_bytes(Method::GET, url, headers, tenant, query, None, None)
    }

    fn post_bytes(
        &self,
        url: &str,
        headers: &BTreeMap<String, String>,
        tenant: Option<&str>,
        body: Vec<u8>,
        content_type: &str,
        extra_headers: &[(&str, &str)],
    ) -> Result<Vec<u8>, String> {
        self.request_bytes(
            Method::POST,
            url,
            headers,
            tenant,
            &[],
            Some((body, content_type)),
            Some(extra_headers),
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn request_bytes(
        &self,
        method: Method,
        url: &str,
        headers: &BTreeMap<String, String>,
        tenant: Option<&str>,
        query: &[(String, String)],
        body: Option<(Vec<u8>, &str)>,
        extra_headers: Option<&[(&str, &str)]>,
    ) -> Result<Vec<u8>, String> {
        let mut request = self.inner.request(method.clone(), url);
        if !query.is_empty() {
            request = request.query(query);
        }
        for (name, value) in headers {
            request = request.header(name, value);
        }
        if let Some(tenant) = tenant {
            request = request.header(TSINK_TENANT_HEADER, tenant);
        }
        if let Some(extra_headers) = extra_headers {
            for (name, value) in extra_headers {
                request = request.header(*name, *value);
            }
        }

        let response = match body {
            Some((body, content_type)) => request
                .header("Content-Type", content_type)
                .body(body)
                .send(),
            None => request.send(),
        }
        .map_err(|err| format!("{method} {url} failed: {err}"))?;

        let status = response.status();
        let body = response
            .bytes()
            .map_err(|err| format!("failed reading response body from {url}: {err}"))?
            .to_vec();
        if !status.is_success() {
            return Err(format!(
                "{method} {url} returned {}: {}",
                status.as_u16(),
                String::from_utf8_lossy(&body)
            ));
        }
        Ok(body)
    }
}

#[derive(Debug, Serialize)]
#[serde(tag = "command", rename_all = "snake_case")]
enum CommandReport {
    Backfill(BackfillReport),
    Verify(VerifyReport),
    Cutover(CutoverReport),
}

impl CommandReport {
    fn is_failure(&self) -> bool {
        match self {
            Self::Backfill(report) => report.status != "pass",
            Self::Verify(report) => report.status != "pass",
            Self::Cutover(report) => report.status != "pass",
        }
    }

    fn console_summary(&self) -> String {
        match self {
            Self::Backfill(report) => format!(
                "tsink-migrate backfill: {}\n  source_kind: {:?}\n  selectors: {}\n  series: {}\n  samples: {}\n  histograms: {}\n  exemplars: {}\n  metadata: {}\n  write_batches: {}",
                report.status,
                report.source_kind,
                report.selector_count,
                report.series_written,
                report.samples_written,
                report.histograms_written,
                report.exemplars_written,
                report.metadata_written,
                report.write_batches,
            ),
            Self::Verify(report) => format!(
                "tsink-migrate verify: {}\n  raw_checks: {}\n  metadata_checks: {}\n  exemplar_checks: {}\n  issues: {}",
                report.status,
                report.raw_checks.len(),
                report.metadata_checks.len(),
                report.exemplar_checks.len(),
                report.issues.len(),
            ),
            Self::Cutover(report) => format!(
                "tsink-migrate cutover-check: {}\n  promql_checks: {}\n  issues: {}",
                report.status,
                report.promql_checks.len(),
                report.issues.len(),
            ),
        }
    }

    fn markdown_summary(&self) -> String {
        match self {
            Self::Backfill(report) => format!(
                "# tsink Migration Backfill\n\n- Result: `{}`\n- Source kind: `{}`\n- Window: `{}` to `{}`\n- Selectors: `{}`\n- Series written: `{}`\n- Samples written: `{}`\n- Histograms written: `{}`\n- Exemplars written: `{}`\n- Metadata entries written: `{}`\n- Write batches: `{}`\n\n## Notes\n{}\n",
                report.status,
                report.source_kind.as_str(),
                report.start_ms,
                report.end_ms,
                report.selector_count,
                report.series_written,
                report.samples_written,
                report.histograms_written,
                report.exemplars_written,
                report.metadata_written,
                report.write_batches,
                markdown_bullets(&report.notes),
            ),
            Self::Verify(report) => format!(
                "# tsink Migration Verify\n\n- Result: `{}`\n- Window: `{}` to `{}`\n- Raw checks: `{}`\n- Metadata checks: `{}`\n- Exemplar checks: `{}`\n\n## Issues\n{}\n",
                report.status,
                report.start_ms,
                report.end_ms,
                report.raw_checks.len(),
                report.metadata_checks.len(),
                report.exemplar_checks.len(),
                markdown_bullets(&report.issues),
            ),
            Self::Cutover(report) => format!(
                "# tsink Migration Cutover Check\n\n- Result: `{}`\n- Window: `{}` to `{}`\n- PromQL checks: `{}`\n\n## Issues\n{}\n",
                report.status,
                report.start_ms,
                report.end_ms,
                report.promql_checks.len(),
                markdown_bullets(&report.issues),
            ),
        }
    }
}

fn markdown_bullets(items: &[String]) -> String {
    if items.is_empty() {
        "- None.".to_string()
    } else {
        items
            .iter()
            .map(|item| format!("- {item}"))
            .collect::<Vec<_>>()
            .join("\n")
    }
}

#[derive(Debug, Serialize)]
struct BackfillReport {
    status: String,
    source_kind: SourceKind,
    start_ms: i64,
    end_ms: i64,
    selector_count: usize,
    series_written: usize,
    samples_written: usize,
    histograms_written: usize,
    exemplars_written: usize,
    metadata_written: usize,
    write_batches: usize,
    notes: Vec<String>,
}

#[derive(Debug, Serialize)]
struct VerifyReport {
    status: String,
    start_ms: i64,
    end_ms: i64,
    raw_checks: Vec<RawCheckReport>,
    metadata_checks: Vec<MetadataCheckReport>,
    exemplar_checks: Vec<ExemplarCheckReport>,
    issues: Vec<String>,
}

#[derive(Debug, Serialize)]
struct RawCheckReport {
    selector: String,
    source_series: usize,
    target_series: usize,
    source_rows: usize,
    target_rows: usize,
    source_samples: usize,
    target_samples: usize,
    source_histograms: usize,
    target_histograms: usize,
    missing_series: usize,
    extra_series: usize,
    sample_mismatches: usize,
    histogram_mismatches: usize,
    issues: Vec<String>,
}

#[derive(Debug, Serialize)]
struct MetadataCheckReport {
    metric: String,
    source_entries: usize,
    target_entries: usize,
    matched: bool,
}

#[derive(Debug, Serialize)]
struct ExemplarCheckReport {
    query: String,
    source_series: usize,
    target_series: usize,
    missing_series: usize,
    extra_series: usize,
    exemplar_mismatches: usize,
    issues: Vec<String>,
}

#[derive(Debug, Serialize)]
struct CutoverReport {
    status: String,
    start_ms: i64,
    end_ms: i64,
    target_payloads: Option<TargetPayloadStatus>,
    raw_verify: VerifyReport,
    promql_checks: Vec<PromqlCheckReport>,
    issues: Vec<String>,
}

#[derive(Debug, Serialize)]
struct PromqlCheckReport {
    query: String,
    step: String,
    matched: bool,
    source_warnings: Vec<String>,
    target_warnings: Vec<String>,
    target_partial_response: bool,
    issues: Vec<String>,
}

#[derive(Debug, Serialize)]
struct TargetPayloadStatus {
    metadata_enabled: bool,
    exemplars_enabled: bool,
    histograms_enabled: bool,
    otlp_enabled: bool,
    otlp_supported_shapes: Vec<String>,
    influx_line_protocol_enabled: bool,
    statsd_enabled: bool,
    graphite_enabled: bool,
}

type SeriesKey = Vec<(String, String)>;

#[derive(Debug, Clone, Default)]
struct WritableSeries {
    labels: Vec<Label>,
    samples: Vec<Sample>,
    histograms: Vec<Histogram>,
    exemplars: Vec<Exemplar>,
}

#[derive(Debug, Clone, Serialize)]
struct CanonicalSeries {
    labels: SeriesKey,
    samples: Vec<CanonicalSample>,
    histograms: Vec<CanonicalHistogram>,
    exemplars: Vec<CanonicalExemplar>,
}

#[derive(Debug, Clone, Serialize)]
struct CanonicalSample {
    timestamp: i64,
    value: f64,
}

#[derive(Debug, Clone, Serialize)]
struct CanonicalHistogram {
    timestamp: i64,
    payload: JsonValue,
}

#[derive(Debug, Clone, Serialize)]
struct CanonicalExemplar {
    timestamp: i64,
    value: f64,
    labels: SeriesKey,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct MetadataEntryKey {
    metric: String,
    metric_type: i32,
    help: String,
    unit: String,
}

#[derive(Debug, Clone, Default)]
struct PreparedSourceData {
    series: BTreeMap<SeriesKey, WritableSeries>,
    metadata: Vec<MetricMetadata>,
    otlp_supported_shapes: Vec<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct CaptureManifestEntry {
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    body: Option<String>,
    #[serde(default)]
    body_base64: Option<String>,
    #[serde(default)]
    received_at_ms: Option<i64>,
    #[serde(default)]
    query_params: BTreeMap<String, String>,
}

#[derive(Debug, Clone)]
struct CompiledSelector {
    matchers: Vec<CompiledSelectorMatcher>,
}

#[derive(Debug, Clone)]
struct CompiledSelectorMatcher {
    name: String,
    op: SeriesMatcherOp,
    value: String,
    regex: Option<Regex>,
}

fn load_plan(path: &Path) -> Result<MigrationPlan, String> {
    let body = fs::read(path)
        .map_err(|err| format!("failed reading migration plan {}: {err}", path.display()))?;
    let mut plan: MigrationPlan =
        serde_json::from_slice(&body).map_err(|err| format!("invalid plan JSON: {err}"))?;
    if plan.selectors.is_empty() {
        return Err("migration plan must contain at least one selector".to_string());
    }
    plan.plan_dir = path
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .to_path_buf();
    Ok(plan)
}

fn run_backfill(plan: &MigrationPlan, args: &RunArgs) -> Result<BackfillReport, String> {
    let client = HttpClient::new(plan.batch.http_timeout_secs)?;
    let mut notes = Vec::new();
    let prepared_source = if plan.source.kind.uses_capture_manifest() {
        Some(prepare_capture_source_data(plan)?)
    } else {
        None
    };
    let mut series = match plan.source.kind {
        SourceKind::Prometheus => fetch_prometheus_writable_series(
            &client,
            &plan.source,
            args.start_ms,
            args.end_ms,
            &plan.selectors,
        )?,
        SourceKind::Victoriametrics => fetch_victoriametrics_writable_series(
            &client,
            &plan.source,
            args.start_ms,
            args.end_ms,
            &plan.selectors,
        )?,
        SourceKind::Otlp
        | SourceKind::InfluxLineProtocol
        | SourceKind::Statsd
        | SourceKind::GraphitePlaintext => select_source_series_for_window(
            prepared_source
                .as_ref()
                .expect("capture source data should be prepared"),
            &plan.selectors,
            args.start_ms,
            args.end_ms,
        )?,
    };

    let metadata_metrics = effective_metadata_metrics(plan)?;
    let metadata = match plan.source.kind {
        SourceKind::Prometheus | SourceKind::Victoriametrics => {
            if metadata_metrics.is_empty() {
                Vec::new()
            } else if let Some(url) = &plan.source.metadata_url {
                fetch_metadata_entries(&client, url, &plan.source.headers, None, &metadata_metrics)?
            } else {
                notes.push(
                    "source metadata URL not configured; metadata backfill was skipped".to_string(),
                );
                Vec::new()
            }
        }
        SourceKind::Otlp
        | SourceKind::InfluxLineProtocol
        | SourceKind::Statsd
        | SourceKind::GraphitePlaintext => filter_prepared_metadata(
            prepared_source
                .as_ref()
                .expect("capture source data should be prepared"),
            &metadata_metrics,
        ),
    };

    let exemplar_checks = effective_exemplar_checks(plan);
    if !exemplar_checks.is_empty() {
        match plan.source.kind {
            SourceKind::Prometheus => {
                if let Some(url) = &plan.source.exemplar_url {
                    merge_exemplar_queries_into_series(
                        &mut series,
                        &client,
                        url,
                        &plan.source.headers,
                        None,
                        &exemplar_checks,
                        args.start_ms,
                        args.end_ms,
                    )?;
                } else {
                    notes.push(
                        "source exemplar URL not configured; exemplar backfill was skipped"
                            .to_string(),
                    );
                }
            }
            SourceKind::Victoriametrics => {}
            SourceKind::Otlp
            | SourceKind::InfluxLineProtocol
            | SourceKind::Statsd
            | SourceKind::GraphitePlaintext => {}
        }
    }

    if prepared_source.is_some() {
        notes.push(
            "capture-manifest sources are normalized locally and imported via remote write; cutover-check still validates the live target ingest surface".to_string(),
        );
    }

    let series_written = series.len();
    let samples_written = series.values().map(|entry| entry.samples.len()).sum();
    let histograms_written = series.values().map(|entry| entry.histograms.len()).sum();
    let exemplars_written = series.values().map(|entry| entry.exemplars.len()).sum();
    let metadata_written = metadata.len();

    let batches = build_write_batches(&series, &metadata, &plan.batch);
    for batch in &batches {
        send_write_request(&client, &plan.target, batch)?;
    }

    Ok(BackfillReport {
        status: "pass".to_string(),
        source_kind: plan.source.kind.clone(),
        start_ms: args.start_ms,
        end_ms: args.end_ms,
        selector_count: plan.selectors.len(),
        series_written,
        samples_written,
        histograms_written,
        exemplars_written,
        metadata_written,
        write_batches: batches.len(),
        notes,
    })
}

fn run_verify(plan: &MigrationPlan, args: &RunArgs) -> Result<VerifyReport, String> {
    let client = HttpClient::new(plan.batch.http_timeout_secs)?;
    let mut issues = Vec::new();
    let prepared_source = if plan.source.kind.uses_capture_manifest() {
        Some(prepare_capture_source_data(plan)?)
    } else {
        None
    };
    let source_raw = fetch_source_raw_checks(
        &client,
        plan,
        args.start_ms,
        args.end_ms,
        prepared_source.as_ref(),
    )?;
    let target_raw = fetch_target_raw_checks(
        &client,
        &plan.target,
        &plan.selectors,
        args.start_ms,
        args.end_ms,
    )?;

    let mut raw_checks = Vec::with_capacity(plan.selectors.len());
    for selector in &plan.selectors {
        let source = source_raw
            .get(selector)
            .ok_or_else(|| format!("missing source raw result for selector {selector}"))?;
        let target = target_raw
            .get(selector)
            .ok_or_else(|| format!("missing target raw result for selector {selector}"))?;
        let report = compare_raw_series(selector, source, target, &plan.compare);
        issues.extend(report.issues.iter().cloned());
        raw_checks.push(report);
    }

    let metadata_checks = verify_metadata(&client, plan, prepared_source.as_ref())?;
    for report in &metadata_checks {
        if !report.matched {
            issues.push(format!(
                "metadata mismatch for {} (source entries {}, target entries {})",
                report.metric, report.source_entries, report.target_entries
            ));
        }
    }

    let exemplar_checks = verify_exemplars(
        &client,
        plan,
        args.start_ms,
        args.end_ms,
        prepared_source.as_ref(),
    )?;
    for report in &exemplar_checks {
        issues.extend(report.issues.iter().cloned());
    }

    let status = if issues.is_empty() { "pass" } else { "fail" };
    Ok(VerifyReport {
        status: status.to_string(),
        start_ms: args.start_ms,
        end_ms: args.end_ms,
        raw_checks,
        metadata_checks,
        exemplar_checks,
        issues,
    })
}

fn run_cutover_check(plan: &MigrationPlan, args: &RunArgs) -> Result<CutoverReport, String> {
    let client = HttpClient::new(plan.batch.http_timeout_secs)?;
    let raw_verify = run_verify(plan, args)?;
    let mut issues = raw_verify.issues.clone();
    let prepared_source = if plan.source.kind.uses_capture_manifest() {
        Some(prepare_capture_source_data(plan)?)
    } else {
        None
    };

    let target_payloads = if let Some(url) = &plan.target.status_url {
        let status = fetch_target_payload_status(&client, &plan.target, url)?;
        if !effective_metadata_metrics(plan)?.is_empty() && !status.metadata_enabled {
            issues.push("target metadata payload support is disabled".to_string());
        }
        if !effective_exemplar_checks(plan).is_empty() && !status.exemplars_enabled {
            issues.push("target exemplar payload support is disabled".to_string());
        }
        match plan.source.kind {
            SourceKind::Otlp => {
                if !status.otlp_enabled {
                    issues.push("target OTLP metrics ingest is disabled".to_string());
                }
                if let Some(prepared_source) = &prepared_source {
                    let missing_shapes = prepared_source
                        .otlp_supported_shapes
                        .iter()
                        .filter(|shape| {
                            !status
                                .otlp_supported_shapes
                                .iter()
                                .any(|item| item == *shape)
                        })
                        .cloned()
                        .collect::<Vec<_>>();
                    if !missing_shapes.is_empty() {
                        issues.push(format!(
                            "target OTLP metrics ingest does not advertise required shapes: {}",
                            missing_shapes.join(", ")
                        ));
                    }
                }
            }
            SourceKind::InfluxLineProtocol => {
                if !status.influx_line_protocol_enabled {
                    issues.push("target Influx line protocol ingest is disabled".to_string());
                }
            }
            SourceKind::Statsd => {
                if !status.statsd_enabled {
                    issues.push("target StatsD ingest is disabled".to_string());
                }
            }
            SourceKind::GraphitePlaintext => {
                if !status.graphite_enabled {
                    issues.push("target Graphite ingest is disabled".to_string());
                }
            }
            SourceKind::Prometheus | SourceKind::Victoriametrics => {}
        }
        Some(status)
    } else {
        None
    };

    let promql_checks = verify_promql_checks(&client, plan, args.start_ms, args.end_ms)?;
    for report in &promql_checks {
        if !report.matched {
            issues.extend(report.issues.iter().cloned());
        }
        if report.target_partial_response {
            issues.push(format!(
                "target returned partialResponse for query {}",
                report.query
            ));
        }
        if !report.target_warnings.is_empty() {
            issues.push(format!(
                "target returned warnings for query {}: {}",
                report.query,
                report.target_warnings.join("; ")
            ));
        }
    }

    if let Some(status) = &target_payloads {
        let raw_has_histograms = fetch_source_raw_checks(
            &client,
            plan,
            args.start_ms,
            args.end_ms,
            prepared_source.as_ref(),
        )?
        .values()
        .any(|series_map| {
            series_map
                .values()
                .any(|series| !series.histograms.is_empty())
        });
        if raw_has_histograms && !status.histograms_enabled {
            issues.push("target histogram payload support is disabled".to_string());
        }
    }

    let status = if issues.is_empty() { "pass" } else { "fail" };
    Ok(CutoverReport {
        status: status.to_string(),
        start_ms: args.start_ms,
        end_ms: args.end_ms,
        target_payloads,
        raw_verify,
        promql_checks,
        issues,
    })
}

fn fetch_prometheus_writable_series(
    client: &HttpClient,
    source: &SourceConfig,
    start_ms: i64,
    end_ms: i64,
    selectors: &[String],
) -> Result<BTreeMap<SeriesKey, WritableSeries>, String> {
    let read_url = source
        .remote_read_url
        .as_deref()
        .ok_or_else(|| "source.remote_read_url is required for Prometheus backfill".to_string())?;
    let response = remote_read_request(
        client,
        read_url,
        &source.headers,
        None,
        selectors,
        start_ms,
        end_ms,
    )?;
    if response.results.len() != selectors.len() {
        return Err(format!(
            "source remote read returned {} query results for {} selectors",
            response.results.len(),
            selectors.len()
        ));
    }
    let mut out = BTreeMap::new();
    for result in response.results {
        merge_query_result_into_writable_series(&mut out, result);
    }
    Ok(out)
}

fn fetch_victoriametrics_writable_series(
    client: &HttpClient,
    source: &SourceConfig,
    start_ms: i64,
    end_ms: i64,
    selectors: &[String],
) -> Result<BTreeMap<SeriesKey, WritableSeries>, String> {
    let export_url = source
        .export_url
        .as_deref()
        .ok_or_else(|| "source.export_url is required for VictoriaMetrics backfill".to_string())?;
    let mut out = BTreeMap::new();
    for selector in selectors {
        let exported = fetch_victoriametrics_export(
            client,
            export_url,
            &source.headers,
            selector,
            start_ms,
            end_ms,
        )?;
        merge_exported_series(&mut out, exported);
    }
    Ok(out)
}

fn fetch_source_raw_checks(
    client: &HttpClient,
    plan: &MigrationPlan,
    start_ms: i64,
    end_ms: i64,
    prepared_source: Option<&PreparedSourceData>,
) -> Result<BTreeMap<String, BTreeMap<SeriesKey, CanonicalSeries>>, String> {
    match plan.source.kind {
        SourceKind::Prometheus => {
            let read_url = plan.source.remote_read_url.as_deref().ok_or_else(|| {
                "source.remote_read_url is required for Prometheus verify".to_string()
            })?;
            let response = remote_read_request(
                client,
                read_url,
                &plan.source.headers,
                None,
                &plan.selectors,
                start_ms,
                end_ms,
            )?;
            if response.results.len() != plan.selectors.len() {
                return Err(format!(
                    "source remote read returned {} query results for {} selectors",
                    response.results.len(),
                    plan.selectors.len()
                ));
            }
            let mut out = BTreeMap::new();
            for (selector, result) in plan.selectors.iter().zip(response.results.into_iter()) {
                out.insert(selector.clone(), canonicalize_query_result(result));
            }
            Ok(out)
        }
        SourceKind::Victoriametrics => {
            let export_url = plan.source.export_url.as_deref().ok_or_else(|| {
                "source.export_url is required for VictoriaMetrics verify".to_string()
            })?;
            let mut out = BTreeMap::new();
            for selector in &plan.selectors {
                let exported = fetch_victoriametrics_export(
                    client,
                    export_url,
                    &plan.source.headers,
                    selector,
                    start_ms,
                    end_ms,
                )?;
                out.insert(selector.clone(), canonicalize_vm_export(&exported));
            }
            Ok(out)
        }
        SourceKind::Otlp
        | SourceKind::InfluxLineProtocol
        | SourceKind::Statsd
        | SourceKind::GraphitePlaintext => {
            let prepared_source = prepared_source
                .ok_or_else(|| "capture source data must be prepared for verify".to_string())?;
            let mut out = BTreeMap::new();
            for selector in &plan.selectors {
                let filtered =
                    filter_source_series_by_selector(prepared_source, selector, start_ms, end_ms)?;
                out.insert(selector.clone(), canonicalize_writable_series(&filtered));
            }
            Ok(out)
        }
    }
}

fn fetch_target_raw_checks(
    client: &HttpClient,
    target: &TargetConfig,
    selectors: &[String],
    start_ms: i64,
    end_ms: i64,
) -> Result<BTreeMap<String, BTreeMap<SeriesKey, CanonicalSeries>>, String> {
    let response = remote_read_request(
        client,
        &target.read_url,
        &target.headers,
        target.tenant.as_deref(),
        selectors,
        start_ms,
        end_ms,
    )?;
    if response.results.len() != selectors.len() {
        return Err(format!(
            "target remote read returned {} query results for {} selectors",
            response.results.len(),
            selectors.len()
        ));
    }
    let mut out = BTreeMap::new();
    for (selector, result) in selectors.iter().zip(response.results.into_iter()) {
        out.insert(selector.clone(), canonicalize_query_result(result));
    }
    Ok(out)
}

fn prepare_capture_source_data(plan: &MigrationPlan) -> Result<PreparedSourceData, String> {
    let capture_entries = load_capture_manifest(plan)?;
    let tenant_id = effective_target_tenant(plan).to_string();
    let mut series = BTreeMap::new();
    let mut metadata = BTreeMap::<MetadataEntryKey, MetricMetadata>::new();
    let mut otlp_supported_shapes = BTreeSet::new();
    let statsd_adapter = StatsdAdapter::default();

    for (entry_idx, entry) in capture_entries.iter().enumerate() {
        let body = capture_entry_body(plan, entry, entry_idx)?;
        match plan.source.kind {
            SourceKind::Otlp => {
                let request =
                    ExportMetricsServiceRequest::decode(body.as_slice()).map_err(|err| {
                        format!(
                        "invalid OTLP protobuf payload at capture manifest entry {entry_idx}: {err}"
                    )
                    })?;
                let (envelope, stats) = normalize_metrics_export_request(
                    request,
                    &tenant_id,
                    TimestampPrecision::Milliseconds,
                )
                .map_err(|err| {
                    format!("failed normalizing OTLP capture manifest entry {entry_idx}: {err}")
                })?;
                merge_normalized_envelope(&mut series, &mut metadata, envelope)?;
                if stats.gauges > 0 {
                    otlp_supported_shapes.insert("gauge".to_string());
                }
                if stats.sums > 0 {
                    otlp_supported_shapes.insert("sum:cumulative".to_string());
                }
                if stats.histograms > 0 {
                    otlp_supported_shapes
                        .insert("histogram:cumulative:explicit_buckets".to_string());
                }
                if stats.summaries > 0 {
                    otlp_supported_shapes.insert("summary".to_string());
                }
            }
            SourceKind::InfluxLineProtocol => {
                let received_at_ms = capture_entry_received_at_ms(entry, entry_idx, &plan.source)?;
                let text = String::from_utf8(body).map_err(|err| {
                    format!(
                        "capture manifest entry {entry_idx} body must be valid UTF-8 for Influx line protocol: {err}"
                    )
                })?;
                let query_labels = influx_query_labels(&entry.query_params);
                let normalized = normalize_influx_line_protocol(
                    &text,
                    &tenant_id,
                    TimestampPrecision::Milliseconds,
                    received_at_ms,
                    influx_line_protocol_config(),
                    query_labels,
                    entry.query_params.get("precision").map(String::as_str),
                )
                .map_err(|err| {
                    format!("failed normalizing Influx capture manifest entry {entry_idx}: {err}")
                })?;
                merge_normalized_envelope(&mut series, &mut metadata, normalized.envelope)?;
            }
            SourceKind::Statsd => {
                let received_at_ms = capture_entry_received_at_ms(entry, entry_idx, &plan.source)?;
                let text = String::from_utf8(body).map_err(|err| {
                    format!(
                        "capture manifest entry {entry_idx} body must be valid UTF-8 for StatsD: {err}"
                    )
                })?;
                let normalized = statsd_adapter
                    .normalize_packet(&text, &tenant_id, received_at_ms, statsd_config())
                    .map_err(|err| {
                        format!(
                            "failed normalizing StatsD capture manifest entry {entry_idx}: {err}"
                        )
                    })?;
                merge_normalized_envelope(&mut series, &mut metadata, normalized.envelope)?;
            }
            SourceKind::GraphitePlaintext => {
                let received_at_ms = capture_entry_received_at_ms(entry, entry_idx, &plan.source)?;
                let text = String::from_utf8(body).map_err(|err| {
                    format!(
                        "capture manifest entry {entry_idx} body must be valid UTF-8 for Graphite plaintext: {err}"
                    )
                })?;
                let line_limit = graphite_config().max_line_bytes;
                for (line_idx, raw_line) in text.lines().enumerate() {
                    if raw_line.len() > line_limit {
                        return Err(format!(
                            "graphite capture manifest entry {entry_idx} line {line_idx} exceeds byte limit: {} > {}",
                            raw_line.len(),
                            line_limit
                        ));
                    }
                    let normalized = normalize_graphite_plaintext_line(
                        raw_line,
                        &tenant_id,
                        TimestampPrecision::Milliseconds,
                        received_at_ms,
                    )
                    .map_err(|err| {
                        format!(
                            "failed normalizing Graphite capture manifest entry {entry_idx} line {line_idx}: {err}"
                        )
                    })?;
                    merge_normalized_envelope(&mut series, &mut metadata, normalized.envelope)?;
                }
            }
            SourceKind::Prometheus | SourceKind::Victoriametrics => {
                return Err(format!(
                    "capture manifest source preparation is not supported for {}",
                    plan.source.kind.as_str()
                ));
            }
        }
    }

    Ok(PreparedSourceData {
        series,
        metadata: metadata.into_values().collect(),
        otlp_supported_shapes: otlp_supported_shapes.into_iter().collect(),
    })
}

fn load_capture_manifest(plan: &MigrationPlan) -> Result<Vec<CaptureManifestEntry>, String> {
    let manifest_path = plan
        .source
        .capture_manifest_path
        .as_deref()
        .ok_or_else(|| {
            format!(
                "source.capture_manifest_path is required for {} migrations",
                plan.source.kind.as_str()
            )
        })?;
    let manifest_path = resolve_plan_relative_path(plan, manifest_path);
    let body = fs::read(&manifest_path).map_err(|err| {
        format!(
            "failed reading capture manifest {}: {err}",
            manifest_path.display()
        )
    })?;
    serde_json::from_slice(&body).map_err(|err| {
        format!(
            "invalid capture manifest JSON {}: {err}",
            manifest_path.display()
        )
    })
}

fn capture_entry_body(
    plan: &MigrationPlan,
    entry: &CaptureManifestEntry,
    entry_idx: usize,
) -> Result<Vec<u8>, String> {
    let mut sources = 0usize;
    if entry.path.is_some() {
        sources += 1;
    }
    if entry.body.is_some() {
        sources += 1;
    }
    if entry.body_base64.is_some() {
        sources += 1;
    }
    if sources != 1 {
        return Err(format!(
            "capture manifest entry {entry_idx} must set exactly one of path, body, or body_base64"
        ));
    }

    if let Some(path) = &entry.path {
        let resolved = resolve_plan_relative_path(plan, path);
        return fs::read(&resolved).map_err(|err| {
            format!(
                "failed reading capture payload {} for entry {entry_idx}: {err}",
                resolved.display()
            )
        });
    }
    if let Some(body) = &entry.body {
        return Ok(body.as_bytes().to_vec());
    }
    if let Some(body_base64) = &entry.body_base64 {
        return base64::engine::general_purpose::STANDARD
            .decode(body_base64)
            .map_err(|err| {
                format!("invalid body_base64 in capture manifest entry {entry_idx}: {err}")
            });
    }
    unreachable!("capture entry validation should ensure a body source exists");
}

fn resolve_plan_relative_path(plan: &MigrationPlan, raw: &str) -> PathBuf {
    let path = PathBuf::from(raw);
    if path.is_absolute() {
        path
    } else {
        plan.plan_dir.join(path)
    }
}

fn capture_entry_received_at_ms(
    entry: &CaptureManifestEntry,
    entry_idx: usize,
    source: &SourceConfig,
) -> Result<i64, String> {
    entry.received_at_ms.ok_or_else(|| {
        format!(
            "capture manifest entry {entry_idx} requires received_at_ms for {} sources when timestamps are omitted",
            source.kind.as_str()
        )
    })
}

fn effective_target_tenant(plan: &MigrationPlan) -> &str {
    plan.target.tenant.as_deref().unwrap_or("default")
}

fn influx_query_labels(query_params: &BTreeMap<String, String>) -> Vec<(String, String)> {
    let mut labels = Vec::new();
    for (param, label) in [
        ("db", "influx_db"),
        ("rp", "influx_rp"),
        ("bucket", "influx_bucket"),
        ("org", "influx_org"),
    ] {
        if let Some(value) = query_params.get(param) {
            let value = value.trim();
            if !value.is_empty() {
                labels.push((label.to_string(), value.to_string()));
            }
        }
    }
    labels
}

fn merge_normalized_envelope(
    out: &mut BTreeMap<SeriesKey, WritableSeries>,
    metadata: &mut BTreeMap<MetadataEntryKey, MetricMetadata>,
    envelope: NormalizedWriteEnvelope,
) -> Result<(), String> {
    for sample in envelope.scalar_samples {
        let value = sample.data_point.value_as_f64().ok_or_else(|| {
            format!(
                "normalized scalar sample for metric '{}' is not representable as f64",
                sample.series.metric
            )
        })?;
        let labels = normalized_series_identity_to_labels(&sample.series);
        let key = labels_to_key(&labels);
        let entry = out.entry(key).or_insert_with(|| WritableSeries {
            labels: canonical_proto_labels(&labels),
            ..WritableSeries::default()
        });
        entry.samples.push(Sample {
            value,
            timestamp: sample.data_point.timestamp,
        });
        dedupe_writable_series(entry);
    }

    for sample in envelope.histogram_samples {
        let labels = normalized_series_identity_to_labels(&sample.series);
        let key = labels_to_key(&labels);
        let entry = out.entry(key).or_insert_with(|| WritableSeries {
            labels: canonical_proto_labels(&labels),
            ..WritableSeries::default()
        });
        entry.histograms.push(normalized_histogram_to_proto(sample));
        dedupe_writable_series(entry);
    }

    for exemplar in envelope.exemplars {
        let labels = normalized_series_identity_to_labels(&exemplar.series);
        let key = labels_to_key(&labels);
        let entry = out.entry(key).or_insert_with(|| WritableSeries {
            labels: canonical_proto_labels(&labels),
            ..WritableSeries::default()
        });
        entry.exemplars.push(Exemplar {
            labels: exemplar
                .labels
                .into_iter()
                .map(|label| Label {
                    name: label.name,
                    value: label.value,
                })
                .collect(),
            value: exemplar.value,
            timestamp: exemplar.timestamp,
        });
        dedupe_writable_series(entry);
    }

    for update in envelope.metadata_updates {
        let key = MetadataEntryKey {
            metric: update.metric_family_name.clone(),
            metric_type: update.metric_type as i32,
            help: update.help.clone(),
            unit: update.unit.clone(),
        };
        metadata
            .entry(key)
            .or_insert_with(|| metadata_update_to_proto(update));
    }

    Ok(())
}

fn normalized_series_identity_to_labels(series: &NormalizedSeriesIdentity) -> Vec<Label> {
    let mut labels = Vec::with_capacity(series.labels.len() + 1);
    labels.push(Label {
        name: "__name__".to_string(),
        value: series.metric.clone(),
    });
    labels.extend(series.labels.iter().cloned().map(|label| Label {
        name: label.name,
        value: label.value,
    }));
    canonical_proto_labels(&labels)
}

fn normalized_histogram_to_proto(sample: NormalizedHistogramSample) -> Histogram {
    Histogram {
        count: sample.count.map(|count| match count {
            NormalizedHistogramCount::Int(value) => histogram::Count::CountInt(value),
            NormalizedHistogramCount::Float(value) => histogram::Count::CountFloat(value),
        }),
        sum: sample.sum,
        schema: sample.schema,
        zero_threshold: sample.zero_threshold,
        zero_count: sample.zero_count.map(|count| match count {
            NormalizedHistogramCount::Int(value) => histogram::ZeroCount::ZeroCountInt(value),
            NormalizedHistogramCount::Float(value) => histogram::ZeroCount::ZeroCountFloat(value),
        }),
        negative_spans: sample
            .negative_spans
            .into_iter()
            .map(|span| BucketSpan {
                offset: span.offset,
                length: span.length,
            })
            .collect(),
        negative_deltas: sample.negative_deltas,
        negative_counts: sample.negative_counts,
        positive_spans: sample
            .positive_spans
            .into_iter()
            .map(|span| BucketSpan {
                offset: span.offset,
                length: span.length,
            })
            .collect(),
        positive_deltas: sample.positive_deltas,
        positive_counts: sample.positive_counts,
        reset_hint: sample.reset_hint as i32,
        timestamp: sample.timestamp,
        custom_values: sample.custom_values,
    }
}

fn metadata_update_to_proto(update: NormalizedMetricMetadataUpdate) -> MetricMetadata {
    MetricMetadata {
        r#type: update.metric_type as i32,
        metric_family_name: update.metric_family_name,
        help: update.help,
        unit: update.unit,
    }
}

fn select_source_series_for_window(
    prepared_source: &PreparedSourceData,
    selectors: &[String],
    start_ms: i64,
    end_ms: i64,
) -> Result<BTreeMap<SeriesKey, WritableSeries>, String> {
    let compiled = selectors
        .iter()
        .map(|selector| compile_selector(selector))
        .collect::<Result<Vec<_>, _>>()?;
    let mut out = BTreeMap::new();
    for (key, series) in &prepared_source.series {
        if !compiled
            .iter()
            .any(|selector| selector_matches_series_key(selector, key))
        {
            continue;
        }
        if let Some(filtered) = filter_writable_series_window(series, start_ms, end_ms) {
            out.insert(key.clone(), filtered);
        }
    }
    Ok(out)
}

fn filter_source_series_by_selector(
    prepared_source: &PreparedSourceData,
    selector: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<BTreeMap<SeriesKey, WritableSeries>, String> {
    let compiled = compile_selector(selector)?;
    let mut out = BTreeMap::new();
    for (key, series) in &prepared_source.series {
        if !selector_matches_series_key(&compiled, key) {
            continue;
        }
        if let Some(filtered) = filter_writable_series_window(series, start_ms, end_ms) {
            out.insert(key.clone(), filtered);
        }
    }
    Ok(out)
}

fn filter_writable_series_window(
    series: &WritableSeries,
    start_ms: i64,
    end_ms: i64,
) -> Option<WritableSeries> {
    let mut filtered = WritableSeries {
        labels: series.labels.clone(),
        ..WritableSeries::default()
    };
    filtered.samples = series
        .samples
        .iter()
        .filter(|sample| sample.timestamp >= start_ms && sample.timestamp <= end_ms)
        .cloned()
        .collect();
    filtered.histograms = series
        .histograms
        .iter()
        .filter(|sample| sample.timestamp >= start_ms && sample.timestamp <= end_ms)
        .cloned()
        .collect();
    filtered.exemplars = series
        .exemplars
        .iter()
        .filter(|sample| sample.timestamp >= start_ms && sample.timestamp <= end_ms)
        .cloned()
        .collect();
    dedupe_writable_series(&mut filtered);
    if filtered.samples.is_empty()
        && filtered.histograms.is_empty()
        && filtered.exemplars.is_empty()
    {
        None
    } else {
        Some(filtered)
    }
}

fn canonicalize_writable_series(
    series: &BTreeMap<SeriesKey, WritableSeries>,
) -> BTreeMap<SeriesKey, CanonicalSeries> {
    let mut out = BTreeMap::new();
    for (key, series) in series {
        let mut samples = series
            .samples
            .iter()
            .map(|sample| CanonicalSample {
                timestamp: sample.timestamp,
                value: sample.value,
            })
            .collect::<Vec<_>>();
        samples.sort_by_key(|sample| sample.timestamp);

        let mut histograms = series
            .histograms
            .iter()
            .map(|histogram| CanonicalHistogram {
                timestamp: histogram.timestamp,
                payload: canonical_histogram_json(histogram),
            })
            .collect::<Vec<_>>();
        histograms.sort_by_key(|histogram| histogram.timestamp);

        let mut exemplars = series
            .exemplars
            .iter()
            .map(|exemplar| CanonicalExemplar {
                timestamp: exemplar.timestamp,
                value: exemplar.value,
                labels: labels_to_key(&exemplar.labels),
            })
            .collect::<Vec<_>>();
        exemplars.sort_by(|left, right| {
            left.timestamp
                .cmp(&right.timestamp)
                .then_with(|| left.labels.cmp(&right.labels))
        });

        out.insert(
            key.clone(),
            CanonicalSeries {
                labels: key.clone(),
                samples,
                histograms,
                exemplars,
            },
        );
    }
    out
}

fn prepared_metadata_map(
    prepared_source: &PreparedSourceData,
    metrics: &[String],
) -> BTreeMap<String, BTreeSet<(String, String, String)>> {
    let metrics = metrics.iter().cloned().collect::<BTreeSet<_>>();
    let mut out = BTreeMap::<String, BTreeSet<(String, String, String)>>::new();
    for metadata in &prepared_source.metadata {
        if !metrics.contains(&metadata.metric_family_name) {
            continue;
        }
        out.entry(metadata.metric_family_name.clone())
            .or_default()
            .insert((
                metadata_type_name(metadata.r#type)
                    .unwrap_or("unknown")
                    .to_string(),
                metadata.help.clone(),
                metadata.unit.clone(),
            ));
    }
    for metric in metrics {
        out.entry(metric).or_default();
    }
    out
}

fn filter_prepared_metadata(
    prepared_source: &PreparedSourceData,
    metrics: &[String],
) -> Vec<MetricMetadata> {
    let metrics = metrics.iter().cloned().collect::<BTreeSet<_>>();
    prepared_source
        .metadata
        .iter()
        .filter(|metadata| metrics.contains(&metadata.metric_family_name))
        .cloned()
        .collect()
}

fn select_source_exemplars(
    prepared_source: &PreparedSourceData,
    query: &str,
    limit: usize,
    start_ms: i64,
    end_ms: i64,
) -> Result<BTreeMap<SeriesKey, Vec<CanonicalExemplar>>, String> {
    let filtered = filter_source_series_by_selector(prepared_source, query, start_ms, end_ms)?;
    let mut remaining = limit.max(1);
    let mut out = BTreeMap::new();
    for (key, series) in filtered {
        if remaining == 0 {
            break;
        }
        let mut exemplars = series
            .exemplars
            .into_iter()
            .map(|exemplar| CanonicalExemplar {
                timestamp: exemplar.timestamp,
                value: exemplar.value,
                labels: labels_to_key(&exemplar.labels),
            })
            .collect::<Vec<_>>();
        exemplars.sort_by(|left, right| {
            left.timestamp
                .cmp(&right.timestamp)
                .then_with(|| left.labels.cmp(&right.labels))
        });
        exemplars.truncate(remaining);
        if exemplars.is_empty() {
            continue;
        }
        remaining = remaining.saturating_sub(exemplars.len());
        out.insert(key, exemplars);
    }
    Ok(out)
}

fn compile_selector(selector: &str) -> Result<CompiledSelector, String> {
    let query = selector_to_read_query(selector, 0, 0)?;
    let matchers = query
        .matchers
        .into_iter()
        .map(|matcher| {
            let op = match MatcherType::try_from(matcher.r#type).map_err(|_| {
                format!(
                    "selector {selector:?} contains unsupported matcher type {}",
                    matcher.r#type
                )
            })? {
                MatcherType::Eq => SeriesMatcherOp::Equal,
                MatcherType::Neq => SeriesMatcherOp::NotEqual,
                MatcherType::Re => SeriesMatcherOp::RegexMatch,
                MatcherType::Nre => SeriesMatcherOp::RegexNoMatch,
            };
            let regex = match op {
                SeriesMatcherOp::RegexMatch | SeriesMatcherOp::RegexNoMatch => {
                    let anchored = format!("^(?:{})$", matcher.value);
                    Some(Regex::new(&anchored).map_err(|err| {
                        format!("invalid selector regex {:?}: {err}", matcher.value)
                    })?)
                }
                SeriesMatcherOp::Equal | SeriesMatcherOp::NotEqual => None,
            };
            Ok(CompiledSelectorMatcher {
                name: matcher.name,
                op,
                value: matcher.value,
                regex,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;
    Ok(CompiledSelector { matchers })
}

fn selector_matches_series_key(selector: &CompiledSelector, key: &SeriesKey) -> bool {
    selector.matchers.iter().all(|matcher| {
        let actual = key
            .iter()
            .find(|(name, _)| name == &matcher.name)
            .map(|(_, value)| value.as_str())
            .unwrap_or("");
        match matcher.op {
            SeriesMatcherOp::Equal => actual == matcher.value,
            SeriesMatcherOp::NotEqual => actual != matcher.value,
            SeriesMatcherOp::RegexMatch => matcher
                .regex
                .as_ref()
                .is_some_and(|regex| regex.is_match(actual)),
            SeriesMatcherOp::RegexNoMatch => !matcher
                .regex
                .as_ref()
                .is_some_and(|regex| regex.is_match(actual)),
        }
    })
}

fn remote_read_request(
    client: &HttpClient,
    url: &str,
    headers: &BTreeMap<String, String>,
    tenant: Option<&str>,
    selectors: &[String],
    start_ms: i64,
    end_ms: i64,
) -> Result<ReadResponse, String> {
    let mut queries = Vec::with_capacity(selectors.len());
    for selector in selectors {
        queries.push(selector_to_read_query(selector, start_ms, end_ms)?);
    }
    let request = ReadRequest {
        queries,
        accepted_response_types: vec![ReadResponseType::Samples as i32],
    };
    let encoded = request.encode_to_vec();
    let compressed = SnappyEncoder::new()
        .compress_vec(&encoded)
        .map_err(|err| format!("snappy encode failed: {err}"))?;
    let response = client.post_bytes(
        url,
        headers,
        tenant,
        compressed,
        "application/x-protobuf",
        &[
            ("Content-Encoding", "snappy"),
            ("Accept-Encoding", "snappy"),
            ("X-Prometheus-Remote-Read-Version", "0.1.0"),
        ],
    )?;
    let decoded = SnappyDecoder::new()
        .decompress_vec(&response)
        .map_err(|err| format!("snappy decode failed: {err}"))?;
    ReadResponse::decode(decoded.as_slice())
        .map_err(|err| format!("failed decoding remote-read response from {url}: {err}"))
}

fn selector_to_read_query(selector: &str, start_ms: i64, end_ms: i64) -> Result<Query, String> {
    let expr = tsink::promql::parse(selector)
        .map_err(|err| format!("invalid selector {selector:?}: {err}"))?;
    let selector = extract_vector_selector(expr)
        .ok_or_else(|| format!("selector must be a vector or matrix selector: {selector}"))?;

    let mut matchers = Vec::new();
    if let Some(metric_name) = selector.metric_name {
        matchers.push(LabelMatcher {
            r#type: MatcherType::Eq as i32,
            name: "__name__".to_string(),
            value: metric_name,
        });
    }
    for matcher in selector.matchers {
        matchers.push(LabelMatcher {
            r#type: match matcher.op {
                MatchOp::Equal => MatcherType::Eq as i32,
                MatchOp::NotEqual => MatcherType::Neq as i32,
                MatchOp::RegexMatch => MatcherType::Re as i32,
                MatchOp::RegexNoMatch => MatcherType::Nre as i32,
            },
            name: matcher.name,
            value: matcher.value,
        });
    }

    Ok(Query {
        start_timestamp_ms: start_ms,
        end_timestamp_ms: end_ms,
        matchers,
        hints: None,
    })
}

fn extract_vector_selector(expr: Expr) -> Option<VectorSelector> {
    match expr {
        Expr::VectorSelector(selector) => Some(selector),
        Expr::MatrixSelector(selector) => Some(selector.vector),
        Expr::Paren(inner) => extract_vector_selector(*inner),
        _ => None,
    }
}

fn merge_query_result_into_writable_series(
    out: &mut BTreeMap<SeriesKey, WritableSeries>,
    result: QueryResult,
) {
    for series in result.timeseries {
        let key = labels_to_key(&series.labels);
        let entry = out.entry(key).or_insert_with(|| WritableSeries {
            labels: canonical_proto_labels(&series.labels),
            ..WritableSeries::default()
        });
        entry.samples.extend(series.samples);
        entry.histograms.extend(series.histograms);
        dedupe_writable_series(entry);
    }
}

fn merge_exported_series(
    out: &mut BTreeMap<SeriesKey, WritableSeries>,
    exported: BTreeMap<SeriesKey, CanonicalSeries>,
) {
    for (key, series) in exported {
        let entry = out.entry(key.clone()).or_insert_with(|| WritableSeries {
            labels: key_to_proto_labels(&key),
            ..WritableSeries::default()
        });
        entry
            .samples
            .extend(series.samples.into_iter().map(|sample| Sample {
                value: sample.value,
                timestamp: sample.timestamp,
            }));
        dedupe_writable_series(entry);
    }
}

#[allow(clippy::too_many_arguments)]
fn merge_exemplar_queries_into_series(
    out: &mut BTreeMap<SeriesKey, WritableSeries>,
    client: &HttpClient,
    url: &str,
    headers: &BTreeMap<String, String>,
    tenant: Option<&str>,
    checks: &[ExemplarCheckPlan],
    start_ms: i64,
    end_ms: i64,
) -> Result<(), String> {
    for check in checks {
        let query_series = fetch_exemplar_series(
            client,
            url,
            headers,
            tenant,
            &check.query,
            check.limit,
            start_ms,
            end_ms,
        )?;
        for (key, exemplars) in query_series {
            let entry = out.entry(key.clone()).or_insert_with(|| WritableSeries {
                labels: key_to_proto_labels(&key),
                ..WritableSeries::default()
            });
            entry
                .exemplars
                .extend(exemplars.into_iter().map(|item| Exemplar {
                    labels: key_to_proto_labels(&item.labels),
                    value: item.value,
                    timestamp: item.timestamp,
                }));
            dedupe_writable_series(entry);
        }
    }
    Ok(())
}

fn dedupe_writable_series(series: &mut WritableSeries) {
    series.labels = canonical_proto_labels(&series.labels);

    series.samples.sort_by(|left, right| {
        left.timestamp
            .cmp(&right.timestamp)
            .then_with(|| left.value.total_cmp(&right.value))
    });
    series
        .samples
        .dedup_by(|left, right| left.timestamp == right.timestamp && left.value == right.value);

    series
        .histograms
        .sort_by_key(|histogram| histogram.timestamp);
    series.histograms.dedup_by(|left, right| {
        left.timestamp == right.timestamp
            && canonical_histogram_json(left) == canonical_histogram_json(right)
    });

    series.exemplars.sort_by(|left, right| {
        left.timestamp
            .cmp(&right.timestamp)
            .then_with(|| left.value.total_cmp(&right.value))
            .then_with(|| labels_to_key(&left.labels).cmp(&labels_to_key(&right.labels)))
    });
    series.exemplars.dedup_by(|left, right| {
        left.timestamp == right.timestamp
            && left.value == right.value
            && labels_to_key(&left.labels) == labels_to_key(&right.labels)
    });
}

fn build_write_batches(
    series: &BTreeMap<SeriesKey, WritableSeries>,
    metadata: &[MetricMetadata],
    batch: &BatchConfig,
) -> Vec<WriteRequest> {
    let mut out = Vec::new();
    let metadata_chunk = batch.max_series_per_write.max(1);
    for chunk in metadata.chunks(metadata_chunk) {
        out.push(WriteRequest {
            timeseries: Vec::new(),
            metadata: chunk.to_vec(),
        });
    }

    let mut current = WriteRequest {
        timeseries: Vec::new(),
        metadata: Vec::new(),
    };
    let mut current_points = 0usize;
    for series in series.values() {
        let points = series.samples.len() + series.histograms.len() + series.exemplars.len();
        if !current.timeseries.is_empty()
            && (current.timeseries.len() >= batch.max_series_per_write
                || current_points + points > batch.max_points_per_write)
        {
            out.push(current);
            current = WriteRequest {
                timeseries: Vec::new(),
                metadata: Vec::new(),
            };
            current_points = 0;
        }
        current_points += points;
        current.timeseries.push(TimeSeries {
            labels: series.labels.clone(),
            samples: series.samples.clone(),
            exemplars: series.exemplars.clone(),
            histograms: series.histograms.clone(),
        });
    }
    if !current.timeseries.is_empty() {
        out.push(current);
    }
    out
}

fn send_write_request(
    client: &HttpClient,
    target: &TargetConfig,
    batch: &WriteRequest,
) -> Result<(), String> {
    let encoded = batch.encode_to_vec();
    let compressed = SnappyEncoder::new()
        .compress_vec(&encoded)
        .map_err(|err| format!("snappy encode failed: {err}"))?;
    client.post_bytes(
        &target.write_url,
        &target.headers,
        target.tenant.as_deref(),
        compressed,
        "application/x-protobuf",
        &[
            ("Content-Encoding", "snappy"),
            ("X-Prometheus-Remote-Write-Version", "0.1.0"),
        ],
    )?;
    Ok(())
}

fn effective_metadata_metrics(plan: &MigrationPlan) -> Result<Vec<String>, String> {
    let mut metrics = BTreeSet::new();
    for metric in &plan.metadata_metrics {
        if !metric.trim().is_empty() {
            metrics.insert(metric.trim().to_string());
        }
    }
    for selector in &plan.selectors {
        if let Some(metric) = metric_name_from_selector(selector)? {
            metrics.insert(metric);
        }
    }
    Ok(metrics.into_iter().collect())
}

fn metric_name_from_selector(selector: &str) -> Result<Option<String>, String> {
    let expr = tsink::promql::parse(selector)
        .map_err(|err| format!("invalid selector {selector:?}: {err}"))?;
    let Some(selector) = extract_vector_selector(expr) else {
        return Ok(None);
    };
    if let Some(metric_name) = selector.metric_name {
        return Ok(Some(metric_name));
    }
    for matcher in selector.matchers {
        if matcher.name == "__name__" && matcher.op == MatchOp::Equal {
            return Ok(Some(matcher.value));
        }
    }
    Ok(None)
}

fn effective_exemplar_checks(plan: &MigrationPlan) -> Vec<ExemplarCheckPlan> {
    if !plan.exemplar_checks.is_empty() {
        return plan.exemplar_checks.clone();
    }
    plan.selectors
        .iter()
        .map(|selector| ExemplarCheckPlan {
            query: selector.clone(),
            limit: default_exemplar_limit(),
        })
        .collect()
}

fn fetch_metadata_entries(
    client: &HttpClient,
    url: &str,
    headers: &BTreeMap<String, String>,
    tenant: Option<&str>,
    metrics: &[String],
) -> Result<Vec<MetricMetadata>, String> {
    let mut unique = BTreeSet::new();
    for metric in metrics {
        let body = client.get_json(
            url,
            headers,
            tenant,
            &[(String::from("metric"), metric.clone())],
        )?;
        let data = require_prometheus_data(&body)?;
        let entries = data
            .get(metric)
            .and_then(JsonValue::as_array)
            .cloned()
            .unwrap_or_default();
        for entry in entries {
            let Some(metric_type) = entry.get("type").and_then(JsonValue::as_str) else {
                continue;
            };
            unique.insert(MetadataEntryKey {
                metric: metric.clone(),
                metric_type: metadata_type_from_str(metric_type)?,
                help: entry
                    .get("help")
                    .and_then(JsonValue::as_str)
                    .unwrap_or_default()
                    .to_string(),
                unit: entry
                    .get("unit")
                    .and_then(JsonValue::as_str)
                    .unwrap_or_default()
                    .to_string(),
            });
        }
    }

    Ok(unique
        .into_iter()
        .map(|entry| MetricMetadata {
            r#type: entry.metric_type,
            metric_family_name: entry.metric,
            help: entry.help,
            unit: entry.unit,
        })
        .collect())
}

fn metadata_type_from_str(metric_type: &str) -> Result<i32, String> {
    let value = match metric_type {
        "unknown" => MetricType::Unknown as i32,
        "counter" => MetricType::Counter as i32,
        "gauge" => MetricType::Gauge as i32,
        "histogram" => MetricType::Histogram as i32,
        "gaugehistogram" => MetricType::Gaugehistogram as i32,
        "summary" => MetricType::Summary as i32,
        "info" => MetricType::Info as i32,
        "stateset" => MetricType::Stateset as i32,
        other => return Err(format!("unsupported metadata type: {other}")),
    };
    Ok(value)
}

fn verify_metadata(
    client: &HttpClient,
    plan: &MigrationPlan,
    prepared_source: Option<&PreparedSourceData>,
) -> Result<Vec<MetadataCheckReport>, String> {
    let metrics = effective_metadata_metrics(plan)?;
    if metrics.is_empty() {
        return Ok(Vec::new());
    }
    let source = match plan.source.kind {
        SourceKind::Prometheus | SourceKind::Victoriametrics => {
            let Some(source_url) = &plan.source.metadata_url else {
                return Err(
                    "source.metadata_url is required when metadata verification is requested"
                        .to_string(),
                );
            };
            fetch_metadata_map(client, source_url, &plan.source.headers, None, &metrics)?
        }
        SourceKind::Otlp
        | SourceKind::InfluxLineProtocol
        | SourceKind::Statsd
        | SourceKind::GraphitePlaintext => prepared_metadata_map(
            prepared_source.ok_or_else(|| {
                "capture source data must be prepared for metadata verification".to_string()
            })?,
            &metrics,
        ),
    };
    let Some(target_url) = &plan.target.metadata_url else {
        return Err(
            "target.metadata_url is required when metadata verification is requested".to_string(),
        );
    };
    let target = fetch_metadata_map(
        client,
        target_url,
        &plan.target.headers,
        plan.target.tenant.as_deref(),
        &metrics,
    )?;

    let mut out = Vec::new();
    for metric in metrics {
        let source_entries = source.get(&metric).cloned().unwrap_or_default();
        let target_entries = target.get(&metric).cloned().unwrap_or_default();
        out.push(MetadataCheckReport {
            metric,
            source_entries: source_entries.len(),
            target_entries: target_entries.len(),
            matched: source_entries == target_entries,
        });
    }
    Ok(out)
}

fn fetch_metadata_map(
    client: &HttpClient,
    url: &str,
    headers: &BTreeMap<String, String>,
    tenant: Option<&str>,
    metrics: &[String],
) -> Result<MetricMetadataMap, String> {
    let mut out = BTreeMap::new();
    for metric in metrics {
        let body = client.get_json(
            url,
            headers,
            tenant,
            &[(String::from("metric"), metric.clone())],
        )?;
        let data = require_prometheus_data(&body)?;
        let entries = data
            .get(metric)
            .and_then(JsonValue::as_array)
            .cloned()
            .unwrap_or_default();
        let mut parsed = BTreeSet::new();
        for entry in entries {
            parsed.insert((
                entry
                    .get("type")
                    .and_then(JsonValue::as_str)
                    .unwrap_or_default()
                    .to_string(),
                entry
                    .get("help")
                    .and_then(JsonValue::as_str)
                    .unwrap_or_default()
                    .to_string(),
                entry
                    .get("unit")
                    .and_then(JsonValue::as_str)
                    .unwrap_or_default()
                    .to_string(),
            ));
        }
        out.insert(metric.clone(), parsed);
    }
    Ok(out)
}

fn verify_exemplars(
    client: &HttpClient,
    plan: &MigrationPlan,
    start_ms: i64,
    end_ms: i64,
    prepared_source: Option<&PreparedSourceData>,
) -> Result<Vec<ExemplarCheckReport>, String> {
    let checks = effective_exemplar_checks(plan);
    if checks.is_empty() {
        return Ok(Vec::new());
    }
    let Some(target_url) = &plan.target.exemplar_url else {
        return Err(
            "target.exemplar_url is required when exemplar verification is requested".to_string(),
        );
    };

    let mut out = Vec::new();
    for check in &checks {
        let source = match plan.source.kind {
            SourceKind::Prometheus => {
                let Some(source_url) = &plan.source.exemplar_url else {
                    return Err(
                        "source.exemplar_url is required when exemplar verification is requested"
                            .to_string(),
                    );
                };
                fetch_exemplar_series(
                    client,
                    source_url,
                    &plan.source.headers,
                    None,
                    &check.query,
                    check.limit,
                    start_ms,
                    end_ms,
                )?
            }
            SourceKind::Victoriametrics => BTreeMap::new(),
            SourceKind::Otlp
            | SourceKind::InfluxLineProtocol
            | SourceKind::Statsd
            | SourceKind::GraphitePlaintext => select_source_exemplars(
                prepared_source.ok_or_else(|| {
                    "capture source data must be prepared for exemplar verification".to_string()
                })?,
                &check.query,
                check.limit,
                start_ms,
                end_ms,
            )?,
        };
        let target = fetch_exemplar_series(
            client,
            target_url,
            &plan.target.headers,
            plan.target.tenant.as_deref(),
            &check.query,
            check.limit,
            start_ms,
            end_ms,
        )?;
        let report = compare_exemplar_series(&check.query, &source, &target, &plan.compare);
        out.push(report);
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn fetch_exemplar_series(
    client: &HttpClient,
    url: &str,
    headers: &BTreeMap<String, String>,
    tenant: Option<&str>,
    query: &str,
    limit: usize,
    start_ms: i64,
    end_ms: i64,
) -> Result<BTreeMap<SeriesKey, Vec<CanonicalExemplar>>, String> {
    let body = client.get_json(
        url,
        headers,
        tenant,
        &[
            (String::from("query"), query.to_string()),
            (String::from("start"), prom_time(start_ms)),
            (String::from("end"), prom_time(end_ms)),
            (String::from("limit"), limit.to_string()),
        ],
    )?;
    let data = require_prometheus_data(&body)?;
    let rows = data
        .as_array()
        .ok_or_else(|| format!("exemplar API {url} returned non-array data"))?;
    let mut out = BTreeMap::new();
    for row in rows {
        let labels = json_labels_to_key(
            row.get("seriesLabels")
                .ok_or_else(|| "exemplar series is missing seriesLabels".to_string())?,
        )?;
        let exemplars = row
            .get("exemplars")
            .and_then(JsonValue::as_array)
            .cloned()
            .unwrap_or_default();
        let parsed = exemplars
            .into_iter()
            .map(|item| {
                let labels = json_labels_to_key(
                    item.get("labels")
                        .ok_or_else(|| "exemplar row is missing labels".to_string())?,
                )?;
                let value = parse_json_f64(
                    item.get("value")
                        .ok_or_else(|| "exemplar row is missing value".to_string())?,
                )?;
                let timestamp = parse_prometheus_timestamp_ms(
                    item.get("timestamp")
                        .ok_or_else(|| "exemplar row is missing timestamp".to_string())?,
                )?;
                Ok(CanonicalExemplar {
                    timestamp,
                    value,
                    labels,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        out.insert(labels, parsed);
    }
    Ok(out)
}

fn fetch_victoriametrics_export(
    client: &HttpClient,
    url: &str,
    headers: &BTreeMap<String, String>,
    selector: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<BTreeMap<SeriesKey, CanonicalSeries>, String> {
    let body = client.get_bytes(
        url,
        headers,
        None,
        &[
            (String::from("match[]"), selector.to_string()),
            (String::from("start"), prom_time(start_ms)),
            (String::from("end"), prom_time(end_ms)),
        ],
    )?;
    let reader = BufReader::new(Cursor::new(body));
    let mut out = BTreeMap::new();
    for line in reader.lines() {
        let line =
            line.map_err(|err| format!("failed reading VictoriaMetrics export line: {err}"))?;
        if line.trim().is_empty() {
            continue;
        }
        let row: VmExportLine = serde_json::from_str(&line)
            .map_err(|err| format!("invalid VictoriaMetrics export row: {err}"))?;
        if row.values.len() != row.timestamps.len() {
            return Err(format!(
                "VictoriaMetrics export row has {} values but {} timestamps",
                row.values.len(),
                row.timestamps.len()
            ));
        }
        let labels = labels_map_to_key(&row.metric);
        let samples = row
            .values
            .into_iter()
            .zip(row.timestamps.into_iter())
            .map(|(value, timestamp)| {
                Ok(CanonicalSample {
                    timestamp,
                    value: parse_json_f64(&value)?,
                })
            })
            .collect::<Result<Vec<_>, String>>()?;
        out.insert(
            labels.clone(),
            CanonicalSeries {
                labels,
                samples,
                histograms: Vec::new(),
                exemplars: Vec::new(),
            },
        );
    }
    Ok(out)
}

#[derive(Debug, Deserialize)]
struct VmExportLine {
    metric: BTreeMap<String, String>,
    values: Vec<JsonValue>,
    timestamps: Vec<i64>,
}

fn canonicalize_vm_export(
    exported: &BTreeMap<SeriesKey, CanonicalSeries>,
) -> BTreeMap<SeriesKey, CanonicalSeries> {
    exported.clone()
}

fn canonicalize_query_result(result: QueryResult) -> BTreeMap<SeriesKey, CanonicalSeries> {
    let mut out = BTreeMap::new();
    for series in result.timeseries {
        let key = labels_to_key(&series.labels);
        let mut samples = series
            .samples
            .into_iter()
            .map(|sample| CanonicalSample {
                timestamp: sample.timestamp,
                value: sample.value,
            })
            .collect::<Vec<_>>();
        samples.sort_by(|left, right| {
            left.timestamp
                .cmp(&right.timestamp)
                .then_with(|| left.value.total_cmp(&right.value))
        });

        let mut histograms = series
            .histograms
            .into_iter()
            .map(|histogram| CanonicalHistogram {
                timestamp: histogram.timestamp,
                payload: canonical_histogram_json(&histogram),
            })
            .collect::<Vec<_>>();
        histograms.sort_by_key(|histogram| histogram.timestamp);

        let mut exemplars = series
            .exemplars
            .into_iter()
            .map(|exemplar| CanonicalExemplar {
                timestamp: exemplar.timestamp,
                value: exemplar.value,
                labels: labels_to_key(&exemplar.labels),
            })
            .collect::<Vec<_>>();
        exemplars.sort_by(|left, right| {
            left.timestamp
                .cmp(&right.timestamp)
                .then_with(|| left.value.total_cmp(&right.value))
                .then_with(|| left.labels.cmp(&right.labels))
        });

        out.insert(
            key.clone(),
            CanonicalSeries {
                labels: key,
                samples,
                histograms,
                exemplars,
            },
        );
    }
    out
}

fn canonical_histogram_json(histogram: &Histogram) -> JsonValue {
    let count = match &histogram.count {
        Some(histogram::Count::CountInt(value)) => json!({
            "kind": "count_int",
            "value": value,
        }),
        Some(histogram::Count::CountFloat(value)) => json!({
            "kind": "count_float",
            "value": value,
        }),
        None => JsonValue::Null,
    };
    let zero_count = match &histogram.zero_count {
        Some(histogram::ZeroCount::ZeroCountInt(value)) => json!({
            "kind": "zero_count_int",
            "value": value,
        }),
        Some(histogram::ZeroCount::ZeroCountFloat(value)) => json!({
            "kind": "zero_count_float",
            "value": value,
        }),
        None => JsonValue::Null,
    };

    json!({
        "timestamp": histogram.timestamp,
        "count": count,
        "sum": histogram.sum,
        "schema": histogram.schema,
        "zeroThreshold": histogram.zero_threshold,
        "zeroCount": zero_count,
        "negativeSpans": histogram.negative_spans.iter().map(bucket_span_json).collect::<Vec<_>>(),
        "negativeDeltas": histogram.negative_deltas,
        "negativeCounts": histogram.negative_counts,
        "positiveSpans": histogram.positive_spans.iter().map(bucket_span_json).collect::<Vec<_>>(),
        "positiveDeltas": histogram.positive_deltas,
        "positiveCounts": histogram.positive_counts,
        "resetHint": histogram.reset_hint,
        "customValues": histogram.custom_values,
    })
}

fn bucket_span_json(span: &BucketSpan) -> JsonValue {
    json!({
        "offset": span.offset,
        "length": span.length,
    })
}

fn labels_to_key(labels: &[Label]) -> SeriesKey {
    let mut out = labels
        .iter()
        .filter(|label| label.name != "__tsink_tenant__")
        .map(|label| (label.name.clone(), label.value.clone()))
        .collect::<Vec<_>>();
    out.sort();
    out
}

fn labels_map_to_key(labels: &BTreeMap<String, String>) -> SeriesKey {
    let mut out = labels
        .iter()
        .filter(|(name, _)| name.as_str() != "__tsink_tenant__")
        .map(|(name, value)| (name.clone(), value.clone()))
        .collect::<Vec<_>>();
    out.sort();
    out
}

fn json_labels_to_key(value: &JsonValue) -> Result<SeriesKey, String> {
    let object = value
        .as_object()
        .ok_or_else(|| "expected JSON object with labels".to_string())?;
    let mut out = object
        .iter()
        .filter(|(name, _)| name.as_str() != "__tsink_tenant__")
        .map(|(name, value)| {
            value
                .as_str()
                .map(|value| (name.clone(), value.to_string()))
                .ok_or_else(|| format!("label {name} must be a string"))
        })
        .collect::<Result<Vec<_>, String>>()?;
    out.sort();
    Ok(out)
}

fn canonical_proto_labels(labels: &[Label]) -> Vec<Label> {
    let mut out = labels
        .iter()
        .filter(|label| label.name != "__tsink_tenant__")
        .cloned()
        .collect::<Vec<_>>();
    out.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| left.value.cmp(&right.value))
    });
    out
}

fn key_to_proto_labels(key: &SeriesKey) -> Vec<Label> {
    key.iter()
        .map(|(name, value)| Label {
            name: name.clone(),
            value: value.clone(),
        })
        .collect()
}

fn compare_raw_series(
    selector: &str,
    source: &BTreeMap<SeriesKey, CanonicalSeries>,
    target: &BTreeMap<SeriesKey, CanonicalSeries>,
    compare: &CompareConfig,
) -> RawCheckReport {
    let mut issues = Vec::new();
    let source_keys = source.keys().cloned().collect::<BTreeSet<_>>();
    let target_keys = target.keys().cloned().collect::<BTreeSet<_>>();
    let source_samples = source.values().map(|series| series.samples.len()).sum();
    let target_samples = target.values().map(|series| series.samples.len()).sum();
    let source_histograms = source.values().map(|series| series.histograms.len()).sum();
    let target_histograms = target.values().map(|series| series.histograms.len()).sum();

    let missing = source_keys
        .difference(&target_keys)
        .take(MAX_ISSUES_PER_CHECK)
        .cloned()
        .collect::<Vec<_>>();
    for key in &missing {
        issues.push(format!("missing series {}", format_series_key(key)));
    }
    let extra = target_keys
        .difference(&source_keys)
        .take(MAX_ISSUES_PER_CHECK)
        .cloned()
        .collect::<Vec<_>>();
    for key in &extra {
        issues.push(format!(
            "unexpected extra series {}",
            format_series_key(key)
        ));
    }

    let mut sample_mismatches = 0usize;
    let mut histogram_mismatches = 0usize;
    for key in source_keys.intersection(&target_keys) {
        let source_series = source.get(key).expect("source key exists");
        let target_series = target.get(key).expect("target key exists");
        sample_mismatches += compare_samples(
            key,
            &source_series.samples,
            &target_series.samples,
            compare,
            &mut issues,
        );
        histogram_mismatches += compare_histograms(
            key,
            &source_series.histograms,
            &target_series.histograms,
            &mut issues,
        );
    }

    RawCheckReport {
        selector: selector.to_string(),
        source_series: source.len(),
        target_series: target.len(),
        source_rows: source_samples + source_histograms,
        target_rows: target_samples + target_histograms,
        source_samples,
        target_samples,
        source_histograms,
        target_histograms,
        missing_series: source_keys.difference(&target_keys).count(),
        extra_series: target_keys.difference(&source_keys).count(),
        sample_mismatches,
        histogram_mismatches,
        issues,
    }
}

fn compare_exemplar_series(
    query: &str,
    source: &BTreeMap<SeriesKey, Vec<CanonicalExemplar>>,
    target: &BTreeMap<SeriesKey, Vec<CanonicalExemplar>>,
    compare: &CompareConfig,
) -> ExemplarCheckReport {
    let mut issues = Vec::new();
    let source_keys = source.keys().cloned().collect::<BTreeSet<_>>();
    let target_keys = target.keys().cloned().collect::<BTreeSet<_>>();
    for key in source_keys
        .difference(&target_keys)
        .take(MAX_ISSUES_PER_CHECK)
    {
        issues.push(format!(
            "missing exemplar series {}",
            format_series_key(key)
        ));
    }
    for key in target_keys
        .difference(&source_keys)
        .take(MAX_ISSUES_PER_CHECK)
    {
        issues.push(format!(
            "unexpected extra exemplar series {}",
            format_series_key(key)
        ));
    }

    let mut exemplar_mismatches = 0usize;
    for key in source_keys.intersection(&target_keys) {
        let source_series = source.get(key).expect("source key exists");
        let target_series = target.get(key).expect("target key exists");
        exemplar_mismatches +=
            compare_exemplars(key, source_series, target_series, compare, &mut issues);
    }

    ExemplarCheckReport {
        query: query.to_string(),
        source_series: source.len(),
        target_series: target.len(),
        missing_series: source_keys.difference(&target_keys).count(),
        extra_series: target_keys.difference(&source_keys).count(),
        exemplar_mismatches,
        issues,
    }
}

fn compare_samples(
    key: &SeriesKey,
    source: &[CanonicalSample],
    target: &[CanonicalSample],
    compare: &CompareConfig,
    issues: &mut Vec<String>,
) -> usize {
    let mut mismatches = 0usize;
    if source.len() != target.len() {
        mismatches += source.len().abs_diff(target.len()).max(1);
        push_limited_issue(
            issues,
            format!(
                "sample count mismatch for {}: source {} target {}",
                format_series_key(key),
                source.len(),
                target.len()
            ),
        );
    }

    for (index, (left, right)) in source.iter().zip(target.iter()).enumerate() {
        if left.timestamp != right.timestamp || !floats_equal(left.value, right.value, compare) {
            mismatches += 1;
            push_limited_issue(
                issues,
                format!(
                    "sample mismatch for {} at index {}: source ({}, {}) target ({}, {})",
                    format_series_key(key),
                    index,
                    left.timestamp,
                    left.value,
                    right.timestamp,
                    right.value
                ),
            );
        }
    }
    mismatches
}

fn compare_histograms(
    key: &SeriesKey,
    source: &[CanonicalHistogram],
    target: &[CanonicalHistogram],
    issues: &mut Vec<String>,
) -> usize {
    let mut mismatches = 0usize;
    if source.len() != target.len() {
        mismatches += source.len().abs_diff(target.len()).max(1);
        push_limited_issue(
            issues,
            format!(
                "histogram count mismatch for {}: source {} target {}",
                format_series_key(key),
                source.len(),
                target.len()
            ),
        );
    }
    for (index, (left, right)) in source.iter().zip(target.iter()).enumerate() {
        if left.timestamp != right.timestamp || left.payload != right.payload {
            mismatches += 1;
            push_limited_issue(
                issues,
                format!(
                    "histogram mismatch for {} at index {} timestamp {}",
                    format_series_key(key),
                    index,
                    left.timestamp
                ),
            );
        }
    }
    mismatches
}

fn compare_exemplars(
    key: &SeriesKey,
    source: &[CanonicalExemplar],
    target: &[CanonicalExemplar],
    compare: &CompareConfig,
    issues: &mut Vec<String>,
) -> usize {
    let mut mismatches = 0usize;
    if source.len() != target.len() {
        mismatches += source.len().abs_diff(target.len()).max(1);
        push_limited_issue(
            issues,
            format!(
                "exemplar count mismatch for {}: source {} target {}",
                format_series_key(key),
                source.len(),
                target.len()
            ),
        );
    }
    for (index, (left, right)) in source.iter().zip(target.iter()).enumerate() {
        if left.timestamp != right.timestamp
            || !floats_equal(left.value, right.value, compare)
            || left.labels != right.labels
        {
            mismatches += 1;
            push_limited_issue(
                issues,
                format!(
                    "exemplar mismatch for {} at index {} timestamp {}",
                    format_series_key(key),
                    index,
                    left.timestamp
                ),
            );
        }
    }
    mismatches
}

fn push_limited_issue(issues: &mut Vec<String>, issue: String) {
    if issues.len() < MAX_ISSUES_PER_CHECK {
        issues.push(issue);
    }
}

fn floats_equal(left: f64, right: f64, compare: &CompareConfig) -> bool {
    if left.is_nan() && right.is_nan() {
        return true;
    }
    if left.is_infinite() || right.is_infinite() {
        return left == right;
    }
    let diff = (left - right).abs();
    if diff <= compare.max_absolute_value_delta {
        return true;
    }
    let scale = left.abs().max(right.abs()).max(1.0);
    diff / scale <= compare.max_relative_value_delta
}

fn format_series_key(key: &SeriesKey) -> String {
    let mut parts = Vec::new();
    for (name, value) in key {
        parts.push(format!(r#"{name}="{value}""#));
    }
    format!("{{{}}}", parts.join(","))
}

fn verify_promql_checks(
    client: &HttpClient,
    plan: &MigrationPlan,
    start_ms: i64,
    end_ms: i64,
) -> Result<Vec<PromqlCheckReport>, String> {
    if plan.promql_checks.is_empty() {
        return Ok(Vec::new());
    }
    let Some(source_url) = &plan.source.query_range_url else {
        return Err(
            "source.query_range_url is required when promql_checks are configured".to_string(),
        );
    };
    let Some(target_url) = &plan.target.query_range_url else {
        return Err(
            "target.query_range_url is required when promql_checks are configured".to_string(),
        );
    };

    let mut out = Vec::new();
    for check in &plan.promql_checks {
        let source = fetch_query_range(
            client,
            source_url,
            &plan.source.headers,
            None,
            &check.query,
            &check.step,
            start_ms,
            end_ms,
        )?;
        let target = fetch_query_range(
            client,
            target_url,
            &plan.target.headers,
            plan.target.tenant.as_deref(),
            &check.query,
            &check.step,
            start_ms,
            end_ms,
        )?;
        let mut issues = Vec::new();
        if source.result_type != target.result_type {
            issues.push(format!(
                "resultType mismatch for query {}: source {} target {}",
                check.query, source.result_type, target.result_type
            ));
        }
        if !promql_results_equal(&source.data, &target.data, &plan.compare) {
            issues.push(format!("PromQL result mismatch for query {}", check.query));
        }

        out.push(PromqlCheckReport {
            query: check.query.clone(),
            step: check.step.clone(),
            matched: issues.is_empty() && !target.partial_response && target.warnings.is_empty(),
            source_warnings: source.warnings,
            target_warnings: target.warnings,
            target_partial_response: target.partial_response,
            issues,
        });
    }
    Ok(out)
}

#[allow(clippy::too_many_arguments)]
fn fetch_query_range(
    client: &HttpClient,
    url: &str,
    headers: &BTreeMap<String, String>,
    tenant: Option<&str>,
    query: &str,
    step: &str,
    start_ms: i64,
    end_ms: i64,
) -> Result<QueryRangeResponse, String> {
    let body = client.get_json(
        url,
        headers,
        tenant,
        &[
            (String::from("query"), query.to_string()),
            (String::from("start"), prom_time(start_ms)),
            (String::from("end"), prom_time(end_ms)),
            (String::from("step"), step.to_string()),
        ],
    )?;
    let data = require_prometheus_data(&body)?;
    let result_type = data
        .get("resultType")
        .and_then(JsonValue::as_str)
        .ok_or_else(|| "query_range response is missing resultType".to_string())?
        .to_string();
    let result = data
        .get("result")
        .cloned()
        .ok_or_else(|| "query_range response is missing result".to_string())?;
    let partial_response = body
        .get("partialResponse")
        .and_then(|value| value.get("enabled"))
        .and_then(JsonValue::as_bool)
        .unwrap_or(false);
    let warnings = body
        .get("warnings")
        .and_then(JsonValue::as_array)
        .map(|warnings| {
            warnings
                .iter()
                .filter_map(JsonValue::as_str)
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    Ok(QueryRangeResponse {
        result_type,
        data: result,
        partial_response,
        warnings,
    })
}

#[derive(Debug)]
struct QueryRangeResponse {
    result_type: String,
    data: JsonValue,
    partial_response: bool,
    warnings: Vec<String>,
}

fn promql_results_equal(left: &JsonValue, right: &JsonValue, compare: &CompareConfig) -> bool {
    let Ok(left) = canonicalize_promql_result(left) else {
        return false;
    };
    let Ok(right) = canonicalize_promql_result(right) else {
        return false;
    };
    canonical_promql_values_equal(&left, &right, compare)
}

#[derive(Debug, Clone)]
enum CanonicalPromqlValue {
    Matrix(Vec<(SeriesKey, Vec<(i64, PromqlSampleValue)>)>),
    Vector(Vec<(SeriesKey, (i64, PromqlSampleValue))>),
    Scalar((i64, PromqlSampleValue)),
    String((i64, String)),
}

#[derive(Debug, Clone)]
enum PromqlSampleValue {
    Number(f64),
    Text(String),
}

fn canonicalize_promql_result(value: &JsonValue) -> Result<CanonicalPromqlValue, String> {
    let Some(items) = value.as_array() else {
        if let Some(sample) = canonicalize_promql_pair(value)? {
            return Ok(CanonicalPromqlValue::Scalar(sample));
        }
        return Err("unsupported PromQL result payload".to_string());
    };

    if items
        .first()
        .is_some_and(|item| item.get("values").is_some())
    {
        let mut out = Vec::new();
        for item in items {
            let labels = json_labels_to_key(
                item.get("metric")
                    .ok_or_else(|| "matrix result entry is missing metric".to_string())?,
            )?;
            let values = item
                .get("values")
                .and_then(JsonValue::as_array)
                .ok_or_else(|| "matrix result entry is missing values".to_string())?;
            let mut samples = Vec::new();
            for value in values {
                let sample = canonicalize_promql_pair(value)?
                    .ok_or_else(|| "invalid matrix sample".to_string())?;
                samples.push(sample);
            }
            out.push((labels, samples));
        }
        out.sort_by(|left, right| left.0.cmp(&right.0));
        return Ok(CanonicalPromqlValue::Matrix(out));
    }

    if items
        .first()
        .is_some_and(|item| item.get("value").is_some())
    {
        let mut out = Vec::new();
        for item in items {
            let labels = json_labels_to_key(
                item.get("metric")
                    .ok_or_else(|| "vector result entry is missing metric".to_string())?,
            )?;
            let sample = canonicalize_promql_pair(
                item.get("value")
                    .ok_or_else(|| "vector result entry is missing value".to_string())?,
            )?
            .ok_or_else(|| "invalid vector sample".to_string())?;
            out.push((labels, sample));
        }
        out.sort_by(|left, right| left.0.cmp(&right.0));
        return Ok(CanonicalPromqlValue::Vector(out));
    }

    if items.len() == 2
        && items
            .first()
            .is_some_and(|item| item.is_number() || item.is_string())
        && items.get(1).is_some_and(|item| item.is_string())
    {
        let scalar =
            canonicalize_promql_pair(value)?.ok_or_else(|| "invalid scalar result".to_string())?;
        if matches!(scalar.1, PromqlSampleValue::Number(_)) {
            return Ok(CanonicalPromqlValue::Scalar(scalar));
        }
        if let PromqlSampleValue::Text(text) = scalar.1 {
            return Ok(CanonicalPromqlValue::String((scalar.0, text)));
        }
    }

    Err("unsupported PromQL result shape".to_string())
}

fn canonicalize_promql_pair(value: &JsonValue) -> Result<Option<(i64, PromqlSampleValue)>, String> {
    let Some(pair) = value.as_array() else {
        return Ok(None);
    };
    if pair.len() != 2 {
        return Ok(None);
    }
    let timestamp = parse_prometheus_timestamp_ms(&pair[0])?;
    let raw = match &pair[1] {
        JsonValue::String(value) => match parse_json_f64(&pair[1]) {
            Ok(number) => PromqlSampleValue::Number(number),
            Err(_) => PromqlSampleValue::Text(value.clone()),
        },
        JsonValue::Number(number) => PromqlSampleValue::Number(
            number
                .as_f64()
                .ok_or_else(|| "PromQL numeric value is out of range".to_string())?,
        ),
        _ => return Err("PromQL sample value must be string or number".to_string()),
    };
    Ok(Some((timestamp, raw)))
}

fn canonical_promql_values_equal(
    left: &CanonicalPromqlValue,
    right: &CanonicalPromqlValue,
    compare: &CompareConfig,
) -> bool {
    match (left, right) {
        (CanonicalPromqlValue::Matrix(left), CanonicalPromqlValue::Matrix(right)) => {
            left.len() == right.len()
                && left.iter().zip(right.iter()).all(
                    |((left_labels, left_values), (right_labels, right_values))| {
                        left_labels == right_labels
                            && promql_sample_vectors_equal(left_values, right_values, compare)
                    },
                )
        }
        (CanonicalPromqlValue::Vector(left), CanonicalPromqlValue::Vector(right)) => {
            left.len() == right.len()
                && left.iter().zip(right.iter()).all(
                    |((left_labels, left_value), (right_labels, right_value))| {
                        left_labels == right_labels
                            && promql_samples_equal(left_value, right_value, compare)
                    },
                )
        }
        (CanonicalPromqlValue::Scalar(left), CanonicalPromqlValue::Scalar(right)) => {
            promql_samples_equal(left, right, compare)
        }
        (CanonicalPromqlValue::String(left), CanonicalPromqlValue::String(right)) => left == right,
        _ => false,
    }
}

fn promql_sample_vectors_equal(
    left: &[(i64, PromqlSampleValue)],
    right: &[(i64, PromqlSampleValue)],
    compare: &CompareConfig,
) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right.iter())
            .all(|(left, right)| promql_samples_equal(left, right, compare))
}

fn promql_samples_equal(
    left: &(i64, PromqlSampleValue),
    right: &(i64, PromqlSampleValue),
    compare: &CompareConfig,
) -> bool {
    if left.0 != right.0 {
        return false;
    }
    match (&left.1, &right.1) {
        (PromqlSampleValue::Number(left), PromqlSampleValue::Number(right)) => {
            floats_equal(*left, *right, compare)
        }
        (PromqlSampleValue::Text(left), PromqlSampleValue::Text(right)) => left == right,
        _ => false,
    }
}

fn fetch_target_payload_status(
    client: &HttpClient,
    target: &TargetConfig,
    url: &str,
) -> Result<TargetPayloadStatus, String> {
    let body = client.get_json(url, &target.headers, target.tenant.as_deref(), &[])?;
    let data = require_prometheus_data(&body)?;
    Ok(TargetPayloadStatus {
        metadata_enabled: data
            .get("prometheusPayloads")
            .and_then(|payloads| payloads.get("metadata"))
            .and_then(|metadata| metadata.get("enabled"))
            .and_then(JsonValue::as_bool)
            .unwrap_or(false),
        exemplars_enabled: data
            .get("prometheusPayloads")
            .and_then(|payloads| payloads.get("exemplars"))
            .and_then(|metadata| metadata.get("enabled"))
            .and_then(JsonValue::as_bool)
            .unwrap_or(false),
        histograms_enabled: data
            .get("prometheusPayloads")
            .and_then(|payloads| payloads.get("histograms"))
            .and_then(|metadata| metadata.get("enabled"))
            .and_then(JsonValue::as_bool)
            .unwrap_or(false),
        otlp_enabled: data
            .get("otlpMetrics")
            .and_then(|otlp| otlp.get("enabled"))
            .and_then(JsonValue::as_bool)
            .unwrap_or(false),
        otlp_supported_shapes: data
            .get("otlpMetrics")
            .and_then(|otlp| otlp.get("supportedShapes"))
            .and_then(JsonValue::as_array)
            .map(|items| {
                items
                    .iter()
                    .filter_map(JsonValue::as_str)
                    .map(str::to_string)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default(),
        influx_line_protocol_enabled: data
            .get("legacyIngest")
            .and_then(|legacy| legacy.get("influxLineProtocol"))
            .and_then(|influx| influx.get("enabled"))
            .and_then(JsonValue::as_bool)
            .unwrap_or(false),
        statsd_enabled: data
            .get("legacyIngest")
            .and_then(|legacy| legacy.get("statsd"))
            .and_then(|statsd| statsd.get("enabled"))
            .and_then(JsonValue::as_bool)
            .unwrap_or(false),
        graphite_enabled: data
            .get("legacyIngest")
            .and_then(|legacy| legacy.get("graphite"))
            .and_then(|graphite| graphite.get("enabled"))
            .and_then(JsonValue::as_bool)
            .unwrap_or(false),
    })
}

fn metadata_type_name(metric_type: i32) -> Result<&'static str, String> {
    match MetricType::try_from(metric_type)
        .map_err(|_| format!("unsupported metadata type value: {metric_type}"))?
    {
        MetricType::Unknown => Ok("unknown"),
        MetricType::Counter => Ok("counter"),
        MetricType::Gauge => Ok("gauge"),
        MetricType::Histogram => Ok("histogram"),
        MetricType::Gaugehistogram => Ok("gaugehistogram"),
        MetricType::Summary => Ok("summary"),
        MetricType::Info => Ok("info"),
        MetricType::Stateset => Ok("stateset"),
    }
}

fn require_prometheus_data(body: &JsonValue) -> Result<&JsonValue, String> {
    match body.get("status").and_then(JsonValue::as_str) {
        Some("success") => body
            .get("data")
            .ok_or_else(|| "Prometheus API response is missing data".to_string()),
        _ => Err(format!(
            "Prometheus API returned failure: {}",
            body.get("error")
                .and_then(JsonValue::as_str)
                .unwrap_or("unknown error")
        )),
    }
}

fn parse_json_f64(value: &JsonValue) -> Result<f64, String> {
    match value {
        JsonValue::Number(number) => number
            .as_f64()
            .ok_or_else(|| "numeric JSON value is out of range".to_string()),
        JsonValue::String(text) => match text.as_str() {
            "NaN" => Ok(f64::NAN),
            "+Inf" | "Inf" => Ok(f64::INFINITY),
            "-Inf" => Ok(f64::NEG_INFINITY),
            _ => text
                .parse::<f64>()
                .map_err(|_| format!("invalid floating-point value: {text}")),
        },
        _ => Err("expected numeric or string JSON value".to_string()),
    }
}

fn parse_prometheus_timestamp_ms(value: &JsonValue) -> Result<i64, String> {
    let raw = match value {
        JsonValue::Number(number) => number.to_string(),
        JsonValue::String(text) => text.clone(),
        _ => return Err("timestamp must be a JSON string or number".to_string()),
    };
    if let Ok(timestamp) = raw.parse::<i64>() {
        return Ok(timestamp);
    }
    let seconds = raw
        .parse::<f64>()
        .map_err(|_| format!("invalid timestamp: {raw}"))?;
    Ok((seconds * 1000.0) as i64)
}

fn prom_time(timestamp_ms: i64) -> String {
    format!("{:.3}", timestamp_ms as f64 / 1000.0)
}

fn write_report_artifacts(dir: &Path, report: &CommandReport) -> Result<(), String> {
    fs::create_dir_all(dir).map_err(|err| {
        format!(
            "failed creating artifact directory {}: {err}",
            dir.display()
        )
    })?;
    let summary_json = serde_json::to_vec_pretty(report)
        .map_err(|err| format!("failed encoding report JSON: {err}"))?;
    fs::write(dir.join("summary.json"), summary_json)
        .map_err(|err| format!("failed writing summary.json: {err}"))?;
    fs::write(dir.join("summary.md"), report.markdown_summary())
        .map_err(|err| format!("failed writing summary.md: {err}"))?;
    Ok(())
}

impl SourceKind {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Prometheus => "prometheus",
            Self::Victoriametrics => "victoriametrics",
            Self::Otlp => "otlp",
            Self::InfluxLineProtocol => "influx_line_protocol",
            Self::Statsd => "statsd",
            Self::GraphitePlaintext => "graphite_plaintext",
        }
    }

    fn uses_capture_manifest(&self) -> bool {
        matches!(
            self,
            Self::Otlp | Self::InfluxLineProtocol | Self::Statsd | Self::GraphitePlaintext
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::otlp::generated::opentelemetry::proto::collector::metrics::v1::ExportMetricsServiceRequest;
    use crate::otlp::generated::opentelemetry::proto::metrics::v1::{
        metric as otlp_metric, number_data_point,
        AggregationTemporality as OtlpAggregationTemporality, Gauge as OtlpGauge,
        Metric as OtlpMetric, NumberDataPoint as OtlpNumberDataPoint, ResourceMetrics,
        ScopeMetrics, Sum as OtlpSum,
    };
    use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
    use tempfile::tempdir;

    fn fixture_dir() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/migration")
    }

    fn sorted_metric_keys(series: &BTreeMap<SeriesKey, WritableSeries>) -> Vec<String> {
        let mut keys = series
            .keys()
            .filter_map(|key| {
                key.iter()
                    .find(|(name, _)| name == "__name__")
                    .map(|(_, value)| value.clone())
            })
            .collect::<Vec<_>>();
        keys.sort();
        keys
    }

    #[test]
    fn parses_prometheus_plan_fixture() {
        let plan =
            load_plan(&fixture_dir().join("prometheus-plan.json")).expect("plan should parse");
        assert_eq!(plan.source.kind, SourceKind::Prometheus);
        assert_eq!(plan.selectors.len(), 2);
        assert_eq!(plan.promql_checks.len(), 2);
    }

    #[test]
    fn parses_victoriametrics_export_fixture() {
        let body = fs::read(fixture_dir().join("victoriametrics-export.ndjson"))
            .expect("fixture should read");
        let reader = BufReader::new(Cursor::new(body));
        let mut rows = 0usize;
        for line in reader.lines() {
            let line = line.expect("line should read");
            if line.trim().is_empty() {
                continue;
            }
            let row: VmExportLine = serde_json::from_str(&line).expect("row should parse");
            assert_eq!(row.values.len(), row.timestamps.len());
            rows += 1;
        }
        assert_eq!(rows, 2);
    }

    #[test]
    fn parses_capture_plan_fixtures() {
        let influx = load_plan(&fixture_dir().join("influx-line-protocol-plan.json"))
            .expect("influx capture plan should parse");
        let statsd =
            load_plan(&fixture_dir().join("statsd-plan.json")).expect("statsd plan should parse");
        let graphite = load_plan(&fixture_dir().join("graphite-plan.json"))
            .expect("graphite plan should parse");

        assert_eq!(influx.source.kind, SourceKind::InfluxLineProtocol);
        assert_eq!(statsd.source.kind, SourceKind::Statsd);
        assert_eq!(graphite.source.kind, SourceKind::GraphitePlaintext);
        assert!(influx.source.kind.uses_capture_manifest());
        assert!(statsd.source.kind.uses_capture_manifest());
        assert!(graphite.source.kind.uses_capture_manifest());
    }

    #[test]
    fn prepares_influx_capture_source_fixture() {
        let plan = load_plan(&fixture_dir().join("influx-line-protocol-plan.json"))
            .expect("influx capture plan should parse");
        let prepared = prepare_capture_source_data(&plan).expect("capture source should prepare");

        assert_eq!(
            sorted_metric_keys(&prepared.series),
            vec!["cpu", "cpu_temp", "mem_used"]
        );
        assert_eq!(prepared.metadata.len(), 3);

        let filtered = filter_source_series_by_selector(
            &prepared,
            r#"cpu{host="node-a",influx_db="telegraf"}"#,
            1_700_000_000_000,
            1_700_000_000_000,
        )
        .expect("selector should filter source series");
        assert_eq!(sorted_metric_keys(&filtered), vec!["cpu"]);
    }

    #[test]
    fn prepares_statsd_capture_source_fixture() {
        let plan = load_plan(&fixture_dir().join("statsd-plan.json"))
            .expect("statsd capture plan should parse");
        let prepared = prepare_capture_source_data(&plan).expect("capture source should prepare");

        assert_eq!(
            sorted_metric_keys(&prepared.series),
            vec!["jobs_completed", "workers_active"]
        );
        assert_eq!(prepared.metadata.len(), 2);
    }

    #[test]
    fn prepares_graphite_capture_source_fixture() {
        let plan = load_plan(&fixture_dir().join("graphite-plan.json"))
            .expect("graphite capture plan should parse");
        let prepared = prepare_capture_source_data(&plan).expect("capture source should prepare");

        assert_eq!(
            sorted_metric_keys(&prepared.series),
            vec!["servers_api_errors", "servers_api_latency"]
        );
        assert_eq!(prepared.metadata.len(), 2);
    }

    #[test]
    fn prepares_otlp_capture_manifest_from_inline_base64() {
        let tempdir = tempdir().expect("temporary directory should be available");
        let request = ExportMetricsServiceRequest {
            resource_metrics: vec![ResourceMetrics {
                resource: None,
                scope_metrics: vec![ScopeMetrics {
                    scope: None,
                    metrics: vec![
                        OtlpMetric {
                            name: "system.cpu.time".to_string(),
                            description: "CPU time".to_string(),
                            unit: "s".to_string(),
                            data: Some(otlp_metric::Data::Gauge(OtlpGauge {
                                data_points: vec![OtlpNumberDataPoint {
                                    attributes: Vec::new(),
                                    start_time_unix_nano: 0,
                                    time_unix_nano: 1_700_000_000_123_000_000,
                                    value: Some(number_data_point::Value::AsDouble(12.5)),
                                    exemplars: Vec::new(),
                                    flags: 0,
                                }],
                            })),
                        },
                        OtlpMetric {
                            name: "http.server.active_requests".to_string(),
                            description: "Active".to_string(),
                            unit: "{request}".to_string(),
                            data: Some(otlp_metric::Data::Sum(OtlpSum {
                                data_points: vec![OtlpNumberDataPoint {
                                    attributes: Vec::new(),
                                    start_time_unix_nano: 0,
                                    time_unix_nano: 1_700_000_000_124_000_000,
                                    value: Some(number_data_point::Value::AsInt(5)),
                                    exemplars: Vec::new(),
                                    flags: 0,
                                }],
                                aggregation_temporality: OtlpAggregationTemporality::Cumulative
                                    as i32,
                                is_monotonic: false,
                            })),
                        },
                    ],
                    schema_url: String::new(),
                }],
                schema_url: String::new(),
            }],
        };

        fs::write(
            tempdir.path().join("otlp-capture.json"),
            serde_json::to_vec_pretty(&json!([
                {
                    "body_base64": BASE64_STANDARD.encode(request.encode_to_vec())
                }
            ]))
            .expect("manifest JSON should encode"),
        )
        .expect("capture manifest should write");
        fs::write(
            tempdir.path().join("otlp-plan.json"),
            serde_json::to_vec_pretty(&json!({
                "source": {
                    "kind": "otlp",
                    "capture_manifest_path": "otlp-capture.json"
                },
                "target": {
                    "write_url": "http://127.0.0.1:9201/api/v1/write",
                    "read_url": "http://127.0.0.1:9201/api/v1/read",
                    "metadata_url": "http://127.0.0.1:9201/api/v1/metadata",
                    "status_url": "http://127.0.0.1:9201/api/v1/status/tsdb",
                    "tenant": "default"
                },
                "selectors": [
                    "system_x2e_cpu_x2e_time",
                    "http_x2e_server_x2e_active__requests"
                ],
                "metadata_metrics": [
                    "system_x2e_cpu_x2e_time",
                    "http_x2e_server_x2e_active__requests"
                ]
            }))
            .expect("plan JSON should encode"),
        )
        .expect("plan should write");

        let plan = load_plan(&tempdir.path().join("otlp-plan.json"))
            .expect("otlp capture plan should parse");
        let prepared = prepare_capture_source_data(&plan).expect("capture source should prepare");

        assert_eq!(
            sorted_metric_keys(&prepared.series),
            vec![
                "http_x2e_server_x2e_active__requests",
                "system_x2e_cpu_x2e_time"
            ]
        );
        assert_eq!(prepared.metadata.len(), 2);
        assert_eq!(
            prepared.otlp_supported_shapes,
            vec!["gauge".to_string(), "sum:cumulative".to_string()]
        );
    }

    #[test]
    fn selector_to_query_carries_metric_and_matchers() {
        let query = selector_to_read_query(
            r#"http_requests_total{job=~"api|worker",instance!="node-b"}"#,
            1,
            2,
        )
        .expect("selector should parse");
        assert_eq!(query.matchers.len(), 3);
        assert_eq!(query.matchers[0].name, "__name__");
        assert_eq!(query.matchers[0].value, "http_requests_total");
        assert_eq!(query.start_timestamp_ms, 1);
        assert_eq!(query.end_timestamp_ms, 2);
    }
}
