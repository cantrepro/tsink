use crate::prom_remote::MetricType;
use crate::prom_write::{
    build_metadata_update, build_series_identity, NormalizedScalarSample, NormalizedWriteEnvelope,
};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use tsink::{DataPoint, TimestampPrecision};

pub const INFLUX_LINE_PROTOCOL_ENABLED_ENV: &str = "TSINK_INFLUX_LINE_PROTOCOL_ENABLED";
pub const INFLUX_LINE_PROTOCOL_MAX_LINES_PER_REQUEST_ENV: &str =
    "TSINK_INFLUX_LINE_PROTOCOL_MAX_LINES_PER_REQUEST";
pub const STATSD_MAX_PACKET_BYTES_ENV: &str = "TSINK_STATSD_MAX_PACKET_BYTES";
pub const STATSD_MAX_EVENTS_PER_PACKET_ENV: &str = "TSINK_STATSD_MAX_EVENTS_PER_PACKET";
pub const GRAPHITE_MAX_LINE_BYTES_ENV: &str = "TSINK_GRAPHITE_MAX_LINE_BYTES";

const DEFAULT_INFLUX_LINE_PROTOCOL_MAX_LINES_PER_REQUEST: usize = 4_096;
const DEFAULT_STATSD_MAX_PACKET_BYTES: usize = 8 * 1_024;
const DEFAULT_STATSD_MAX_EVENTS_PER_PACKET: usize = 1_024;
const DEFAULT_GRAPHITE_MAX_LINE_BYTES: usize = 8 * 1_024;

static INFLUX_ACCEPTED_REQUESTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static INFLUX_REJECTED_REQUESTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static INFLUX_THROTTLED_REQUESTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static INFLUX_ACCEPTED_SAMPLES_TOTAL: AtomicU64 = AtomicU64::new(0);
static INFLUX_REJECTED_SAMPLES_TOTAL: AtomicU64 = AtomicU64::new(0);

static STATSD_LISTENER_ENABLED: AtomicU64 = AtomicU64::new(0);
static STATSD_ACCEPTED_REQUESTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static STATSD_REJECTED_REQUESTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static STATSD_THROTTLED_REQUESTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static STATSD_ACCEPTED_SAMPLES_TOTAL: AtomicU64 = AtomicU64::new(0);
static STATSD_REJECTED_SAMPLES_TOTAL: AtomicU64 = AtomicU64::new(0);

static GRAPHITE_LISTENER_ENABLED: AtomicU64 = AtomicU64::new(0);
static GRAPHITE_ACCEPTED_REQUESTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static GRAPHITE_REJECTED_REQUESTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static GRAPHITE_THROTTLED_REQUESTS_TOTAL: AtomicU64 = AtomicU64::new(0);
static GRAPHITE_ACCEPTED_SAMPLES_TOTAL: AtomicU64 = AtomicU64::new(0);
static GRAPHITE_REJECTED_SAMPLES_TOTAL: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LegacyAdapterKind {
    InfluxLineProtocol,
    Statsd,
    Graphite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InfluxLineProtocolConfig {
    pub enabled: bool,
    pub max_lines_per_request: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatsdConfig {
    pub max_packet_bytes: usize,
    pub max_events_per_packet: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphiteConfig {
    pub max_line_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdapterCounterSnapshot {
    pub accepted_requests_total: u64,
    pub rejected_requests_total: u64,
    pub throttled_requests_total: u64,
    pub accepted_samples_total: u64,
    pub rejected_samples_total: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct InfluxLineProtocolStatusSnapshot {
    pub enabled: bool,
    pub max_lines_per_request: usize,
    pub counters: AdapterCounterSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StatsdStatusSnapshot {
    pub enabled: bool,
    pub max_packet_bytes: usize,
    pub max_events_per_packet: usize,
    pub counters: AdapterCounterSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GraphiteStatusSnapshot {
    pub enabled: bool,
    pub max_line_bytes: usize,
    pub counters: AdapterCounterSnapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LegacyIngestStatusSnapshot {
    pub influx: InfluxLineProtocolStatusSnapshot,
    pub statsd: StatsdStatusSnapshot,
    pub graphite: GraphiteStatusSnapshot,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NormalizedLegacyWrite {
    pub envelope: NormalizedWriteEnvelope,
    pub request_units: usize,
    pub sample_count: usize,
}

#[derive(Debug)]
pub struct StatsdAdapter {
    gauge_state: Mutex<BTreeMap<String, f64>>,
}

impl StatsdAdapter {
    pub fn new() -> Self {
        Self {
            gauge_state: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn normalize_packet(
        &self,
        packet: &str,
        tenant_id: &str,
        received_at: i64,
        config: StatsdConfig,
    ) -> Result<NormalizedLegacyWrite, String> {
        if packet.len() > config.max_packet_bytes {
            return Err(format!(
                "statsd packet exceeds byte limit: {} > {}",
                packet.len(),
                config.max_packet_bytes
            ));
        }

        let mut envelope = NormalizedWriteEnvelope::default();
        let mut metadata_updates = BTreeMap::new();
        let mut request_units = 0usize;

        for (line_idx, raw_line) in packet.lines().enumerate() {
            let line = raw_line.trim();
            if line.is_empty() {
                continue;
            }
            request_units = request_units.saturating_add(1);
            if request_units > config.max_events_per_packet {
                return Err(format!(
                    "statsd packet exceeds event limit: {} > {}",
                    request_units, config.max_events_per_packet
                ));
            }

            let event = parse_statsd_event(line, line_idx)?;
            let context = format!("statsd event index {line_idx}");
            let mut labels = normalize_legacy_tags(event.tags, &context)?;
            let metric = normalize_metric_name(&event.metric, &context)?;

            let (value, metadata_type, metadata_help) = match event.kind.as_str() {
                "c" => {
                    let sample_rate = event.sample_rate.unwrap_or(1.0);
                    (
                        parse_finite_f64(&event.value, &format!("{context} counter value"))?
                            / sample_rate,
                        MetricType::Counter,
                        "StatsD counter sample",
                    )
                }
                "g" => {
                    if event.sample_rate.is_some() {
                        return Err(format!("{context} gauge sample rates are not supported"));
                    }
                    let value = normalize_statsd_gauge_value(
                        metric.as_str(),
                        &labels,
                        &event.value,
                        &self.gauge_state,
                        &context,
                    )?;
                    (value, MetricType::Gauge, "StatsD gauge sample")
                }
                "ms" => {
                    if let Some(sample_rate) = event.sample_rate {
                        labels.push((
                            "statsd_sample_rate".to_string(),
                            canonical_float(sample_rate),
                        ));
                    }
                    (
                        parse_finite_f64(&event.value, &format!("{context} timer value"))?,
                        MetricType::Summary,
                        "StatsD timer sample in milliseconds",
                    )
                }
                "h" => {
                    if let Some(sample_rate) = event.sample_rate {
                        labels.push((
                            "statsd_sample_rate".to_string(),
                            canonical_float(sample_rate),
                        ));
                    }
                    (
                        parse_finite_f64(&event.value, &format!("{context} histogram value"))?,
                        MetricType::Histogram,
                        "StatsD histogram sample",
                    )
                }
                "d" => {
                    if let Some(sample_rate) = event.sample_rate {
                        labels.push((
                            "statsd_sample_rate".to_string(),
                            canonical_float(sample_rate),
                        ));
                    }
                    (
                        parse_finite_f64(&event.value, &format!("{context} distribution value"))?,
                        MetricType::Histogram,
                        "StatsD distribution sample",
                    )
                }
                "s" => {
                    if event.sample_rate.is_some() {
                        return Err(format!("{context} set sample rates are not supported"));
                    }
                    labels.push(("statsd_set_member".to_string(), event.value));
                    (1.0, MetricType::Stateset, "StatsD set membership event")
                }
                other => return Err(format!("{context} uses unsupported StatsD type '{other}'")),
            };

            let series = build_series_identity(metric.clone(), labels, tenant_id, &context)?;
            envelope.scalar_samples.push(NormalizedScalarSample {
                series,
                data_point: DataPoint::new(received_at, value),
            });
            insert_metadata_update(
                &mut metadata_updates,
                &metric,
                metadata_type,
                metadata_help,
                &context,
            )?;
        }

        envelope.metadata_updates = metadata_updates.into_values().collect();
        let sample_count = envelope.scalar_samples.len();
        Ok(NormalizedLegacyWrite {
            envelope,
            request_units,
            sample_count,
        })
    }
}

impl Default for StatsdAdapter {
    fn default() -> Self {
        Self::new()
    }
}

pub fn normalize_influx_line_protocol(
    body: &str,
    tenant_id: &str,
    server_precision: TimestampPrecision,
    now: i64,
    config: InfluxLineProtocolConfig,
    query_labels: Vec<(String, String)>,
    input_precision: Option<&str>,
) -> Result<NormalizedLegacyWrite, String> {
    let precision = parse_influx_precision(input_precision)?;
    let mut envelope = NormalizedWriteEnvelope::default();
    let mut metadata_updates = BTreeMap::new();
    let mut request_units = 0usize;

    for (line_idx, raw_line) in body.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        request_units = request_units.saturating_add(1);
        if request_units > config.max_lines_per_request {
            return Err(format!(
                "influx line protocol request exceeds line limit: {} > {}",
                request_units, config.max_lines_per_request
            ));
        }
        let context = format!("influx line protocol line index {line_idx}");
        let (series_spec, field_spec, timestamp_spec) = split_influx_sections(line, &context)?;
        let (measurement, tag_pairs) = parse_influx_series_spec(series_spec, &context)?;
        let measurement = normalize_metric_name(&measurement, &context)?;
        let timestamp = match timestamp_spec {
            Some(value) => convert_influx_timestamp(
                value,
                precision,
                server_precision,
                &format!("{context} timestamp"),
            )?,
            None => now,
        };
        let fields = parse_influx_field_assignments(field_spec, &context)?;
        for (field_name_raw, field_value) in fields {
            let field_name = normalize_metric_name(&field_name_raw, &context)?;
            let metric = if field_name_raw == "value" {
                measurement.clone()
            } else {
                format!("{measurement}_{field_name}")
            };
            let mut labels = query_labels.clone();
            labels.extend(tag_pairs.iter().cloned());
            let series = build_series_identity(metric.clone(), labels, tenant_id, &context)?;
            envelope.scalar_samples.push(NormalizedScalarSample {
                series,
                data_point: DataPoint::new(timestamp, field_value),
            });
            insert_metadata_update(
                &mut metadata_updates,
                &metric,
                MetricType::Gauge,
                "Influx line protocol field coerced to gauge samples",
                &context,
            )?;
        }
    }

    envelope.metadata_updates = metadata_updates.into_values().collect();
    let sample_count = envelope.scalar_samples.len();
    Ok(NormalizedLegacyWrite {
        envelope,
        request_units,
        sample_count,
    })
}

pub fn normalize_graphite_plaintext_line(
    line: &str,
    tenant_id: &str,
    server_precision: TimestampPrecision,
    now: i64,
) -> Result<NormalizedLegacyWrite, String> {
    let line = line.trim();
    if line.is_empty() {
        return Ok(NormalizedLegacyWrite {
            envelope: NormalizedWriteEnvelope::default(),
            request_units: 0,
            sample_count: 0,
        });
    }

    let parts = line.split_whitespace().collect::<Vec<_>>();
    if !(parts.len() == 2 || parts.len() == 3) {
        return Err(format!(
            "graphite plaintext line must contain metric, value, and optional timestamp: '{line}'"
        ));
    }

    let context = "graphite plaintext line";
    let (metric_path, raw_tags) = parse_graphite_metric_spec(parts[0], context)?;
    let metric = normalize_metric_name(&metric_path, context)?;
    let value = parse_finite_f64(parts[1], "graphite plaintext value")?;
    let timestamp = if let Some(raw_timestamp) = parts.get(2) {
        parse_graphite_timestamp(
            raw_timestamp,
            server_precision,
            "graphite plaintext timestamp",
        )?
    } else {
        now
    };

    let mut envelope = NormalizedWriteEnvelope::default();
    let series = build_series_identity(metric.clone(), raw_tags, tenant_id, context)?;
    envelope.scalar_samples.push(NormalizedScalarSample {
        series,
        data_point: DataPoint::new(timestamp, value),
    });
    envelope.metadata_updates.push(build_metadata_update(
        metric,
        MetricType::Gauge,
        "Graphite plaintext sample coerced to gauge samples",
        "",
        context,
    )?);

    Ok(NormalizedLegacyWrite {
        envelope,
        request_units: 1,
        sample_count: 1,
    })
}

pub fn influx_line_protocol_config() -> InfluxLineProtocolConfig {
    InfluxLineProtocolConfig {
        enabled: parse_env_bool(INFLUX_LINE_PROTOCOL_ENABLED_ENV, true),
        max_lines_per_request: parse_env_usize(
            INFLUX_LINE_PROTOCOL_MAX_LINES_PER_REQUEST_ENV,
            DEFAULT_INFLUX_LINE_PROTOCOL_MAX_LINES_PER_REQUEST,
        )
        .max(1),
    }
}

pub fn statsd_config() -> StatsdConfig {
    StatsdConfig {
        max_packet_bytes: parse_env_usize(
            STATSD_MAX_PACKET_BYTES_ENV,
            DEFAULT_STATSD_MAX_PACKET_BYTES,
        )
        .max(64),
        max_events_per_packet: parse_env_usize(
            STATSD_MAX_EVENTS_PER_PACKET_ENV,
            DEFAULT_STATSD_MAX_EVENTS_PER_PACKET,
        )
        .max(1),
    }
}

pub fn graphite_config() -> GraphiteConfig {
    GraphiteConfig {
        max_line_bytes: parse_env_usize(
            GRAPHITE_MAX_LINE_BYTES_ENV,
            DEFAULT_GRAPHITE_MAX_LINE_BYTES,
        )
        .max(64),
    }
}

pub fn set_listener_enabled(kind: LegacyAdapterKind, enabled: bool) {
    let value = u64::from(enabled);
    match kind {
        LegacyAdapterKind::InfluxLineProtocol => {}
        LegacyAdapterKind::Statsd => {
            STATSD_LISTENER_ENABLED.store(value, Ordering::Relaxed);
        }
        LegacyAdapterKind::Graphite => {
            GRAPHITE_LISTENER_ENABLED.store(value, Ordering::Relaxed);
        }
    }
}

pub fn record_request_accepted(kind: LegacyAdapterKind, sample_count: usize) {
    let sample_count = sample_count as u64;
    let (requests, samples) = counter_pair(kind, Outcome::Accepted);
    requests.fetch_add(1, Ordering::Relaxed);
    samples.fetch_add(sample_count, Ordering::Relaxed);
}

pub fn record_request_rejected(kind: LegacyAdapterKind, rejected_samples: usize) {
    let rejected_samples = rejected_samples as u64;
    let (requests, samples) = counter_pair(kind, Outcome::Rejected);
    requests.fetch_add(1, Ordering::Relaxed);
    samples.fetch_add(rejected_samples, Ordering::Relaxed);
}

pub fn record_request_throttled(kind: LegacyAdapterKind, rejected_samples: usize) {
    let rejected_samples = rejected_samples as u64;
    let (requests, samples) = counter_pair(kind, Outcome::Throttled);
    requests.fetch_add(1, Ordering::Relaxed);
    samples.fetch_add(rejected_samples, Ordering::Relaxed);
}

pub fn status_snapshot() -> LegacyIngestStatusSnapshot {
    LegacyIngestStatusSnapshot {
        influx: InfluxLineProtocolStatusSnapshot {
            enabled: influx_line_protocol_config().enabled,
            max_lines_per_request: influx_line_protocol_config().max_lines_per_request,
            counters: snapshot_counters(LegacyAdapterKind::InfluxLineProtocol),
        },
        statsd: StatsdStatusSnapshot {
            enabled: STATSD_LISTENER_ENABLED.load(Ordering::Relaxed) > 0,
            max_packet_bytes: statsd_config().max_packet_bytes,
            max_events_per_packet: statsd_config().max_events_per_packet,
            counters: snapshot_counters(LegacyAdapterKind::Statsd),
        },
        graphite: GraphiteStatusSnapshot {
            enabled: GRAPHITE_LISTENER_ENABLED.load(Ordering::Relaxed) > 0,
            max_line_bytes: graphite_config().max_line_bytes,
            counters: snapshot_counters(LegacyAdapterKind::Graphite),
        },
    }
}

#[derive(Debug, Clone)]
struct StatsdEvent {
    metric: String,
    value: String,
    kind: String,
    sample_rate: Option<f64>,
    tags: Vec<(String, String)>,
}

#[derive(Debug, Clone, Copy)]
enum InfluxTimestampPrecision {
    Ns,
    Us,
    Ms,
    S,
    M,
    H,
}

#[derive(Debug, Clone, Copy)]
enum Outcome {
    Accepted,
    Rejected,
    Throttled,
}

fn counter_pair(
    kind: LegacyAdapterKind,
    outcome: Outcome,
) -> (&'static AtomicU64, &'static AtomicU64) {
    match (kind, outcome) {
        (LegacyAdapterKind::InfluxLineProtocol, Outcome::Accepted) => (
            &INFLUX_ACCEPTED_REQUESTS_TOTAL,
            &INFLUX_ACCEPTED_SAMPLES_TOTAL,
        ),
        (LegacyAdapterKind::InfluxLineProtocol, Outcome::Rejected) => (
            &INFLUX_REJECTED_REQUESTS_TOTAL,
            &INFLUX_REJECTED_SAMPLES_TOTAL,
        ),
        (LegacyAdapterKind::InfluxLineProtocol, Outcome::Throttled) => (
            &INFLUX_THROTTLED_REQUESTS_TOTAL,
            &INFLUX_REJECTED_SAMPLES_TOTAL,
        ),
        (LegacyAdapterKind::Statsd, Outcome::Accepted) => (
            &STATSD_ACCEPTED_REQUESTS_TOTAL,
            &STATSD_ACCEPTED_SAMPLES_TOTAL,
        ),
        (LegacyAdapterKind::Statsd, Outcome::Rejected) => (
            &STATSD_REJECTED_REQUESTS_TOTAL,
            &STATSD_REJECTED_SAMPLES_TOTAL,
        ),
        (LegacyAdapterKind::Statsd, Outcome::Throttled) => (
            &STATSD_THROTTLED_REQUESTS_TOTAL,
            &STATSD_REJECTED_SAMPLES_TOTAL,
        ),
        (LegacyAdapterKind::Graphite, Outcome::Accepted) => (
            &GRAPHITE_ACCEPTED_REQUESTS_TOTAL,
            &GRAPHITE_ACCEPTED_SAMPLES_TOTAL,
        ),
        (LegacyAdapterKind::Graphite, Outcome::Rejected) => (
            &GRAPHITE_REJECTED_REQUESTS_TOTAL,
            &GRAPHITE_REJECTED_SAMPLES_TOTAL,
        ),
        (LegacyAdapterKind::Graphite, Outcome::Throttled) => (
            &GRAPHITE_THROTTLED_REQUESTS_TOTAL,
            &GRAPHITE_REJECTED_SAMPLES_TOTAL,
        ),
    }
}

fn snapshot_counters(kind: LegacyAdapterKind) -> AdapterCounterSnapshot {
    let (accepted_requests, accepted_samples) = counter_pair(kind, Outcome::Accepted);
    let (rejected_requests, rejected_samples) = counter_pair(kind, Outcome::Rejected);
    let (throttled_requests, _) = counter_pair(kind, Outcome::Throttled);
    AdapterCounterSnapshot {
        accepted_requests_total: accepted_requests.load(Ordering::Relaxed),
        rejected_requests_total: rejected_requests.load(Ordering::Relaxed),
        throttled_requests_total: throttled_requests.load(Ordering::Relaxed),
        accepted_samples_total: accepted_samples.load(Ordering::Relaxed),
        rejected_samples_total: rejected_samples.load(Ordering::Relaxed),
    }
}

fn parse_statsd_event(line: &str, line_idx: usize) -> Result<StatsdEvent, String> {
    let context = format!("statsd event index {line_idx}");
    let Some((metric, rest)) = line.split_once(':') else {
        return Err(format!("{context} is missing ':' separator"));
    };
    let mut segments = rest.split('|');
    let value = segments
        .next()
        .ok_or_else(|| format!("{context} is missing a value"))?;
    let kind = segments
        .next()
        .ok_or_else(|| format!("{context} is missing a metric type"))?;

    let mut sample_rate = None;
    let mut tags = Vec::new();
    for segment in segments {
        if let Some(rate) = segment.strip_prefix('@') {
            if sample_rate.is_some() {
                return Err(format!("{context} contains duplicate sample-rate segments"));
            }
            let value = parse_finite_f64(rate, &format!("{context} sample rate"))?;
            if !(0.0 < value && value <= 1.0) {
                return Err(format!(
                    "{context} sample rate must be within (0, 1], got {value}"
                ));
            }
            sample_rate = Some(value);
            continue;
        }
        if let Some(raw_tags) = segment.strip_prefix('#') {
            for raw_tag in raw_tags.split(',').filter(|tag| !tag.is_empty()) {
                let (name, value) = if let Some((name, value)) = raw_tag.split_once(':') {
                    (name, value)
                } else {
                    (raw_tag, "true")
                };
                let label_name = normalize_label_name(name, &format!("{context} tag"))?;
                tags.push((label_name, value.to_string()));
            }
            continue;
        }
        return Err(format!(
            "{context} contains unsupported segment '{segment}'"
        ));
    }

    Ok(StatsdEvent {
        metric: metric.to_string(),
        value: value.to_string(),
        kind: kind.to_ascii_lowercase(),
        sample_rate,
        tags,
    })
}

fn normalize_statsd_gauge_value(
    metric: &str,
    labels: &[(String, String)],
    raw_value: &str,
    state: &Mutex<BTreeMap<String, f64>>,
    context: &str,
) -> Result<f64, String> {
    let key = series_state_key(metric, labels);
    if let Some(delta) = raw_value.strip_prefix('+') {
        let delta = parse_finite_f64(delta, &format!("{context} relative gauge delta"))?;
        let mut state = state
            .lock()
            .map_err(|_| "statsd gauge state lock poisoned".to_string())?;
        let value = state.get(&key).copied().unwrap_or(0.0) + delta;
        state.insert(key, value);
        return Ok(value);
    }
    if let Some(delta) = raw_value.strip_prefix('-') {
        let delta = parse_finite_f64(delta, &format!("{context} relative gauge delta"))?;
        let mut state = state
            .lock()
            .map_err(|_| "statsd gauge state lock poisoned".to_string())?;
        let value = state.get(&key).copied().unwrap_or(0.0) - delta;
        state.insert(key, value);
        return Ok(value);
    }

    let value = parse_finite_f64(raw_value, &format!("{context} gauge value"))?;
    let mut state = state
        .lock()
        .map_err(|_| "statsd gauge state lock poisoned".to_string())?;
    state.insert(key, value);
    Ok(value)
}

fn series_state_key(metric: &str, labels: &[(String, String)]) -> String {
    let mut labels = labels.to_vec();
    labels.sort();
    let mut key = String::with_capacity(metric.len() + labels.len() * 16 + 2);
    key.push_str(metric);
    key.push('{');
    for (name, value) in labels {
        key.push_str(&name);
        key.push('=');
        key.push_str(&value);
        key.push(',');
    }
    key.push('}');
    key
}

fn split_influx_sections<'a>(
    line: &'a str,
    context: &str,
) -> Result<(&'a str, &'a str, Option<&'a str>), String> {
    let first_space = find_unescaped_unquoted_byte(line, b' ')
        .ok_or_else(|| format!("{context} is missing the field section"))?;
    let series_spec = &line[..first_space];
    let remainder = line[first_space + 1..].trim_start();
    if remainder.is_empty() {
        return Err(format!("{context} is missing the field section"));
    }
    if let Some(second_space) = find_unescaped_unquoted_byte(remainder, b' ') {
        let field_spec = &remainder[..second_space];
        let timestamp = remainder[second_space + 1..].trim();
        if timestamp.is_empty() {
            return Err(format!("{context} timestamp must not be empty"));
        }
        Ok((series_spec, field_spec, Some(timestamp)))
    } else {
        Ok((series_spec, remainder, None))
    }
}

fn parse_influx_series_spec(
    raw: &str,
    context: &str,
) -> Result<(String, Vec<(String, String)>), String> {
    let mut parts = split_unescaped_unquoted(raw, b',');
    let measurement = influx_unescape(
        parts
            .next()
            .ok_or_else(|| format!("{context} is missing a measurement"))?,
    );
    if measurement.is_empty() {
        return Err(format!("{context} measurement must not be empty"));
    }
    let mut labels = Vec::new();
    for raw_tag in parts {
        let (name, value) = split_first_unescaped_unquoted(raw_tag, b'=')
            .ok_or_else(|| format!("{context} tag '{raw_tag}' is missing '='"))?;
        labels.push((
            normalize_label_name(&influx_unescape(name), &format!("{context} tag"))?,
            influx_unescape(value),
        ));
    }
    Ok((measurement, labels))
}

fn parse_influx_field_assignments(raw: &str, context: &str) -> Result<Vec<(String, f64)>, String> {
    let mut fields = Vec::new();
    for raw_field in split_unescaped_unquoted(raw, b',') {
        let (name, value) = split_first_unescaped_unquoted(raw_field, b'=')
            .ok_or_else(|| format!("{context} field '{raw_field}' is missing '='"))?;
        let field_name = influx_unescape(name);
        if field_name.is_empty() {
            return Err(format!("{context} field name must not be empty"));
        }
        let field_value =
            parse_influx_field_value(value, &format!("{context} field '{field_name}'"))?;
        fields.push((field_name, field_value));
    }
    if fields.is_empty() {
        return Err(format!("{context} must contain at least one field"));
    }
    Ok(fields)
}

fn parse_influx_field_value(raw: &str, context: &str) -> Result<f64, String> {
    if raw.starts_with('"') && raw.ends_with('"') && raw.len() >= 2 {
        return Err(format!("{context} string fields are not supported"));
    }
    if matches_ignore_ascii_case(raw, &["true", "t", "false", "f"]) {
        return Err(format!("{context} boolean fields are not supported"));
    }
    if let Some(value) = raw.strip_suffix('i') {
        let value = value
            .parse::<i64>()
            .map_err(|_| format!("{context} has invalid integer value '{raw}'"))?;
        return Ok(value as f64);
    }
    if let Some(value) = raw.strip_suffix('u') {
        let value = value
            .parse::<u64>()
            .map_err(|_| format!("{context} has invalid unsigned integer value '{raw}'"))?;
        return Ok(value as f64);
    }
    parse_finite_f64(raw, context)
}

fn parse_influx_precision(raw: Option<&str>) -> Result<InfluxTimestampPrecision, String> {
    match raw.unwrap_or("ns").to_ascii_lowercase().as_str() {
        "ns" => Ok(InfluxTimestampPrecision::Ns),
        "u" | "us" => Ok(InfluxTimestampPrecision::Us),
        "ms" => Ok(InfluxTimestampPrecision::Ms),
        "s" => Ok(InfluxTimestampPrecision::S),
        "m" => Ok(InfluxTimestampPrecision::M),
        "h" => Ok(InfluxTimestampPrecision::H),
        other => Err(format!(
            "unsupported influx line protocol precision '{other}'"
        )),
    }
}

fn convert_influx_timestamp(
    raw: &str,
    input_precision: InfluxTimestampPrecision,
    output_precision: TimestampPrecision,
    context: &str,
) -> Result<i64, String> {
    let timestamp = raw
        .parse::<i64>()
        .map_err(|_| format!("{context} must be a signed integer"))?;
    let nanos = match input_precision {
        InfluxTimestampPrecision::Ns => i128::from(timestamp),
        InfluxTimestampPrecision::Us => i128::from(timestamp) * 1_000,
        InfluxTimestampPrecision::Ms => i128::from(timestamp) * 1_000_000,
        InfluxTimestampPrecision::S => i128::from(timestamp) * 1_000_000_000,
        InfluxTimestampPrecision::M => i128::from(timestamp) * 60 * 1_000_000_000,
        InfluxTimestampPrecision::H => i128::from(timestamp) * 60 * 60 * 1_000_000_000,
    };
    let scaled = match output_precision {
        TimestampPrecision::Seconds => nanos / 1_000_000_000,
        TimestampPrecision::Milliseconds => nanos / 1_000_000,
        TimestampPrecision::Microseconds => nanos / 1_000,
        TimestampPrecision::Nanoseconds => nanos,
    };
    i64::try_from(scaled).map_err(|_| format!("{context} is out of range"))
}

fn parse_graphite_metric_spec(
    raw: &str,
    context: &str,
) -> Result<(String, Vec<(String, String)>), String> {
    let mut segments = raw.split(';');
    let metric = segments
        .next()
        .ok_or_else(|| format!("{context} is missing a metric name"))?;
    if metric.is_empty() {
        return Err(format!("{context} metric name must not be empty"));
    }
    let mut labels = Vec::new();
    for raw_tag in segments {
        let (name, value) = raw_tag
            .split_once('=')
            .ok_or_else(|| format!("{context} tag '{raw_tag}' is missing '='"))?;
        labels.push((
            normalize_label_name(name, &format!("{context} tag"))?,
            value.to_string(),
        ));
    }
    Ok((metric.to_string(), labels))
}

fn parse_graphite_timestamp(
    raw: &str,
    precision: TimestampPrecision,
    context: &str,
) -> Result<i64, String> {
    let seconds = raw
        .parse::<f64>()
        .map_err(|_| format!("{context} must be a Unix timestamp in seconds"))?;
    if !seconds.is_finite() {
        return Err(format!("{context} must be finite"));
    }
    let scaled = match precision {
        TimestampPrecision::Seconds => seconds,
        TimestampPrecision::Milliseconds => seconds * 1_000.0,
        TimestampPrecision::Microseconds => seconds * 1_000_000.0,
        TimestampPrecision::Nanoseconds => seconds * 1_000_000_000.0,
    };
    Ok(scaled as i64)
}

fn normalize_legacy_tags(
    tags: Vec<(String, String)>,
    _context: &str,
) -> Result<Vec<(String, String)>, String> {
    let mut normalized = Vec::with_capacity(tags.len());
    for (name, value) in tags {
        let value = value.trim();
        normalized.push((name, value.to_string()));
    }
    Ok(normalized)
}

fn normalize_metric_name(raw: &str, context: &str) -> Result<String, String> {
    normalize_identifier(raw, true, &format!("{context} metric name"))
}

fn normalize_label_name(raw: &str, context: &str) -> Result<String, String> {
    normalize_identifier(raw, false, &format!("{context} label name"))
}

fn normalize_identifier(raw: &str, allow_colon: bool, context: &str) -> Result<String, String> {
    if raw.is_empty() {
        return Err(format!("{context} must not be empty"));
    }

    let mut out = String::new();
    for (idx, byte) in raw.bytes().enumerate() {
        match byte {
            b'a'..=b'z' | b'A'..=b'Z' | b'_' => out.push(byte as char),
            b'0'..=b'9' => {
                if idx == 0 {
                    out.push('_');
                }
                out.push(byte as char);
            }
            b':' if allow_colon => out.push(':'),
            b'.' | b'-' | b'/' | b' ' | b':' => out.push('_'),
            _ => {
                out.push_str("_x");
                out.push_str(&format!("{byte:02x}"));
                out.push('_');
            }
        }
    }
    if out.is_empty() {
        return Err(format!("{context} '{raw}' could not be normalized"));
    }
    Ok(out)
}

fn insert_metadata_update(
    metadata_updates: &mut BTreeMap<String, crate::prom_write::NormalizedMetricMetadataUpdate>,
    metric: &str,
    metric_type: MetricType,
    help: &str,
    context: &str,
) -> Result<(), String> {
    let candidate = build_metadata_update(metric, metric_type, help, "", context)?;
    if let Some(existing) = metadata_updates.get(metric) {
        if existing != &candidate {
            return Err(format!(
                "{context} produced conflicting metadata updates for metric '{metric}'"
            ));
        }
        return Ok(());
    }
    metadata_updates.insert(metric.to_string(), candidate);
    Ok(())
}

fn parse_finite_f64(raw: &str, context: &str) -> Result<f64, String> {
    let value = raw
        .parse::<f64>()
        .map_err(|_| format!("{context} has invalid numeric value '{raw}'"))?;
    if !value.is_finite() {
        return Err(format!("{context} must be finite"));
    }
    Ok(value)
}

fn canonical_float(value: f64) -> String {
    if value.fract() == 0.0 {
        format!("{value:.0}")
    } else {
        value.to_string()
    }
}

fn matches_ignore_ascii_case(raw: &str, candidates: &[&str]) -> bool {
    candidates
        .iter()
        .any(|candidate| raw.eq_ignore_ascii_case(candidate))
}

fn influx_unescape(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut escaped = false;
    for byte in raw.bytes() {
        if escaped {
            out.push(byte as char);
            escaped = false;
            continue;
        }
        if byte == b'\\' {
            escaped = true;
            continue;
        }
        out.push(byte as char);
    }
    if escaped {
        out.push('\\');
    }
    out
}

fn find_unescaped_unquoted_byte(raw: &str, needle: u8) -> Option<usize> {
    let mut in_quotes = false;
    let mut escaped = false;
    for (idx, byte) in raw.bytes().enumerate() {
        if escaped {
            escaped = false;
            continue;
        }
        match byte {
            b'\\' => escaped = true,
            b'"' => in_quotes = !in_quotes,
            _ if byte == needle && !in_quotes => return Some(idx),
            _ => {}
        }
    }
    None
}

fn split_unescaped_unquoted(raw: &str, needle: u8) -> impl Iterator<Item = &str> {
    let mut parts = Vec::new();
    let mut start = 0usize;
    let mut in_quotes = false;
    let mut escaped = false;
    for (idx, byte) in raw.bytes().enumerate() {
        if escaped {
            escaped = false;
            continue;
        }
        match byte {
            b'\\' => escaped = true,
            b'"' => in_quotes = !in_quotes,
            _ if byte == needle && !in_quotes => {
                parts.push(&raw[start..idx]);
                start = idx + 1;
            }
            _ => {}
        }
    }
    parts.push(&raw[start..]);
    parts.into_iter()
}

fn split_first_unescaped_unquoted(raw: &str, needle: u8) -> Option<(&str, &str)> {
    let idx = find_unescaped_unquoted_byte(raw, needle)?;
    Some((&raw[..idx], &raw[idx + 1..]))
}

fn parse_env_bool(var: &str, default: bool) -> bool {
    match std::env::var(var) {
        Ok(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => default,
    }
}

fn parse_env_usize(var: &str, default: usize) -> usize {
    match std::env::var(var) {
        Ok(value) => value.trim().parse::<usize>().unwrap_or(default),
        Err(_) => default,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenant;

    #[test]
    fn statsd_adapter_normalizes_common_events() {
        let adapter = StatsdAdapter::new();
        let normalized = adapter
            .normalize_packet(
                "api.requests:2|c|@0.5|#env:prod\nqueue.depth:+3|g|#worker:ingest\nusers.uniques:abc|s",
                tenant::DEFAULT_TENANT_ID,
                1_700_000_000_000,
                StatsdConfig {
                    max_packet_bytes: 8_192,
                    max_events_per_packet: 16,
                },
            )
            .expect("statsd packet should normalize");

        assert_eq!(normalized.request_units, 3);
        assert_eq!(normalized.sample_count, 3);
        assert_eq!(
            normalized.envelope.scalar_samples[0].series.metric,
            "api_requests"
        );
        assert_eq!(
            normalized.envelope.scalar_samples[0]
                .data_point
                .value_as_f64(),
            Some(4.0)
        );
        assert_eq!(
            normalized.envelope.scalar_samples[1]
                .data_point
                .value_as_f64(),
            Some(3.0)
        );
        assert_eq!(
            normalized.envelope.scalar_samples[2]
                .series
                .labels
                .iter()
                .find(|label| label.name == "statsd_set_member")
                .map(|label| label.value.as_str()),
            Some("abc")
        );
        assert_eq!(normalized.envelope.metadata_updates.len(), 3);
    }

    #[test]
    fn statsd_relative_gauges_keep_state() {
        let adapter = StatsdAdapter::new();
        let first = adapter
            .normalize_packet(
                "queue.depth:+5|g",
                tenant::DEFAULT_TENANT_ID,
                1,
                statsd_config(),
            )
            .expect("first gauge update should normalize");
        let second = adapter
            .normalize_packet(
                "queue.depth:-2|g",
                tenant::DEFAULT_TENANT_ID,
                2,
                statsd_config(),
            )
            .expect("second gauge update should normalize");

        assert_eq!(
            first.envelope.scalar_samples[0].data_point.value_as_f64(),
            Some(5.0)
        );
        assert_eq!(
            second.envelope.scalar_samples[0].data_point.value_as_f64(),
            Some(3.0)
        );
    }

    #[test]
    fn influx_line_protocol_normalizes_measurements_tags_and_timestamps() {
        let normalized = normalize_influx_line_protocol(
            "weather,host=web-01 value=42i,temp=22.5 1700000000000000000",
            "tenant-a",
            TimestampPrecision::Milliseconds,
            99,
            InfluxLineProtocolConfig {
                enabled: true,
                max_lines_per_request: 16,
            },
            vec![("influx_db".to_string(), "telegraf".to_string())],
            Some("ns"),
        )
        .expect("line protocol should normalize");

        assert_eq!(normalized.request_units, 1);
        assert_eq!(normalized.sample_count, 2);
        assert_eq!(
            normalized.envelope.scalar_samples[0].series.metric,
            "weather"
        );
        assert_eq!(
            normalized.envelope.scalar_samples[0].data_point.timestamp,
            1_700_000_000_000
        );
        assert_eq!(
            normalized.envelope.scalar_samples[1].series.metric,
            "weather_temp"
        );
        assert!(normalized.envelope.scalar_samples[0]
            .series
            .labels
            .iter()
            .any(|label| label.name == "influx_db" && label.value == "telegraf"));
        assert_eq!(normalized.envelope.metadata_updates.len(), 2);
    }

    #[test]
    fn influx_line_protocol_rejects_boolean_fields() {
        let err = normalize_influx_line_protocol(
            "weather ok=true",
            "tenant-a",
            TimestampPrecision::Milliseconds,
            99,
            InfluxLineProtocolConfig {
                enabled: true,
                max_lines_per_request: 16,
            },
            Vec::new(),
            None,
        )
        .expect_err("boolean fields must be rejected");

        assert!(err.contains("boolean fields are not supported"));
    }

    #[test]
    fn graphite_plaintext_normalizes_tagged_metrics() {
        let normalized = normalize_graphite_plaintext_line(
            "servers.api.latency;env=prod;region=us-west 42.5 1700000000",
            "tenant-a",
            TimestampPrecision::Milliseconds,
            99,
        )
        .expect("graphite line should normalize");

        assert_eq!(normalized.sample_count, 1);
        assert_eq!(
            normalized.envelope.scalar_samples[0].series.metric,
            "servers_api_latency"
        );
        assert_eq!(
            normalized.envelope.scalar_samples[0].data_point.timestamp,
            1_700_000_000_000
        );
        assert!(normalized.envelope.scalar_samples[0]
            .series
            .labels
            .iter()
            .any(|label| label.name == "region" && label.value == "us-west"));
    }

    #[test]
    fn graphite_plaintext_rejects_bad_shapes() {
        let err = normalize_graphite_plaintext_line(
            "servers.api.latency 42.5 now",
            "tenant-a",
            TimestampPrecision::Milliseconds,
            99,
        )
        .expect_err("invalid graphite timestamp must be rejected");

        assert!(err.contains("Unix timestamp in seconds"));
    }
}
