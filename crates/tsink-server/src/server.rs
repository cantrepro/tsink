use crate::prom_remote::{
    Label as PromLabel, LabelMatcher, MatcherType, Query, QueryResult, ReadRequest, ReadResponse,
    Sample as PromSample, TimeSeries, WriteRequest,
};
use prost::Message;
use regex::Regex;
use snap::raw::{Decoder as SnappyDecoder, Encoder as SnappyEncoder};
use std::collections::HashMap;
use std::fmt::Write as FmtWrite;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tsink::{
    DataPoint, Label, MetricSeries, Row, Storage, StorageBuilder, TimestampPrecision, TsinkError,
};

const MAX_HEADER_BYTES: usize = 64 * 1024;
const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub listen: String,
    pub data_path: Option<PathBuf>,
    pub wal_enabled: bool,
    pub timestamp_precision: TimestampPrecision,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:9201".to_string(),
            data_path: None,
            wal_enabled: true,
            timestamp_precision: TimestampPrecision::Milliseconds,
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct HttpRequest {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: Vec<u8>,
}

impl HttpRequest {
    fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
    }

    fn path_without_query(&self) -> &str {
        self.path.split('?').next().unwrap_or("/")
    }
}

#[derive(Debug, Clone)]
pub(crate) struct HttpResponse {
    status: u16,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
}

impl HttpResponse {
    fn new(status: u16, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: body.into(),
        }
    }

    fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.push((name.into(), value.into()));
        self
    }
}

pub fn run_server(config: ServerConfig) -> Result<(), String> {
    let storage =
        build_storage(&config).map_err(|err| format!("failed to build storage: {err}"))?;
    let listener = TcpListener::bind(&config.listen)
        .map_err(|err| format!("bind {} failed: {err}", config.listen))?;

    println!("tsink server listening on {}", config.listen);
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let storage = Arc::clone(&storage);
                std::thread::spawn(move || {
                    if let Err(err) = handle_connection(stream, &storage) {
                        eprintln!("connection error: {err}");
                    }
                });
            }
            Err(err) => eprintln!("accept error: {err}"),
        }
    }

    Ok(())
}

fn build_storage(config: &ServerConfig) -> tsink::Result<Arc<dyn Storage>> {
    let mut builder = StorageBuilder::new()
        .with_wal_enabled(config.wal_enabled)
        .with_timestamp_precision(config.timestamp_precision);

    if let Some(path) = &config.data_path {
        builder = builder.with_data_path(path);
    }

    builder.build()
}

fn handle_connection(mut stream: TcpStream, storage: &Arc<dyn Storage>) -> Result<(), String> {
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .map_err(|err| format!("failed to set read timeout: {err}"))?;
    stream
        .set_write_timeout(Some(Duration::from_secs(30)))
        .map_err(|err| format!("failed to set write timeout: {err}"))?;

    let request = match read_http_request(&mut stream) {
        Ok(request) => request,
        Err(err) => {
            let response = text_response(400, &err);
            write_http_response(&mut stream, &response).map_err(|write_err| {
                format!("failed to write bad-request response: {write_err}")
            })?;
            return Ok(());
        }
    };

    let response = handle_request(storage, request);
    write_http_response(&mut stream, &response)
        .map_err(|err| format!("write response failed: {err}"))
}

pub(crate) fn handle_request(storage: &Arc<dyn Storage>, request: HttpRequest) -> HttpResponse {
    match (request.method.as_str(), request.path_without_query()) {
        ("GET", "/healthz") => {
            HttpResponse::new(200, "ok\n").with_header("Content-Type", "text/plain")
        }
        ("POST", "/api/v1/write") => handle_remote_write(storage, &request),
        ("POST", "/api/v1/read") => handle_remote_read(storage, &request),
        _ => text_response(404, "not found"),
    }
}

fn handle_remote_write(storage: &Arc<dyn Storage>, request: &HttpRequest) -> HttpResponse {
    let decoded = match decode_body(request) {
        Ok(body) => body,
        Err(err) => return text_response(400, &err),
    };

    let write_req = match WriteRequest::decode(decoded.as_slice()) {
        Ok(req) => req,
        Err(err) => return text_response(400, &format!("invalid protobuf body: {err}")),
    };

    let mut rows = Vec::new();
    for series in write_req.timeseries {
        let Some((metric, labels)) = extract_metric_and_labels(series.labels) else {
            continue;
        };
        for sample in series.samples {
            rows.push(Row::with_labels(
                metric.clone(),
                labels.clone(),
                DataPoint::new(sample.timestamp, sample.value),
            ));
        }
    }

    if rows.is_empty() {
        return HttpResponse::new(200, Vec::<u8>::new());
    }

    match storage.insert_rows(&rows) {
        Ok(()) => HttpResponse::new(200, Vec::<u8>::new()),
        Err(err) => text_response(500, &format!("insert failed: {err}")),
    }
}

fn handle_remote_read(storage: &Arc<dyn Storage>, request: &HttpRequest) -> HttpResponse {
    let decoded = match decode_body(request) {
        Ok(body) => body,
        Err(err) => return text_response(400, &err),
    };

    let read_req = match ReadRequest::decode(decoded.as_slice()) {
        Ok(req) => req,
        Err(err) => return text_response(400, &format!("invalid protobuf body: {err}")),
    };

    let mut results = Vec::with_capacity(read_req.queries.len());
    for query in &read_req.queries {
        match execute_query(storage, query) {
            Ok(result) => results.push(result),
            Err(err) => return text_response(400, &err),
        }
    }

    let response = ReadResponse { results };
    let mut raw = Vec::new();
    if let Err(err) = response.encode(&mut raw) {
        return text_response(500, &format!("failed to encode response: {err}"));
    }

    let compressed = match SnappyEncoder::new().compress_vec(&raw) {
        Ok(bytes) => bytes,
        Err(err) => return text_response(500, &format!("snappy encode failed: {err}")),
    };

    HttpResponse::new(200, compressed)
        .with_header("Content-Type", "application/x-protobuf")
        .with_header("Content-Encoding", "snappy")
        .with_header("X-Prometheus-Remote-Read-Version", "0.1.0")
}

fn execute_query(storage: &Arc<dyn Storage>, query: &Query) -> Result<QueryResult, String> {
    let series_list = storage
        .list_metrics_with_wal()
        .map_err(|err| format!("failed to list metrics: {err}"))?;

    let mut out_series = Vec::new();
    let end = query.end_timestamp_ms.saturating_add(1);
    for series in series_list {
        if !matches_series(&series, &query.matchers)? {
            continue;
        }

        let points =
            match storage.select(&series.name, &series.labels, query.start_timestamp_ms, end) {
                Ok(points) => points,
                Err(TsinkError::NoDataPoints { .. }) => continue,
                Err(err) => return Err(format!("query failed for {}: {err}", series.name)),
            };

        let samples: Vec<PromSample> = points
            .into_iter()
            .filter_map(|point| {
                point.value.as_f64().map(|value| PromSample {
                    value,
                    timestamp: point.timestamp,
                })
            })
            .collect();

        if samples.is_empty() {
            continue;
        }

        let mut labels = Vec::with_capacity(series.labels.len() + 1);
        labels.push(PromLabel {
            name: "__name__".to_string(),
            value: series.name.clone(),
        });
        labels.extend(series.labels.into_iter().map(|label| PromLabel {
            name: label.name,
            value: label.value,
        }));
        labels.sort_by(|a, b| a.name.cmp(&b.name).then(a.value.cmp(&b.value)));

        out_series.push(TimeSeries { labels, samples });
    }

    Ok(QueryResult {
        timeseries: out_series,
    })
}

fn extract_metric_and_labels(labels: Vec<PromLabel>) -> Option<(String, Vec<Label>)> {
    let mut metric = None;
    let mut out_labels = Vec::new();

    for label in labels {
        if label.name == "__name__" {
            if !label.value.is_empty() {
                metric = Some(label.value);
            }
            continue;
        }

        out_labels.push(Label::new(label.name, label.value));
    }

    metric.map(|name| (name, out_labels))
}

fn matches_series(series: &MetricSeries, matchers: &[LabelMatcher]) -> Result<bool, String> {
    for matcher in matchers {
        let matcher_type = MatcherType::try_from(matcher.r#type)
            .map_err(|_| format!("unknown matcher type: {}", matcher.r#type))?;
        let label_value = label_value(series, &matcher.name);

        match matcher_type {
            MatcherType::Eq => {
                if label_value != Some(matcher.value.as_str()) {
                    return Ok(false);
                }
            }
            MatcherType::Neq => {
                if label_value == Some(matcher.value.as_str()) {
                    return Ok(false);
                }
            }
            MatcherType::Re => {
                let regex = compile_prometheus_regex(&matcher.value)?;
                if !label_value.is_some_and(|value| regex.is_match(value)) {
                    return Ok(false);
                }
            }
            MatcherType::Nre => {
                let regex = compile_prometheus_regex(&matcher.value)?;
                if label_value.is_some_and(|value| regex.is_match(value)) {
                    return Ok(false);
                }
            }
        }
    }

    Ok(true)
}

fn compile_prometheus_regex(pattern: &str) -> Result<Regex, String> {
    let wrapped = format!("^(?:{})$", pattern);
    Regex::new(&wrapped).map_err(|err| format!("invalid regex matcher '{pattern}': {err}"))
}

fn label_value<'a>(series: &'a MetricSeries, name: &str) -> Option<&'a str> {
    if name == "__name__" {
        return Some(series.name.as_str());
    }

    series
        .labels
        .iter()
        .find(|label| label.name == name)
        .map(|label| label.value.as_str())
}

fn decode_body(request: &HttpRequest) -> Result<Vec<u8>, String> {
    match request.header("content-encoding") {
        None => Ok(request.body.clone()),
        Some(encoding) if encoding.eq_ignore_ascii_case("identity") => Ok(request.body.clone()),
        Some(encoding) if encoding.eq_ignore_ascii_case("snappy") => SnappyDecoder::new()
            .decompress_vec(&request.body)
            .map_err(|err| format!("snappy decode failed: {err}")),
        Some(encoding) => Err(format!("unsupported content-encoding: {encoding}")),
    }
}

fn read_http_request(stream: &mut TcpStream) -> Result<HttpRequest, String> {
    let mut buffer = Vec::with_capacity(8 * 1024);
    let mut chunk = [0_u8; 4096];
    let header_end = loop {
        let read = stream
            .read(&mut chunk)
            .map_err(|err| format!("failed to read request: {err}"))?;
        if read == 0 {
            return Err("connection closed while reading request".to_string());
        }

        buffer.extend_from_slice(&chunk[..read]);
        if buffer.len() > MAX_HEADER_BYTES {
            return Err("request headers too large".to_string());
        }

        if let Some(pos) = find_sequence(&buffer, b"\r\n\r\n") {
            break pos + 4;
        }
    };

    let headers_text = std::str::from_utf8(&buffer[..header_end])
        .map_err(|_| "request headers must be valid UTF-8")?;
    let mut lines = headers_text.split("\r\n");

    let request_line = lines
        .next()
        .ok_or_else(|| "missing request line".to_string())?;
    let mut parts = request_line.split_whitespace();
    let method = parts
        .next()
        .ok_or_else(|| "missing HTTP method".to_string())?
        .to_string();
    let path = parts
        .next()
        .ok_or_else(|| "missing HTTP path".to_string())?
        .to_string();
    let _version = parts
        .next()
        .ok_or_else(|| "missing HTTP version".to_string())?;

    let mut headers = HashMap::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let Some((name, value)) = line.split_once(':') else {
            return Err(format!("malformed header line: {line}"));
        };
        headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
    }

    let content_length = headers
        .get("content-length")
        .map(|value| value.parse::<usize>())
        .transpose()
        .map_err(|_| "invalid content-length".to_string())?
        .unwrap_or(0);

    if content_length > MAX_BODY_BYTES {
        return Err("request body too large".to_string());
    }

    let mut body = buffer[header_end..].to_vec();
    while body.len() < content_length {
        let to_read = (content_length - body.len()).min(chunk.len());
        let read = stream
            .read(&mut chunk[..to_read])
            .map_err(|err| format!("failed to read request body: {err}"))?;
        if read == 0 {
            return Err("connection closed while reading request body".to_string());
        }
        body.extend_from_slice(&chunk[..read]);
    }
    if body.len() > content_length {
        body.truncate(content_length);
    }

    Ok(HttpRequest {
        method,
        path,
        headers,
        body,
    })
}

fn find_sequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn write_http_response(stream: &mut TcpStream, response: &HttpResponse) -> std::io::Result<()> {
    let reason = status_reason(response.status);
    let mut headers = String::new();
    let _ = write!(&mut headers, "HTTP/1.1 {} {}\r\n", response.status, reason);
    let _ = write!(&mut headers, "Content-Length: {}\r\n", response.body.len());
    let _ = write!(&mut headers, "Connection: close\r\n");

    for (name, value) in &response.headers {
        let _ = write!(&mut headers, "{}: {}\r\n", name, value);
    }

    headers.push_str("\r\n");

    stream.write_all(headers.as_bytes())?;
    stream.write_all(&response.body)?;
    stream.flush()
}

fn status_reason(code: u16) -> &'static str {
    match code {
        200 => "OK",
        400 => "Bad Request",
        404 => "Not Found",
        500 => "Internal Server Error",
        _ => "Unknown",
    }
}

fn text_response(status: u16, text: &str) -> HttpResponse {
    HttpResponse::new(status, text.as_bytes().to_vec()).with_header("Content-Type", "text/plain")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::prom_remote::{
        LabelMatcher, MatcherType, Query, ReadRequest, ReadResponse, WriteRequest,
    };
    use std::sync::Arc;
    use tsink::{StorageBuilder, TimestampPrecision};

    fn make_storage() -> Arc<dyn Storage> {
        StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .build()
            .expect("storage should build")
    }

    fn snappy_encode(data: &[u8]) -> Vec<u8> {
        SnappyEncoder::new()
            .compress_vec(data)
            .expect("snappy encode should succeed")
    }

    fn snappy_decode(data: &[u8]) -> Vec<u8> {
        SnappyDecoder::new()
            .decompress_vec(data)
            .expect("snappy decode should succeed")
    }

    #[test]
    fn remote_write_inserts_points() {
        let storage = make_storage();

        let write = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![
                    PromLabel {
                        name: "__name__".to_string(),
                        value: "cpu_usage".to_string(),
                    },
                    PromLabel {
                        name: "host".to_string(),
                        value: "server-a".to_string(),
                    },
                ],
                samples: vec![PromSample {
                    value: 11.5,
                    timestamp: 1_700_000_000_000,
                }],
            }],
        };
        let mut encoded = Vec::new();
        write
            .encode(&mut encoded)
            .expect("protobuf encode should work");
        let compressed = snappy_encode(&encoded);

        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/write".to_string(),
            headers: HashMap::from([
                ("content-encoding".to_string(), "snappy".to_string()),
                (
                    "content-type".to_string(),
                    "application/x-protobuf".to_string(),
                ),
            ]),
            body: compressed,
        };

        let response = handle_request(&storage, request);
        assert_eq!(response.status, 200);

        let points = storage
            .select(
                "cpu_usage",
                &[Label::new("host", "server-a")],
                1_700_000_000_000,
                1_700_000_000_001,
            )
            .expect("point must be persisted");
        assert_eq!(points.len(), 1);
        assert_eq!(points[0].value.as_f64(), Some(11.5));
    }

    #[test]
    fn remote_read_returns_snappy_protobuf() {
        let storage = make_storage();
        storage
            .insert_rows(&[Row::with_labels(
                "http_requests_total",
                vec![Label::new("method", "GET")],
                DataPoint::new(1_700_000_000_100, 42.0),
            )])
            .expect("insert should work");

        let read = ReadRequest {
            queries: vec![Query {
                start_timestamp_ms: 1_700_000_000_000,
                end_timestamp_ms: 1_700_000_000_200,
                matchers: vec![
                    LabelMatcher {
                        r#type: MatcherType::Eq as i32,
                        name: "__name__".to_string(),
                        value: "http_requests_total".to_string(),
                    },
                    LabelMatcher {
                        r#type: MatcherType::Eq as i32,
                        name: "method".to_string(),
                        value: "GET".to_string(),
                    },
                ],
            }],
        };

        let mut encoded = Vec::new();
        read.encode(&mut encoded)
            .expect("protobuf encode should work");
        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/read".to_string(),
            headers: HashMap::from([
                ("content-encoding".to_string(), "snappy".to_string()),
                (
                    "content-type".to_string(),
                    "application/x-protobuf".to_string(),
                ),
            ]),
            body: snappy_encode(&encoded),
        };

        let response = handle_request(&storage, request);
        assert_eq!(response.status, 200);
        assert_eq!(
            response
                .headers
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case("content-encoding"))
                .map(|(_, v)| v.as_str()),
            Some("snappy")
        );

        let decoded = snappy_decode(&response.body);
        let read_response =
            ReadResponse::decode(decoded.as_slice()).expect("response should decode");
        assert_eq!(read_response.results.len(), 1);
        assert_eq!(read_response.results[0].timeseries.len(), 1);
        let series = &read_response.results[0].timeseries[0];
        assert_eq!(series.samples.len(), 1);
        assert_eq!(series.samples[0].value, 42.0);
    }
}
