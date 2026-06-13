use std::collections::HashMap;
use std::fmt::Write as FmtWrite;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub const MAX_HEADER_BYTES: usize = 64 * 1024;
pub const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;

#[derive(Debug, Clone)]
pub struct HttpRequest {
    pub method: String,
    pub path: String,
    pub headers: HashMap<String, String>,
    pub body: Vec<u8>,
}

impl HttpRequest {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .get(&name.to_ascii_lowercase())
            .map(String::as_str)
    }

    pub fn path_without_query(&self) -> &str {
        self.path.split('?').next().unwrap_or("/")
    }

    pub fn query_param(&self, name: &str) -> Option<String> {
        let query = self.path.split_once('?').map(|(_, q)| q)?;
        for pair in query.split('&') {
            if pair.is_empty() {
                continue;
            }
            let (key, value) = if let Some((k, v)) = pair.split_once('=') {
                (k, v)
            } else {
                (pair, "")
            };
            if decoded_component_matches(key, name) {
                return Some(percent_decode(value));
            }
        }
        None
    }

    pub fn query_param_all(&self, name: &str) -> Vec<String> {
        let Some(query) = self.path.split_once('?').map(|(_, q)| q) else {
            return Vec::new();
        };
        query
            .split('&')
            .filter(|s| !s.is_empty())
            .filter_map(|pair| {
                let (key, value) = if let Some((k, v)) = pair.split_once('=') {
                    (k, v)
                } else {
                    (pair, "")
                };
                if decoded_component_matches(key, name) {
                    Some(percent_decode(value))
                } else {
                    None
                }
            })
            .collect()
    }

    pub fn form_params(&self) -> Vec<(String, String)> {
        let Ok(body_str) = std::str::from_utf8(&self.body) else {
            return Vec::new();
        };
        body_str
            .split('&')
            .filter(|s| !s.is_empty())
            .map(|pair| {
                let (key, value) = if let Some((k, v)) = pair.split_once('=') {
                    (k, v)
                } else {
                    (pair, "")
                };
                (percent_decode(key), percent_decode(value))
            })
            .collect()
    }

    /// Get a parameter from query string or form body (for GET/POST PromQL endpoints).
    pub fn param(&self, name: &str) -> Option<String> {
        if let Some(val) = self.query_param(name) {
            return Some(val);
        }
        if self.has_form_urlencoded_body() {
            for (k, v) in self.form_params() {
                if k == name {
                    return Some(v);
                }
            }
        }
        None
    }

    /// Get all values for a parameter from query string and form body.
    pub fn param_all(&self, name: &str) -> Vec<String> {
        let mut values = self.query_param_all(name);
        if self.has_form_urlencoded_body() {
            let Ok(body_str) = std::str::from_utf8(&self.body) else {
                return values;
            };
            for pair in body_str.split('&') {
                if pair.is_empty() {
                    continue;
                }
                let (key, value) = if let Some((k, v)) = pair.split_once('=') {
                    (k, v)
                } else {
                    (pair, "")
                };
                if decoded_component_matches(key, name) {
                    values.push(percent_decode(value));
                }
            }
        }
        values
    }

    fn has_form_urlencoded_body(&self) -> bool {
        self.header("content-type")
            .and_then(|content_type| content_type.split(';').next())
            .is_some_and(|mime| {
                mime.trim()
                    .eq_ignore_ascii_case("application/x-www-form-urlencoded")
            })
    }
}

#[derive(Debug, Clone)]
pub struct HttpResponse {
    pub status: u16,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl HttpResponse {
    pub fn new(status: u16, body: impl Into<Vec<u8>>) -> Self {
        Self {
            status,
            headers: Vec::new(),
            body: body.into(),
        }
    }

    pub fn with_header(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        let name = name.into();
        let value = value.into();
        if let Some((existing_name, existing_value)) = self
            .headers
            .iter_mut()
            .find(|(existing_name, _)| existing_name.eq_ignore_ascii_case(&name))
        {
            *existing_name = name;
            *existing_value = value;
        } else {
            self.headers.push((name, value));
        }
        self
    }
}

pub async fn read_http_request<R: AsyncRead + Unpin>(
    stream: &mut R,
    buffer: &mut Vec<u8>,
) -> Result<HttpRequest, String> {
    let mut chunk = [0_u8; 4096];
    let header_end = loop {
        if let Some(pos) = find_sequence(buffer, b"\r\n\r\n") {
            let end = pos + 4;
            if end > MAX_HEADER_BYTES {
                return Err("request headers too large".to_string());
            }
            break end;
        }

        let read = stream
            .read(&mut chunk)
            .await
            .map_err(|err| format!("failed to read request: {err}"))?;
        if read == 0 {
            return Err("connection closed while reading request".to_string());
        }

        buffer.extend_from_slice(&chunk[..read]);
        if buffer.len() > MAX_HEADER_BYTES {
            return Err("request headers too large".to_string());
        }

        if find_sequence(buffer, b"\r\n\r\n").is_none() && buffer.len() > MAX_HEADER_BYTES {
            return Err("request headers too large".to_string());
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
    let version = parts
        .next()
        .ok_or_else(|| "missing HTTP version".to_string())?;
    if parts.next().is_some() {
        return Err("malformed request line".to_string());
    }
    if version != "HTTP/1.1" && version != "HTTP/1.0" {
        return Err(format!("unsupported HTTP version: {version}"));
    }

    let mut headers = HashMap::new();
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let Some((name, value)) = line.split_once(':') else {
            return Err(format!("malformed header line: {line}"));
        };
        let name = name.trim();
        if name.is_empty() {
            return Err(format!("malformed header line: {line}"));
        }
        let name = name.to_ascii_lowercase();
        let value = value.trim().to_string();
        match name.as_str() {
            "content-length" if headers.contains_key("content-length") => {
                return Err("duplicate content-length header".to_string());
            }
            "transfer-encoding" => {
                return Err("transfer-encoding is not supported".to_string());
            }
            _ => {}
        }
        headers.insert(name, value);
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

    let request_end = header_end + content_length;
    while buffer.len() < request_end {
        let to_read = (request_end - buffer.len()).min(chunk.len());
        let read = stream
            .read(&mut chunk[..to_read])
            .await
            .map_err(|err| format!("failed to read request body: {err}"))?;
        if read == 0 {
            return Err("connection closed while reading request body".to_string());
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
    let body = buffer[header_end..request_end].to_vec();
    buffer.drain(..request_end);

    Ok(HttpRequest {
        method,
        path,
        headers,
        body,
    })
}

pub async fn write_http_response<W: AsyncWrite + Unpin>(
    stream: &mut W,
    response: &HttpResponse,
) -> Result<(), String> {
    let reason = status_reason(response.status);
    let mut header_buf = String::new();
    let _ = write!(
        &mut header_buf,
        "HTTP/1.1 {} {}\r\n",
        response.status, reason
    );
    let _ = write!(
        &mut header_buf,
        "Content-Length: {}\r\n",
        response.body.len()
    );

    for (name, value) in &response.headers {
        if name.eq_ignore_ascii_case("content-length")
            || name.eq_ignore_ascii_case("transfer-encoding")
        {
            continue;
        }
        let _ = write!(&mut header_buf, "{}: {}\r\n", name, value);
    }

    header_buf.push_str("\r\n");

    stream
        .write_all(header_buf.as_bytes())
        .await
        .map_err(|err| format!("write response failed: {err}"))?;
    stream
        .write_all(&response.body)
        .await
        .map_err(|err| format!("write response failed: {err}"))?;
    stream
        .flush()
        .await
        .map_err(|err| format!("flush response failed: {err}"))
}

fn find_sequence(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}

fn decoded_component_matches(raw: &str, expected: &str) -> bool {
    raw == expected
        || ((raw.as_bytes().contains(&b'%') || raw.as_bytes().contains(&b'+'))
            && percent_decode(raw) == expected)
}

pub fn status_reason(code: u16) -> &'static str {
    match code {
        200 => "OK",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        404 => "Not Found",
        409 => "Conflict",
        413 => "Payload Too Large",
        422 => "Unprocessable Entity",
        429 => "Too Many Requests",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        503 => "Service Unavailable",
        _ => "Unknown",
    }
}

pub fn text_response(status: u16, text: &str) -> HttpResponse {
    HttpResponse::new(status, text.as_bytes().to_vec()).with_header("Content-Type", "text/plain")
}

pub fn json_response(status: u16, value: &impl serde::Serialize) -> HttpResponse {
    match serde_json::to_vec(value) {
        Ok(body) => HttpResponse::new(status, body).with_header("Content-Type", "application/json"),
        Err(err) => text_response(500, &format!("json serialization failed: {err}")),
    }
}

fn percent_decode(input: &str) -> String {
    let mut result = Vec::with_capacity(input.len());
    let bytes = input.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'+' {
            result.push(b' ');
            i += 1;
        } else if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(hi), Some(lo)) = (hex_digit(bytes[i + 1]), hex_digit(bytes[i + 2])) {
                result.push(hi << 4 | lo);
                i += 3;
            } else {
                result.push(bytes[i]);
                i += 1;
            }
        } else {
            result.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(result).unwrap_or_else(|e| String::from_utf8_lossy(e.as_bytes()).into_owned())
}

fn hex_digit(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    #[test]
    fn query_and_form_params_decode_percent_encoded_keys() {
        let request = HttpRequest {
            method: "POST".to_string(),
            path: "/api/v1/series?match%5B%5D=up&match%5B%5D=node_cpu_seconds_total".to_string(),
            headers: HashMap::from([(
                "content-type".to_string(),
                "Application/X-Www-Form-Urlencoded; charset=utf-8".to_string(),
            )]),
            body: b"match%5B%5D=process_cpu_seconds_total".to_vec(),
        };

        assert_eq!(request.query_param("match[]").as_deref(), Some("up"));
        assert_eq!(
            request.param_all("match[]"),
            vec![
                "up".to_string(),
                "node_cpu_seconds_total".to_string(),
                "process_cpu_seconds_total".to_string(),
            ]
        );
    }

    #[tokio::test]
    async fn read_http_request_preserves_pipelined_bytes() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        client
            .write_all(
                b"GET /one HTTP/1.1\r\nHost: localhost\r\n\r\nGET /two HTTP/1.1\r\nHost: localhost\r\n\r\n",
            )
            .await
            .expect("write should work");
        drop(client);

        let mut read_buffer = Vec::new();
        let first = read_http_request(&mut server, &mut read_buffer)
            .await
            .expect("first request should parse");
        let second = read_http_request(&mut server, &mut read_buffer)
            .await
            .expect("second request should parse");

        assert_eq!(first.method, "GET");
        assert_eq!(first.path, "/one");
        assert_eq!(second.method, "GET");
        assert_eq!(second.path, "/two");
    }

    #[tokio::test]
    async fn read_http_request_rejects_transfer_encoding() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        client
            .write_all(
                b"POST /write HTTP/1.1\r\nHost: localhost\r\nTransfer-Encoding: chunked\r\n\r\n4\r\ntest\r\n0\r\n\r\n",
            )
            .await
            .expect("write should work");
        drop(client);

        let mut read_buffer = Vec::new();
        let err = read_http_request(&mut server, &mut read_buffer)
            .await
            .expect_err("chunked request should be rejected");
        assert_eq!(err, "transfer-encoding is not supported");
    }

    #[tokio::test]
    async fn read_http_request_rejects_duplicate_content_length() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        client
            .write_all(
                b"POST /write HTTP/1.1\r\nHost: localhost\r\nContent-Length: 4\r\nContent-Length: 5\r\n\r\ntest!",
            )
            .await
            .expect("write should work");
        drop(client);

        let mut read_buffer = Vec::new();
        let err = read_http_request(&mut server, &mut read_buffer)
            .await
            .expect_err("duplicate content-length should be rejected");
        assert_eq!(err, "duplicate content-length header");
    }

    #[tokio::test]
    async fn read_http_request_rejects_malformed_request_line() {
        let (mut client, mut server) = tokio::io::duplex(4096);
        client
            .write_all(b"GET / HTTP/1.1 unexpected\r\nHost: localhost\r\n\r\n")
            .await
            .expect("write should work");
        drop(client);

        let mut read_buffer = Vec::new();
        let err = read_http_request(&mut server, &mut read_buffer)
            .await
            .expect_err("extra request-line token should be rejected");
        assert_eq!(err, "malformed request line");
    }

    #[test]
    fn with_header_replaces_existing_header_case_insensitively() {
        let response = HttpResponse::new(200, Vec::<u8>::new())
            .with_header("content-type", "text/plain")
            .with_header("Content-Type", "application/json");

        assert_eq!(
            response.headers,
            vec![("Content-Type".to_string(), "application/json".to_string())]
        );
    }

    #[tokio::test]
    async fn write_http_response_ignores_conflicting_framing_headers() {
        let response = HttpResponse::new(200, b"ok".to_vec())
            .with_header("Content-Length", "999")
            .with_header("Transfer-Encoding", "chunked")
            .with_header("content-type", "text/plain")
            .with_header("Content-Type", "application/json");
        let (mut client, mut server) = tokio::io::duplex(4096);

        let write_task = tokio::spawn(async move {
            write_http_response(&mut server, &response)
                .await
                .expect("response write should succeed");
        });

        let mut raw = Vec::new();
        client
            .read_to_end(&mut raw)
            .await
            .expect("response should be readable");
        write_task.await.expect("writer task should not panic");

        let response_text = String::from_utf8(raw).expect("response should be utf8");
        assert!(response_text.contains("Content-Length: 2\r\n"));
        assert!(!response_text.contains("Content-Length: 999\r\n"));
        assert!(!response_text.contains("Transfer-Encoding: chunked\r\n"));

        let content_type_headers = response_text
            .lines()
            .filter(|line| line.to_ascii_lowercase().starts_with("content-type:"))
            .collect::<Vec<_>>();
        assert_eq!(content_type_headers, vec!["Content-Type: application/json"]);
    }

    #[test]
    fn status_reason_covers_emitted_statuses() {
        assert_eq!(status_reason(204), "No Content");
        assert_eq!(status_reason(403), "Forbidden");
        assert_eq!(status_reason(413), "Payload Too Large");
        assert_eq!(status_reason(429), "Too Many Requests");
    }
}
