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
            if key == name {
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
                if key == name {
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
        if self
            .header("content-type")
            .is_some_and(|ct| ct.starts_with("application/x-www-form-urlencoded"))
        {
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
        if self
            .header("content-type")
            .is_some_and(|ct| ct.starts_with("application/x-www-form-urlencoded"))
        {
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
                if key == name {
                    values.push(percent_decode(value));
                }
            }
        }
        values
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
        self.headers.push((name.into(), value.into()));
        self
    }
}

pub async fn read_http_request<R: AsyncRead + Unpin>(
    stream: &mut R,
) -> Result<HttpRequest, String> {
    let mut buffer = Vec::with_capacity(8 * 1024);
    let mut chunk = [0_u8; 4096];
    let header_end = loop {
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
            .await
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

pub fn status_reason(code: u16) -> &'static str {
    match code {
        200 => "OK",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        422 => "Unprocessable Entity",
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
