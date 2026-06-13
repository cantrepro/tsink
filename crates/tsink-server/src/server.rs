use crate::handlers;
use crate::http::{read_http_request, text_response, write_http_response, HttpResponse};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpListener;
use tokio::sync::{watch, Semaphore};
use tsink::promql::Engine;
use tsink::{Storage, StorageBuilder, TimestampPrecision, WalSyncMode};

const MAX_CONNECTIONS: usize = 1024;
const KEEP_ALIVE_TIMEOUT: Duration = Duration::from_secs(30);
const HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);
const SHUTDOWN_GRACE_PERIOD: Duration = Duration::from_secs(10);

#[derive(Debug, Clone)]
pub struct ServerConfig {
    pub listen: String,
    pub data_path: Option<PathBuf>,
    pub wal_enabled: bool,
    pub timestamp_precision: TimestampPrecision,
    pub retention: Option<Duration>,
    pub memory_limit: Option<usize>,
    pub cardinality_limit: Option<usize>,
    pub chunk_points: Option<usize>,
    pub max_writers: Option<usize>,
    pub wal_sync_mode: Option<WalSyncMode>,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    pub auth_token: Option<String>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: "127.0.0.1:9201".to_string(),
            data_path: None,
            wal_enabled: true,
            timestamp_precision: TimestampPrecision::Milliseconds,
            retention: None,
            memory_limit: None,
            cardinality_limit: None,
            chunk_points: None,
            max_writers: None,
            wal_sync_mode: None,
            tls_cert: None,
            tls_key: None,
            auth_token: None,
        }
    }
}

pub async fn run_server(config: ServerConfig) -> Result<(), String> {
    let storage =
        build_storage(&config).map_err(|err| format!("failed to build storage: {err}"))?;

    let tls_acceptor = build_tls_acceptor(&config)?;

    let listener = TcpListener::bind(&config.listen)
        .await
        .map_err(|err| format!("bind {} failed: {err}", config.listen))?;

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let semaphore = Arc::new(Semaphore::new(MAX_CONNECTIONS));
    let server_start = Instant::now();
    let auth_token = config.auth_token.clone();
    let precision = config.timestamp_precision;

    // Spawn signal handler
    tokio::spawn(async move {
        if let Err(err) = wait_for_shutdown_signal().await {
            eprintln!("signal handler error: {err}");
        }
        eprintln!("shutting down...");
        let _ = shutdown_tx.send(true);
    });

    println!("tsink server listening on {}", config.listen);

    let active_connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));

    loop {
        let permit = tokio::select! {
            permit = semaphore.clone().acquire_owned() => {
                match permit {
                    Ok(p) => p,
                    Err(_) => break,
                }
            }
            _ = shutdown_rx_changed(shutdown_rx.clone()) => {
                break;
            }
        };

        let stream = tokio::select! {
            result = listener.accept() => {
                match result {
                    Ok((stream, _addr)) => stream,
                    Err(err) => {
                        eprintln!("accept error: {err}");
                        drop(permit);
                        continue;
                    }
                }
            }
            _ = shutdown_rx_changed(shutdown_rx.clone()) => {
                break;
            }
        };

        let storage = Arc::clone(&storage);
        let tls_acceptor = tls_acceptor.clone();
        let auth_token = auth_token.clone();
        let active = Arc::clone(&active_connections);

        active.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

        let task_engine = Engine::with_precision(Arc::clone(&storage), precision);
        let mut shutdown_rx = shutdown_rx.clone();
        tokio::spawn(async move {
            let _permit = permit;
            let result = handle_connection(
                stream,
                &storage,
                &task_engine,
                server_start,
                precision,
                tls_acceptor.as_ref(),
                auth_token.as_deref(),
                &mut shutdown_rx,
            )
            .await;
            if let Err(err) = result {
                eprintln!("connection error: {err}");
            }
            active.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
        });
    }

    // Wait for in-flight connections to drain
    let drain_start = Instant::now();
    while active_connections.load(std::sync::atomic::Ordering::Relaxed) > 0 {
        if drain_start.elapsed() > SHUTDOWN_GRACE_PERIOD {
            eprintln!("shutdown grace period expired, closing with active connections");
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Close storage
    let storage_clone = Arc::clone(&storage);
    let _ = tokio::task::spawn_blocking(move || storage_clone.close()).await;

    Ok(())
}

async fn shutdown_rx_changed(mut rx: watch::Receiver<bool>) {
    loop {
        if *rx.borrow() {
            return;
        }
        if rx.changed().await.is_err() {
            return;
        }
        if *rx.borrow() {
            return;
        }
    }
}

async fn wait_for_shutdown_signal() -> Result<(), String> {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut sigterm =
            signal(SignalKind::terminate()).map_err(|e| format!("SIGTERM handler: {e}"))?;
        tokio::select! {
            _ = tokio::signal::ctrl_c() => Ok(()),
            _ = sigterm.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c()
            .await
            .map_err(|e| format!("ctrl_c handler: {e}"))
    }
}

fn build_tls_acceptor(config: &ServerConfig) -> Result<Option<tokio_rustls::TlsAcceptor>, String> {
    let (cert_path, key_path) = match (&config.tls_cert, &config.tls_key) {
        (Some(cert), Some(key)) => (cert, key),
        (None, None) => return Ok(None),
        (Some(_), None) => return Err("--tls-cert requires --tls-key".to_string()),
        (None, Some(_)) => return Err("--tls-key requires --tls-cert".to_string()),
    };

    let cert_file = std::fs::File::open(cert_path)
        .map_err(|e| format!("failed to open TLS cert file {}: {e}", cert_path.display()))?;
    let mut cert_reader = std::io::BufReader::new(cert_file);
    let certs: Vec<_> = rustls_pemfile::certs(&mut cert_reader)
        .collect::<Result<_, _>>()
        .map_err(|e| format!("failed to parse TLS certs: {e}"))?;

    if certs.is_empty() {
        return Err("TLS cert file contains no certificates".to_string());
    }

    let key_file = std::fs::File::open(key_path)
        .map_err(|e| format!("failed to open TLS key file {}: {e}", key_path.display()))?;
    let mut key_reader = std::io::BufReader::new(key_file);
    let key = rustls_pemfile::private_key(&mut key_reader)
        .map_err(|e| format!("failed to parse TLS key: {e}"))?
        .ok_or_else(|| "TLS key file contains no private key".to_string())?;

    let tls_config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| format!("failed to build TLS config: {e}"))?;

    Ok(Some(tokio_rustls::TlsAcceptor::from(Arc::new(tls_config))))
}

#[allow(clippy::too_many_arguments)]
async fn handle_connection(
    stream: tokio::net::TcpStream,
    storage: &Arc<dyn Storage>,
    engine: &Engine,
    server_start: Instant,
    precision: TimestampPrecision,
    tls_acceptor: Option<&tokio_rustls::TlsAcceptor>,
    auth_token: Option<&str>,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Result<(), String> {
    // Set TCP keepalive
    let sock_ref = socket2::SockRef::from(&stream);
    let keepalive = socket2::TcpKeepalive::new().with_time(Duration::from_secs(60));
    let _ = sock_ref.set_tcp_keepalive(&keepalive);

    if let Some(acceptor) = tls_acceptor {
        let tls_stream = tokio::time::timeout(HANDSHAKE_TIMEOUT, acceptor.accept(stream))
            .await
            .map_err(|_| "TLS handshake timed out".to_string())?
            .map_err(|e| format!("TLS handshake failed: {e}"))?;
        let (mut reader, mut writer) = tokio::io::split(tls_stream);
        handle_http_loop(
            &mut reader,
            &mut writer,
            storage,
            engine,
            server_start,
            precision,
            auth_token,
            shutdown_rx,
        )
        .await
    } else {
        let (mut reader, mut writer) = tokio::io::split(stream);
        handle_http_loop(
            &mut reader,
            &mut writer,
            storage,
            engine,
            server_start,
            precision,
            auth_token,
            shutdown_rx,
        )
        .await
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_http_loop<R, W>(
    reader: &mut R,
    writer: &mut W,
    storage: &Arc<dyn Storage>,
    engine: &Engine,
    server_start: Instant,
    precision: TimestampPrecision,
    auth_token: Option<&str>,
    shutdown_rx: &mut watch::Receiver<bool>,
) -> Result<(), String>
where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    loop {
        // Read request with keep-alive timeout
        let request = tokio::select! {
            result = tokio::time::timeout(KEEP_ALIVE_TIMEOUT, read_http_request(reader)) => {
                match result {
                    Ok(Ok(req)) => req,
                    Ok(Err(err)) => {
                        if err.contains("connection closed") {
                            return Ok(());
                        }
                        let response = text_response(400, &err);
                        let _ = write_http_response(writer, &response).await;
                        return Ok(());
                    }
                    Err(_) => {
                        // Keep-alive timeout; close connection silently
                        return Ok(());
                    }
                }
            }
            _ = shutdown_rx.changed() => {
                return Ok(());
            }
        };

        // Check auth
        if let Some(token) = auth_token {
            let path = request.path_without_query();
            let skip_auth = path == "/healthz" || path == "/ready";
            if !skip_auth {
                let authorized = request
                    .header("authorization")
                    .is_some_and(|val| val.strip_prefix("Bearer ") == Some(token));
                if !authorized {
                    let response = HttpResponse::new(401, "unauthorized")
                        .with_header("Content-Type", "text/plain")
                        .with_header("WWW-Authenticate", "Bearer");
                    write_http_response(writer, &response).await?;
                    return Ok(());
                }
            }
        }

        // Check for Connection: close
        let close = request
            .header("connection")
            .is_some_and(|v| v.eq_ignore_ascii_case("close"));

        let response =
            handlers::handle_request(storage, engine, request, server_start, precision).await;

        let response = if close {
            response.with_header("Connection", "close")
        } else {
            response.with_header("Connection", "keep-alive")
        };

        write_http_response(writer, &response).await?;

        if close {
            return Ok(());
        }
    }
}

fn build_storage(config: &ServerConfig) -> tsink::Result<Arc<dyn Storage>> {
    let mut builder = StorageBuilder::new()
        .with_wal_enabled(config.wal_enabled)
        .with_timestamp_precision(config.timestamp_precision);

    if let Some(path) = &config.data_path {
        builder = builder.with_data_path(path);
    }
    if let Some(retention) = config.retention {
        builder = builder.with_retention(retention);
    }
    if let Some(limit) = config.memory_limit {
        builder = builder.with_memory_limit(limit);
    }
    if let Some(limit) = config.cardinality_limit {
        builder = builder.with_cardinality_limit(limit);
    }
    if let Some(points) = config.chunk_points {
        builder = builder.with_chunk_points(points);
    }
    if let Some(writers) = config.max_writers {
        builder = builder.with_max_writers(writers);
    }
    if let Some(mode) = config.wal_sync_mode {
        builder = builder.with_wal_sync_mode(mode);
    }

    builder.build()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tsink::StorageBuilder;

    fn make_storage() -> Arc<dyn Storage> {
        StorageBuilder::new()
            .with_timestamp_precision(TimestampPrecision::Milliseconds)
            .build()
            .expect("storage should build")
    }

    async fn send_request(raw_request: &[u8], auth_token: Option<&str>) -> Vec<u8> {
        let storage = make_storage();
        let engine = Engine::with_precision(Arc::clone(&storage), TimestampPrecision::Milliseconds);
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let auth_token_owned = auth_token.map(String::from);

        let (client, server) = tokio::io::duplex(64 * 1024);

        // Spawn server handler
        let server_handle = tokio::spawn(async move {
            let (mut reader, mut writer) = tokio::io::split(server);
            let mut shutdown_rx = shutdown_rx;
            let _ = handle_http_loop(
                &mut reader,
                &mut writer,
                &storage,
                &engine,
                Instant::now(),
                TimestampPrecision::Milliseconds,
                auth_token_owned.as_deref(),
                &mut shutdown_rx,
            )
            .await;
        });

        let (mut client_read, mut client_write) = tokio::io::split(client);

        // Write request from client side
        client_write.write_all(raw_request).await.unwrap();
        drop(client_write);

        // Read response
        let mut response = Vec::new();
        client_read.read_to_end(&mut response).await.unwrap();

        let _ = server_handle.await;
        response
    }

    fn extract_status_code(response: &[u8]) -> u16 {
        let text = std::str::from_utf8(response).unwrap_or("");
        text.split_whitespace()
            .nth(1)
            .and_then(|s| s.parse().ok())
            .unwrap_or(0)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn auth_token_required_when_configured() {
        // Request without auth header should get 401
        let response = send_request(
            b"GET /api/v1/labels HTTP/1.1\r\nConnection: close\r\n\r\n",
            Some("secret-token"),
        )
        .await;
        assert_eq!(extract_status_code(&response), 401);
        let text = String::from_utf8_lossy(&response);
        assert!(text.contains("WWW-Authenticate: Bearer"));

        // Request with correct auth should succeed
        let response = send_request(
            b"GET /api/v1/labels HTTP/1.1\r\nAuthorization: Bearer secret-token\r\nConnection: close\r\n\r\n",
            Some("secret-token"),
        )
        .await;
        assert_eq!(extract_status_code(&response), 200);

        // Request with wrong token should get 401
        let response = send_request(
            b"GET /api/v1/labels HTTP/1.1\r\nAuthorization: Bearer wrong-token\r\nConnection: close\r\n\r\n",
            Some("secret-token"),
        )
        .await;
        assert_eq!(extract_status_code(&response), 401);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn auth_skipped_for_health_probes() {
        // /healthz should work without auth
        let response = send_request(
            b"GET /healthz HTTP/1.1\r\nConnection: close\r\n\r\n",
            Some("secret-token"),
        )
        .await;
        assert_eq!(extract_status_code(&response), 200);

        // /ready should work without auth
        let response = send_request(
            b"GET /ready HTTP/1.1\r\nConnection: close\r\n\r\n",
            Some("secret-token"),
        )
        .await;
        assert_eq!(extract_status_code(&response), 200);
    }
}
