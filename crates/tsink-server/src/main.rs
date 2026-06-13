mod handlers;
mod http;
mod prom_remote;
mod server;

use std::env;
use std::path::PathBuf;
use std::time::Duration;
use tsink::{TimestampPrecision, WalSyncMode};

use crate::server::{run_server, ServerConfig};

#[tokio::main]
async fn main() {
    if let Err(err) = run().await {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

async fn run() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let first = args.next();

    if matches!(first.as_deref(), Some("-h" | "--help" | "help")) {
        print_usage();
        return Ok(());
    }

    if matches!(first.as_deref(), Some("-V" | "--version")) {
        println!("tsink-server {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    let mode = first.unwrap_or_else(|| "server".to_string());
    if mode != "server" {
        return Err(format!("unknown mode '{mode}'. Run with --help for usage."));
    }

    let config = parse_server_args(args)?;
    run_server(config).await
}

fn parse_server_args<I>(mut args: I) -> Result<ServerConfig, String>
where
    I: Iterator<Item = String>,
{
    let mut config = ServerConfig::default();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--listen requires an address".to_string())?;
                config.listen = value;
            }
            "--data-path" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--data-path requires a filesystem path".to_string())?;
                config.data_path = Some(PathBuf::from(value));
            }
            "--wal-enabled" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--wal-enabled requires true or false".to_string())?;
                config.wal_enabled = parse_bool(&value)?;
            }
            "--no-wal" => {
                config.wal_enabled = false;
            }
            "--timestamp-precision" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--timestamp-precision requires a value".to_string())?;
                config.timestamp_precision = parse_timestamp_precision(&value)?;
            }
            "--retention" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--retention requires a duration".to_string())?;
                config.retention = Some(parse_duration(&value)?);
            }
            "--memory-limit" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--memory-limit requires a byte size".to_string())?;
                config.memory_limit = Some(parse_byte_size(&value)?);
            }
            "--cardinality-limit" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--cardinality-limit requires a number".to_string())?;
                config.cardinality_limit = Some(
                    value
                        .parse::<usize>()
                        .map_err(|_| format!("invalid cardinality limit: '{value}'"))?,
                );
            }
            "--chunk-points" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--chunk-points requires a number".to_string())?;
                config.chunk_points = Some(
                    value
                        .parse::<usize>()
                        .map_err(|_| format!("invalid chunk-points value: '{value}'"))?,
                );
            }
            "--max-writers" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--max-writers requires a number".to_string())?;
                config.max_writers = Some(
                    value
                        .parse::<usize>()
                        .map_err(|_| format!("invalid max-writers value: '{value}'"))?,
                );
            }
            "--wal-sync-mode" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--wal-sync-mode requires a value".to_string())?;
                config.wal_sync_mode = Some(parse_wal_sync_mode(&value)?);
            }
            "--tls-cert" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--tls-cert requires a file path".to_string())?;
                config.tls_cert = Some(PathBuf::from(value));
            }
            "--tls-key" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--tls-key requires a file path".to_string())?;
                config.tls_key = Some(PathBuf::from(value));
            }
            "--auth-token" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--auth-token requires a token value".to_string())?;
                config.auth_token = Some(value);
            }
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            other => return Err(format!("unknown argument '{other}'")),
        }
    }

    Ok(config)
}

fn parse_bool(value: &str) -> Result<bool, String> {
    match value {
        "true" | "1" | "yes" | "on" => Ok(true),
        "false" | "0" | "no" | "off" => Ok(false),
        _ => Err(format!("invalid boolean value '{value}'")),
    }
}

fn parse_timestamp_precision(value: &str) -> Result<TimestampPrecision, String> {
    match value.to_ascii_lowercase().as_str() {
        "seconds" | "s" => Ok(TimestampPrecision::Seconds),
        "milliseconds" | "ms" => Ok(TimestampPrecision::Milliseconds),
        "microseconds" | "us" => Ok(TimestampPrecision::Microseconds),
        "nanoseconds" | "ns" => Ok(TimestampPrecision::Nanoseconds),
        _ => Err(format!(
            "invalid timestamp precision '{value}', expected one of: s, ms, us, ns"
        )),
    }
}

fn parse_duration(value: &str) -> Result<Duration, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("empty duration value".to_string());
    }
    if let Ok(secs) = value.parse::<u64>() {
        return Ok(Duration::from_secs(secs));
    }

    let (num_str, unit) = if let Some(stripped) = value.strip_suffix("ms") {
        (stripped, "ms")
    } else if value.len() > 1 {
        (&value[..value.len() - 1], &value[value.len() - 1..])
    } else {
        return Err(format!("invalid duration: '{value}'"));
    };

    let num: f64 = num_str
        .parse()
        .map_err(|_| format!("invalid duration: '{value}'"))?;

    let secs = match unit {
        "ms" => num / 1_000.0,
        "s" => num,
        "m" => num * 60.0,
        "h" => num * 3_600.0,
        "d" => num * 86_400.0,
        "w" => num * 604_800.0,
        "y" => num * 365.25 * 86_400.0,
        _ => return Err(format!("invalid duration unit: '{unit}'")),
    };

    if secs < 0.0 {
        return Err(format!("duration must be non-negative: '{value}'"));
    }

    Ok(Duration::from_secs_f64(secs))
}

fn parse_byte_size(value: &str) -> Result<usize, String> {
    let value = value.trim();
    if value.is_empty() {
        return Err("empty byte size value".to_string());
    }
    if let Ok(bytes) = value.parse::<usize>() {
        return Ok(bytes);
    }

    let last = value.as_bytes()[value.len() - 1];
    let (num_str, multiplier) = match last {
        b'K' | b'k' => (&value[..value.len() - 1], 1_024_usize),
        b'M' | b'm' => (&value[..value.len() - 1], 1_024 * 1_024),
        b'G' | b'g' => (&value[..value.len() - 1], 1_024 * 1_024 * 1_024),
        b'T' | b't' => (&value[..value.len() - 1], 1_024 * 1_024 * 1_024 * 1_024),
        _ => return Err(format!("invalid byte size: '{value}'")),
    };

    let num: f64 = num_str
        .parse()
        .map_err(|_| format!("invalid byte size: '{value}'"))?;

    Ok((num * multiplier as f64) as usize)
}

fn parse_wal_sync_mode(value: &str) -> Result<WalSyncMode, String> {
    match value.to_ascii_lowercase().as_str() {
        "per-append" | "per_append" | "perappend" => Ok(WalSyncMode::PerAppend),
        "periodic" => Ok(WalSyncMode::Periodic(Duration::from_secs(1))),
        _ => Err(format!(
            "invalid WAL sync mode '{value}', expected 'per-append' or 'periodic'"
        )),
    }
}

fn print_usage() {
    println!(
        "Usage:
  tsink-server server [OPTIONS]

General:
  -V, --version                     Print version and exit

Server options:
  --listen <ADDR>                   Bind address (default: 127.0.0.1:9201)
  --data-path <PATH>                Persist tsink data under PATH
  --wal-enabled <BOOL>              Enable WAL (default: true)
  --no-wal                          Disable WAL
  --timestamp-precision <s|ms|us|ns>
  --retention <DURATION>            Data retention period (e.g. 14d, 720h)
  --memory-limit <BYTES>            Memory budget (e.g. 1G, 1073741824)
  --cardinality-limit <N>           Max unique series
  --chunk-points <N>                Target points per chunk
  --max-writers <N>                 Concurrent writer threads
  --wal-sync-mode <MODE>            WAL fsync policy (per-append, periodic)
  --tls-cert <PATH>                 TLS certificate file (PEM)
  --tls-key <PATH>                  TLS private key file (PEM)
  --auth-token <TOKEN>              Require Bearer token on requests

Endpoints:
  GET  /healthz
  GET  /ready
  GET  /metrics
  GET/POST /api/v1/query            PromQL instant query
  GET/POST /api/v1/query_range      PromQL range query
  GET  /api/v1/series               Series metadata
  GET  /api/v1/labels               Label names
  GET  /api/v1/label/<name>/values  Label values
  POST /api/v1/write                Prometheus remote write
  POST /api/v1/read                 Prometheus remote read
  POST /api/v1/import/prometheus    Text exposition format ingestion
  GET  /api/v1/status/tsdb          TSDB stats
  POST /api/v1/admin/snapshot       Create atomic snapshot
  POST /api/v1/admin/restore        Restore snapshot to data path
  POST /api/v1/admin/delete_series  Delete series (stub)"
    );
}
