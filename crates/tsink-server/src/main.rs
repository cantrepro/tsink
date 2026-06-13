mod prom_remote;
mod server;

use std::env;
use std::path::PathBuf;
use tsink::TimestampPrecision;

use crate::server::{run_server, ServerConfig};

fn main() {
    if let Err(err) = run() {
        eprintln!("{err}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut args = env::args().skip(1);
    let first = args.next();

    if matches!(first.as_deref(), Some("-h" | "--help" | "help")) {
        print_usage();
        return Ok(());
    }

    let mode = first.unwrap_or_else(|| "server".to_string());
    if mode != "server" {
        return Err(format!("unknown mode '{mode}'. Run with --help for usage."));
    }

    let config = parse_server_args(args)?;
    run_server(config)
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

fn print_usage() {
    println!(
        "Usage:
  tsink-server server [OPTIONS]

Server options:
  --listen <ADDR>                   Bind address (default: 127.0.0.1:9201)
  --data-path <PATH>                Persist tsink data under PATH
  --wal-enabled <BOOL>              Enable WAL (default: true)
  --no-wal                          Disable WAL
  --timestamp-precision <s|ms|us|ns>

Endpoints:
  GET  /healthz
  POST /api/v1/write   (Prometheus remote write wire format)
  POST /api/v1/read    (Prometheus remote read wire format)"
    );
}
