use std::path::PathBuf;
use std::process::ExitCode;
use std::{io, io::Write};

use clap::{error::ErrorKind, Args, Parser, Subcommand};
use serde_json::json;
use tsink::inspection::{
    inspect_data_directory, salvage_data_directory, DataDirectoryInspectionLimits,
    DataDirectorySalvageLimits, InspectionHealth,
};

#[derive(Debug, Parser)]
#[command(
    name = "tsink-inspect",
    about = "Strict bounded inspection and explicit destination-only salvage for tsink data directories"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Inspect a source directory without opening storage or creating its process lock.
    Inspect(InspectArgs),
    /// Create a safe destination with an empty WAL after explicitly reporting all discarded WAL.
    Salvage(SalvageArgs),
}

#[derive(Debug, Args)]
struct InspectArgs {
    /// Source data directory. A missing path is reported as empty and is not created.
    source: PathBuf,

    #[command(flatten)]
    limits: InspectionLimitArgs,
}

#[derive(Debug, Args)]
struct SalvageArgs {
    /// Source data directory. It is never opened for writing.
    source: PathBuf,
    /// New destination directory. It must not already exist.
    destination: PathBuf,

    #[command(flatten)]
    limits: InspectionLimitArgs,

    /// Maximum staging entries created, including the staging root and rewritten WAL marker.
    #[arg(long, default_value_t = 100_000)]
    max_copy_entries: u64,
    /// Maximum regular-file bytes copied or generated in staging.
    #[arg(long, default_value_t = 8 * 1024 * 1024 * 1024)]
    max_copy_bytes: u64,
    /// Attest that every writer is stopped and the source cannot change during salvage.
    #[arg(long)]
    source_is_offline_and_immutable: bool,
}

#[derive(Debug, Args)]
struct InspectionLimitArgs {
    #[arg(long, default_value_t = 100_000)]
    max_namespace_entries: u64,
    #[arg(long, default_value_t = 16)]
    max_namespace_depth: u16,
    #[arg(long, default_value_t = 128 * 1024 * 1024)]
    max_namespace_retained_bytes: u64,
    #[arg(long, default_value_t = 8 * 1024 * 1024 * 1024)]
    max_bytes_read: u64,
    #[arg(long, default_value_t = 8 * 1024 * 1024 * 1024)]
    max_bytes_hashed: u64,
    #[arg(long, default_value_t = 100_000)]
    max_report_items: u64,
    #[arg(long, default_value_t = 16 * 1024 * 1024)]
    max_retained_path_bytes: u64,
    #[arg(long, default_value_t = 4 * 1024)]
    max_path_bytes: u32,
    #[arg(long, default_value_t = 10_000)]
    max_issues: u64,
    #[arg(long, default_value_t = 10_000_000)]
    max_wal_frames: u64,
    #[arg(long, default_value_t = 256 * 1024 * 1024)]
    max_wal_decoded_bytes: u64,
    #[arg(long, default_value_t = 100_000)]
    max_wal_segments: u64,
    #[arg(long, default_value_t = 100_000)]
    max_segments: u64,
    #[arg(long, default_value_t = 64 * 1024 * 1024)]
    max_catalog_bytes: u64,
    #[arg(long, default_value_t = 256 * 1024 * 1024)]
    max_registry_bytes: u64,
}

impl InspectionLimitArgs {
    fn limits(&self) -> DataDirectoryInspectionLimits {
        DataDirectoryInspectionLimits {
            max_namespace_entries: self.max_namespace_entries,
            max_namespace_depth: self.max_namespace_depth,
            max_namespace_retained_bytes: self.max_namespace_retained_bytes,
            max_bytes_read: self.max_bytes_read,
            max_bytes_hashed: self.max_bytes_hashed,
            max_report_items: self.max_report_items,
            max_retained_path_bytes: self.max_retained_path_bytes,
            max_path_bytes: self.max_path_bytes,
            max_issues: self.max_issues,
            max_wal_frames: self.max_wal_frames,
            max_wal_decoded_bytes: self.max_wal_decoded_bytes,
            max_wal_segments: self.max_wal_segments,
            max_segments: self.max_segments,
            max_catalog_bytes: self.max_catalog_bytes,
            max_registry_bytes: self.max_registry_bytes,
        }
    }
}

fn main() -> ExitCode {
    let cli = match Cli::try_parse() {
        Ok(cli) => cli,
        Err(err)
            if matches!(
                err.kind(),
                ErrorKind::DisplayHelp | ErrorKind::DisplayVersion
            ) =>
        {
            return if err.print().is_ok() {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(2)
            };
        }
        Err(err) => {
            let _ = print_error("cli.invalid_arguments", &err);
            return ExitCode::from(2);
        }
    };
    match cli.command {
        Command::Inspect(args) => run_inspect(args),
        Command::Salvage(args) => run_salvage(args),
    }
}

fn run_inspect(args: InspectArgs) -> ExitCode {
    match inspect_data_directory(&args.source, args.limits.limits()) {
        Ok(report) => {
            if let Err(err) = print_json(&report) {
                let _ = print_error("inspection.report_output_failed", &err);
                return ExitCode::from(2);
            }
            if report.health == InspectionHealth::Clean {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(2)
            }
        }
        Err(err) => {
            let _ = print_error("inspection.failed", &err);
            ExitCode::from(2)
        }
    }
}

fn run_salvage(args: SalvageArgs) -> ExitCode {
    let limits = DataDirectorySalvageLimits {
        inspection: args.limits.limits(),
        max_copy_entries: args.max_copy_entries,
        max_copy_bytes: args.max_copy_bytes,
        source_is_offline_and_immutable: args.source_is_offline_and_immutable,
    };
    match salvage_data_directory(&args.source, &args.destination, limits) {
        Ok(report) => match print_json(&report) {
            Ok(()) => ExitCode::SUCCESS,
            Err(err) => {
                let _ = print_error("salvage.report_output_failed", &err);
                ExitCode::from(2)
            }
        },
        Err(err) => {
            let _ = print_error("salvage.failed", &err);
            ExitCode::from(2)
        }
    }
}

fn print_json(value: &impl serde::Serialize) -> Result<(), String> {
    let stdout = io::stdout();
    let mut output = stdout.lock();
    serde_json::to_writer_pretty(&mut output, value)
        .map_err(|err| format!("JSON encoding or output failed: {err}"))?;
    output
        .write_all(b"\n")
        .and_then(|_| output.flush())
        .map_err(|err| format!("JSON output failed: {err}"))
}

fn print_error(code: &str, error: &impl std::fmt::Display) -> Result<(), String> {
    print_json(&json!({
        "report_schema_version": 1,
        "error_code": code,
        "message": error.to_string(),
    }))
}
