//! External fixture driver for the historical tsink 0.10.1 writer.
//!
//! This source is intentionally not a Cargo target in the current tree. To reproduce the fixture,
//! export the exact historical commit recorded in the fixture provenance, copy this file into that
//! export's `examples/` directory, and compile it there. The package-version assertion prevents
//! accidentally using the current writer.

use std::error::Error;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};

use tsink::{
    DataPoint, HistogramBucketSpan, HistogramCount, HistogramResetHint, Label, NativeHistogram,
    Row, SeriesSelection, Storage, StorageBuilder, TimestampPrecision, Value, WalSyncMode,
    WriteAcknowledgement,
};
use xxhash_rust::xxh64::xxh64;

const HISTORICAL_PACKAGE_VERSION: &str = "0.10.1";
const HISTORICAL_SOURCE_REVISION: &str = "00cc627df7b36ae1838f68da273c42949f0a5d52";
const FIXTURE_GENERATED_DATE: &str = "2026-07-26";
const CONTENT_HASH_SEED: u64 = 0;
const CHILD_MODE_ENV: &str = "TSINK_0_10_1_FIXTURE_WAL_CHILD";
const CHILD_DATA_PATH_ENV: &str = "TSINK_0_10_1_FIXTURE_DATA_PATH";
const CHILD_READY_RECORD: &str = "TSINK_0_10_1_FIXTURE_WAL_DURABLE";
const FIXTURE_ID: &str = "storage-format-v2-tsink-0.10.1";
const CHUNK_POINTS: usize = 8;

const NUMERIC_METRIC: &str = "historical_v2_numeric";
const BLOB_METRIC: &str = "historical_v2_blob";
const HISTOGRAM_METRIC: &str = "historical_v2_histogram";
const RETAINED_METRIC: &str = "historical_v2_retained";
const TOMBSTONED_METRIC: &str = "historical_v2_tombstoned";
const WAL_RECOVERED_METRIC: &str = "historical_v2_wal_recovered";

fn fixture_labels(kind: &str) -> Vec<Label> {
    vec![
        Label::new("fixture", "tsink_0_10_1"),
        Label::new("kind", kind),
    ]
}

fn fixture_histogram() -> NativeHistogram {
    NativeHistogram {
        count: Some(HistogramCount::Int(84)),
        sum: 35.25,
        schema: 1,
        zero_threshold: 0.001,
        zero_count: Some(HistogramCount::Int(9)),
        negative_spans: vec![HistogramBucketSpan {
            offset: -3,
            length: 2,
        }],
        negative_deltas: vec![4, -1],
        negative_counts: vec![],
        positive_spans: vec![HistogramBucketSpan {
            offset: 1,
            length: 2,
        }],
        positive_deltas: vec![5, 2],
        positive_counts: vec![],
        reset_hint: HistogramResetHint::No,
        custom_values: vec![0.125, 0.75],
    }
}

fn open_fixture_storage(data_path: &Path) -> Result<std::sync::Arc<dyn Storage>, Box<dyn Error>> {
    Ok(StorageBuilder::new()
        .with_data_path(data_path)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(CHUNK_POINTS)
        .with_retention_enforced(false)
        .with_wal_sync_mode(WalSyncMode::PerAppend)
        .build()?)
}

fn write_clean_baseline(data_path: &Path) -> Result<(), Box<dyn Error>> {
    let storage = open_fixture_storage(data_path)?;
    let result = storage.insert_rows_with_result(&[
        Row::with_labels(
            NUMERIC_METRIC,
            fixture_labels("numeric"),
            DataPoint::new(200, -4.25),
        ),
        Row::with_labels(
            NUMERIC_METRIC,
            fixture_labels("numeric"),
            DataPoint::new(201, 18.75),
        ),
        Row::with_labels(
            BLOB_METRIC,
            fixture_labels("blob"),
            DataPoint::new(210, Value::Bytes(b"historical-0.10.1\0blob".to_vec())),
        ),
        Row::with_labels(
            HISTOGRAM_METRIC,
            fixture_labels("histogram"),
            DataPoint::new(220, fixture_histogram()),
        ),
        Row::with_labels(
            RETAINED_METRIC,
            fixture_labels("retained"),
            DataPoint::new(230, Value::U64(10_001)),
        ),
        Row::with_labels(
            TOMBSTONED_METRIC,
            fixture_labels("tombstoned"),
            DataPoint::new(240, 101.5),
        ),
    ])?;
    if result.acknowledgement != WriteAcknowledgement::Durable {
        return Err(format!(
            "historical baseline write returned {:?}, expected Durable",
            result.acknowledgement
        )
        .into());
    }
    storage.close()?;

    let storage = open_fixture_storage(data_path)?;
    let deletion = storage.delete_series(
        &SeriesSelection::new()
            .with_metric(TOMBSTONED_METRIC)
            .with_time_range(0, 1_000),
    )?;
    if deletion.matched_series != 1 || deletion.tombstones_applied != 1 {
        return Err(format!(
            "historical fixture tombstone expected one matched/applied series, got {deletion:?}"
        )
        .into());
    }
    storage.close()?;
    Ok(())
}

fn run_wal_suffix_child(data_path: &Path) -> Result<(), Box<dyn Error>> {
    let storage = open_fixture_storage(data_path)?;
    let result = storage.insert_rows_with_result(&[Row::with_labels(
        WAL_RECOVERED_METRIC,
        fixture_labels("wal_recovered"),
        DataPoint::new(250, Value::String("historical-durable-wal".to_string())),
    )])?;
    if result.acknowledgement != WriteAcknowledgement::Durable {
        return Err(format!(
            "historical WAL suffix returned {:?}, expected Durable",
            result.acknowledgement
        )
        .into());
    }
    println!("{CHILD_READY_RECORD}");
    std::io::stdout().flush()?;

    // The parent terminates this process so the final record remains a genuine WAL-recovery case.
    loop {
        std::thread::park();
        std::hint::black_box(&storage);
    }
}

struct KillAndReapChild {
    child: Child,
    reaped: bool,
}

impl KillAndReapChild {
    fn new(child: Child) -> Self {
        Self {
            child,
            reaped: false,
        }
    }

    fn kill_and_reap(&mut self) -> Result<std::process::ExitStatus, Box<dyn Error>> {
        self.child.kill()?;
        let status = self.child.wait()?;
        self.reaped = true;
        Ok(status)
    }
}

impl Drop for KillAndReapChild {
    fn drop(&mut self) {
        if self.reaped {
            return;
        }
        let _ = self.child.kill();
        if self.child.wait().is_ok() {
            self.reaped = true;
        }
    }
}

fn write_wal_only_suffix(data_path: &Path) -> Result<(), Box<dyn Error>> {
    let current_exe = std::env::current_exe()?;
    let mut child = KillAndReapChild::new(
        Command::new(current_exe)
            .env(CHILD_MODE_ENV, "1")
            .env(CHILD_DATA_PATH_ENV, data_path)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()?,
    );
    let stdout = child
        .child
        .stdout
        .take()
        .ok_or("historical fixture WAL child stdout was not piped")?;
    let mut reader = BufReader::new(stdout);
    let mut ready = String::new();
    if reader.read_line(&mut ready)? == 0 || ready.trim() != CHILD_READY_RECORD {
        return Err(
            format!("historical WAL child did not report a durable suffix: {ready:?}").into(),
        );
    }
    let status = child.kill_and_reap()?;
    if status.success() {
        return Err("historical fixture WAL child exited normally instead of being killed".into());
    }
    Ok(())
}

fn normalized_relative_path(root: &Path, path: &Path) -> Result<String, Box<dyn Error>> {
    let relative = path.strip_prefix(root)?;
    let mut normalized = String::new();
    for component in relative.components() {
        let component = component
            .as_os_str()
            .to_str()
            .ok_or("fixture path is not valid UTF-8")?;
        if !normalized.is_empty() {
            normalized.push('/');
        }
        normalized.push_str(component);
    }
    Ok(normalized)
}

fn collect_data_files(root: &Path) -> Result<Vec<(String, PathBuf)>, Box<dyn Error>> {
    fn visit(
        root: &Path,
        directory: &Path,
        files: &mut Vec<(String, PathBuf)>,
    ) -> Result<(), Box<dyn Error>> {
        for entry in fs::read_dir(directory)? {
            let entry = entry?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.file_type().is_symlink() {
                return Err(format!("fixture generator refuses symlink {}", path.display()).into());
            }
            if metadata.file_type().is_dir() {
                visit(root, &path, files)?;
            } else if metadata.file_type().is_file() {
                files.push((normalized_relative_path(root, &path)?, path));
            } else {
                return Err(
                    format!("fixture generator refuses special entry {}", path.display()).into(),
                );
            }
        }
        Ok(())
    }

    let mut files = Vec::new();
    visit(root, root, &mut files)?;
    files.sort_by(|left, right| left.0.cmp(&right.0));
    Ok(files)
}

fn write_content_hashes(fixture_root: &Path, data_path: &Path) -> Result<(), Box<dyn Error>> {
    let mut hashes = String::from("# xxh64(seed=0) content_hash  size_bytes  data_relative_path\n");
    for (relative, path) in collect_data_files(data_path)? {
        let bytes = fs::read(path)?;
        let hash = xxh64(&bytes, CONTENT_HASH_SEED);
        hashes.push_str(&format!("{hash:016x}  {}  {relative}\n", bytes.len()));
    }
    fs::write(fixture_root.join("CONTENT_HASHES.txt"), hashes)?;
    Ok(())
}

fn write_provenance(fixture_root: &Path) -> Result<(), Box<dyn Error>> {
    let provenance = format!(
        "# `{FIXTURE_ID}` provenance\n\n\
         - Historical writer: tsink package `{HISTORICAL_PACKAGE_VERSION}` built from local Git \
         commit `{HISTORICAL_SOURCE_REVISION}` (commit subject: `release version 0.10.1`).\n\
         - Source isolation: `git archive {HISTORICAL_SOURCE_REVISION}` was extracted beneath \
         `/tmp`; the export contained no `.git` directory and the repository worktree was not \
         switched or rewritten.\n\
         - Fixture driver: \
         `tests/fixture-generators/generate_storage_format_v2_tsink_0_10_1.rs` was copied into the \
         historical export's `examples/` directory and compiled against that export. The driver \
         rejects every package version other than `{HISTORICAL_PACKAGE_VERSION}`.\n\
         - Logical storage format: `2` (`framed_segment_v2`, `framed_wal_v2`, \
         `registry_snapshot_v2`, and the blob value lane).\n\
         - Data-directory manifest: absent because the historical release predates \
         `tsink-manifest.json`; no file was removed from the generated data directory.\n\
         - Generated: `{FIXTURE_GENERATED_DATE}` on `aarch64-apple-darwin` with \
         `cargo run --offline --locked`.\n\
         - Retention metadata: not applicable. Storage format 2 does not persist the builder's \
         runtime retention policy. Generation disables retention and includes a named retained \
         sample that proves ordinary non-tombstoned data remains visible.\n\
         - WAL recovery: after cleanly closing the segment/tombstone baseline, the driver starts \
         a child, waits for its `PerAppend` write to return `Durable`, flushes a readiness record, \
         and abruptly kills the child while storage remains live.\n\
         - Lock file: the historical writer's zero-byte `.tsink.lock` file is intentionally \
         retained and covered by the inventory.\n\
         - Byte reproducibility: not promised. Segment creation timestamps and concurrent series \
         registration can change equivalent bytes. `CONTENT_HASHES.txt` freezes this reviewed \
         instance and is never updated automatically by tests.\n\n\
         Reproduction is manual and destructive regeneration is forbidden:\n\n\
         ```console\n\
         export_dir=$(mktemp -d /tmp/tsink-0.10.1-fixture.XXXXXX)\n\
         mkdir \"$export_dir/source\"\n\
         git archive {HISTORICAL_SOURCE_REVISION} | tar -x -C \"$export_dir/source\"\n\
         mkdir \"$export_dir/source/examples\"\n\
         cp tests/fixture-generators/generate_storage_format_v2_tsink_0_10_1.rs \\\n\
           \"$export_dir/source/examples/\"\n\
         CARGO_TARGET_DIR=\"$export_dir/target\" cargo run --offline --locked \\\n\
           --manifest-path \"$export_dir/source/Cargo.toml\" -p tsink \\\n\
           --example generate_storage_format_v2_tsink_0_10_1 -- \\\n\
           \"$export_dir/{FIXTURE_ID}\"\n\
         ```\n\n\
         Tests never invoke this driver. A successor must use a new fixture ID and directory; \
         never replace these frozen bytes in place.\n"
    );
    fs::write(fixture_root.join("PROVENANCE.md"), provenance)?;
    Ok(())
}

fn generate_fixture(fixture_root: &Path) -> Result<(), Box<dyn Error>> {
    if env!("CARGO_PKG_VERSION") != HISTORICAL_PACKAGE_VERSION {
        return Err(format!(
            "this driver requires tsink package version {HISTORICAL_PACKAGE_VERSION}, got {}",
            env!("CARGO_PKG_VERSION")
        )
        .into());
    }
    if fixture_root.file_name().and_then(|name| name.to_str()) != Some(FIXTURE_ID) {
        return Err(format!("fixture output directory must be named {FIXTURE_ID}").into());
    }
    if fixture_root.exists() {
        return Err(format!(
            "refusing to overwrite existing fixture {}",
            fixture_root.display()
        )
        .into());
    }
    std::fs::create_dir_all(fixture_root)?;
    let data_path = fixture_root.join("data");
    write_clean_baseline(&data_path)?;
    write_wal_only_suffix(&data_path)?;
    write_content_hashes(fixture_root, &data_path)?;
    write_provenance(fixture_root)?;
    println!("generated historical fixture {}", fixture_root.display());
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    if std::env::var_os(CHILD_MODE_ENV).is_some() {
        let data_path = PathBuf::from(
            std::env::var_os(CHILD_DATA_PATH_ENV)
                .ok_or("historical fixture child data path is missing")?,
        );
        return run_wal_suffix_child(&data_path);
    }

    let mut args = std::env::args_os();
    let program = args.next().unwrap_or_default();
    let fixture_root = args.next().ok_or_else(|| {
        format!(
            "usage: {} <new-versioned-fixture-directory>",
            Path::new(&program).display()
        )
    })?;
    if args.next().is_some() {
        return Err("historical fixture generator accepts exactly one output directory".into());
    }
    generate_fixture(Path::new(&fixture_root))
}
