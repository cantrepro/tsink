//! Manually generates the frozen pre-manifest storage-format-v2 compatibility fixture.
//!
//! This executable is intentionally not invoked by any test or build script. Run it only when
//! deliberately creating a new fixture version, review every generated file and hash, and never
//! overwrite an existing historical fixture in place. Its `FIXTURE_ID` and data contract are
//! versioned together; a successor must update both deliberately.

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

const CHILD_MODE_ENV: &str = "TSINK_V2_FIXTURE_WAL_CHILD";
const CHILD_DATA_PATH_ENV: &str = "TSINK_V2_FIXTURE_DATA_PATH";
const CHILD_READY_RECORD: &str = "TSINK_V2_FIXTURE_WAL_DURABLE";
const DATA_DIRECTORY_MANIFEST_FILE_NAME: &str = "tsink-manifest.json";
const FIXTURE_ID: &str = "storage-format-v2-pre-manifest-v1";
const FIXTURE_GENERATED_DATE: &str = "2026-07-26";
const CONTENT_HASH_SEED: u64 = 0;
const CHUNK_POINTS: usize = 8;

const NUMERIC_METRIC: &str = "fixture_v2_numeric";
const BLOB_METRIC: &str = "fixture_v2_blob";
const HISTOGRAM_METRIC: &str = "fixture_v2_histogram";
// Pre-manifest format v2 does not persist the runtime retention policy. This sample records the
// retention-side expectation that an un-tombstoned value remains visible; it is not policy
// metadata.
const RETAINED_METRIC: &str = "fixture_v2_retained";
const TOMBSTONED_METRIC: &str = "fixture_v2_tombstoned";
const WAL_RECOVERED_METRIC: &str = "fixture_v2_wal_recovered";

fn fixture_labels(kind: &str) -> Vec<Label> {
    vec![
        Label::new("fixture", "storage_format_v2"),
        Label::new("kind", kind),
    ]
}

fn fixture_histogram() -> NativeHistogram {
    NativeHistogram {
        count: Some(HistogramCount::Int(42)),
        sum: 17.5,
        schema: 1,
        zero_threshold: 0.001,
        zero_count: Some(HistogramCount::Int(7)),
        negative_spans: vec![HistogramBucketSpan {
            offset: -2,
            length: 1,
        }],
        negative_deltas: vec![2],
        negative_counts: vec![],
        positive_spans: vec![HistogramBucketSpan {
            offset: 0,
            length: 2,
        }],
        positive_deltas: vec![3, 1],
        positive_counts: vec![],
        reset_hint: HistogramResetHint::No,
        custom_values: vec![0.25, 0.5],
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
            DataPoint::new(100, 12.5),
        ),
        Row::with_labels(
            NUMERIC_METRIC,
            fixture_labels("numeric"),
            DataPoint::new(101, 13.5),
        ),
        Row::with_labels(
            BLOB_METRIC,
            fixture_labels("blob"),
            DataPoint::new(110, Value::Bytes(b"legacy-blob\0v2".to_vec())),
        ),
        Row::with_labels(
            HISTOGRAM_METRIC,
            fixture_labels("histogram"),
            DataPoint::new(120, fixture_histogram()),
        ),
        Row::with_labels(
            RETAINED_METRIC,
            fixture_labels("retained"),
            DataPoint::new(130, Value::I64(-17)),
        ),
        Row::with_labels(
            TOMBSTONED_METRIC,
            fixture_labels("tombstoned"),
            DataPoint::new(140, 99.0),
        ),
    ])?;
    if result.acknowledgement != WriteAcknowledgement::Durable {
        return Err(format!(
            "baseline fixture write returned {:?}, expected Durable",
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
            "fixture tombstone expected one matched/applied series, got {deletion:?}"
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
        DataPoint::new(150, Value::String("durable-wal-suffix".to_string())),
    )])?;
    if result.acknowledgement != WriteAcknowledgement::Durable {
        return Err(format!(
            "WAL fixture suffix returned {:?}, expected Durable",
            result.acknowledgement
        )
        .into());
    }
    println!("{CHILD_READY_RECORD}");
    std::io::stdout().flush()?;

    // The generator parent terminates this process. Keep storage live so the suffix remains a
    // genuine WAL-recovery case rather than being flushed by close or Drop.
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
        .ok_or("fixture WAL child stdout was not piped")?;
    let mut reader = BufReader::new(stdout);
    let mut ready = String::new();
    if reader.read_line(&mut ready)? == 0 || ready.trim() != CHILD_READY_RECORD {
        return Err(format!("fixture WAL child did not report a durable suffix: {ready:?}").into());
    }
    let status = child.kill_and_reap()?;
    if status.success() {
        return Err("fixture WAL child exited normally instead of being killed".into());
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
         - Logical storage format: `2` (`framed_segment_v2`, `framed_wal_v2`, \
         `registry_snapshot_v2`, and the blob value lane).\n\
         - Data-directory manifest: intentionally absent, modeling the supported layout before \
         `tsink-manifest.json` existed.\n\
         - Legacy creator identity: unknowable. A successful legacy open therefore records JSON \
         `null` for `creating_tsink_version` and records the current package version only as the \
         last successful opener.\n\
         - Generator: `examples/generate_storage_format_v2_fixture.rs` from the same reviewed \
         source change as this fixture.\n\
         - Generator package version: `{}`.\n\
         - Generated: `{FIXTURE_GENERATED_DATE}`.\n\
         - Evidence boundary: this fixture was produced by the current storage-format-v2 writer \
         and then had only its manifest removed. It is a frozen example of the supported prior \
         pre-manifest layout, not independent evidence from a historical release binary and not \
         an older-format migration fixture.\n\
         - Retention metadata: not applicable. This layout does not persist the builder's runtime \
         retention policy; generation disables retention and includes a named retained sample to \
         prove ordinary non-tombstoned data remains visible.\n\
         - Lock file: the known legacy `.tsink.lock` file is intentionally retained as a frozen \
         zero-byte data-root entry and covered by the hash inventory.\n\
         - Byte reproducibility: not promised. Segment creation timestamps and concurrent series \
         registration can change otherwise equivalent generated bytes. The inventory freezes the \
         reviewed checked-in instance; a fresh candidate is for logical comparison, not silent \
         hash replacement.\n\
         - Hash inventory: `CONTENT_HASHES.txt`, xxHash64 with seed 0 over every regular file \
         beneath `data/`; the hash list and provenance files are outside the copied database.\n\n\
         Generation is manual and destructive regeneration is forbidden. To generate an audit candidate \
         in an absent staging directory, run:\n\n\
         ```console\n\
         cargo run -p tsink --example generate_storage_format_v2_fixture -- target/manual-fixtures/{FIXTURE_ID}\n\
         ```\n\n\
         A successor requires a reviewed change to the generator's `FIXTURE_ID` and data contract \
         plus a new checked-in directory; do not reuse the unchanged generator under a new ID.\n\n\
         The generator cleanly closes the persisted numeric/blob/histogram/tombstone baseline, \
         then starts and kills a child immediately after a `PerAppend`-Durable suffix. It removes \
         only `tsink-manifest.json`; tests never invoke the generator.\n",
        env!("CARGO_PKG_VERSION"),
    );
    fs::write(fixture_root.join("PROVENANCE.md"), provenance)?;
    Ok(())
}

fn generate_fixture(fixture_root: &Path) -> Result<(), Box<dyn Error>> {
    if fixture_root.file_name().and_then(|name| name.to_str()) != Some(FIXTURE_ID) {
        return Err(format!(
            "fixture output directory must be named {FIXTURE_ID}; a successor requires a reviewed generator change"
        )
        .into());
    }
    if fixture_root.exists() {
        return Err(format!(
            "refusing to overwrite existing fixture {}; choose a new versioned directory",
            fixture_root.display()
        )
        .into());
    }
    if let Some(parent) = fixture_root.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::create_dir(fixture_root)?;
    let data_path = fixture_root.join("data");

    write_clean_baseline(&data_path)?;
    write_wal_only_suffix(&data_path)?;
    let manifest_path = data_path.join(DATA_DIRECTORY_MANIFEST_FILE_NAME);
    fs::remove_file(&manifest_path)?;
    if manifest_path.exists() {
        return Err("pre-manifest fixture still contains tsink-manifest.json".into());
    }

    write_content_hashes(fixture_root, &data_path)?;
    write_provenance(fixture_root)?;
    println!("generated frozen fixture {}", fixture_root.display());
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    if std::env::var_os(CHILD_MODE_ENV).is_some() {
        let data_path = PathBuf::from(
            std::env::var_os(CHILD_DATA_PATH_ENV).ok_or("fixture child data path is missing")?,
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
        return Err("fixture generator accepts exactly one output directory".into());
    }
    generate_fixture(Path::new(&fixture_root))
}
