use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use tempfile::TempDir;
use tsink::{
    DataPoint, HistogramBucketSpan, HistogramCount, HistogramResetHint, Label, NativeHistogram,
    Row, Storage, StorageBuilder, TimestampPrecision, Value, WalSyncMode, WriteAcknowledgement,
};
use xxhash_rust::xxh64::xxh64;

const CURRENT_WRITER_FIXTURE_ROOT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/storage-format-v2-pre-manifest-v1"
);
const HISTORICAL_WRITER_FIXTURE_ROOT: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/storage-format-v2-tsink-0.10.1"
);
const DATA_DIRECTORY_MANIFEST_FILE_NAME: &str = "tsink-manifest.json";
const CONTENT_HASH_SEED: u64 = 0;
const CHUNK_POINTS: usize = 8;

const NUMERIC_METRIC: &str = "fixture_v2_numeric";
const BLOB_METRIC: &str = "fixture_v2_blob";
const HISTOGRAM_METRIC: &str = "fixture_v2_histogram";
// The pre-manifest v2 layout has no persisted retention-policy metadata. This is an ordinary
// non-tombstoned sample retained across upgrade/reopen, not a policy-metadata assertion.
const RETAINED_METRIC: &str = "fixture_v2_retained";
const TOMBSTONED_METRIC: &str = "fixture_v2_tombstoned";
const WAL_RECOVERED_METRIC: &str = "fixture_v2_wal_recovered";
const NEW_METRIC: &str = "fixture_v2_new_after_upgrade";

const HISTORICAL_NUMERIC_METRIC: &str = "historical_v2_numeric";
const HISTORICAL_BLOB_METRIC: &str = "historical_v2_blob";
const HISTORICAL_HISTOGRAM_METRIC: &str = "historical_v2_histogram";
const HISTORICAL_RETAINED_METRIC: &str = "historical_v2_retained";
const HISTORICAL_TOMBSTONED_METRIC: &str = "historical_v2_tombstoned";
const HISTORICAL_WAL_RECOVERED_METRIC: &str = "historical_v2_wal_recovered";
const HISTORICAL_NEW_METRIC: &str = "historical_v2_new_after_upgrade";

#[derive(Debug)]
struct ExpectedContentHash {
    hash: u64,
    size: usize,
}

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

fn historical_fixture_labels(kind: &str) -> Vec<Label> {
    vec![
        Label::new("fixture", "tsink_0_10_1"),
        Label::new("kind", kind),
    ]
}

fn historical_fixture_histogram() -> NativeHistogram {
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

fn open_fixture_copy(data_path: &Path) -> std::sync::Arc<dyn Storage> {
    StorageBuilder::new()
        .with_data_path(data_path)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        .with_chunk_points(CHUNK_POINTS)
        .with_retention_enforced(false)
        .with_wal_sync_mode(WalSyncMode::PerAppend)
        .build()
        .expect("the frozen pre-manifest format-v2 fixture should open")
}

fn normalized_relative_path(root: &Path, path: &Path) -> String {
    let relative = path
        .strip_prefix(root)
        .expect("fixture entry should remain under its root");
    relative
        .components()
        .map(|component| {
            component
                .as_os_str()
                .to_str()
                .expect("checked-in fixture paths must be UTF-8")
        })
        .collect::<Vec<_>>()
        .join("/")
}

fn collect_regular_files(root: &Path) -> BTreeMap<String, PathBuf> {
    fn visit(root: &Path, directory: &Path, files: &mut BTreeMap<String, PathBuf>) {
        for entry in fs::read_dir(directory).expect("fixture directory should be readable") {
            let entry = entry.expect("fixture entry should be readable");
            let path = entry.path();
            let metadata =
                fs::symlink_metadata(&path).expect("fixture entry metadata should be readable");
            assert!(
                !metadata.file_type().is_symlink(),
                "frozen fixture must not contain symlink {}",
                path.display()
            );
            if metadata.file_type().is_dir() {
                visit(root, &path, files);
            } else {
                assert!(
                    metadata.file_type().is_file(),
                    "frozen fixture must contain only files/directories: {}",
                    path.display()
                );
                let relative = normalized_relative_path(root, &path);
                assert!(
                    files.insert(relative.clone(), path).is_none(),
                    "duplicate fixture path {relative}"
                );
            }
        }
    }

    let mut files = BTreeMap::new();
    visit(root, root, &mut files);
    files
}

fn read_expected_content_hashes(fixture_root: &Path) -> BTreeMap<String, ExpectedContentHash> {
    let inventory = fs::read_to_string(fixture_root.join("CONTENT_HASHES.txt"))
        .expect("fixture content-hash inventory should be readable");
    let mut expected = BTreeMap::new();
    for (line_index, line) in inventory.lines().enumerate() {
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let mut fields = line.split_whitespace();
        let hash_text = fields
            .next()
            .unwrap_or_else(|| panic!("hash line {} is missing its hash", line_index + 1));
        let size_text = fields
            .next()
            .unwrap_or_else(|| panic!("hash line {} is missing its size", line_index + 1));
        let relative = fields
            .next()
            .unwrap_or_else(|| panic!("hash line {} is missing its path", line_index + 1));
        assert!(
            fields.next().is_none(),
            "hash line {} has trailing fields",
            line_index + 1
        );
        let hash = u64::from_str_radix(hash_text, 16)
            .unwrap_or_else(|err| panic!("hash line {} is invalid: {err}", line_index + 1));
        let size = size_text
            .parse::<usize>()
            .unwrap_or_else(|err| panic!("hash size on line {} is invalid: {err}", line_index + 1));
        assert!(
            expected
                .insert(relative.to_string(), ExpectedContentHash { hash, size })
                .is_none(),
            "hash inventory repeats {relative}"
        );
    }
    assert!(
        !expected.is_empty(),
        "fixture hash inventory must cover at least one data file"
    );
    expected
}

fn verify_frozen_fixture(
    fixture_root: &Path,
    fixture_id: &str,
    required_provenance_markers: &[&str],
) {
    let provenance = fs::read_to_string(fixture_root.join("PROVENANCE.md"))
        .expect("fixture provenance should be readable");
    assert!(provenance.contains(fixture_id));
    for marker in required_provenance_markers {
        assert!(
            provenance.contains(marker),
            "fixture provenance is missing required marker {marker:?}"
        );
    }

    let data_root = fixture_root.join("data");
    assert!(
        !data_root.join(DATA_DIRECTORY_MANIFEST_FILE_NAME).exists(),
        "the prior-format fixture must remain manifestless"
    );
    let expected = read_expected_content_hashes(fixture_root);
    let expected_lock = expected
        .get(".tsink.lock")
        .expect("the intentionally frozen legacy lock file must be inventoried");
    assert_eq!(
        expected_lock.size, 0,
        "the frozen legacy lock file should remain empty"
    );
    let actual = collect_regular_files(&data_root);
    assert_eq!(
        actual.keys().collect::<Vec<_>>(),
        expected.keys().collect::<Vec<_>>(),
        "fixture file set changed without updating the reviewed hash inventory"
    );
    for (relative, path) in actual {
        let bytes = fs::read(path).expect("fixture data file should be readable");
        let expected = expected
            .get(&relative)
            .expect("actual fixture file should have an expected hash");
        assert_eq!(
            bytes.len(),
            expected.size,
            "fixture size changed for {relative}"
        );
        assert_eq!(
            xxh64(&bytes, CONTENT_HASH_SEED),
            expected.hash,
            "fixture content changed for {relative}"
        );
    }
}

fn copy_fixture_directory(source: &Path, destination: &Path) {
    fs::create_dir(destination).expect("fixture copy destination should be created once");
    for entry in fs::read_dir(source).expect("fixture source directory should be readable") {
        let entry = entry.expect("fixture source entry should be readable");
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let metadata =
            fs::symlink_metadata(&source_path).expect("fixture source metadata should be readable");
        assert!(!metadata.file_type().is_symlink());
        if metadata.file_type().is_dir() {
            copy_fixture_directory(&source_path, &destination_path);
        } else {
            assert!(metadata.file_type().is_file());
            fs::copy(&source_path, &destination_path)
                .expect("fixture file should copy to the temporary database");
        }
    }
}

fn assert_current_manifest(data_path: &Path) {
    let manifest_path = data_path.join(DATA_DIRECTORY_MANIFEST_FILE_NAME);
    let manifest: serde_json::Value = serde_json::from_slice(
        &fs::read(&manifest_path).expect("successful legacy open should install the manifest"),
    )
    .expect("installed current manifest should be valid JSON");
    assert_eq!(manifest["magic"].as_str(), Some("TSINK_DATA_DIRECTORY"));
    assert_eq!(manifest["manifest_schema_version"].as_u64(), Some(1));
    assert_eq!(
        manifest["payload"]["storage_format_version"].as_u64(),
        Some(2)
    );
    assert_eq!(
        manifest["payload"]["minimum_reader_storage_format_version"].as_u64(),
        Some(2)
    );
    assert!(
        manifest["payload"]["creating_tsink_version"].is_null(),
        "the legacy fixture's original creating version is unknowable"
    );
    assert_eq!(
        manifest["payload"]["last_successfully_opened_tsink_version"].as_str(),
        Some(env!("CARGO_PKG_VERSION"))
    );
    assert_eq!(
        manifest["payload"]["timestamp_precision"].as_str(),
        Some("seconds")
    );
    assert_eq!(
        manifest["payload"]["chunk_point_capacity"].as_u64(),
        Some(CHUNK_POINTS as u64)
    );
}

fn assert_prior_data(storage: &dyn Storage) {
    assert_eq!(
        storage
            .select(NUMERIC_METRIC, &fixture_labels("numeric"), 0, 1_000)
            .expect("numeric fixture query should succeed"),
        vec![DataPoint::new(100, 12.5), DataPoint::new(101, 13.5)]
    );
    assert_eq!(
        storage
            .select(BLOB_METRIC, &fixture_labels("blob"), 0, 1_000)
            .expect("blob fixture query should succeed"),
        vec![DataPoint::new(
            110,
            Value::Bytes(b"legacy-blob\0v2".to_vec())
        )]
    );
    assert_eq!(
        storage
            .select(HISTOGRAM_METRIC, &fixture_labels("histogram"), 0, 1_000,)
            .expect("native histogram fixture query should succeed"),
        vec![DataPoint::new(120, fixture_histogram())]
    );
    assert_eq!(
        storage
            .select(RETAINED_METRIC, &fixture_labels("retained"), 0, 1_000)
            .expect("retained fixture query should succeed"),
        vec![DataPoint::new(130, Value::I64(-17))]
    );
    assert!(
        storage
            .select(TOMBSTONED_METRIC, &fixture_labels("tombstoned"), 0, 1_000,)
            .expect("tombstoned fixture query should succeed")
            .is_empty(),
        "the persisted legacy tombstone must continue hiding its sample"
    );
    assert_eq!(
        storage
            .select(
                WAL_RECOVERED_METRIC,
                &fixture_labels("wal_recovered"),
                0,
                1_000,
            )
            .expect("WAL-recovered fixture query should succeed"),
        vec![DataPoint::new(
            150,
            Value::String("durable-wal-suffix".to_string())
        )]
    );
}

fn assert_historical_prior_data(storage: &dyn Storage) {
    assert_eq!(
        storage
            .select(
                HISTORICAL_NUMERIC_METRIC,
                &historical_fixture_labels("numeric"),
                0,
                1_000,
            )
            .expect("historical numeric fixture query should succeed"),
        vec![DataPoint::new(200, -4.25), DataPoint::new(201, 18.75)]
    );
    assert_eq!(
        storage
            .select(
                HISTORICAL_BLOB_METRIC,
                &historical_fixture_labels("blob"),
                0,
                1_000,
            )
            .expect("historical blob fixture query should succeed"),
        vec![DataPoint::new(
            210,
            Value::Bytes(b"historical-0.10.1\0blob".to_vec())
        )]
    );
    assert_eq!(
        storage
            .select(
                HISTORICAL_HISTOGRAM_METRIC,
                &historical_fixture_labels("histogram"),
                0,
                1_000,
            )
            .expect("historical native histogram fixture query should succeed"),
        vec![DataPoint::new(220, historical_fixture_histogram())]
    );
    assert_eq!(
        storage
            .select(
                HISTORICAL_RETAINED_METRIC,
                &historical_fixture_labels("retained"),
                0,
                1_000,
            )
            .expect("historical retained fixture query should succeed"),
        vec![DataPoint::new(230, Value::U64(10_001))]
    );
    assert!(
        storage
            .select(
                HISTORICAL_TOMBSTONED_METRIC,
                &historical_fixture_labels("tombstoned"),
                0,
                1_000,
            )
            .expect("historical tombstoned fixture query should succeed")
            .is_empty(),
        "the historical release's persisted tombstone must continue hiding its sample"
    );
    assert_eq!(
        storage
            .select(
                HISTORICAL_WAL_RECOVERED_METRIC,
                &historical_fixture_labels("wal_recovered"),
                0,
                1_000,
            )
            .expect("historical WAL-recovered fixture query should succeed"),
        vec![DataPoint::new(
            250,
            Value::String("historical-durable-wal".to_string())
        )]
    );
}

fn assert_snapshot_restore_round_trip(
    source: &dyn Storage,
    source_path: &Path,
    snapshot_path: &Path,
    restored_path: &Path,
    assert_restored_data: impl FnOnce(&dyn Storage),
) {
    let source_manifest = fs::read(source_path.join(DATA_DIRECTORY_MANIFEST_FILE_NAME))
        .expect("upgraded source manifest should be readable before snapshot");
    source
        .snapshot(snapshot_path)
        .expect("upgraded compatibility fixture should snapshot successfully");
    let snapshot_manifest = fs::read(snapshot_path.join(DATA_DIRECTORY_MANIFEST_FILE_NAME))
        .expect("snapshot should preserve the upgraded manifest");
    assert_eq!(
        snapshot_manifest, source_manifest,
        "snapshot must preserve the upgraded fixture manifest byte-for-byte"
    );

    StorageBuilder::restore_from_snapshot(snapshot_path, restored_path)
        .expect("compatibility snapshot should restore through the public API");
    assert_eq!(
        fs::read(restored_path.join(DATA_DIRECTORY_MANIFEST_FILE_NAME))
            .expect("restored manifest should be readable before strict open"),
        snapshot_manifest,
        "restore must preserve the snapshot manifest byte-for-byte"
    );

    // `open_fixture_copy` uses the public builder's default strict WAL recovery path.
    let restored = open_fixture_copy(restored_path);
    assert_current_manifest(restored_path);
    assert_restored_data(restored.as_ref());
    restored
        .close()
        .expect("strictly reopened compatibility restore should close cleanly");
}

#[test]
fn pre_manifest_storage_format_v2_fixture_upgrades_and_remains_writable() {
    let fixture_root = Path::new(CURRENT_WRITER_FIXTURE_ROOT);
    verify_frozen_fixture(
        fixture_root,
        "storage-format-v2-pre-manifest-v1",
        &[
            "generate_storage_format_v2_fixture.rs",
            "Legacy creator identity: unknowable",
            "Retention metadata: not applicable",
            "`.tsink.lock`",
            "Byte reproducibility: not promised",
            "tests never invoke the generator",
        ],
    );

    let temp_dir = TempDir::new().expect("fixture test temporary directory should be created");
    let data_path = temp_dir.path().join("database");
    copy_fixture_directory(&fixture_root.join("data"), &data_path);
    assert!(!data_path.join(DATA_DIRECTORY_MANIFEST_FILE_NAME).exists());

    let storage = open_fixture_copy(&data_path);
    assert_current_manifest(&data_path);
    assert_prior_data(storage.as_ref());
    assert!(storage
        .select(NEW_METRIC, &fixture_labels("new"), 0, 1_000)
        .expect("new-series precondition query should succeed")
        .is_empty());
    let result = storage
        .insert_rows_with_result(&[Row::with_labels(
            NEW_METRIC,
            fixture_labels("new"),
            DataPoint::new(160, Value::U64(2_026)),
        )])
        .expect("writing after legacy open should succeed");
    assert_eq!(result.acknowledgement, WriteAcknowledgement::Durable);
    storage
        .close()
        .expect("upgraded fixture copy should close cleanly");

    let reopened = open_fixture_copy(&data_path);
    assert_current_manifest(&data_path);
    assert_prior_data(reopened.as_ref());
    assert_eq!(
        reopened
            .select(NEW_METRIC, &fixture_labels("new"), 0, 1_000)
            .expect("new data should remain queryable after reopen"),
        vec![DataPoint::new(160, Value::U64(2_026))]
    );

    let snapshot_path = temp_dir.path().join("snapshot");
    let restored_path = temp_dir.path().join("restored");
    assert_snapshot_restore_round_trip(
        reopened.as_ref(),
        &data_path,
        &snapshot_path,
        &restored_path,
        |restored| {
            assert_prior_data(restored);
            assert_eq!(
                restored
                    .select(NEW_METRIC, &fixture_labels("new"), 0, 1_000)
                    .expect("new data should remain queryable after snapshot restore"),
                vec![DataPoint::new(160, Value::U64(2_026))]
            );
        },
    );
    reopened
        .close()
        .expect("reopened upgraded fixture should close cleanly");
}

#[test]
fn historical_tsink_0_10_1_storage_format_v2_fixture_upgrades_and_remains_writable() {
    let fixture_root = Path::new(HISTORICAL_WRITER_FIXTURE_ROOT);
    verify_frozen_fixture(
        fixture_root,
        "storage-format-v2-tsink-0.10.1",
        &[
            "tsink package `0.10.1`",
            "00cc627df7b36ae1838f68da273c42949f0a5d52",
            "release version 0.10.1",
            "git archive",
            "generate_storage_format_v2_tsink_0_10_1.rs",
            "no file was removed from the generated data directory",
            "Retention metadata: not applicable",
            "abruptly kills the child",
            "Tests never invoke this driver",
        ],
    );

    let temp_dir = TempDir::new().expect("fixture test temporary directory should be created");
    let data_path = temp_dir.path().join("database");
    copy_fixture_directory(&fixture_root.join("data"), &data_path);
    assert!(!data_path.join(DATA_DIRECTORY_MANIFEST_FILE_NAME).exists());

    let storage = open_fixture_copy(&data_path);
    assert_current_manifest(&data_path);
    assert_historical_prior_data(storage.as_ref());
    assert!(storage
        .select(
            HISTORICAL_NEW_METRIC,
            &historical_fixture_labels("new"),
            0,
            1_000,
        )
        .expect("historical fixture new-series precondition query should succeed")
        .is_empty());
    let result = storage
        .insert_rows_with_result(&[Row::with_labels(
            HISTORICAL_NEW_METRIC,
            historical_fixture_labels("new"),
            DataPoint::new(260, Value::I64(-2_026)),
        )])
        .expect("writing after historical legacy open should succeed");
    assert_eq!(result.acknowledgement, WriteAcknowledgement::Durable);
    storage
        .close()
        .expect("upgraded historical fixture copy should close cleanly");

    let reopened = open_fixture_copy(&data_path);
    assert_current_manifest(&data_path);
    assert_historical_prior_data(reopened.as_ref());
    assert_eq!(
        reopened
            .select(
                HISTORICAL_NEW_METRIC,
                &historical_fixture_labels("new"),
                0,
                1_000,
            )
            .expect("new historical-fixture data should remain queryable after reopen"),
        vec![DataPoint::new(260, Value::I64(-2_026))]
    );

    let snapshot_path = temp_dir.path().join("snapshot");
    let restored_path = temp_dir.path().join("restored");
    assert_snapshot_restore_round_trip(
        reopened.as_ref(),
        &data_path,
        &snapshot_path,
        &restored_path,
        |restored| {
            assert_historical_prior_data(restored);
            assert_eq!(
                restored
                    .select(
                        HISTORICAL_NEW_METRIC,
                        &historical_fixture_labels("new"),
                        0,
                        1_000,
                    )
                    .expect(
                        "new historical-fixture data should remain queryable after snapshot restore",
                    ),
                vec![DataPoint::new(260, Value::I64(-2_026))]
            );
        },
    );
    reopened
        .close()
        .expect("reopened historical fixture should close cleanly");
}
