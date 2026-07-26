use std::collections::{BTreeMap, BTreeSet};
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tempfile::TempDir;
use tsink::{
    DataPoint, Label, Row, Storage, StorageBuilder, TimestampPrecision, WalSyncMode,
    WriteAcknowledgement,
};

const CHILD_MODE_ENV: &str = "TSINK_PROCESS_CRASH_CHILD_MODE";
const CHILD_DATA_PATH_ENV: &str = "TSINK_PROCESS_CRASH_DATA_PATH";
const CHILD_SEED_ENV: &str = "TSINK_PROCESS_CRASH_SEED";
const CHILD_SAMPLE_COUNT_ENV: &str = "TSINK_PROCESS_CRASH_SAMPLE_COUNT";
const CHILD_TEST_NAME: &str = "process_crash_durability_child";
const ACK_RECORD_PREFIX: &str = "TSINK_PROCESS_CRASH_ACK\t";
const CONTINUE_COMMAND: &str = "continue";
const HARNESS_METRIC: &str = "tsink_process_crash_durability_sample";
const CHILD_RESPONSE_TIMEOUT: Duration = Duration::from_secs(10);
const PERIODIC_SYNC_INTERVAL: Duration = Duration::from_secs(60 * 60);
const SAMPLES_AVAILABLE_TO_CHILD: usize = 12;
const CRASH_SEEDS: [u64; 3] = [
    0x2f6e_2b1d_4c73_9a05,
    0x8b19_f3a4_05d2_6ec7,
    0xd41c_7e90_b35a_128f,
];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum HarnessWalMode {
    PerAppend,
    Periodic,
}

impl HarnessWalMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::PerAppend => "per_append",
            Self::Periodic => "periodic",
        }
    }

    fn parse(value: &str) -> Self {
        match value {
            "per_append" => Self::PerAppend,
            "periodic" => Self::Periodic,
            other => panic!("unknown process-crash WAL mode: {other}"),
        }
    }

    fn wal_sync_mode(self) -> WalSyncMode {
        match self {
            Self::PerAppend => WalSyncMode::PerAppend,
            Self::Periodic => WalSyncMode::Periodic(PERIODIC_SYNC_INTERVAL),
        }
    }

    fn expected_write_acknowledgement(self) -> WriteAcknowledgement {
        match self {
            Self::PerAppend => WriteAcknowledgement::Durable,
            Self::Periodic => WriteAcknowledgement::Appended,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct AcknowledgedSample {
    sequence: usize,
    acknowledgement: WriteAcknowledgement,
}

fn open_harness_storage(data_path: &Path, mode: HarnessWalMode) -> std::sync::Arc<dyn Storage> {
    StorageBuilder::new()
        .with_data_path(data_path)
        .with_timestamp_precision(TimestampPrecision::Seconds)
        // Each numbered sample has a distinct series identity. This maximum chunk target leaves
        // every one-point head below timed background-flush eligibility, so the normal periodic
        // worker cannot advance these 12 writes behind the acknowledgement protocol.
        .with_chunk_points(u16::MAX as usize)
        .with_wal_buffer_size(256)
        .with_wal_sync_mode(mode.wal_sync_mode())
        .build()
        .expect("process-crash harness storage should open")
}

fn sample_labels(mode: HarnessWalMode, seed: u64, sequence: usize) -> Vec<Label> {
    // Deliberately submit a non-canonical order. Recovery must reconstruct the same canonical
    // identity, not merely a value attached to some series.
    vec![
        Label::new("sample", format!("{sequence:04}")),
        Label::new("seed", format!("{seed:016x}")),
        Label::new("mode", mode.as_str()),
    ]
}

fn canonical_sample_labels(mode: HarnessWalMode, seed: u64, sequence: usize) -> Vec<Label> {
    let mut labels = sample_labels(mode, seed, sequence);
    labels.sort();
    labels
}

fn sample_point(seed: u64, sequence: usize) -> DataPoint {
    let seed_component = (seed ^ seed.rotate_right(29)) & 0x000f_ffff;
    let timestamp = 1_000_000i64
        .saturating_add(i64::try_from(seed_component).unwrap().saturating_mul(32))
        .saturating_add(i64::try_from(sequence).unwrap());
    let value = seed_component as f64 + sequence as f64 / 16.0;
    DataPoint::new(timestamp, value)
}

fn crash_after_for_seed(seed: u64) -> usize {
    let mixed = seed.wrapping_mul(0x9e37_79b9_7f4a_7c15).rotate_left(17) ^ seed.rotate_right(11);
    // Never let the child exhaust its loop. At the crash point it is blocked waiting for a
    // parent command and the storage remains live for a possible subsequent write.
    2 + usize::try_from(mixed % u64::try_from(SAMPLES_AVAILABLE_TO_CHILD - 3).unwrap()).unwrap()
}

fn run_child_mode() {
    let mode = HarnessWalMode::parse(
        &std::env::var(CHILD_MODE_ENV).expect("child WAL mode environment is required"),
    );
    let data_path = PathBuf::from(
        std::env::var_os(CHILD_DATA_PATH_ENV).expect("child data-path environment is required"),
    );
    let seed = std::env::var(CHILD_SEED_ENV)
        .expect("child seed environment is required")
        .parse::<u64>()
        .expect("child seed must be canonical u64 text");
    let sample_count = std::env::var(CHILD_SAMPLE_COUNT_ENV)
        .expect("child sample-count environment is required")
        .parse::<usize>()
        .expect("child sample count must be canonical usize text");
    assert!(sample_count > 0);

    let storage = open_harness_storage(&data_path, mode);
    let stdin = io::stdin();
    let mut commands = BufReader::new(stdin.lock());
    let stderr = io::stderr();
    let mut acknowledgements = BufWriter::new(stderr.lock());

    for sequence in 1..=sample_count {
        let result = storage
            .insert_rows_with_result(&[Row::with_labels(
                HARNESS_METRIC,
                sample_labels(mode, seed, sequence),
                sample_point(seed, sequence),
            )])
            .expect("child sample write must succeed");

        // This is the only acknowledgement boundary observed by the parent: the write has
        // returned, the complete record has been written to the IPC pipe, and that userspace
        // writer has been flushed before the child accepts permission to start another write.
        writeln!(
            acknowledgements,
            "{ACK_RECORD_PREFIX}{sequence}\t{}",
            result.acknowledgement.as_str()
        )
        .expect("child acknowledgement IPC write must succeed");
        acknowledgements
            .flush()
            .expect("child acknowledgement IPC record must flush");

        let mut command = String::new();
        let bytes = commands
            .read_line(&mut command)
            .expect("child command IPC read must succeed");
        assert_ne!(
            bytes, 0,
            "parent closed the command pipe instead of terminating the child abruptly"
        );
        assert_eq!(
            command.trim_end_matches(['\r', '\n']),
            CONTINUE_COMMAND,
            "unknown process-crash harness command"
        );
    }

    panic!("parent allowed the crash-harness child to finish instead of terminating it abruptly");
}

#[test]
fn process_crash_durability_child() {
    if std::env::var_os(CHILD_MODE_ENV).is_some() {
        run_child_mode();
    }
}

fn parse_acknowledgement(value: &str) -> Result<WriteAcknowledgement, String> {
    match value {
        "volatile" => Ok(WriteAcknowledgement::Volatile),
        "appended" => Ok(WriteAcknowledgement::Appended),
        "durable" => Ok(WriteAcknowledgement::Durable),
        other => Err(format!("unknown acknowledgement level {other:?}")),
    }
}

fn parse_ack_record(line: &str) -> Result<Option<AcknowledgedSample>, String> {
    let Some(offset) = line.find(ACK_RECORD_PREFIX) else {
        return Ok(None);
    };
    let payload = line[offset + ACK_RECORD_PREFIX.len()..].trim();
    let mut fields = payload.split('\t');
    let sequence = fields
        .next()
        .ok_or_else(|| "acknowledgement record is missing its sequence".to_string())?
        .parse::<usize>()
        .map_err(|err| format!("acknowledgement sequence is invalid: {err}"))?;
    let acknowledgement = parse_acknowledgement(
        fields
            .next()
            .ok_or_else(|| "acknowledgement record is missing its level".to_string())?,
    )?;
    if fields.next().is_some() {
        return Err("acknowledgement record has trailing fields".to_string());
    }
    Ok(Some(AcknowledgedSample {
        sequence,
        acknowledgement,
    }))
}

fn spawn_child_output_reader(
    child_stderr: std::process::ChildStderr,
) -> (Receiver<Result<String, String>>, Option<JoinHandle<()>>) {
    let (sender, receiver) = mpsc::channel();
    let reader = std::thread::spawn(move || {
        let mut stderr = BufReader::new(child_stderr);
        loop {
            let mut line = String::new();
            match stderr.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => {
                    if sender.send(Ok(line)).is_err() {
                        break;
                    }
                }
                Err(err) => {
                    let _ = sender.send(Err(format!("child stderr read failed: {err}")));
                    break;
                }
            }
        }
    });
    (receiver, Some(reader))
}

fn receive_ack_record(
    receiver: &Receiver<Result<String, String>>,
    transcript: &mut String,
) -> Result<AcknowledgedSample, String> {
    let deadline = Instant::now() + CHILD_RESPONSE_TIMEOUT;
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        if remaining.is_zero() {
            return Err(format!(
                "timed out waiting for child acknowledgement; stderr so far:\n{transcript}"
            ));
        }
        match receiver.recv_timeout(remaining) {
            Ok(Ok(line)) => {
                transcript.push_str(&line);
                if let Some(record) = parse_ack_record(&line)? {
                    return Ok(record);
                }
            }
            Ok(Err(err)) => return Err(format!("{err}; stderr so far:\n{transcript}")),
            Err(RecvTimeoutError::Timeout) => {
                return Err(format!(
                    "timed out waiting for child acknowledgement; stderr so far:\n{transcript}"
                ));
            }
            Err(RecvTimeoutError::Disconnected) => {
                return Err(format!(
                    "child closed acknowledgement IPC before the requested record; stderr:\n{transcript}"
                ));
            }
        }
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

    fn take_stdin(&mut self) -> Option<ChildStdin> {
        self.child.stdin.take()
    }

    fn take_stderr(&mut self) -> Option<std::process::ChildStderr> {
        self.child.stderr.take()
    }

    fn kill_and_reap(&mut self) -> std::process::ExitStatus {
        let kill_result = self.child.kill();
        let status = self
            .child
            .wait()
            .expect("crash-harness child must be reaped");
        self.reaped = true;
        kill_result.expect("crash-harness child must still be alive at the controlled kill point");
        status
    }
}

impl Drop for KillAndReapChild {
    fn drop(&mut self) {
        if self.reaped {
            return;
        }
        // Any assertion or IPC failure after spawn must not strand a child that is deliberately
        // blocked forever waiting for the parent. Ignore cleanup errors only while unwinding.
        let _ = self.child.kill();
        if self.child.wait().is_ok() {
            self.reaped = true;
        }
    }
}

fn join_child_output_reader(reader: &mut Option<JoinHandle<()>>) {
    if let Some(reader) = reader.take() {
        reader
            .join()
            .expect("crash-harness child output reader must not panic");
    }
}

fn continue_child(
    child: &mut KillAndReapChild,
    child_stdin: &mut ChildStdin,
    reader: &mut Option<JoinHandle<()>>,
    transcript: &str,
) {
    if let Err(err) = writeln!(child_stdin, "{CONTINUE_COMMAND}").and_then(|()| child_stdin.flush())
    {
        let status = child.kill_and_reap();
        join_child_output_reader(reader);
        panic!(
            "failed to continue crash-harness child: {err}; status={status}; stderr:\n{transcript}"
        );
    }
}

fn verify_recovered_samples(
    data_path: &Path,
    mode: HarnessWalMode,
    seed: u64,
    acknowledged: &[AcknowledgedSample],
) {
    let storage = open_harness_storage(data_path, mode);
    let acknowledgements_by_sequence = acknowledged
        .iter()
        .map(|record| (record.sequence, record.acknowledgement))
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        acknowledgements_by_sequence.len(),
        acknowledged.len(),
        "parent must not accept duplicate acknowledgement records"
    );

    let mut listed_sequences = BTreeSet::new();
    for series in storage
        .list_metrics()
        .expect("reopened harness database must list recovered identities")
    {
        assert_eq!(
            series.name, HARNESS_METRIC,
            "recovery produced an unknown metric identity"
        );
        let sequence = series
            .labels
            .iter()
            .find(|label| label.name == "sample")
            .unwrap_or_else(|| panic!("recovered identity has no sample label: {series:?}"))
            .value
            .parse::<usize>()
            .unwrap_or_else(|err| {
                panic!("recovered sample label is not canonical numeric text: {err}")
            });
        assert!(
            acknowledgements_by_sequence.contains_key(&sequence),
            "recovery exposed an identity the child had not acknowledged: {series:?}"
        );
        assert_eq!(
            series.labels,
            canonical_sample_labels(mode, seed, sequence),
            "recovery changed the canonical series identity for sample {sequence}"
        );
        assert!(
            listed_sequences.insert(sequence),
            "recovery listed sample identity {sequence} more than once"
        );
    }

    for record in acknowledged {
        let expected = sample_point(seed, record.sequence);
        let points = storage
            .select(
                HARNESS_METRIC,
                &sample_labels(mode, seed, record.sequence),
                0,
                i64::MAX,
            )
            .unwrap_or_else(|err| {
                panic!(
                    "reopened query failed for acknowledged sample {}: {err}",
                    record.sequence
                )
            });
        match record.acknowledgement {
            WriteAcknowledgement::Durable => {
                assert_eq!(
                    points,
                    vec![expected],
                    "Durable-acknowledged sample {} did not survive abrupt process termination",
                    record.sequence
                );
                assert!(
                    listed_sequences.contains(&record.sequence),
                    "Durable sample {} survived without its exact listed identity",
                    record.sequence
                );
            }
            WriteAcknowledgement::Appended | WriteAcknowledgement::Volatile => {
                assert!(
                    points.is_empty() || points == vec![expected],
                    "non-durable sample {} may be absent or present, but must never recover with a corrupt value: {points:?}",
                    record.sequence
                );
                if !points.is_empty() {
                    assert!(
                        listed_sequences.contains(&record.sequence),
                        "recovered non-durable sample {} has no exact listed identity",
                        record.sequence
                    );
                }
            }
        }
    }

    storage
        .close()
        .expect("reopened crash-harness storage should close cleanly");
}

fn run_crash_case(mode: HarnessWalMode, seed: u64) {
    let temp_dir = TempDir::new().expect("crash-harness temporary directory should be created");
    let crash_after = crash_after_for_seed(seed);
    assert!(crash_after < SAMPLES_AVAILABLE_TO_CHILD);

    let current_test_binary = std::env::current_exe()
        .expect("current integration-test executable should be discoverable");
    let mut child = KillAndReapChild::new(
        Command::new(current_test_binary)
            .arg(CHILD_TEST_NAME)
            .arg("--exact")
            .arg("--nocapture")
            .arg("--test-threads=1")
            .env(CHILD_MODE_ENV, mode.as_str())
            .env(CHILD_DATA_PATH_ENV, temp_dir.path())
            .env(CHILD_SEED_ENV, seed.to_string())
            .env(
                CHILD_SAMPLE_COUNT_ENV,
                SAMPLES_AVAILABLE_TO_CHILD.to_string(),
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("crash-harness child should spawn"),
    );
    let mut child_stdin = child.take_stdin().expect("child stdin pipe should exist");
    let child_stderr = child.take_stderr().expect("child stderr pipe should exist");
    let (receiver, mut output_reader) = spawn_child_output_reader(child_stderr);
    let mut transcript = String::new();
    let mut acknowledged = Vec::with_capacity(crash_after);

    for expected_sequence in 1..=crash_after {
        let record = match receive_ack_record(&receiver, &mut transcript) {
            Ok(record) => record,
            Err(err) => {
                let status = child.kill_and_reap();
                join_child_output_reader(&mut output_reader);
                panic!(
                    "crash-harness child protocol failed: {err}; status={status}; mode={mode:?}; seed={seed:#x}"
                );
            }
        };
        if record.sequence != expected_sequence {
            let status = child.kill_and_reap();
            join_child_output_reader(&mut output_reader);
            panic!(
                "child acknowledgements must be uniquely numbered and contiguous: expected \
                 {expected_sequence}, received {}; status={status}; stderr:\n{transcript}",
                record.sequence
            );
        }
        let expected_acknowledgement = mode.expected_write_acknowledgement();
        if record.acknowledgement != expected_acknowledgement {
            let status = child.kill_and_reap();
            join_child_output_reader(&mut output_reader);
            panic!(
                "child returned {:?} for {mode:?}, expected {expected_acknowledgement:?}; \
                 status={status}; stderr:\n{transcript}",
                record.acknowledgement
            );
        }
        acknowledged.push(record);

        if expected_sequence < crash_after {
            continue_child(
                &mut child,
                &mut child_stdin,
                &mut output_reader,
                &transcript,
            );
        }
    }

    // The final record was received after its write returned and its IPC record flushed. Do not
    // send the next command: terminate the child while the storage is live and blocked before
    // another write, with no call to close() and no Rust Drop path.
    let status = child.kill_and_reap();
    drop(child_stdin);
    join_child_output_reader(&mut output_reader);
    assert!(
        !status.success(),
        "controlled crash child unexpectedly exited successfully"
    );

    verify_recovered_samples(temp_dir.path(), mode, seed, &acknowledged);
}

#[test]
fn process_crash_harness_preserves_durable_acknowledgements() {
    for mode in [HarnessWalMode::PerAppend, HarnessWalMode::Periodic] {
        for seed in CRASH_SEEDS {
            run_crash_case(mode, seed);
        }
    }
}
