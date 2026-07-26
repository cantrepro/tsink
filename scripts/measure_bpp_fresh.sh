#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd -P)"
cd "$ROOT_DIR"

WORKLOAD_PRESET="${1:-full}"
case "$WORKLOAD_PRESET" in
  quick|mixed-order)
    DEFAULT_RUNS=1
    EXPECTED_RESULT_KIND=RUN_RESULT
    EXPECTED_RESOURCE_PROFILE=embedded
    ;;
  full|embedded)
    DEFAULT_RUNS=3
    EXPECTED_RESULT_KIND=RUN_RESULT
    EXPECTED_RESOURCE_PROFILE=embedded
    ;;
  test)
    DEFAULT_RUNS=3
    EXPECTED_RESULT_KIND=RUN_RESULT
    EXPECTED_RESOURCE_PROFILE=test
    ;;
  edge)
    DEFAULT_RUNS=3
    EXPECTED_RESULT_KIND=RUN_RESULT
    EXPECTED_RESOURCE_PROFILE=edge
    ;;
  server)
    DEFAULT_RUNS=3
    EXPECTED_RESULT_KIND=RUN_RESULT
    EXPECTED_RESOURCE_PROFILE=server
    ;;
  server-writers)
    DEFAULT_RUNS=3
    EXPECTED_RESULT_KIND=NEW_SERIES_RESULT
    EXPECTED_RESOURCE_PROFILE=server
    ;;
  server-queries)
    DEFAULT_RUNS=3
    EXPECTED_RESULT_KIND=QUERY_SATURATION_RESULT
    EXPECTED_RESOURCE_PROFILE=server
    ;;
  *)
    echo "Unknown workload preset: $WORKLOAD_PRESET (expected: quick|full|mixed-order|test|edge|embedded|server|server-writers|server-queries)" >&2
    exit 1
    ;;
esac

FRESH_RUNS="${TSINK_BPP_RUNS:-$DEFAULT_RUNS}"
if ! [[ "$FRESH_RUNS" =~ ^[1-9][0-9]*$ ]]; then
  echo "TSINK_BPP_RUNS must be a positive integer, got '$FRESH_RUNS'" >&2
  exit 1
fi

case "${TSINK_BPP_ALLOW_PRESET_OVERRIDES:-0}" in
  0|false|FALSE|no|NO|n|N) STRICT_PRESET_VALIDATION=true ;;
  1|true|TRUE|yes|YES|y|Y) STRICT_PRESET_VALIDATION=false ;;
  *)
    echo "TSINK_BPP_ALLOW_PRESET_OVERRIDES must be boolean" >&2
    exit 1
    ;;
esac
case "${TSINK_BPP_ALLOW_KEEP_DIR:-0}" in
  0|false|FALSE|no|NO|n|N) ALLOW_KEEP_DIR=false ;;
  1|true|TRUE|yes|YES|y|Y) ALLOW_KEEP_DIR=true ;;
  *)
    echo "TSINK_BPP_ALLOW_KEEP_DIR must be boolean" >&2
    exit 1
    ;;
esac
CONFIGURED_KEEP_ROOT="${TSINK_BPP_KEEP_DIR:-}"
if [[ -n "$CONFIGURED_KEEP_ROOT" && "$ALLOW_KEEP_DIR" != true ]]; then
  echo "TSINK_BPP_KEEP_DIR is diagnostic-only for fresh suites; set TSINK_BPP_ALLOW_KEEP_DIR=1 to opt in" >&2
  exit 1
fi

OUTPUT_DIR="${TSINK_BPP_OUTPUT_DIR:-target/bpp-results}"
mkdir -p "$OUTPUT_DIR"
RUN_STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RAW_OUTPUT="$OUTPUT_DIR/${WORKLOAD_PRESET}-${RUN_STAMP}-$$.log"
OUTPUT_DIR_ABSOLUTE="$(cd "$OUTPUT_DIR" && pwd -P)"
RAW_OUTPUT_ABSOLUTE="$OUTPUT_DIR_ABSOLUTE/${WORKLOAD_PRESET}-${RUN_STAMP}-$$.log"
RAW_OUTPUT_REPO_RELATIVE=""
case "$RAW_OUTPUT_ABSOLUTE" in
  "$ROOT_DIR"/*) RAW_OUTPUT_REPO_RELATIVE="${RAW_OUTPUT_ABSOLUTE#"$ROOT_DIR"/}" ;;
esac
if ! (set -o noclobber; : > "$RAW_OUTPUT") 2>/dev/null; then
  echo "Refusing to overwrite existing raw result file: $RAW_OUTPUT" >&2
  exit 1
fi

FRESH_KEEP_ROOT=""
FRESH_KEEP_ROOT_REPO_RELATIVE=""
CONFIGURED_STORAGE=false
if [[ -n "$CONFIGURED_KEEP_ROOT" ]]; then
  CONFIGURED_STORAGE=true
  mkdir -p "$CONFIGURED_KEEP_ROOT"
  CONFIGURED_KEEP_ROOT="$(cd "$CONFIGURED_KEEP_ROOT" && pwd -P)"
  FRESH_KEEP_ROOT="$CONFIGURED_KEEP_ROOT/fresh-${WORKLOAD_PRESET}-${RUN_STAMP}-$$"
  if [[ -e "$FRESH_KEEP_ROOT" ]]; then
    echo "Refusing to reuse existing fresh-run storage: $FRESH_KEEP_ROOT" >&2
    exit 1
  fi
  mkdir "$FRESH_KEEP_ROOT"
  case "$FRESH_KEEP_ROOT" in
    "$ROOT_DIR"/*) FRESH_KEEP_ROOT_REPO_RELATIVE="${FRESH_KEEP_ROOT#"$ROOT_DIR"/}" ;;
  esac
fi

echo "Running $FRESH_RUNS fresh-process repetitions for preset=$WORKLOAD_PRESET"
echo "Raw output: $RAW_OUTPUT"

sha256_stream() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum | awk '{print $1}'
  else
    shasum -a 256 | awk '{print $1}'
  fi
}

sha256_file() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1" | awk '{print $1}'
  else
    shasum -a 256 "$1" | awk '{print $1}'
  fi
}

source_state_sha256() {
  git ls-files --cached --others --exclude-standard |
    LC_ALL=C sort |
    while IFS= read -r source_path; do
      if [[ "$source_path" == benchmark-results/resource-profiles/* ||
        "$source_path" == target/* ||
        ( -n "$RAW_OUTPUT_REPO_RELATIVE" &&
          "$source_path" == "$RAW_OUTPUT_REPO_RELATIVE" ) ||
        ( -n "$FRESH_KEEP_ROOT_REPO_RELATIVE" &&
          ( "$source_path" == "$FRESH_KEEP_ROOT_REPO_RELATIVE" ||
            "$source_path" == "$FRESH_KEEP_ROOT_REPO_RELATIVE/"* ) ) ]]; then
        continue
      fi
      printf '%s\t%s\n' "$(git hash-object -- "$source_path")" "$source_path"
    done |
    sha256_stream
}

filesystem_type() {
  local path="$1"
  if [[ "$(uname -s)" == "Darwin" ]]; then
    local device
    device="$(df "$path" | awk 'END {print $1}')"
    mount | awk -v device="$device" '
      !found && $1 == device {
        sub(/^\(/, "", $4)
        sub(/,$/, "", $4)
        print $4
        found = 1
      }
      END {
        if (!found) {
          print "unavailable"
        }
      }
    '
  elif stat -f -c '%T' "$path" >/dev/null 2>&1; then
    stat -f -c '%T' "$path"
  else
    echo "unavailable"
  fi
}

CHECKOUT_FILESYSTEM_TYPE="$(filesystem_type .)"
STORAGE_FILESYSTEM_TYPE="$(
  filesystem_type "${CONFIGURED_KEEP_ROOT:-${TMPDIR:-/tmp}}"
)"

if PHYSICAL_MEMORY_BYTES="$(sysctl -n hw.memsize 2>/dev/null)"; then
  :
elif [[ -r /proc/meminfo ]]; then
  PHYSICAL_MEMORY_BYTES="$(
    awk '/^MemTotal:/ {printf "%.0f", $2 * 1024; exit}' /proc/meminfo
  )"
elif command -v system_profiler >/dev/null 2>&1; then
  PHYSICAL_MEMORY_BYTES="$(
    system_profiler SPHardwareDataType 2>/dev/null | awk '
      /Memory:/ && $3 == "GB" {
        printf "%.0f", $2 * 1024 * 1024 * 1024
        exit
      }
    '
  )"
  PHYSICAL_MEMORY_BYTES="${PHYSICAL_MEMORY_BYTES:-unavailable}"
else
  PHYSICAL_MEMORY_BYTES="unavailable"
fi

if CPU_MODEL="$(sysctl -n machdep.cpu.brand_string 2>/dev/null)"; then
  :
elif [[ -r /proc/cpuinfo ]]; then
  CPU_MODEL="$(
    awk -F': ' '/^model name/ {print $2; exit}' /proc/cpuinfo
  )"
elif command -v system_profiler >/dev/null 2>&1; then
  CPU_MODEL="$(
    system_profiler SPHardwareDataType 2>/dev/null |
      awk -F': ' '/Chip:|Processor Name:/ {print $2; exit}'
  )"
  CPU_MODEL="${CPU_MODEL:-unavailable}"
else
  CPU_MODEL="unavailable"
fi
CPU_MODEL="${CPU_MODEL// /_}"
CPU_THREADS="$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo unavailable)"

GIT_PROVENANCE_PATHS=(
  .
  ':(exclude)benchmark-results/resource-profiles/**'
  ':(exclude)target/**'
)
if [[ -n "$RAW_OUTPUT_REPO_RELATIVE" ]]; then
  GIT_PROVENANCE_PATHS+=(":(exclude,top,literal)$RAW_OUTPUT_REPO_RELATIVE")
fi
if [[ -n "$FRESH_KEEP_ROOT_REPO_RELATIVE" ]]; then
  GIT_PROVENANCE_PATHS+=(":(exclude,top,literal)$FRESH_KEEP_ROOT_REPO_RELATIVE")
fi

if [[ -n "$(
  git status --porcelain --untracked-files=normal -- \
    "${GIT_PROVENANCE_PATHS[@]}"
)" ]]; then
  GIT_DIRTY=true
else
  GIT_DIRTY=false
fi

INITIAL_SOURCE_STATE_SHA256="$(source_state_sha256)"
TRACKED_DIFF_SHA256="$(
  git diff --binary --no-ext-diff HEAD -- "${GIT_PROVENANCE_PATHS[@]}" |
    sha256_stream
)"
{
  echo "FRESH_PROCESS_ENVIRONMENT timestamp_utc=$RUN_STAMP preset=$WORKLOAD_PRESET runs=$FRESH_RUNS strict_preset_validation=$STRICT_PRESET_VALIDATION configured_storage=$CONFIGURED_STORAGE"
  echo "FRESH_PROCESS_SOURCE package_version=$(awk -F'\"' '/^version = \"/ {print $2; exit}' Cargo.toml) git_revision=$(git rev-parse HEAD) source_dirty=$GIT_DIRTY source_state_sha256=$INITIAL_SOURCE_STATE_SHA256 tracked_diff_sha256=$TRACKED_DIFF_SHA256"
  echo "FRESH_PROCESS_HARNESS workload_sha256=$(sha256_file benches/workload.rs) measure_bpp_sha256=$(sha256_file scripts/measure_bpp.sh) fresh_runner_sha256=$(sha256_file scripts/measure_bpp_fresh.sh) summarizer_sha256=$(sha256_file scripts/summarize_bpp_fresh.py) sanitizer_sha256=$(sha256_file scripts/sanitize_bpp_output.py)"
  echo "FRESH_PROCESS_HOST os=$(uname -s) release=$(uname -r) architecture=$(uname -m) cpu_model=$CPU_MODEL cpu_threads=$CPU_THREADS physical_memory_bytes=$PHYSICAL_MEMORY_BYTES checkout_filesystem=$CHECKOUT_FILESYSTEM_TYPE storage_filesystem=$STORAGE_FILESYSTEM_TYPE"
  echo "FRESH_PROCESS_TOOLCHAIN rustc=$(rustc --version | tr ' ' '_') cargo=$(cargo --version | tr ' ' '_') build_profile=bench"
} | tee -a "$RAW_OUTPUT"

SANITIZER_ARGS=(
  --root "$ROOT_DIR"
  --temp-root "${TMPDIR:-/tmp}"
)
if [[ -n "$FRESH_KEEP_ROOT" ]]; then
  SANITIZER_ARGS+=(--keep-root "$CONFIGURED_KEEP_ROOT" --keep-root "$FRESH_KEEP_ROOT")
fi

for ((fresh_run = 1; fresh_run <= FRESH_RUNS; fresh_run += 1)); do
  {
    echo "FRESH_PROCESS_RUN run=$fresh_run total_runs=$FRESH_RUNS preset=$WORKLOAD_PRESET"
    if [[ "$STRICT_PRESET_VALIDATION" == true ]]; then
      unset TSINK_RESOURCE_PROFILE
      unset TSINK_MEMORY_LIMIT_BYTES TSINK_MAINTENANCE_MAX_BYTES_PER_PASS
      unset TSINK_ACTIVE_SERIES TSINK_SHARED_METRIC_NAMES
      unset TSINK_NEW_SERIES_WRITERS TSINK_NEW_SERIES_PER_WRITER
      unset TSINK_NEW_SERIES_SHARED_METRIC_NAMES
      unset TSINK_NEW_SERIES_CACHED_MISSING_LABEL_NAMES
      unset TSINK_QUERY_SATURATION_BENCH TSINK_QUERY_SATURATION_WORKERS
      unset TSINK_QUERY_SATURATION_SERIES TSINK_QUERY_SATURATION_POINTS_PER_SERIES
      unset TSINK_QUERY_SATURATION_SERIES_PER_QUERY
      unset TSINK_QUERY_SATURATION_WRITER_POINTS TSINK_QUERY_SATURATION_BATCH_SIZE
      unset TSINK_PRIME_ALL_SERIES TSINK_WARMUP_POINTS TSINK_MEASURE_POINTS
      unset TSINK_MIN_POINTS_PER_SERIES TSINK_BATCH_SIZE
      unset TSINK_OOO_MAX_SECONDS TSINK_OOO_PERMILLE
      unset TSINK_OOO_CROSS_PARTITION_PERMILLE TSINK_OOO_CROSS_PARTITIONS
      unset TSINK_MAX_ACTIVE_PARTITION_HEADS_PER_SERIES
      unset TSINK_SPARSE_EMIT_PERMILLE TSINK_SHORT_LIVED_LIFETIME_STEPS
      unset TSINK_RETENTION_SECONDS TSINK_PARTITION_SECONDS TSINK_STEP_SECONDS
      unset TSINK_SETTLE_MILLIS TSINK_SEED TSINK_FAIL_ON_TARGET
      unset TSINK_METADATA_SELECTOR_BENCH TSINK_METADATA_SELECTOR_SERIES
      unset TSINK_INGEST_LATENCY_BENCH TSINK_INGEST_LATENCY_WRITERS
      unset TSINK_INGEST_LATENCY_SERIES_PER_WRITER
      unset TSINK_INGEST_LATENCY_DURATION_SECS
      unset TSINK_INGEST_LATENCY_MEMORY_LIMIT_BYTES
      unset TSINK_INGEST_LATENCY_CHUNK_POINTS TSINK_INGEST_LATENCY_BATCH_SIZE
      unset TSINK_WAL_APPEND_BENCH TSINK_WAL_APPEND_DURATION_SECS
      unset TSINK_WAL_APPEND_BATCH_POINTS TSINK_BPP_DRY_RUN
    fi
    export TSINK_BPP_RUNS=1
    export TSINK_BPP_RUN_ID_OFFSET=$((fresh_run - 1))
    if [[ -n "$FRESH_KEEP_ROOT" ]]; then
      export TSINK_BPP_KEEP_DIR="$FRESH_KEEP_ROOT"
    else
      unset TSINK_BPP_KEEP_DIR
    fi
    scripts/measure_bpp.sh "$WORKLOAD_PRESET"
  } 2>&1 |
    python3 scripts/sanitize_bpp_output.py "${SANITIZER_ARGS[@]}" |
    tee -a "$RAW_OUTPUT"
done

FINAL_SOURCE_STATE_SHA256="$(source_state_sha256)"
if [[ "$FINAL_SOURCE_STATE_SHA256" != "$INITIAL_SOURCE_STATE_SHA256" ]]; then
  echo "FRESH_PROCESS_SOURCE_FINAL source_state_sha256=$FINAL_SOURCE_STATE_SHA256 stable=false" |
    tee -a "$RAW_OUTPUT"
  echo "Source state changed during the fresh-process suite; refusing to summarize it." >&2
  exit 1
fi
echo "FRESH_PROCESS_SOURCE_FINAL source_state_sha256=$FINAL_SOURCE_STATE_SHA256 stable=true" |
  tee -a "$RAW_OUTPUT"

SUMMARIZER_ARGS=(
  --expected-runs "$FRESH_RUNS"
  --require-configuration
  --require-storage-path-kind
)
if [[ "$STRICT_PRESET_VALIDATION" == true ]]; then
  SUMMARIZER_ARGS+=(
    --expected-kind "$EXPECTED_RESULT_KIND"
    --expected-profile "$EXPECTED_RESOURCE_PROFILE"
  )
fi
python3 scripts/summarize_bpp_fresh.py \
  "${SUMMARIZER_ARGS[@]}" \
  "$RAW_OUTPUT" | tee -a "$RAW_OUTPUT"
