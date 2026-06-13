#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

usage() {
  cat <<'USAGE'
Usage: scripts/measure_server_replication_performance.sh [quick|full]

Profiles:
  quick  Fast RF/consistency matrix for local smoke checks (default)
  full   Heavier run for baseline refresh

Environment overrides:
  TSINK_CLUSTER_BENCH_ARTIFACT_ROOT
  TSINK_CLUSTER_BENCH_OUTPUT
  TSINK_CLUSTER_PERF_RF_VALUES
  TSINK_CLUSTER_PERF_MODES
  TSINK_CLUSTER_PERF_SHARDS
  TSINK_CLUSTER_PERF_INTERNAL_AUTH_TOKEN
  TSINK_CLUSTER_PERF_TIMESTAMP_MARGIN_MS
  TSINK_CLUSTER_PERF_LATENCY_REQUESTS
  TSINK_CLUSTER_PERF_THROUGHPUT_REQUESTS
  TSINK_CLUSTER_PERF_THROUGHPUT_POINTS_PER_REQUEST
  TSINK_CLUSTER_PERF_THROUGHPUT_SERIES_PER_REQUEST
USAGE
}

PROFILE="${1:-quick}"
if [[ "$#" -gt 1 ]]; then
  usage
  exit 1
fi

case "$PROFILE" in
  quick)
    LATENCY_REQUESTS="${TSINK_CLUSTER_PERF_LATENCY_REQUESTS:-30}"
    THROUGHPUT_REQUESTS="${TSINK_CLUSTER_PERF_THROUGHPUT_REQUESTS:-18}"
    THROUGHPUT_POINTS_PER_REQUEST="${TSINK_CLUSTER_PERF_THROUGHPUT_POINTS_PER_REQUEST:-256}"
    THROUGHPUT_SERIES_PER_REQUEST="${TSINK_CLUSTER_PERF_THROUGHPUT_SERIES_PER_REQUEST:-32}"
    TIMESTAMP_MARGIN_MS="${TSINK_CLUSTER_PERF_TIMESTAMP_MARGIN_MS:-60000}"
    TIMESTAMP_GAP_MS=1000
    WARMUP_POINTS=512
    ;;
  full)
    LATENCY_REQUESTS="${TSINK_CLUSTER_PERF_LATENCY_REQUESTS:-90}"
    THROUGHPUT_REQUESTS="${TSINK_CLUSTER_PERF_THROUGHPUT_REQUESTS:-64}"
    THROUGHPUT_POINTS_PER_REQUEST="${TSINK_CLUSTER_PERF_THROUGHPUT_POINTS_PER_REQUEST:-512}"
    THROUGHPUT_SERIES_PER_REQUEST="${TSINK_CLUSTER_PERF_THROUGHPUT_SERIES_PER_REQUEST:-64}"
    TIMESTAMP_MARGIN_MS="${TSINK_CLUSTER_PERF_TIMESTAMP_MARGIN_MS:-60000}"
    TIMESTAMP_GAP_MS=1000
    WARMUP_POINTS=512
    ;;
  *)
    usage
    exit 1
    ;;
esac

ARTIFACT_ROOT="${TSINK_CLUSTER_BENCH_ARTIFACT_ROOT:-target/server-bench/cluster}"
RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)-${PROFILE}"
ARTIFACT_DIR="${ARTIFACT_ROOT}/${RUN_ID}"
LATEST_DIR="${ARTIFACT_ROOT}/latest"
OUTPUT_PATH="${TSINK_CLUSTER_BENCH_OUTPUT:-target/server-bench/cluster-latest.json}"
RESULTS_TSV="${ARTIFACT_DIR}/results.tsv"
SUMMARY_TXT="${ARTIFACT_DIR}/summary.txt"
SUMMARY_MD="${ARTIFACT_DIR}/summary.md"
SUMMARY_JSON="${ARTIFACT_DIR}/summary.json"
RUN_LOG="${ARTIFACT_DIR}/run.log"

RF_VALUES_CSV="${TSINK_CLUSTER_PERF_RF_VALUES:-3}"
CONSISTENCY_MODES_CSV="${TSINK_CLUSTER_PERF_MODES:-one,quorum,all}"
SHARDS="${TSINK_CLUSTER_PERF_SHARDS:-128}"
CLUSTER_INTERNAL_AUTH_TOKEN="${TSINK_CLUSTER_PERF_INTERNAL_AUTH_TOKEN:-tsink-cluster-bench-token}"
HOST="${TSINK_CLUSTER_PERF_HOST:-127.0.0.1}"
PUBLIC_BASE_PORT="${TSINK_CLUSTER_PERF_PUBLIC_BASE_PORT:-19411}"
WRITE_MAX_ATTEMPTS="${TSINK_CLUSTER_PERF_WRITE_MAX_ATTEMPTS:-5}"
WRITE_RETRY_SLEEP_SECS="${TSINK_CLUSTER_PERF_WRITE_RETRY_SLEEP_SECS:-0.2}"

for cmd in cargo curl jq awk sort ps date mktemp mkdir cp tail tee tr find rm; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "missing required command: $cmd" >&2
    exit 1
  fi
done

IFS=',' read -r -a RF_VALUES <<< "$RF_VALUES_CSV"
IFS=',' read -r -a CONSISTENCY_MODES <<< "$CONSISTENCY_MODES_CSV"
if [[ "${#RF_VALUES[@]}" -eq 0 ]]; then
  echo "TSINK_CLUSTER_PERF_RF_VALUES must contain at least one value" >&2
  exit 1
fi
if [[ "${#CONSISTENCY_MODES[@]}" -eq 0 ]]; then
  echo "TSINK_CLUSTER_PERF_MODES must contain at least one value" >&2
  exit 1
fi

mkdir -p "$ARTIFACT_DIR"
touch "$RUN_LOG"
printf "replication_factor\twrite_consistency\twrite_p95_ms\tingest_points_per_sec\tcluster_memory_before_bytes\tcluster_memory_after_bytes\tcluster_memory_growth_bytes\tcpu_pct_sum_snapshot\n" >"$RESULTS_TSV"

log() {
  local message="$1"
  local ts
  ts="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "[$ts] $message" | tee -a "$RUN_LOG"
}

TMP_DIR="$(mktemp -d)"
RESULTS_FILE="${TMP_DIR}/scenarios.jsonl"
: > "$RESULTS_FILE"

SERVER_PIDS=()
SERVER_LOGS=()
NODE_URLS=()
TMP_ARCHIVED=0

trim() {
  local value="$1"
  echo "$value" | awk '{gsub(/^[[:space:]]+|[[:space:]]+$/, ""); print}'
}

archive_tmp_dir() {
  if [[ "$TMP_ARCHIVED" == "1" || ! -d "$TMP_DIR" ]]; then
    return 0
  fi
  rm -rf "${ARTIFACT_DIR}/harness"
  cp -R "$TMP_DIR" "${ARTIFACT_DIR}/harness"
  TMP_ARCHIVED=1
}

cleanup() {
  if [[ "${#SERVER_PIDS[@]}" -gt 0 ]]; then
    for pid in "${SERVER_PIDS[@]}"; do
      if kill -0 "$pid" >/dev/null 2>&1; then
        kill "$pid" >/dev/null 2>&1 || true
      fi
    done
    for pid in "${SERVER_PIDS[@]}"; do
      wait "$pid" >/dev/null 2>&1 || true
    done
  fi
  archive_tmp_dir
  rm -rf "$TMP_DIR"
}

print_failure_context() {
  log "replication performance harness failed; tsink-server log tails follow"
  for log_file in "${SERVER_LOGS[@]}"; do
    if [[ -f "$log_file" ]]; then
      log "log_tail=${log_file}"
      tail -n 80 "$log_file" | tee -a "$RUN_LOG" >&2
    fi
  done
}

trap print_failure_context ERR
trap cleanup EXIT

generate_prom_payload() {
  local output_file="$1"
  local metric="$2"
  local points="$3"
  local base_ts="$4"
  local series_count="$5"
  awk \
    -v metric="$metric" \
    -v points="$points" \
    -v base_ts="$base_ts" \
    -v series_count="$series_count" \
    'BEGIN {
      for (i = 0; i < points; i++) {
        value = (i % 1000) / 10.0;
        printf "%s{job=\"cluster-bench\",series=\"s%d\"} %.3f %d\n", metric, (i % series_count), value, base_ts + i;
      }
    }' > "$output_file"
}

wait_for_server() {
  local url="$1"
  local pid="$2"
  local max_attempts=180
  local attempt=1
  while (( attempt <= max_attempts )); do
    if curl -fsS "${url}/healthz" >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "$pid" >/dev/null 2>&1; then
      echo "tsink-server (pid=$pid) exited before becoming healthy" >&2
      return 1
    fi
    sleep 1
    attempt=$((attempt + 1))
  done
  echo "timed out waiting for ${url}/healthz" >&2
  return 1
}

post_write_request() {
  local mode="$1"
  local payload_file="$2"
  local url="$3"
  local include_latency="$4"

  local attempt=1
  while (( attempt <= WRITE_MAX_ATTEMPTS )); do
    if [[ "$include_latency" == "1" ]]; then
      local time_total
      if time_total="$(curl -fsS -o /dev/null -w '%{time_total}' \
        -H "Content-Type: text/plain" \
        -H "x-tsink-write-consistency: ${mode}" \
        --data-binary "@${payload_file}" \
        "${url}/api/v1/import/prometheus")"; then
        echo "$time_total"
        return 0
      fi
    else
      if curl -fsS -o /dev/null \
        -H "Content-Type: text/plain" \
        -H "x-tsink-write-consistency: ${mode}" \
        --data-binary "@${payload_file}" \
        "${url}/api/v1/import/prometheus"; then
        return 0
      fi
    fi

    if (( attempt < WRITE_MAX_ATTEMPTS )); then
      sleep "$WRITE_RETRY_SLEEP_SECS"
    fi
    attempt=$((attempt + 1))
  done

  echo "failed write request after ${WRITE_MAX_ATTEMPTS} attempts (mode=${mode}, payload=${payload_file})" >&2
  return 1
}

cluster_memory_sum_bytes() {
  local total=0
  local memory=0
  for url in "${NODE_URLS[@]}"; do
    memory="$(curl -fsS "${url}/api/v1/status/tsdb" | jq -r '.data.memoryUsedBytes // 0')"
    total=$((total + memory))
  done
  echo "$total"
}

cluster_cpu_pct_sum() {
  if [[ "${#SERVER_PIDS[@]}" -eq 0 ]]; then
    echo "0.00"
    return 0
  fi
  local pid_list
  pid_list="$(IFS=,; echo "${SERVER_PIDS[*]}")"
  ps -p "$pid_list" -o %cpu= | awk '{sum += $1} END {printf "%.2f", sum + 0.0}'
}

stop_cluster() {
  if [[ "${#SERVER_PIDS[@]}" -eq 0 ]]; then
    return 0
  fi
  for pid in "${SERVER_PIDS[@]}"; do
    if kill -0 "$pid" >/dev/null 2>&1; then
      kill "$pid" >/dev/null 2>&1 || true
    fi
  done
  for pid in "${SERVER_PIDS[@]}"; do
    wait "$pid" >/dev/null 2>&1 || true
  done
  SERVER_PIDS=()
  SERVER_LOGS=()
  NODE_URLS=()
}

start_cluster() {
  local rf="$1"
  local run_idx="$2"

  if (( rf < 1 || rf > 3 )); then
    echo "unsupported replication factor for 3-node harness: ${rf}" >&2
    exit 1
  fi

  stop_cluster

  local offset=$((run_idx * 40))
  local public_a=$((PUBLIC_BASE_PORT + offset))
  local public_b=$((PUBLIC_BASE_PORT + offset + 1))
  local public_c=$((PUBLIC_BASE_PORT + offset + 2))

  local endpoint_a="${HOST}:${public_a}"
  local endpoint_b="${HOST}:${public_b}"
  local endpoint_c="${HOST}:${public_c}"

  local seed_a="node-b@${endpoint_b},node-c@${endpoint_c}"
  local seed_b="node-a@${endpoint_a},node-c@${endpoint_c}"
  local seed_c="node-a@${endpoint_a},node-b@${endpoint_b}"

  mkdir -p "${TMP_DIR}/rf-${rf}"

  local node_ids=("node-a" "node-b" "node-c")
  local public_ports=("$public_a" "$public_b" "$public_c")
  local binds=("$endpoint_a" "$endpoint_b" "$endpoint_c")
  local seeds=("$seed_a" "$seed_b" "$seed_c")

  for idx in 0 1 2; do
    local node_id="${node_ids[$idx]}"
    local public_port="${public_ports[$idx]}"
    local bind="${binds[$idx]}"
    local seed_value="${seeds[$idx]}"
    local data_dir="${TMP_DIR}/rf-${rf}/${node_id}-data"
    local log_file="${TMP_DIR}/rf-${rf}/${node_id}.log"

    mkdir -p "$data_dir"

    cargo run -p tsink-server --bin tsink-server --release -- \
      --listen "${HOST}:${public_port}" \
      --data-path "$data_dir" \
      --cluster-enabled true \
      --cluster-node-id "$node_id" \
      --cluster-bind "$bind" \
      --cluster-seeds "$seed_value" \
      --cluster-internal-auth-token "$CLUSTER_INTERNAL_AUTH_TOKEN" \
      --cluster-shards "$SHARDS" \
      --cluster-replication-factor "$rf" \
      --cluster-write-consistency all \
      --cluster-read-consistency eventual \
      >"$log_file" 2>&1 &

    local pid="$!"
    SERVER_PIDS+=("$pid")
    SERVER_LOGS+=("$log_file")
    NODE_URLS+=("http://${HOST}:${public_port}")
  done

  for idx in 0 1 2; do
    wait_for_server "${NODE_URLS[$idx]}" "${SERVER_PIDS[$idx]}"
  done
}

measure_mode() {
  local rf="$1"
  local mode="$2"
  local mode_slug
  mode_slug="$(echo "$mode" | tr '[:upper:]' '[:lower:]')"

  local base_url="${NODE_URLS[0]}"
  local mode_dir="${TMP_DIR}/rf-${rf}/${mode_slug}"
  mkdir -p "$mode_dir"

  local throughput_points_total=$((THROUGHPUT_REQUESTS * THROUGHPUT_POINTS_PER_REQUEST))
  local timestamp_span_ms=$((WARMUP_POINTS + TIMESTAMP_GAP_MS + LATENCY_REQUESTS + TIMESTAMP_GAP_MS + throughput_points_total))
  local current_time_ms=$(( $(date +%s) * 1000 ))
  local timestamp_base_ms=$((current_time_ms - TIMESTAMP_MARGIN_MS - timestamp_span_ms))
  local warmup_base_ts=$timestamp_base_ms
  local latency_base_ts=$((warmup_base_ts + WARMUP_POINTS + TIMESTAMP_GAP_MS))
  local throughput_base_ts=$((latency_base_ts + LATENCY_REQUESTS + TIMESTAMP_GAP_MS))

  local warmup_file="${mode_dir}/warmup.prom"
  generate_prom_payload \
    "$warmup_file" \
    "cluster_warmup_rf${rf}_${mode_slug}" \
    "$WARMUP_POINTS" \
    "$warmup_base_ts" \
    32
  post_write_request "$mode_slug" "$warmup_file" "$base_url" 0 >/dev/null

  local latency_times="${mode_dir}/latency_times.txt"
  : > "$latency_times"

  for request_id in $(seq 1 "$LATENCY_REQUESTS"); do
    local request_file="${mode_dir}/latency_${request_id}.prom"
    local ts=$((latency_base_ts + request_id))
    printf 'cluster_write_latency{rf="%d",mode="%s",request="%d"} 1 %d\n' \
      "$rf" "$mode_slug" "$request_id" "$ts" > "$request_file"

    local time_total
    time_total="$(post_write_request "$mode_slug" "$request_file" "$base_url" 1)"
    echo "$time_total" >> "$latency_times"
  done

  local write_p95_ms
  write_p95_ms="$(sort -n "$latency_times" | awk '
    { values[NR] = $1 }
    END {
      if (NR == 0) {
        printf "0.000"
        exit
      }
      idx = int((NR * 95 + 99) / 100)
      if (idx < 1) idx = 1
      if (idx > NR) idx = NR
      printf "%.3f", values[idx] * 1000.0
    }')"

  local throughput_dir="${mode_dir}/throughput"
  mkdir -p "$throughput_dir"
  for request_id in $(seq 1 "$THROUGHPUT_REQUESTS"); do
    local start_ts=$((throughput_base_ts + (request_id - 1) * THROUGHPUT_POINTS_PER_REQUEST))
    local metric="cluster_ingest_rf${rf}_${mode_slug}_${request_id}"
    generate_prom_payload \
      "${throughput_dir}/${request_id}.prom" \
      "$metric" \
      "$THROUGHPUT_POINTS_PER_REQUEST" \
      "$start_ts" \
      "$THROUGHPUT_SERIES_PER_REQUEST"
  done

  local memory_before
  memory_before="$(cluster_memory_sum_bytes)"

  local start_ns
  start_ns="$(date +%s%N)"
  for request_id in $(seq 1 "$THROUGHPUT_REQUESTS"); do
    post_write_request \
      "$mode_slug" \
      "${throughput_dir}/${request_id}.prom" \
      "$base_url" \
      0 >/dev/null
  done
  local end_ns
  end_ns="$(date +%s%N)"

  local memory_after
  memory_after="$(cluster_memory_sum_bytes)"
  local cpu_pct
  cpu_pct="$(cluster_cpu_pct_sum)"

  local elapsed_ns=$((end_ns - start_ns))
  local total_points=$((THROUGHPUT_REQUESTS * THROUGHPUT_POINTS_PER_REQUEST))
  local ingest_points_per_sec
  ingest_points_per_sec="$(awk -v points="$total_points" -v elapsed_ns="$elapsed_ns" '
    BEGIN {
      if (elapsed_ns <= 0) {
        printf "0.000"
      } else {
        printf "%.3f", (points * 1000000000.0) / elapsed_ns
      }
    }')"

  local memory_growth_bytes=0
  if (( memory_after > memory_before )); then
    memory_growth_bytes=$((memory_after - memory_before))
  fi

  log "rf=${rf} mode=${mode_slug} p95_ms=${write_p95_ms} ingest_pps=${ingest_points_per_sec} mem_growth=${memory_growth_bytes} cpu_pct_sum=${cpu_pct}"

  printf "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n" \
    "$rf" \
    "$mode_slug" \
    "$write_p95_ms" \
    "$ingest_points_per_sec" \
    "$memory_before" \
    "$memory_after" \
    "$memory_growth_bytes" \
    "$cpu_pct" >>"$RESULTS_TSV"

  jq -n \
    --argjson rf "$rf" \
    --arg mode "$mode_slug" \
    --arg endpoint "$base_url" \
    --argjson writeP95Ms "$write_p95_ms" \
    --argjson ingestPointsPerSec "$ingest_points_per_sec" \
    --argjson memoryBeforeBytes "$memory_before" \
    --argjson memoryAfterBytes "$memory_after" \
    --argjson memoryGrowthBytes "$memory_growth_bytes" \
    --argjson cpuPctSum "$cpu_pct" \
    '{
      replicationFactor: $rf,
      writeConsistency: $mode,
      endpoint: $endpoint,
      metrics: {
        write_p95_ms: $writeP95Ms,
        ingest_points_per_sec: $ingestPointsPerSec,
        cluster_memory_before_bytes: $memoryBeforeBytes,
        cluster_memory_after_bytes: $memoryAfterBytes,
        cluster_memory_growth_bytes: $memoryGrowthBytes,
        cpu_pct_sum_snapshot: $cpuPctSum
      }
    }' >> "$RESULTS_FILE"
}

run_idx=0
for rf_raw in "${RF_VALUES[@]}"; do
  rf="$(trim "$rf_raw")"
  if [[ -z "$rf" ]]; then
    continue
  fi
  if ! [[ "$rf" =~ ^[0-9]+$ ]]; then
    echo "invalid replication factor: ${rf}" >&2
    exit 1
  fi

  run_idx=$((run_idx + 1))
  log "starting cluster for rf=${rf}"
  start_cluster "$rf" "$run_idx"

  for mode_raw in "${CONSISTENCY_MODES[@]}"; do
    mode="$(trim "$mode_raw")"
    mode="$(echo "$mode" | tr '[:upper:]' '[:lower:]')"
    case "$mode" in
      one|quorum|all) ;;
      *)
        echo "invalid write consistency mode: ${mode}" >&2
        exit 1
        ;;
    esac
    measure_mode "$rf" "$mode"
  done

  stop_cluster
done

mkdir -p "$(dirname "$OUTPUT_PATH")"

SCENARIOS_JSON="$(jq -s '.' "$RESULTS_FILE")"
GENERATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
SCENARIO_COUNT="$(jq 'length' <<<"$SCENARIOS_JSON")"
RESULT="pass"
if ! jq -e 'length > 0 and all(.[]; (.metrics.write_p95_ms > 0) and (.metrics.ingest_points_per_sec > 0))' <<<"$SCENARIOS_JSON" >/dev/null; then
  RESULT="fail"
fi

jq -n \
  --arg profile "$PROFILE" \
  --arg generatedAt "$GENERATED_AT" \
  --arg artifactDir "$ARTIFACT_DIR" \
  --arg outputPath "$OUTPUT_PATH" \
  --arg rfValues "$RF_VALUES_CSV" \
  --arg modes "$CONSISTENCY_MODES_CSV" \
  --arg result "$RESULT" \
  --argjson scenarioCount "$SCENARIO_COUNT" \
  --argjson shards "$SHARDS" \
  --argjson latencyRequests "$LATENCY_REQUESTS" \
  --argjson throughputRequests "$THROUGHPUT_REQUESTS" \
  --argjson throughputPointsPerRequest "$THROUGHPUT_POINTS_PER_REQUEST" \
  --argjson throughputSeriesPerRequest "$THROUGHPUT_SERIES_PER_REQUEST" \
  --argjson scenarios "$SCENARIOS_JSON" \
  '{
    profile: $profile,
    generatedAt: $generatedAt,
    artifactDir: $artifactDir,
    compatibilityOutputPath: $outputPath,
    result: $result,
    scenarioCount: $scenarioCount,
    config: {
      rfValues: ($rfValues | split(",") | map(gsub("[[:space:]]"; "") | select(length > 0) | tonumber)),
      consistencyModes: ($modes | split(",") | map(gsub("[[:space:]]"; "") | select(length > 0))),
      shards: $shards,
      latencyRequests: $latencyRequests,
      throughputRequests: $throughputRequests,
      throughputPointsPerRequest: $throughputPointsPerRequest,
      throughputSeriesPerRequest: $throughputSeriesPerRequest
    },
    publication: {
      reviewThreshold: "All matrix scenarios must complete with positive throughput and latency samples; release packets compare deltas against the previous published artifact instead of hard-coding hardware-specific absolute floors."
    },
    scenarios: $scenarios
  }' > "$SUMMARY_JSON"

cp "$SUMMARY_JSON" "$OUTPUT_PATH"

{
  echo "tsink-server replication performance benchmark summary"
  echo "  profile: ${PROFILE}"
  echo "  artifact_dir: ${ARTIFACT_DIR}"
  echo "  output_path: ${OUTPUT_PATH}"
  echo "  scenario_count: ${SCENARIO_COUNT}"
  echo "  result: ${RESULT}"
  echo "  scenarios:"
  jq -r '.scenarios[] | "    - rf=\(.replicationFactor) mode=\(.writeConsistency) p95_ms=\(.metrics.write_p95_ms) ingest_pps=\(.metrics.ingest_points_per_sec) mem_growth=\(.metrics.cluster_memory_growth_bytes) cpu_pct_sum=\(.metrics.cpu_pct_sum_snapshot)"' "$SUMMARY_JSON"
} | tee "$SUMMARY_TXT"

{
  echo "# tsink-server Replication Performance Benchmark"
  echo
  echo "- Profile: \`${PROFILE}\`"
  echo "- Artifact directory: \`${ARTIFACT_DIR}\`"
  echo "- Legacy JSON output: \`${OUTPUT_PATH}\`"
  echo "- Scenario count: \`${SCENARIO_COUNT}\`"
  echo "- Result: \`${RESULT}\`"
  echo
  echo "## Matrix Results"
  echo
  echo "| RF | Consistency | Write p95 (ms) | Ingest points/sec | Memory growth (bytes) | CPU % snapshot |"
  echo "|---:|---|---:|---:|---:|---:|"
  tail -n +2 "$RESULTS_TSV" | while IFS=$'\t' read -r rf mode write_p95 ingest_pps mem_before mem_after mem_growth cpu_pct; do
    printf '| %s | `%s` | %s | %s | %s | %s |\n' \
      "$rf" \
      "$mode" \
      "$write_p95" \
      "$ingest_pps" \
      "$mem_growth" \
      "$cpu_pct"
  done
  echo
  echo "Harness inputs, generated workloads, and node logs are archived under \`harness/\` in the per-run artifact directory."
} >"$SUMMARY_MD"

archive_tmp_dir

mkdir -p "$LATEST_DIR"
find "$LATEST_DIR" -mindepth 1 -maxdepth 1 -exec rm -rf {} +
cp "$SUMMARY_TXT" "${LATEST_DIR}/summary.txt"
cp "$SUMMARY_MD" "${LATEST_DIR}/summary.md"
cp "$SUMMARY_JSON" "${LATEST_DIR}/summary.json"
cp "$RESULTS_TSV" "${LATEST_DIR}/results.tsv"
cp "$RUN_LOG" "${LATEST_DIR}/run.log"
cp "$OUTPUT_PATH" "${LATEST_DIR}/cluster-latest.json"
cp -R "${ARTIFACT_DIR}/harness" "${LATEST_DIR}/harness"

if [[ "$RESULT" == "pass" ]]; then
  exit 0
fi

exit 1
