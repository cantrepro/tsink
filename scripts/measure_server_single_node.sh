#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

usage() {
  cat <<'USAGE'
Usage: scripts/measure_server_single_node.sh [quick|full]

Profiles:
  quick  Fast smoke profile for CI and local checks (default)
  full   Heavier local profile for baseline refresh
USAGE
}

PROFILE="${1:-quick}"
if [[ "$#" -gt 1 ]]; then
  usage
  exit 1
fi

case "$PROFILE" in
  quick)
    LATENCY_REQUESTS="${TSINK_SERVER_LATENCY_REQUESTS:-60}"
    THROUGHPUT_REQUESTS="${TSINK_SERVER_THROUGHPUT_REQUESTS:-24}"
    THROUGHPUT_POINTS_PER_REQUEST="${TSINK_SERVER_THROUGHPUT_POINTS_PER_REQUEST:-256}"
    THROUGHPUT_SERIES_PER_REQUEST="${TSINK_SERVER_THROUGHPUT_SERIES_PER_REQUEST:-32}"
    IDLE_SECONDS="${TSINK_SERVER_IDLE_SECONDS:-2}"
    ;;
  full)
    LATENCY_REQUESTS="${TSINK_SERVER_LATENCY_REQUESTS:-180}"
    THROUGHPUT_REQUESTS="${TSINK_SERVER_THROUGHPUT_REQUESTS:-64}"
    THROUGHPUT_POINTS_PER_REQUEST="${TSINK_SERVER_THROUGHPUT_POINTS_PER_REQUEST:-512}"
    THROUGHPUT_SERIES_PER_REQUEST="${TSINK_SERVER_THROUGHPUT_SERIES_PER_REQUEST:-64}"
    IDLE_SECONDS="${TSINK_SERVER_IDLE_SECONDS:-5}"
    ;;
  *)
    usage
    exit 1
    ;;
esac

OUTPUT_PATH="${TSINK_SERVER_BENCH_OUTPUT:-target/server-bench/latest.json}"
HOST="${TSINK_SERVER_BENCH_HOST:-127.0.0.1}"
PORT="${TSINK_SERVER_BENCH_PORT:-19201}"
BASE_URL="http://${HOST}:${PORT}"
WARMUP_POINTS=256
WARMUP_SERIES=16
TIMESTAMP_GAP_MS=1000
TIMESTAMP_MARGIN_MS="${TSINK_SERVER_TIMESTAMP_MARGIN_MS:-60000}"

for cmd in cargo curl jq awk sort; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "missing required command: $cmd" >&2
    exit 1
  fi
done

TMP_DIR="$(mktemp -d)"
SERVER_PID=""
SERVER_LOG="${TMP_DIR}/server.log"

cleanup() {
  if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" >/dev/null 2>&1; then
    kill "$SERVER_PID" >/dev/null 2>&1 || true
    wait "$SERVER_PID" >/dev/null 2>&1 || true
  fi
  rm -rf "$TMP_DIR"
}

print_failure_context() {
  echo "benchmark harness failed; tsink-server log tail:" >&2
  if [[ -f "$SERVER_LOG" ]]; then
    tail -n 80 "$SERVER_LOG" >&2 || true
  fi
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
        printf "%s{job=\"bench\",series=\"s%d\"} %.3f %d\n", metric, (i % series_count), value, base_ts + i;
      }
    }' > "$output_file"
}

wait_for_server() {
  local max_attempts=180
  local attempt=1
  while (( attempt <= max_attempts )); do
    if curl -fsS "${BASE_URL}/healthz" >/dev/null 2>&1; then
      return 0
    fi
    if ! kill -0 "$SERVER_PID" >/dev/null 2>&1; then
      echo "tsink-server exited before becoming healthy" >&2
      return 1
    fi
    sleep 1
    attempt=$((attempt + 1))
  done
  echo "timed out waiting for tsink-server health endpoint at ${BASE_URL}/healthz" >&2
  return 1
}

mkdir -p "${TMP_DIR}/data"
mkdir -p "$(dirname "$OUTPUT_PATH")"

# Keep synthetic writes recent enough for default retention and far enough in the
# past to stay inside the future-skew allowance throughout the run.
THROUGHPUT_POINTS_TOTAL=$((THROUGHPUT_REQUESTS * THROUGHPUT_POINTS_PER_REQUEST))
TIMESTAMP_SPAN_MS=$((WARMUP_POINTS + TIMESTAMP_GAP_MS + LATENCY_REQUESTS + TIMESTAMP_GAP_MS + THROUGHPUT_POINTS_TOTAL))
CURRENT_TIME_MS=$(( $(date +%s) * 1000 ))
TIMESTAMP_BASE_MS=$((CURRENT_TIME_MS - TIMESTAMP_MARGIN_MS - TIMESTAMP_SPAN_MS))
WARMUP_BASE_TS=$TIMESTAMP_BASE_MS
LATENCY_BASE_TS=$((WARMUP_BASE_TS + WARMUP_POINTS + TIMESTAMP_GAP_MS))
THROUGHPUT_BASE_TS=$((LATENCY_BASE_TS + LATENCY_REQUESTS + TIMESTAMP_GAP_MS))

echo "Starting tsink-server for single-node regression benchmark (${PROFILE})..."
cargo run -p tsink-server --bin tsink-server --release -- \
  --listen "${HOST}:${PORT}" \
  --data-path "${TMP_DIR}/data" \
  --cluster-enabled false \
  >"$SERVER_LOG" 2>&1 &
SERVER_PID="$!"

wait_for_server

WARMUP_FILE="${TMP_DIR}/warmup.prom"
generate_prom_payload "$WARMUP_FILE" "bench_warmup" "$WARMUP_POINTS" "$WARMUP_BASE_TS" "$WARMUP_SERIES"
curl -fsS -o /dev/null \
  -H "Content-Type: text/plain" \
  --data-binary "@${WARMUP_FILE}" \
  "${BASE_URL}/api/v1/import/prometheus"

LATENCY_TIMES="${TMP_DIR}/latency_times.txt"
: > "$LATENCY_TIMES"

for request_id in $(seq 1 "$LATENCY_REQUESTS"); do
  request_file="${TMP_DIR}/latency_${request_id}.prom"
  ts=$((LATENCY_BASE_TS + request_id))
  printf 'bench_write_latency{job="bench",request="%d"} 1 %d\n' "$request_id" "$ts" > "$request_file"
  time_total=$(curl -fsS -o /dev/null -w '%{time_total}' \
    -H "Content-Type: text/plain" \
    --data-binary "@${request_file}" \
    "${BASE_URL}/api/v1/import/prometheus")
  echo "$time_total" >> "$LATENCY_TIMES"
done

write_p95_ms=$(
  sort -n "$LATENCY_TIMES" | awk '
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
    }'
)

THROUGHPUT_DIR="${TMP_DIR}/throughput"
mkdir -p "$THROUGHPUT_DIR"
for request_id in $(seq 1 "$THROUGHPUT_REQUESTS"); do
  start_ts=$((THROUGHPUT_BASE_TS + (request_id - 1) * THROUGHPUT_POINTS_PER_REQUEST))
  metric="bench_ingest_${request_id}"
  generate_prom_payload \
    "${THROUGHPUT_DIR}/${request_id}.prom" \
    "$metric" \
    "$THROUGHPUT_POINTS_PER_REQUEST" \
    "$start_ts" \
    "$THROUGHPUT_SERIES_PER_REQUEST"
done

start_ns=$(date +%s%N)
for request_id in $(seq 1 "$THROUGHPUT_REQUESTS"); do
  curl -fsS -o /dev/null \
    -H "Content-Type: text/plain" \
    --data-binary "@${THROUGHPUT_DIR}/${request_id}.prom" \
    "${BASE_URL}/api/v1/import/prometheus"
done
end_ns=$(date +%s%N)

elapsed_ns=$((end_ns - start_ns))
total_points=$((THROUGHPUT_REQUESTS * THROUGHPUT_POINTS_PER_REQUEST))
ingest_points_per_sec=$(
  awk -v points="$total_points" -v elapsed_ns="$elapsed_ns" '
    BEGIN {
      if (elapsed_ns <= 0) {
        printf "0.000"
      } else {
        printf "%.3f", (points * 1000000000.0) / elapsed_ns
      }
    }'
)

status_before="$(curl -fsS "${BASE_URL}/api/v1/status/tsdb")"
memory_before="$(echo "$status_before" | jq -r '.data.memoryUsedBytes // 0')"
sleep "$IDLE_SECONDS"
status_after="$(curl -fsS "${BASE_URL}/api/v1/status/tsdb")"
memory_after="$(echo "$status_after" | jq -r '.data.memoryUsedBytes // 0')"

if (( memory_after > memory_before )); then
  idle_memory_growth_bytes=$((memory_after - memory_before))
else
  idle_memory_growth_bytes=0
fi

idle_memory_growth_pct=$(
  awk -v before="$memory_before" -v growth="$idle_memory_growth_bytes" '
    BEGIN {
      if (before <= 0) {
        printf "0.000"
      } else {
        printf "%.3f", (growth * 100.0) / before
      }
    }'
)

generated_at="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

jq -n \
  --arg profile "$PROFILE" \
  --arg generatedAt "$generated_at" \
  --arg endpoint "$BASE_URL" \
  --argjson latencyRequests "$LATENCY_REQUESTS" \
  --argjson throughputRequests "$THROUGHPUT_REQUESTS" \
  --argjson throughputPointsPerRequest "$THROUGHPUT_POINTS_PER_REQUEST" \
  --argjson idleSeconds "$IDLE_SECONDS" \
  --argjson writeP95Ms "$write_p95_ms" \
  --argjson ingestPointsPerSec "$ingest_points_per_sec" \
  --argjson idleMemoryGrowthBytes "$idle_memory_growth_bytes" \
  --argjson idleMemoryGrowthPct "$idle_memory_growth_pct" \
  '{
    profile: $profile,
    generatedAt: $generatedAt,
    endpoint: $endpoint,
    config: {
      latencyRequests: $latencyRequests,
      throughputRequests: $throughputRequests,
      throughputPointsPerRequest: $throughputPointsPerRequest,
      idleSeconds: $idleSeconds
    },
    metrics: {
      write_p95_ms: $writeP95Ms,
      ingest_points_per_sec: $ingestPointsPerSec,
      idle_memory_growth_bytes: $idleMemoryGrowthBytes,
      idle_memory_growth_pct: $idleMemoryGrowthPct
    }
  }' > "$OUTPUT_PATH"

cat <<SUMMARY
single-node benchmark complete (${PROFILE})
  output: ${OUTPUT_PATH}
  write p95 (ms): ${write_p95_ms}
  ingest throughput (points/sec): ${ingest_points_per_sec}
  idle memory growth (bytes): ${idle_memory_growth_bytes}
  idle memory growth (%): ${idle_memory_growth_pct}
SUMMARY
