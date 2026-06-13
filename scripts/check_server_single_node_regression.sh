#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

LATEST_PATH="${TSINK_SERVER_BENCH_OUTPUT:-target/server-bench/latest.json}"
BASELINE_PATH="${TSINK_SERVER_BENCH_BASELINE:-target/server-bench/baseline.json}"
WRITE_THRESHOLD_PCT="${TSINK_SERVER_WRITE_P95_THRESHOLD_PCT:-3}"
INGEST_THRESHOLD_PCT="${TSINK_SERVER_INGEST_THRESHOLD_PCT:-2}"
IDLE_GROWTH_THRESHOLD_BYTES="${TSINK_SERVER_IDLE_GROWTH_THRESHOLD_BYTES:-1048576}"
UPDATE_BASELINE="${TSINK_SERVER_UPDATE_BASELINE:-0}"

if ! command -v jq >/dev/null 2>&1; then
  echo "missing required command: jq" >&2
  exit 1
fi
if ! command -v awk >/dev/null 2>&1; then
  echo "missing required command: awk" >&2
  exit 1
fi

if [[ ! -f "$LATEST_PATH" ]]; then
  echo "latest benchmark file not found: $LATEST_PATH" >&2
  exit 1
fi

mkdir -p "$(dirname "$BASELINE_PATH")"

if [[ ! -f "$BASELINE_PATH" ]]; then
  cp "$LATEST_PATH" "$BASELINE_PATH"
  echo "No baseline found. Bootstrapped baseline at: $BASELINE_PATH"
  exit 0
fi

baseline_write_p95="$(jq -r '.metrics.write_p95_ms // 0' "$BASELINE_PATH")"
baseline_ingest="$(jq -r '.metrics.ingest_points_per_sec // 0' "$BASELINE_PATH")"
baseline_idle_growth="$(jq -r '.metrics.idle_memory_growth_bytes // 0' "$BASELINE_PATH")"

current_write_p95="$(jq -r '.metrics.write_p95_ms // 0' "$LATEST_PATH")"
current_ingest="$(jq -r '.metrics.ingest_points_per_sec // 0' "$LATEST_PATH")"
current_idle_growth="$(jq -r '.metrics.idle_memory_growth_bytes // 0' "$LATEST_PATH")"

write_regression_pct="$(
  awk -v current="$current_write_p95" -v baseline="$baseline_write_p95" '
    BEGIN {
      if (baseline <= 0) {
        printf "0.000"
      } else {
        printf "%.3f", ((current - baseline) * 100.0) / baseline
      }
    }'
)"

ingest_regression_pct="$(
  awk -v current="$current_ingest" -v baseline="$baseline_ingest" '
    BEGIN {
      if (baseline <= 0) {
        printf "0.000"
      } else {
        printf "%.3f", ((baseline - current) * 100.0) / baseline
      }
    }'
)"

echo "Server single-node regression report"
echo "  baseline: $BASELINE_PATH"
echo "  current:  $LATEST_PATH"
echo ""
echo "  write_p95_ms: baseline=${baseline_write_p95} current=${current_write_p95} regression=${write_regression_pct}%"
echo "  ingest_points_per_sec: baseline=${baseline_ingest} current=${current_ingest} regression=${ingest_regression_pct}%"
echo "  idle_memory_growth_bytes: baseline=${baseline_idle_growth} current=${current_idle_growth}"
echo ""

FAILED=0

if awk "BEGIN { exit !($write_regression_pct > $WRITE_THRESHOLD_PCT) }"; then
  echo "FAIL: write p95 regression ${write_regression_pct}% exceeds ${WRITE_THRESHOLD_PCT}% threshold"
  FAILED=1
else
  echo "ok: write p95 regression ${write_regression_pct}% within ${WRITE_THRESHOLD_PCT}% threshold"
fi

if awk "BEGIN { exit !($ingest_regression_pct > $INGEST_THRESHOLD_PCT) }"; then
  echo "FAIL: ingest throughput regression ${ingest_regression_pct}% exceeds ${INGEST_THRESHOLD_PCT}% threshold"
  FAILED=1
else
  echo "ok: ingest throughput regression ${ingest_regression_pct}% within ${INGEST_THRESHOLD_PCT}% threshold"
fi

if awk "BEGIN { exit !($current_idle_growth > $IDLE_GROWTH_THRESHOLD_BYTES) }"; then
  echo "FAIL: idle memory growth ${current_idle_growth} bytes exceeds ${IDLE_GROWTH_THRESHOLD_BYTES} byte threshold"
  FAILED=1
else
  echo "ok: idle memory growth ${current_idle_growth} bytes within ${IDLE_GROWTH_THRESHOLD_BYTES} byte threshold"
fi

if [[ "$UPDATE_BASELINE" == "1" || "$UPDATE_BASELINE" == "true" ]]; then
  cp "$LATEST_PATH" "$BASELINE_PATH"
  echo "Baseline updated at: $BASELINE_PATH"
fi

if [[ "$FAILED" -eq 1 ]]; then
  exit 1
fi
