#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

MODE="${1:-quick}"

case "$MODE" in
  quick)
    CRITERION_ARGS=(--quick --noplot)
    FILTER='^(insert_rows|select)/|^persist_refresh_long_history/64$'
    REFRESH_CASES='64 (smoke)'
    ;;
  full)
    CRITERION_ARGS=(--noplot)
    FILTER='^(insert_rows|select|persist_refresh_long_history)/'
    REFRESH_CASES='64, 256, 1024'
    ;;
  *)
    echo "Usage: $0 [quick|full] [extra criterion args...]" >&2
    exit 1
    ;;
esac

BENCH_ARGS=("$FILTER" "${CRITERION_ARGS[@]}")
if (($# > 1)); then
  BENCH_ARGS+=("${@:2}")
fi

cat <<INFO
Running storage performance matrix ($MODE):
  insert_rows: 1, 10, 1000
  select: 1, 10, 1000, 1000000
  persist_refresh_long_history: $REFRESH_CASES
INFO

TSINK_STORAGE_BENCH_SUITE="$MODE" \
  cargo bench --bench storage_benchmarks -- "${BENCH_ARGS[@]}"
