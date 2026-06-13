#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

MODE="${1:-quick}"
EXTRA_ARGS=()
if (($# > 1)); then
  EXTRA_ARGS=("${@:2}")
fi

case "$MODE" in
  quick)
    CRITERION_ARGS=(--quick --noplot)
    ;;
  full)
    CRITERION_ARGS=(--noplot)
    ;;
  *)
    echo "Usage: $0 [quick|full] [extra criterion args...]" >&2
    exit 1
    ;;
esac

FILTER='^(insert_rows|select)/'

cat <<INFO
Running storage performance matrix ($MODE):
  insert_rows: 1, 10, 1000
  select: 1, 10, 1000, 1000000
INFO

cargo bench --bench storage_benchmarks -- "$FILTER" "${CRITERION_ARGS[@]}" "${EXTRA_ARGS[@]}"
