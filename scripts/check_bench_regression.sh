#!/usr/bin/env bash
set -euo pipefail

THRESHOLD="${1:-50}"
THRESHOLD_FRAC=$(awk "BEGIN {printf \"%.6f\", $THRESHOLD / 100}")
BENCH_FILTER="${TSINK_BENCH_REGRESSION_FILTER:-}"

FILES=()
while IFS= read -r f; do
  bench="${f#target/criterion/}"
  bench="${bench%/change/estimates.json}"
  if [[ -n "$BENCH_FILTER" ]] && ! [[ "$bench" =~ $BENCH_FILTER ]]; then
    continue
  fi
  FILES+=("$f")
done < <(
  if [[ -d target/criterion ]]; then
    find target/criterion -type f -path '*/change/estimates.json' -print | LC_ALL=C sort
  fi
)

if [[ ${#FILES[@]} -eq 0 ]]; then
  echo "REGRESSION_CHECK status=skipped reason=no_change_estimates"
  echo "No in-scope Criterion change estimates were produced; no regression comparison was performed."
  exit 0
fi

FAILED=0
for f in "${FILES[@]}"; do
  bench="${f#target/criterion/}"
  bench="${bench%/change/estimates.json}"

  if ! pct=$(jq -er '.mean.point_estimate | select(type == "number")' "$f"); then
    echo "FAIL: $bench has no numeric mean point estimate"
    FAILED=1
    continue
  fi
  display=$(awk "BEGIN {printf \"%.1f\", $pct * 100}")

  if awk "BEGIN {exit !($pct > $THRESHOLD_FRAC)}"; then
    echo "FAIL: $bench regressed by ${display}% (threshold: ${THRESHOLD}%)"
    FAILED=1
  else
    echo "  ok: $bench changed by ${display}%"
  fi
done

echo ""
if [[ "$FAILED" -eq 1 ]]; then
  echo "Criterion regression check failed: a comparison exceeded the ${THRESHOLD}% threshold or a change estimate was invalid."
  exit 1
else
  echo "All in-scope cached benchmark comparisons are within the ${THRESHOLD}% threshold."
fi
