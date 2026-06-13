set -euo pipefail

THRESHOLD="${1:-50}"
THRESHOLD_FRAC=$(awk "BEGIN {printf \"%.6f\", $THRESHOLD / 100}")

shopt -s globstar nullglob
FILES=(target/criterion/**/change/estimates.json)

if [[ ${#FILES[@]} -eq 0 ]]; then
  echo "No baseline found — skipping regression check (first run)."
  exit 0
fi

FAILED=0
for f in "${FILES[@]}"; do
  bench="${f#target/criterion/}"
  bench="${bench%/change/estimates.json}"

  pct=$(jq -r '.mean.point_estimate' "$f")
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
  echo "One or more benchmarks regressed beyond the ${THRESHOLD}% threshold."
  exit 1
else
  echo "All benchmarks within the ${THRESHOLD}% regression threshold."
fi
