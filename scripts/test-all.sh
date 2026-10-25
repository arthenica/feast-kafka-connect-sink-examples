#!/usr/bin/env bash
set -euo pipefail

if [[ $# == 1 && ( "$1" == --help || "$1" == -h ) ]]; then
  echo "Usage: $0 /path/to/feast-kafka-connect-sink-<version>.zip"
  exit 0
fi
if [[ $# != 1 ]]; then
  echo "Usage: $0 /path/to/feast-kafka-connect-sink-<version>.zip" >&2
  echo "[FAIL] end-to-end suite (invalid arguments)" >&2
  exit 2
fi
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
results_dir="${E2E_LOG_DIR:-$script_dir/../test-results/all-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
mkdir -p "$results_dir"
results_dir="$(cd "$results_dir" && pwd)"
scenarios=(json avro protobuf dlq-commit)
statuses=()
failures=0
trap 'exit 130' INT
trap 'exit 143' TERM
for scenario in "${scenarios[@]}"; do
  if E2E_LOG_DIR="$results_dir/$scenario" "$script_dir/test-$scenario.sh" "$1"; then
    statuses+=(PASS)
  else
    result=$?
    statuses+=("FAIL (exit $result)")
    failures=$((failures + 1))
  fi
done
{
  printf '\nEnd-to-end test summary\n'
  for index in "${!scenarios[@]}"; do
    printf '  %-12s %s\n' "${scenarios[$index]}" "${statuses[$index]}"
  done
  printf '%s passed, %s failed\n' "$((${#scenarios[@]} - failures))" "$failures"
} | tee "$results_dir/summary.txt"
echo "Logs and summary: $results_dir"
[[ "$failures" == 0 ]]
