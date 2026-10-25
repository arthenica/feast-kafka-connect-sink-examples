#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 || "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
  echo "Usage: $0 /path/to/feast-kafka-connect-sink-<version>.zip" >&2
  [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]] && exit 0
  exit 2
fi

for command in docker unzip; do
  command -v "$command" >/dev/null || { echo "$command is required" >&2; exit 1; }
done
docker compose version >/dev/null
[[ -f "$1" ]] || { echo "Plugin ZIP not found: $1" >&2; exit 1; }

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
test_dir="$(mktemp -d "${TMPDIR:-/tmp}/feast-dlq-commit.XXXXXX")"
project="feast-dlq-commit-$$-$(date +%s)"
compose=(docker compose --project-name "$project" --file "$repo_root/tests/dlq-commit/compose.yml")

cleanup() {
  local result=$?
  trap - EXIT
  "${compose[@]}" down --volumes --rmi local --remove-orphans --timeout 5 >/dev/null 2>&1 || true
  rm -rf "$test_dir"
  exit "$result"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

export DLQ_COMMIT_CONNECT_CONTEXT="$test_dir/connect"
mkdir -p "$DLQ_COMMIT_CONNECT_CONTEXT/plugin"
unzip -q "$1" -d "$DLQ_COMMIT_CONNECT_CONTEXT/plugin"
cp "$repo_root/tests/dlq-commit/worker.properties" "$repo_root/tests/dlq-commit/Connect.Dockerfile" \
  "$DLQ_COMMIT_CONNECT_CONTEXT/"

echo "Testing DLQ commit durability with plugin: $1"
echo "Starting isolated Kafka and Connect containers (no host ports)."
if "${compose[@]}" up --build --abort-on-container-exit --exit-code-from test >"$test_dir/compose.log" 2>&1; then
  "${compose[@]}" logs --no-color test
else
  cat "$test_dir/compose.log" >&2
  exit 1
fi
