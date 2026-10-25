#!/usr/bin/env bash
# Shared lifecycle and exit-status reporting for the standalone E2E commands.

e2e_cleanup() {
  local result=$?
  trap - EXIT
  if [[ "$E2E_STARTED" == 1 ]]; then
    if ! "${E2E_COMPOSE[@]}" down --volumes --rmi local --remove-orphans --timeout 5 \
      >"$E2E_LOG_DIR/cleanup.log" 2>&1; then
      echo "Container cleanup failed; see $E2E_LOG_DIR/cleanup.log" >&2
      result=1
    fi
  fi
  if [[ -n "$E2E_WORK_DIR" ]] && ! rm -rf "$E2E_WORK_DIR"; then
    echo "Temporary build-context cleanup failed: $E2E_WORK_DIR" >&2
    result=1
  fi
  if [[ "$result" == 0 ]]; then
    echo "[PASS] $E2E_LABEL"
  else
    echo "[FAIL] $E2E_LABEL (exit $result)" >&2
  fi
  if [[ -n "${E2E_LOG_DIR:-}" && -d "$E2E_LOG_DIR" ]]; then
    echo "Logs: $E2E_LOG_DIR"
  fi
  exit "$result"
}

run_e2e() {
  local scenario="$1"
  shift
  E2E_LABEL="$scenario end-to-end"
  if [[ $# == 1 && ( "$1" == --help || "$1" == -h ) ]]; then
    echo "Usage: $0 /path/to/feast-kafka-connect-sink-<version>.zip"
    return 0
  fi
  E2E_STARTED=0
  E2E_WORK_DIR=""
  trap e2e_cleanup EXIT
  trap 'exit 130' INT
  trap 'exit 143' TERM
  if [[ $# != 1 ]]; then
    echo "Usage: $0 /path/to/feast-kafka-connect-sink-<version>.zip" >&2
    return 2
  fi
  [[ -f "$1" ]] || { echo "Plugin ZIP not found: $1" >&2; return 2; }
  local command
  for command in docker unzip; do
    command -v "$command" >/dev/null || { echo "$command is required" >&2; return 1; }
  done
  docker compose version >/dev/null
  local repo_root
  repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
  E2E_LOG_DIR="${E2E_LOG_DIR:-$repo_root/test-results/$scenario-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
  mkdir -p "$E2E_LOG_DIR"
  E2E_LOG_DIR="$(cd "$E2E_LOG_DIR" && pwd)"
  E2E_WORK_DIR="$(mktemp -d "${TMPDIR:-/tmp}/feast-e2e.XXXXXX")"
  local project="feast-e2e-$scenario-$$-$(date +%s)"
  local connect_context="$E2E_WORK_DIR/connect"
  mkdir -p "$connect_context/plugin"
  unzip -q "$1" -d "$connect_context/plugin"

  if [[ "$scenario" == dlq-commit ]]; then
    export DLQ_COMMIT_CONNECT_CONTEXT="$connect_context"
    cp "$repo_root/tests/dlq-commit/worker.properties" "$repo_root/tests/dlq-commit/Connect.Dockerfile" \
      "$connect_context/"
    E2E_COMPOSE=(docker compose -p "$project" -f "$repo_root/tests/dlq-commit/compose.yml")
  else
    export E2E_SCENARIO="$scenario"
    export E2E_CONNECT_CONTEXT="$connect_context"
    export E2E_TEST_CONTEXT="$E2E_WORK_DIR/test"
    export E2E_FEAST_CONTEXT="$E2E_WORK_DIR/feast"
    cp "$repo_root/tests/e2e/worker.properties" "$repo_root/tests/e2e/Connect.Dockerfile" "$connect_context/"
    cp "$repo_root/smt/kafka-source-id/src/main/java/com/arthenica/kafka/connect/smt/KafkaSourceId.java" \
      "$connect_context/"
    mkdir -p "$E2E_TEST_CONTEXT/producers" "$E2E_TEST_CONTEXT/config" "$E2E_FEAST_CONTEXT"
    cp "$repo_root/tests/e2e/Test.Dockerfile" "$repo_root/tests/e2e/run.py" \
      "$repo_root/tests/e2e/fixtures.py" "$E2E_TEST_CONTEXT/"
    cp "$repo_root/producer/replay_producer.py" "$repo_root/avro-producer/avro_producer.py" \
      "$repo_root/protobuf-producer/protobuf_producer.py" "$repo_root/protobuf-producer/taxi_trip.proto" \
      "$E2E_TEST_CONTEXT/producers/"
    cp "$repo_root/config/"*.json "$E2E_TEST_CONTEXT/config/"
    cat "$repo_root/producer/requirements.txt" "$repo_root/avro-producer/requirements.txt" \
      "$repo_root/protobuf-producer/requirements.txt" >"$E2E_TEST_CONTEXT/requirements.txt"
    cp "$repo_root/feast/Dockerfile" "$repo_root/feast/features.py" "$repo_root/feast/seed_data.py" \
      "$repo_root/feast/feature_store.yaml" "$E2E_FEAST_CONTEXT/"
    cp "$repo_root/tests/e2e/fixtures.py" "$repo_root/tests/e2e/verify_offline.py" "$E2E_FEAST_CONTEXT/"
    printf '\nCOPY fixtures.py verify_offline.py ./\n' >>"$E2E_FEAST_CONTEXT/Dockerfile"
    E2E_COMPOSE=(docker compose -p "$project" -f "$repo_root/tests/e2e/compose.yml")
    [[ "$scenario" == json ]] || E2E_COMPOSE+=(--profile registry)
  fi

  echo "[RUN] $E2E_LABEL with $1"
  echo "Logs: $E2E_LOG_DIR"
  E2E_STARTED=1
  local result=0
  "${E2E_COMPOSE[@]}" up --build --abort-on-container-exit --exit-code-from test \
    >"$E2E_LOG_DIR/compose.log" 2>&1 || result=$?
  "${E2E_COMPOSE[@]}" logs --no-color test >"$E2E_LOG_DIR/test.log" 2>&1 || true
  if [[ "$result" != 0 ]]; then
    tail -n 80 "$E2E_LOG_DIR/compose.log" >&2
    cat "$E2E_LOG_DIR/test.log" >&2
    return "$result"
  fi
  cat "$E2E_LOG_DIR/test.log"
  if [[ "$scenario" != dlq-commit ]]; then
    if ! "${E2E_COMPOSE[@]}" run --rm --no-deps -T --entrypoint python feast \
      verify_offline.py "$scenario" >"$E2E_LOG_DIR/offline.log" 2>&1; then
      cat "$E2E_LOG_DIR/offline.log" >&2
      return 1
    fi
    cat "$E2E_LOG_DIR/offline.log"
  fi
}
