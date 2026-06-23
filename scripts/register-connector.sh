#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 [json|avro|protobuf]" >&2
}

profile="${1:-json}"
case "$profile" in
  json)
    config_name="feast-sink-connector.json"
    ;;
  avro)
    config_name="feast-sink-connector-avro.json"
    ;;
  protobuf)
    config_name="feast-sink-connector-protobuf.json"
    ;;
  -h|--help)
    usage
    exit 0
    ;;
  *)
    usage
    exit 2
    ;;
esac

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
config_path="$repo_root/config/$config_name"
connect_url="${CONNECT_URL:-http://localhost:8083}"

if [[ ! -s "$config_path" ]]; then
  echo "Connector config is missing or empty: $config_path" >&2
  exit 1
fi

if command -v python3 >/dev/null 2>&1; then
  python3 -m json.tool "$config_path" >/dev/null
fi

curl --fail-with-body -sS \
  -X POST \
  -H 'Content-Type: application/json' \
  --data-binary "@$config_path" \
  "$connect_url/connectors"
echo
