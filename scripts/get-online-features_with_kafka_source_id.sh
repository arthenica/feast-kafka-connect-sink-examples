#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 <trip_id>" >&2
  echo "Set FEAST_URL to override the default http://localhost:6566." >&2
  echo "Set FEATURE_VIEW to override the default taxi_trip_features_with_source_id." >&2
}

if [[ "${1:-}" == "-h" || "${1:-}" == "--help" ]]; then
  usage
  exit 0
fi

if [[ $# -ne 1 || -z "${1:-}" ]]; then
  usage
  exit 2
fi

trip_id="$1"
feast_url="${FEAST_URL:-http://localhost:6566}"
feature_view="${FEATURE_VIEW:-taxi_trip_features_with_source_id}"

if ! command -v python3 >/dev/null 2>&1; then
  echo "python3 is required to build the JSON request body." >&2
  exit 1
fi

request_body="$(
  FEATURE_VIEW="$feature_view" TRIP_ID="$trip_id" python3 - <<'PY'
import json
import os

feature_view = os.environ["FEATURE_VIEW"]

print(
    json.dumps(
        {
            "features": [
                f"{feature_view}:pickup_location_id",
                f"{feature_view}:dropoff_location_id",
                f"{feature_view}:passenger_count",
                f"{feature_view}:trip_distance",
                f"{feature_view}:fare_amount",
                f"{feature_view}:tip_amount",
                f"{feature_view}:kafka_source_id",
            ],
            "entities": {
                "trip_id": [os.environ["TRIP_ID"]],
            },
        }
    )
)
PY
)"

response="$(
  curl --fail-with-body -sS \
    -X POST \
    -H 'Content-Type: application/json' \
    -d "$request_body" \
    "$feast_url/get-online-features"
)"

if command -v jq >/dev/null 2>&1; then
  jq . <<<"$response"
else
  printf '%s\n' "$response"
fi
