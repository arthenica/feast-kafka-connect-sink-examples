"""Reads features back from the Feast feature server to prove the round trip.

Usage:
  FEAST_URL=http://localhost:6566 python verify.py [trip_id ...]
"""

import json
import os
import sys
import urllib.request

FEAST_URL = os.environ.get("FEAST_URL", "http://localhost:6566")
TRIP_IDS = sys.argv[1:] or ["yellow_2024_01_000000", "yellow_2024_01_000001"]

body = {
    "features": [
        "taxi_trip_features:fare_amount",
        "taxi_trip_features:trip_distance",
        "taxi_trip_features:pickup_location_id",
    ],
    "entities": {"trip_id": TRIP_IDS},
}

request = urllib.request.Request(
    f"{FEAST_URL}/get-online-features",
    data=json.dumps(body).encode("utf-8"),
    headers={"Content-Type": "application/json"},
    method="POST",
)

with urllib.request.urlopen(request) as response:
    print(json.dumps(json.load(response), indent=2))
