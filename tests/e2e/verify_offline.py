"""Runs in the stopped test stack's Feast data volume after online checks succeed."""
import sys
import pandas as pd
from fixtures import assert_value, expected_rows, timestamp

scenario = sys.argv[1]
suffix = "_with_source_id" if scenario == "json" else ""
frame = pd.read_parquet(f"data/taxi_trips{suffix}.parquet")
rows = expected_rows(scenario)
ids = [row["trip_id"] for row in rows]
actual = frame[frame["trip_id"].isin(ids)]
assert len(actual) == len(rows), f"Expected {len(rows)} offline rows, got {len(actual)}"
for expected in rows:
    matches = actual[actual["trip_id"] == expected["trip_id"]]
    assert len(matches) == 1, f"Missing or duplicate offline row: {expected['trip_id']}"
    row = matches.iloc[0]
    for field, value in expected.items():
        if field == "event_timestamp":
            assert pd.Timestamp(row[field]) == pd.Timestamp(timestamp(value)), \
                f"Incorrect offline timestamp for {expected['trip_id']}"
        else:
            assert_value(row[field], value, f"offline {expected['trip_id']}.{field}")
print(f"Verified {len(rows)} offline rows for {scenario}, including values and timestamps.")
