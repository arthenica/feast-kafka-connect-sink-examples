"""Small deterministic inputs and independent expected results for the demo producers."""
from datetime import datetime
import math

VALUES = [
    dict(pickup_location_id=161, dropoff_location_id=162, passenger_count=1,
         trip_distance=2.25, fare_amount=12.5, tip_amount=1.5),
    dict(pickup_location_id=163, dropoff_location_id=164, passenger_count=0,
         trip_distance=0.0, fare_amount=8.0, tip_amount=0.0),
    dict(pickup_location_id=165, dropoff_location_id=166, passenger_count=2,
         trip_distance=3.5, fare_amount=24.0, tip_amount=4.25),
]
TIMESTAMPS = [f"2024-01-01T00:0{i}:00Z" for i in range(len(VALUES))]
TOPICS = dict(json="taxi-trips", avro="taxi-trips-avro", protobuf="taxi-trips-protobuf")
PREFIXES = dict(json="yellow_2024_01", avro="yellow_avro_2024_02", protobuf="yellow_protobuf_2024_03")


def expected_rows(scenario):
    rows = []
    for index, values in enumerate(VALUES):
        row = dict(values, trip_id=f"{PREFIXES[scenario]}_{index:06d}",
                   event_timestamp=TIMESTAMPS[index])
        if scenario == "json":
            row["kafka_source_id"] = f"{TOPICS[scenario]}:0:{index}"
        rows.append(row)
    return rows


def assert_value(actual, expected, context):
    if isinstance(expected, (int, float)):
        assert actual is not None and math.isclose(float(actual), expected, abs_tol=1e-6), \
            f"{context}: expected {expected!r}, got {actual!r}"
    else:
        assert actual == expected, f"{context}: expected {expected!r}, got {actual!r}"


def timestamp(value):
    return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
