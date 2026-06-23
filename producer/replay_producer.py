# Copyright 2026 Taner Sener
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Replays NYC TLC yellow-taxi trips into a Kafka topic as JSON feature events.

Downloads the monthly TLC parquet at runtime, maps TLC columns to the event schema the
connector expects, and produces one JSON message per trip keyed by trip_id.

Environment variables:
  BOOTSTRAP_SERVERS  Kafka bootstrap servers           (default: kafka:9092)
  TOPIC              Target topic                       (default: taxi-trips)
  PARQUET_URL        TLC parquet URL                    (default: yellow_tripdata_2024-01)
  ROW_LIMIT          Max rows to replay, 0 = all        (default: 5000)
  EMIT_RATE          Messages per second, 0 = unthrottled (default: 200)
"""

import json
import os
import tempfile
import time
import urllib.request

import pyarrow.parquet as pq

import pandas as pd
from confluent_kafka import Producer

REQUIRED_COLUMNS = [
    "tpep_pickup_datetime",
    "PULocationID",
    "DOLocationID",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "tip_amount",
]

BOOTSTRAP = os.environ.get("BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.environ.get("TOPIC", "taxi-trips")
PARQUET_URL = os.environ.get(
    "PARQUET_URL",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet",
)
ROW_LIMIT = int(os.environ.get("ROW_LIMIT", "5000"))
EMIT_RATE = float(os.environ.get("EMIT_RATE", "200"))


def download(url: str) -> str:
    print(f"Downloading {url} ...", flush=True)
    fd, path = tempfile.mkstemp(suffix=".parquet")
    os.close(fd)
    urllib.request.urlretrieve(url, path)
    print(f"Saved parquet to {path}", flush=True)
    return path


def to_event(index: int, row: pd.Series) -> dict:
    timestamp = pd.to_datetime(row["tpep_pickup_datetime"]).strftime("%Y-%m-%dT%H:%M:%SZ")
    passengers = row.get("passenger_count")
    return {
        "trip_id": f"yellow_2024_01_{index:06d}",
        "event_timestamp": timestamp,
        "pickup_location_id": int(row["PULocationID"]),
        "dropoff_location_id": int(row["DOLocationID"]),
        "passenger_count": int(passengers) if pd.notna(passengers) else 0,
        "trip_distance": float(row["trip_distance"]),
        "fare_amount": float(row["fare_amount"]),
        "tip_amount": float(row["tip_amount"]),
    }


def main() -> None:
    path = download(PARQUET_URL)
    parquet = pq.ParquetFile(path)
    count = 0

    producer = Producer({"bootstrap.servers": BOOTSTRAP})
    interval = 1.0 / EMIT_RATE if EMIT_RATE > 0 else 0.0

    for batch in parquet.iter_batches(batch_size=1024, columns=REQUIRED_COLUMNS):
        frame = batch.to_pandas()

        for _, row in frame.iterrows():
            if ROW_LIMIT > 0 and count >= ROW_LIMIT:
                break

            event = to_event(count, row)
            producer.produce(TOPIC, key=event["trip_id"], value=json.dumps(event))
            count += 1
            if count % 500 == 0:
                producer.poll(0)
                print(f"Produced {count} events", flush=True)
            if interval:
                time.sleep(interval)

        if ROW_LIMIT > 0 and count >= ROW_LIMIT:
            break

    producer.flush()
    print(f"Done. Produced {count} events to topic '{TOPIC}'.", flush=True)


if __name__ == "__main__":
    main()
