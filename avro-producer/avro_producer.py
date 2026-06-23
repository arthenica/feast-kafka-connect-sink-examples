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

"""Replays NYC TLC yellow-taxi trips into Kafka as Confluent-framed Avro.

Serializes each event with the Confluent ``AvroSerializer`` (registers the schema in Schema
Registry and frames the payload as magic-byte + schema-id + Avro body). The connector consumes
the bytes via ``ByteArrayConverter`` and decodes them itself using ``schema.registry.url``.

``event_timestamp`` is an Avro ``timestamp-millis`` logical type; the connector's AvroRecordDecoder
converts it back to an ISO-8601 string for the Feast push.

Environment variables:
  BOOTSTRAP_SERVERS    Kafka bootstrap servers           (default: kafka:9092)
  SCHEMA_REGISTRY_URL  Schema Registry URL               (default: http://schema-registry:8081)
  TOPIC                Target topic                      (default: taxi-trips-avro)
  PARQUET_URL          TLC parquet URL                   (default: yellow_tripdata_2024-01)
  ROW_LIMIT            Max rows to replay, 0 = all       (default: 5000)
  EMIT_RATE            Messages per second, 0 = unthrottled (default: 200)
"""

import os
import tempfile
import time
import urllib.request
from datetime import timezone

import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

BOOTSTRAP = os.environ.get("BOOTSTRAP_SERVERS", "kafka:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPIC = os.environ.get("TOPIC", "taxi-trips-avro")
PARQUET_URL = os.environ.get(
    "PARQUET_URL",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-02.parquet",
)
ROW_LIMIT = int(os.environ.get("ROW_LIMIT", "5000"))
EMIT_RATE = float(os.environ.get("EMIT_RATE", "200"))

SCHEMA_STR = """
{
  "type": "record",
  "name": "TaxiTrip",
  "namespace": "com.arthenica.feast.demo",
  "fields": [
    {"name": "trip_id", "type": "string"},
    {"name": "event_timestamp", "type": {"type": "long", "logicalType": "timestamp-millis"}},
    {"name": "pickup_location_id", "type": "long"},
    {"name": "dropoff_location_id", "type": "long"},
    {"name": "passenger_count", "type": "long"},
    {"name": "trip_distance", "type": "double"},
    {"name": "fare_amount", "type": "double"},
    {"name": "tip_amount", "type": "double"}
  ]
}
"""


def download(url: str) -> str:
    print(f"Downloading {url} ...", flush=True)
    fd, path = tempfile.mkstemp(suffix=".parquet")
    os.close(fd)
    urllib.request.urlretrieve(url, path)
    print(f"Saved parquet to {path}", flush=True)
    return path


def to_event(index: int, row: pd.Series) -> dict:
    # Aware datetime so fastavro encodes the timestamp-millis logical type correctly.
    timestamp = pd.to_datetime(row["tpep_pickup_datetime"]).to_pydatetime().replace(tzinfo=timezone.utc)
    passengers = row.get("passenger_count")
    return {
        "trip_id": f"yellow_avro_2024_02_{index:06d}",
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
    frame = pd.read_parquet(path)
    if ROW_LIMIT > 0:
        frame = frame.head(ROW_LIMIT)

    schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    avro_serializer = AvroSerializer(schema_registry, SCHEMA_STR, lambda obj, ctx: obj)
    key_serializer = StringSerializer("utf_8")
    producer = Producer({"bootstrap.servers": BOOTSTRAP})

    interval = 1.0 / EMIT_RATE if EMIT_RATE > 0 else 0.0
    count = 0
    for index, row in frame.iterrows():
        event = to_event(int(index), row)
        value = avro_serializer(event, SerializationContext(TOPIC, MessageField.VALUE))
        key = key_serializer(event["trip_id"])
        producer.produce(TOPIC, key=key, value=value)
        count += 1
        if count % 500 == 0:
            producer.poll(0)
            print(f"Produced {count} Avro events", flush=True)
        if interval:
            time.sleep(interval)

    producer.flush()
    print(f"Done. Produced {count} Avro events to topic '{TOPIC}'.", flush=True)


if __name__ == "__main__":
    main()
