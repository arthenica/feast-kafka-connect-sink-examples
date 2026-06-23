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

"""Replays NYC TLC yellow-taxi trips into Kafka as Confluent-framed Protobuf.

Serializes each event with the Confluent ``ProtobufSerializer`` (registers the .proto in Schema
Registry and frames the payload as magic-byte + schema-id + message-index + protobuf body, using
the modern ``use.deprecated.format=False`` encoding). The connector consumes the bytes via
``ByteArrayConverter`` and decodes them itself using ``schema.registry.url``.

``taxi_trip_pb2`` is generated from ``taxi_trip.proto`` at image build time (see Dockerfile).

Environment variables:
  BOOTSTRAP_SERVERS    Kafka bootstrap servers           (default: kafka:9092)
  SCHEMA_REGISTRY_URL  Schema Registry URL               (default: http://schema-registry:8081)
  TOPIC                Target topic                      (default: taxi-trips-protobuf)
  PARQUET_URL          TLC parquet URL                   (default: yellow_tripdata_2024-01)
  ROW_LIMIT            Max rows to replay, 0 = all       (default: 5000)
  EMIT_RATE            Messages per second, 0 = unthrottled (default: 200)
"""

import os
import tempfile
import time
import urllib.request

import pandas as pd
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.protobuf import ProtobufSerializer
from confluent_kafka.serialization import MessageField, SerializationContext, StringSerializer

import taxi_trip_pb2

BOOTSTRAP = os.environ.get("BOOTSTRAP_SERVERS", "kafka:9092")
SCHEMA_REGISTRY_URL = os.environ.get("SCHEMA_REGISTRY_URL", "http://schema-registry:8081")
TOPIC = os.environ.get("TOPIC", "taxi-trips-protobuf")
PARQUET_URL = os.environ.get(
    "PARQUET_URL",
    "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-03.parquet",
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


def to_message(index: int, row: pd.Series) -> "taxi_trip_pb2.TaxiTrip":
    timestamp = pd.to_datetime(row["tpep_pickup_datetime"]).strftime("%Y-%m-%dT%H:%M:%SZ")
    passengers = row.get("passenger_count")
    return taxi_trip_pb2.TaxiTrip(
        trip_id=f"yellow_protobuf_2024_03_{index:06d}",
        event_timestamp=timestamp,
        pickup_location_id=int(row["PULocationID"]),
        dropoff_location_id=int(row["DOLocationID"]),
        passenger_count=int(passengers) if pd.notna(passengers) else 0,
        trip_distance=float(row["trip_distance"]),
        fare_amount=float(row["fare_amount"]),
        tip_amount=float(row["tip_amount"]),
    )


def main() -> None:
    path = download(PARQUET_URL)
    frame = pd.read_parquet(path)
    if ROW_LIMIT > 0:
        frame = frame.head(ROW_LIMIT)

    schema_registry = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    protobuf_serializer = ProtobufSerializer(
        taxi_trip_pb2.TaxiTrip, schema_registry, {"use.deprecated.format": False}
    )
    key_serializer = StringSerializer("utf_8")
    producer = Producer({"bootstrap.servers": BOOTSTRAP})

    interval = 1.0 / EMIT_RATE if EMIT_RATE > 0 else 0.0
    count = 0
    for index, row in frame.iterrows():
        message = to_message(int(index), row)
        value = protobuf_serializer(message, SerializationContext(TOPIC, MessageField.VALUE))
        key = key_serializer(message.trip_id)
        producer.produce(TOPIC, key=key, value=value)
        count += 1
        if count % 500 == 0:
            producer.poll(0)
            print(f"Produced {count} Protobuf events", flush=True)
        if interval:
            time.sleep(interval)

    producer.flush()
    print(f"Done. Produced {count} Protobuf events to topic '{TOPIC}'.", flush=True)


if __name__ == "__main__":
    main()
