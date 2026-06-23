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

"""Feast feature definitions for the NYC taxi demo.

Entity:  trip (join key: trip_id)
Source:  a PushSource backed by a FileSource, so the connector can push rows to the
         online and/or offline stores via POST {feast.url}/push.
"""

from datetime import timedelta

from feast import Entity, FeatureView, Field, FileSource, PushSource, ValueType
from feast.types import Float64, Int64, String

trip = Entity(name="trip", join_keys=["trip_id"], value_type=ValueType.STRING)

taxi_batch_source = FileSource(
    name="taxi_batch_source",
    path="data/taxi_trips.parquet",
    timestamp_field="event_timestamp",
)

taxi_with_source_id_batch_source = FileSource(
    name="taxi_with_source_id_batch_source",
    path="data/taxi_trips_with_source_id.parquet",
    timestamp_field="event_timestamp",
)

taxi_trips_push_source = PushSource(
    name="taxi_trips_push_source",
    batch_source=taxi_batch_source,
)

taxi_trips_with_source_id_push_source = PushSource(
    name="taxi_trips_with_source_id_push_source",
    batch_source=taxi_with_source_id_batch_source,
)

taxi_trip_fields = [
    Field(name="pickup_location_id", dtype=Int64),
    Field(name="dropoff_location_id", dtype=Int64),
    Field(name="passenger_count", dtype=Int64),
    Field(name="trip_distance", dtype=Float64),
    Field(name="fare_amount", dtype=Float64),
    Field(name="tip_amount", dtype=Float64),
]

taxi_trip_features = FeatureView(
    name="taxi_trip_features",
    entities=[trip],
    ttl=timedelta(days=3650),
    online=True,
    source=taxi_trips_push_source,
    schema=taxi_trip_fields,
)

taxi_trip_features_with_source_id = FeatureView(
    name="taxi_trip_features_with_source_id",
    entities=[trip],
    ttl=timedelta(days=3650),
    online=True,
    source=taxi_trips_with_source_id_push_source,
    schema=taxi_trip_fields + [
        Field(name="kafka_source_id", dtype=String),
    ],
)
