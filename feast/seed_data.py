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

"""Writes seed parquets matching each FeatureView schema so FileSource paths exist.

Real feature values arrive at runtime via the connector.
"""

from datetime import datetime, timezone

import pandas as pd

base_row = {
    "trip_id": "seed_0",
    "event_timestamp": datetime(2024, 1, 1, tzinfo=timezone.utc),
    "pickup_location_id": 0,
    "dropoff_location_id": 0,
    "passenger_count": 0,
    "trip_distance": 0.0,
    "fare_amount": 0.0,
    "tip_amount": 0.0,
}

pd.DataFrame([base_row]).to_parquet("data/taxi_trips.parquet", index=False)

source_id_row = {
    **base_row,
    "kafka_source_id": "seed",
}

pd.DataFrame([source_id_row]).to_parquet("data/taxi_trips_with_source_id.parquet", index=False)
