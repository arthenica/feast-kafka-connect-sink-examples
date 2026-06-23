# feast-kafka-connect-sink-examples

A runnable end-to-end demo for the
[`feast-kafka-connect-sink`](https://github.com/arthenica/feast-kafka-connect-sink) connector:
it replays NYC TLC yellow-taxi trips into Kafka and lands them in a real Feast feature server via
the connector.

## Stack

| Service | Image / build | Purpose |
|---|---|---|
| `kafka` | `apache/kafka:4.3.0` (KRaft) | Single-node broker |
| `connect` | `connect/Dockerfile` (Apache Kafka + the plugin) | Kafka Connect worker running the sink |
| `feast` | `feast/` | `feast serve` feature server (sqlite online + file offline) |
| `producer` | `producer/` | Downloads the TLC parquet and replays it as JSON (profile `replay`) |
| `schema-registry` | `confluentinc/cp-schema-registry` | Schema Registry for the Avro/Protobuf paths (profiles `avro`, `protobuf`) |
| `avro-producer` | `avro-producer/` | Replays the TLC parquet as Confluent-framed Avro (profile `avro`) |
| `protobuf-producer` | `protobuf-producer/` | Replays the TLC parquet as Confluent-framed Protobuf (profile `protobuf`) |

## Prerequisites

- Docker + Docker Compose
- The connector plugin ZIP (built from the connector repo with `./gradlew connectPluginZip`)
- JDK 21 to build the local Kafka source-id SMT with its Gradle Wrapper
- Network access (the producer downloads the TLC parquet at runtime)

## Run it

```bash
# 1. Supply the plugin: unzip the connector's distribution ZIP into ./plugin/.
#    (Build it in the connector repo first: ./gradlew connectPluginZip)
unzip /path/to/feast-kafka-connect-sink-0.1.0.zip -d plugin/
#    -> plugin/feast-kafka-connect-sink-0.1.0/lib/*.jar

# 2. Build the local SMT that adds kafka_source_id=<topic>:<partition>:<offset>.
./scripts/build-kafka-source-id-smt.sh
#    -> plugin/kafka-source-id-smt/kafka-source-id-smt.jar

# 3. Start Kafka, Feast, and the Connect worker.
docker compose up --build -d kafka feast connect

# 4. Wait for the Connect REST API, then register the connector.
until curl -sf http://localhost:8083/connectors >/dev/null; do sleep 3; done
./scripts/register-connector.sh json | jq .

# 5. Check the connector is RUNNING.
curl -s http://localhost:8083/connectors/feast-taxi-sink/status | jq .

# 6. Replay the taxi data into the taxi-trips topic.
docker compose --profile replay up --build producer

# 7. Verify features landed in Feast (online store), including kafka_source_id.
./scripts/get-online-features_with_kafka_source_id.sh yellow_2024_01_000000
```

You should see non-null `fare_amount`, `trip_distance`, and `pickup_location_id` for the queried
`trip_id`s, plus a `kafka_source_id` such as `taxi-trips:0:12345`. The script queries the
`taxi_trip_features_with_source_id` feature view by default.

## Kafka source-id SMT

The JSON connector config enables a local SMT that adds Kafka's source position to the record value
before the Feast sink connector receives it:

```json
"transforms": "addSourceId",
"transforms.addSourceId.type": "com.arthenica.kafka.connect.smt.KafkaSourceId$Value",
"transforms.addSourceId.field.name": "kafka_source_id"
```

The SMT is intentionally packaged separately from the Feast sink connector. Build it with:

```bash
./scripts/build-kafka-source-id-smt.sh
```

The script runs the Gradle Wrapper build in `smt/kafka-source-id/` and writes:

```text
plugin/kafka-source-id-smt/kafka-source-id-smt.jar
```

The existing Connect Docker image copies everything under `plugin/` into Kafka Connect's
`plugin.path`, so rebuild and recreate the worker after rebuilding the SMT:

```bash
docker compose build connect
docker compose up -d --force-recreate connect
```

Kafka Connect discovers plugin classes when the worker starts. If the connector was already
registered, unregister and register it again so the updated transform config is applied:

```bash
curl -X DELETE http://localhost:8083/connectors/feast-taxi-sink
./scripts/register-connector.sh json | jq .
```

This SMT works for the JSON demo path because the value is a schemaless JSON object when SMTs run.
The JSON connector writes to `taxi_trips_with_source_id_push_source`, which backs
`taxi_trip_features_with_source_id`.

The original `taxi_trips_push_source` and `taxi_trip_features` remain available for the Avro/Protobuf
demo configs. The SMT is not enabled there because those configs use `ByteArrayConverter`; the record
value is still raw bytes when SMTs run, and the Feast sink connector decodes the payload later.
The original and source-id push sources use separate backing `FileSource` parquet files so offline
writes validate against the correct column set.

## Avro path (Confluent Schema Registry)

The connector also accepts **Confluent-framed Avro**: set
`value.converter=org.apache.kafka.connect.converters.ByteArrayConverter` and `schema.registry.url`,
and the connector decodes the bytes itself using the registry — **no converter JARs are added to the
Connect worker**. This path lives behind the `avro` Compose profile.

```bash
# 1. Start the core stack plus Schema Registry.
docker compose --profile avro up --build -d kafka feast connect schema-registry

# 2. Register the Avro connector (reads taxi-trips-avro, value.converter=ByteArrayConverter).
until curl -sf http://localhost:8083/connectors >/dev/null; do sleep 3; done
./scripts/register-connector.sh avro | jq .

# 3. Produce NYC-taxi events as Avro (registers the schema, frames with magic byte + schema id).
docker compose --profile avro up --build avro-producer

# 4. Verify the Avro-sourced features landed in Feast.
./scripts/get-online-features.sh yellow_avro_2024_02_000000
```

The Avro schema uses a `timestamp-millis` logical type for `event_timestamp`, which the connector's
decoder converts back to ISO-8601 before the push — so both the JSON and Avro paths land in the same
`taxi_trips_push_source`.

> `cp-schema-registry` runs as a plain Kafka client against the `apache/kafka` broker. Match its
> Confluent Platform tag to your Kafka version (CP 8.x ↔ Kafka 4.x) in `docker-compose.yml`.

## Protobuf path (Confluent Schema Registry)

Protobuf works exactly like the Avro path — same `ByteArrayConverter` + `schema.registry.url`, and
the same `schema-registry` service (it serves both) — only the producer and topic differ. Behind
the `protobuf` Compose profile.

```bash
# 1. Start the core stack plus Schema Registry.
docker compose --profile protobuf up --build -d kafka feast connect schema-registry

# 2. Register the Protobuf connector (reads taxi-trips-protobuf).
until curl -sf http://localhost:8083/connectors >/dev/null; do sleep 3; done
./scripts/register-connector.sh protobuf | jq .

# 3. Produce NYC-taxi events as Protobuf (the .proto is compiled at image build time).
docker compose --profile protobuf up --build protobuf-producer

# 4. Verify the Protobuf-sourced features landed in Feast.
FEAST_URL=http://localhost:6566 python verify/verify.py yellow_protobuf_2024_03_000000
```

The connector auto-detects the schema type (Avro vs Protobuf) from the registry, so the same
connector-config shape works for both. The `TaxiTrip` message uses an ISO-8601 string
`event_timestamp`, and the serializer's single-message `[0]` index framing is handled by the
connector's decoder.

## Tuning the replay

Set environment variables on the `producer` service (in `docker-compose.yml` or your shell):

| Variable | Default | Meaning |
|---|---|---|
| `ROW_LIMIT` | `5000` | Rows to replay (`0` = the whole file) |
| `EMIT_RATE` | `200` | Messages per second (`0` = unthrottled) |
| `PARQUET_URL` | yellow_tripdata_2024-01 | TLC parquet to download |

## Inspecting the dead-letter queue

Malformed records and permanently-rejected batches go to the `taxi-trips-dlq` topic with
`__feast_error_*` headers:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic taxi-trips-dlq --from-beginning --property print.headers=true
```

## Tear down

```bash
docker compose --profile replay --profile avro --profile protobuf down -v
```

## Notes

- `config/feast-sink-connector.json` is a copy of the connector's example config; `feast.url`
  is `http://feast:6566`, matching the `feast` service hostname on the compose network.
- The connector reads from the beginning of the topic
  (`consumer.auto.offset.reset=earliest` in `connect/connect-distributed.properties`), so the
  replay can run before or after registering the connector.
- `plugin/` is git-ignored except for `.gitkeep`; you supply the plugin jars at demo time.

## License

**feast-kafka-connect-sink-examples** is licensed under the terms of the Apache License version 2.0. See LICENSE for more details.
