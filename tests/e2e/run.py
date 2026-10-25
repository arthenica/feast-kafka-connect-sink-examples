"""Exercises the real demo producer, Connect worker, Schema Registry, and Feast server."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import traceback
import urllib.error
import urllib.request

import pandas as pd
from confluent_kafka import Consumer, ConsumerGroupTopicPartitions, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic
from fixtures import TIMESTAMPS, TOPICS, VALUES, assert_value, expected_rows, timestamp

CASE = os.environ["E2E_SCENARIO"]
ROWS = expected_rows(CASE)
TOPIC = TOPICS[CASE]
CONSUMER = Consumer({"bootstrap.servers": "kafka:9092", "group.id": "e2e-observer",
                     "enable.auto.commit": False})


def request(url, body=None):
    data = None if body is None else json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as response:
        return json.load(response)


def wait_for(description, predicate, seconds=90):
    deadline = time.monotonic() + seconds
    last_error = None
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except (urllib.error.URLError, TimeoutError) as error:
            last_error = error
        time.sleep(0.25)
    raise AssertionError(f"Timed out waiting for {description}; last error: {last_error}")


def task_running(name):
    status = request(f"http://connect:8083/connectors/{name}/status")
    for task in status.get("tasks", []):
        assert task["state"] != "FAILED", f"Connect task failed: {status}"
    return bool(status.get("tasks")) and all(t["state"] == "RUNNING" for t in status["tasks"])


def verify_online(name, view, dlq):
    assert task_running(name), "Connect task is not running"
    assert CONSUMER.get_watermark_offsets(TopicPartition(dlq, 0), timeout=5)[1] == 0, \
        "Unexpected records in the DLQ"
    fields = list(VALUES[0]) + (["kafka_source_id"] if CASE == "json" else [])
    response = request("http://feast:6566/get-online-features", {
        "features": [f"{view}:{field}" for field in fields],
        "entities": {"trip_id": [row["trip_id"] for row in ROWS]},
    })
    names = response["metadata"]["feature_names"]
    results = response["results"]
    assert len(names) == len(results), f"Malformed Feast response: {response}"
    columns = {name.split(":")[-1].split("__")[-1]: result for name, result in zip(names, results)}
    for field in fields:
        assert field in columns, f"Missing feature {field}: {response}"
        result = columns[field]
        if result.get("statuses") != ["PRESENT"] * len(ROWS):
            return False
        assert len(result["values"]) == len(ROWS), f"Wrong value count for {field}: {result}"
        assert len(result.get("event_timestamps", [])) == len(ROWS), f"Missing timestamps: {result}"
        for index, row in enumerate(ROWS):
            assert_value(result["values"][index], row[field], f"online {row['trip_id']}.{field}")
            assert timestamp(result["event_timestamps"][index]) == timestamp(row["event_timestamp"]), \
                f"Wrong event timestamp for {row['trip_id']}.{field}: {result}"
    return True


def run():
    wait_for("Connect REST API", lambda: isinstance(request("http://connect:8083/connectors"), list))
    if CASE != "json":
        wait_for("Schema Registry", lambda: isinstance(request("http://schema-registry:8081/subjects"), list))
    config_file = "feast-sink-connector" + ("" if CASE == "json" else f"-{CASE}") + ".json"
    registration = json.loads(Path("config", config_file).read_text())
    config = registration["config"]
    # Keep the demo format, mapping, SMT, and push mode; flush the partial batch at preCommit.
    config.update({"batch.size": "500", "linger.ms": "86400000", "max.retries": "0"})
    name = registration["name"]
    dlq = config["errors.deadletterqueue.topic.name"]
    admin = AdminClient({"bootstrap.servers": "kafka:9092"})
    for future in admin.create_topics([NewTopic(t, num_partitions=1, replication_factor=1)
                                      for t in (TOPIC, dlq)]).values():
        future.result(timeout=20)
    request("http://connect:8083/connectors", registration)
    wait_for("running Connect task", lambda: task_running(name))

    fixture = Path("/tmp/taxi-fixture.parquet")
    data = []
    for index, values in enumerate(VALUES):
        data.append(dict(tpep_pickup_datetime=pd.Timestamp(TIMESTAMPS[index]).tz_localize(None),
                         PULocationID=values["pickup_location_id"], DOLocationID=values["dropoff_location_id"],
                         passenger_count=None if index == 1 else values["passenger_count"],
                         trip_distance=values["trip_distance"], fare_amount=values["fare_amount"],
                         tip_amount=values["tip_amount"]))
    pd.DataFrame(data).to_parquet(fixture, index=False)
    producer = dict(json="replay_producer.py", avro="avro_producer.py", protobuf="protobuf_producer.py")[CASE]
    env = dict(os.environ, BOOTSTRAP_SERVERS="kafka:9092", TOPIC=TOPIC,
               SCHEMA_REGISTRY_URL="http://schema-registry:8081", PARQUET_URL=fixture.as_uri(),
               ROW_LIMIT=str(len(ROWS)), EMIT_RATE="0")
    subprocess.run([sys.executable, str(Path("producers", producer))], env=env, check=True, timeout=90)
    assert CONSUMER.get_watermark_offsets(TopicPartition(TOPIC, 0), timeout=10)[1] == len(ROWS), \
        "Producer did not deliver all fixture records"
    if CASE != "json":
        registered = request(f"http://schema-registry:8081/subjects/{TOPIC}-value/versions/latest")
        expected_type = "AVRO" if CASE == "avro" else "PROTOBUF"
        assert registered.get("schemaType", "AVRO") == expected_type, f"Wrong registered schema: {registered}"

    view = "taxi_trip_features_with_source_id" if CASE == "json" else "taxi_trip_features"
    wait_for("all expected online features", lambda: verify_online(name, view, dlq))

    def committed():
        assert task_running(name), "Connect task is not running"
        group = "connect-" + name
        offsets = admin.list_consumer_group_offsets([
            ConsumerGroupTopicPartitions(group, [TopicPartition(TOPIC, 0)])
        ])[group].result(timeout=10).topic_partitions
        return len(offsets) == 1 and offsets[0].offset == len(ROWS)

    wait_for("committed input offsets", committed, seconds=30)
    assert CONSUMER.get_watermark_offsets(TopicPartition(dlq, 0), timeout=5)[1] == 0, "DLQ is not empty"
    print(f"Verified {CASE}: {len(ROWS)} online rows, event timestamps, committed offsets, and empty DLQ.")


try:
    run()
except Exception:
    traceback.print_exc()
    sys.exit(1)
finally:
    CONSUMER.close()
