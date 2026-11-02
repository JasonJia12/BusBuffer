# Publishes recorded trips files to Kafka, one message per trip per poll.

import glob
import json
import sys
import time
from datetime import datetime

from google.protobuf.message import DecodeError
from kafka import KafkaAdminClient, KafkaProducer
from kafka.admin import NewTopic

from busbuffer.rt_parser import read_feed, trip_rows
from busbuffer.static_gtfs import TZ, read_table

BOOTSTRAP = "localhost:9092"
TOPIC = "metro.trip_updates"


def file_ts(path):
    name = path.split("/")[-1]
    return int(name.split(".")[0])


def trip_files(start, end):
    paths = sorted(glob.glob("data/raw/*/trips/*.pb.gz"))
    return [p for p in paths if start <= file_ts(p) < end]


def trip_messages(feed, scheduled):
    trips = {}
    for r in trip_rows(feed):
        trip_id = r["trip_id"]
        # added trips (negative ids) aren't in the timetable
        if trip_id not in scheduled:
            continue
        if trip_id not in trips:
            msg = {"feed_ts": r["feed_ts"], "trip_id": trip_id, "route_id": r["route_id"], "stops": []}
            trips[trip_id] = msg
        stop = {"stop_sequence": r["stop_sequence"], "stop_id": r["stop_id"], "arrival": r["arrival"]}
        trips[trip_id]["stops"].append(stop)
    return list(trips.values())


def publish(producer, path, scheduled):
    try:
        feed = read_feed(path)
    except DecodeError:
        return  # Metro's server sometimes sends an error page
    # since Oct 20 about half the responses are a minute old and don't mark detours
    if not any(e.HasField("trip_modifications") for e in feed.entity):
        return
    for msg in trip_messages(feed, scheduled):
        producer.send(TOPIC, key=msg["trip_id"].encode(), value=json.dumps(msg).encode())


def replay(producer, start, end, speed, scheduled):
    prev_ts = None
    for path in trip_files(start, end):
        ts = file_ts(path)
        if prev_ts is not None:
            time.sleep((ts - prev_ts) / speed)
        publish(producer, path, scheduled)
        prev_ts = ts
    producer.flush()


def live(producer, scheduled):
    next_ts = time.time()
    while True:
        # files are named by when the fetch started, so give the recorder time to finish
        for path in trip_files(next_ts, time.time() - 20):
            publish(producer, path, scheduled)
            next_ts = file_ts(path) + 1
        time.sleep(5)


if __name__ == "__main__":
    admin = KafkaAdminClient(bootstrap_servers=BOOTSTRAP)
    if TOPIC not in admin.list_topics():
        admin.create_topics([NewTopic(TOPIC, num_partitions=4, replication_factor=1)])

    scheduled = {r["trip_id"] for r in read_table("trips")}
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP, compression_type="gzip")
    if sys.argv[1] == "live":
        live(producer, scheduled)
    else:
        start = datetime.fromisoformat(sys.argv[2]).replace(tzinfo=TZ).timestamp()
        end = datetime.fromisoformat(sys.argv[3]).replace(tzinfo=TZ).timestamp()
        replay(producer, start, end, float(sys.argv[4]), scheduled)
