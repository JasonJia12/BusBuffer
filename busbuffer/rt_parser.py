# Turns a recorded .pb.gz file into rows: one per (trip, stop) for trips, one per bus for vehicles.

import gzip
import sys

from google.transit import gtfs_realtime_pb2 as pb

STOP_RELATIONSHIP = pb.TripUpdate.StopTimeUpdate.ScheduleRelationship


def read_feed(path):
    feed = pb.FeedMessage()
    feed.ParseFromString(gzip.open(path).read())
    return feed


def get(msg, field):
    return getattr(msg, field) if msg.HasField(field) else None


def trip_rows(feed):
    rows = []
    for entity in feed.entity:
        # skip detoured trips, their stops don't match the schedule
        if not entity.HasField("trip_update") or entity.trip_update.trip.HasField("modified_trip"):
            continue
        tu = entity.trip_update
        for stu in tu.stop_time_update:
            rows.append({
                "feed_ts": feed.header.timestamp,
                "trip_id": get(tu.trip, "trip_id"),
                "route_id": get(tu.trip, "route_id"),
                "stop_sequence": get(stu, "stop_sequence"),
                "stop_id": get(stu, "stop_id"),
                "arrival": get(stu.arrival, "time"),
                "departure": get(stu.departure, "time"),
                "relationship": STOP_RELATIONSHIP.Name(stu.schedule_relationship),
            })
    return rows


def vehicle_rows(feed):
    rows = []
    for entity in feed.entity:
        v = entity.vehicle
        rows.append({
            "feed_ts": feed.header.timestamp,
            "vehicle_ts": get(v, "timestamp"),
            "vehicle_id": get(v.vehicle, "id"),
            "trip_id": get(v.trip, "trip_id"),
            "route_id": get(v.trip, "route_id"),
            "lat": v.position.latitude,
            "lon": v.position.longitude,
        })
    return rows


if __name__ == "__main__":
    path = sys.argv[1]
    feed = read_feed(path)
    rows = trip_rows(feed) if "trips" in path else vehicle_rows(feed)
    for row in rows:
        print(row)
