# Arrival prediction error by minutes ahead, over all recorded trips files.

import glob
import statistics

from google.protobuf.message import DecodeError

from busbuffer.prediction_errors import MINUTES_AHEAD, update_trip
from busbuffer.rt_parser import read_feed, trip_rows

paths = sorted(glob.glob("data/raw/*/trips/*.pb.gz"))
states = {}
errors = {m: [] for m in MINUTES_AHEAD}
for path in paths:
    try:
        feed = read_feed(path)
    except DecodeError:
        continue  # Metro's server sometimes sends an error page
    trips = {}
    for r in trip_rows(feed):
        trips.setdefault(r["trip_id"], {})[r["stop_sequence"]] = r["arrival"]
    for trip, stops in trips.items():
        states[trip], passed = update_trip(states.get(trip), feed.header.timestamp, stops)
        for e in passed:
            errors[e["minutes_ahead"]].append(e["actual"] - e["predicted"])

print(len(paths), "trips files from", paths[0].split("/")[2], "to", paths[-1].split("/")[2])
print("error = actual - predicted arrival in seconds, positive = later than predicted")
print("minutes ahead       n  median   p10   p90  within 1 min")
for m, errs in errors.items():
    p = statistics.quantiles(errs, n=10)
    within = sum(abs(e) <= 60 for e in errs) / len(errs)
    print(f"{m:13} {len(errs):7} {statistics.median(errs):7.0f} {p[0]:5.0f} {p[8]:5.0f} {within:13.0%}")
