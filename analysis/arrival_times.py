# AI generated
# Two ways to tell when a bus was at a stop, compared on Oct 9, 7-10 am:
#   feed: a stop drops out of its trip in the trips feed once the bus has passed it
#   gps:  the bus reports a position within 30 m of the stop

import glob
import math
import statistics

from busbuffer.rt_parser import read_feed, trip_rows, vehicle_rows
from busbuffer.static_gtfs import read_table

START, END = 1791547200, 1791558000  # Oct 9, 7-10 am Chicago time


def files(feed):
    paths = sorted(glob.glob(f"data/raw/2026-10-09/{feed}/*.pb.gz"))
    return [p for p in paths if START <= int(p.split("/")[-1].split(".")[0]) < END]


def feed_times(paths):
    # time = halfway between the last poll that still listed the stop and the first one that didn't
    last = {}
    times = {}
    for path in paths:
        feed = read_feed(path)
        ts = feed.header.timestamp
        now = {}
        for r in trip_rows(feed):
            now.setdefault(r["trip_id"], set()).add(r["stop_sequence"])
        for trip, seqs in now.items():
            if trip in last:
                old_seqs, old_ts = last[trip]
                for seq in old_seqs - seqs:
                    if seq < min(seqs):
                        times[(trip, seq)] = (old_ts + ts) / 2
            last[trip] = (seqs, ts)
    return times


def distance(a, b):
    dy = (a[0] - b[0]) * 111000
    dx = (a[1] - b[1]) * 111000 * math.cos(math.radians(a[0]))
    return math.hypot(dx, dy)


def gps_times(paths, stop_pos):
    # time = first position report within 30 m of the stop
    track = {}
    for path in paths:
        for r in vehicle_rows(read_feed(path)):
            if r["trip_id"]:
                track.setdefault(r["trip_id"], {})[r["vehicle_ts"]] = (r["lat"], r["lon"])
    times = {}
    for (trip, seq), stop in stop_pos.items():
        near = [t for t, p in track.get(trip, {}).items() if distance(p, stop) <= 30]
        if near:
            times[(trip, seq)] = min(near)
    return times


stops = {s["stop_id"]: (float(s["stop_lat"]), float(s["stop_lon"])) for s in read_table("stops")}
stop_pos = {(r["trip_id"], int(r["stop_sequence"])): stops[r["stop_id"]] for r in read_table("stop_times")}

from_feed = feed_times(files("trips"))
from_gps = gps_times(files("vehicles"), stop_pos)

# first stops are left out: buses wait there before leaving, so the first GPS report is too early
diffs = [from_gps[k] - from_feed[k] for k in from_feed if k in from_gps and k[1] > 1]

print("stops with a feed time:", len(from_feed))
print("stops with a gps time:", len(from_gps))
print("stops with both (not first stop):", len(diffs))
print("gps - feed, median:", statistics.median(diffs))
print("gps - feed, quartiles:", statistics.quantiles(diffs, n=4))
print("within 30 s:", sum(abs(d) <= 30 for d in diffs) / len(diffs))
print("within 60 s:", sum(abs(d) <= 60 for d in diffs) / len(diffs))
