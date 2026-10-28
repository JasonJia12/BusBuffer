# Reads the static GTFS zip, and works out which service day a trip belongs to.

import csv
import io
import zipfile
from datetime import datetime
from zoneinfo import ZoneInfo

TZ = ZoneInfo("America/Chicago")


def read_table(name):
    with zipfile.ZipFile("data/gtfs/mmt_gtfs.zip") as z:
        with z.open(name + ".txt") as f:
            return list(csv.DictReader(io.TextIOWrapper(f, "utf-8")))


def trip_starts():
    starts = {}
    for r in read_table("stop_times"):
        h, m, s = r["departure_time"].split(":")
        sec = int(h) * 3600 + int(m) * 60 + int(s)
        if r["trip_id"] not in starts or sec < starts[r["trip_id"]]:
            starts[r["trip_id"]] = sec
    return starts


def service_date(predicted, start):
    # GTFS times count from midnight of the service day and can pass 24:00
    noon = datetime.fromtimestamp(predicted - start + 12 * 3600, TZ)
    return noon.date()
