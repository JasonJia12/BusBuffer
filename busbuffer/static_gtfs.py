# Reads a table out of the static GTFS zip.

import csv
import io
import zipfile


def read_table(name):
    with zipfile.ZipFile("data/gtfs/mmt_gtfs.zip") as z:
        with z.open(name + ".txt") as f:
            return list(csv.DictReader(io.TextIOWrapper(f, "utf-8")))
