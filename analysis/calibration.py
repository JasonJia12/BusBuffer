# Calibration: quantiles from the days before each day only, then how many of its errors fall below them.

from pyspark.sql import SparkSession

from busbuffer.planner import PERCENTS, pick_quantiles, quantile_keys
from busbuffer.static_gtfs import read_table

FEED_CHANGE = "2026-10-20 13:20:00"  # same cutoff as error_quantiles.py
DAYS = [f"2026-10-{d}" for d in range(11, 21)]
CHECK = [5, 10, 20, 50, 80, 90, 95]

spark = (
    SparkSession.builder.appName("calibration")
    .master("spark://spark-master:7077")
    .config("spark.sql.session.timeZone", "America/Chicago")
    .getOrCreate()
)

directions = [(r["trip_id"], int(r["direction_id"])) for r in read_table("trips")]
spark.createDataFrame(directions, "trip_id string, direction_id int").createOrReplaceTempView("trips")
spark.read.parquet("/project/data/lake/prediction_errors").createOrReplaceTempView("lake")

errors = spark.sql(f"""
    SELECT e.route_id, t.direction_id, e.service_date, e.minutes_ahead, e.error, hour(e.predicted_at) AS hour,
           CASE dayofweek(e.service_date)
               WHEN 1 THEN 'sunday'
               WHEN 7 THEN 'saturday'
               ELSE 'weekday'
           END AS day_type
    FROM lake e JOIN trips t ON e.trip_id = t.trip_id
    WHERE e.actual < '{FEED_CHANGE}'
""")
errors.cache()
errors.createOrReplaceTempView("errors")

by_day = {}
by_minutes = {}
for day in DAYS:
    # the same groups as error_quantiles.py, but only from the days before
    quantiles = spark.sql(f"""
        SELECT coalesce(route_id, 'all') AS route_id,
               coalesce(direction_id, -1) AS direction_id,
               coalesce(day_type, 'all') AS day_type,
               coalesce(hour, -1) AS hour,
               minutes_ahead,
               percentile_approx(error, array(
                   0.05, 0.1, 0.15, 0.2, 0.25, 0.3, 0.35, 0.4, 0.45, 0.5,
                   0.55, 0.6, 0.65, 0.7, 0.75, 0.8, 0.85, 0.9, 0.95
               )) AS quantiles,
               count(*) AS n
        FROM errors
        WHERE service_date < '{day}'
        GROUP BY GROUPING SETS (
            (route_id, direction_id, day_type, hour, minutes_ahead),
            (route_id, day_type, minutes_ahead),
            (route_id, minutes_ahead),
            (minutes_ahead)
        )
    """)
    groups = {}
    for r in quantiles.collect():
        key = (r["route_id"], r["direction_id"], r["day_type"], r["hour"])
        if key not in groups:
            groups[key] = {}
        groups[key][r["minutes_ahead"]] = {"quantiles": r["quantiles"], "n": r["n"]}

    by_day[day] = [0] * (len(CHECK) + 1)
    test = spark.sql(f"SELECT * FROM errors WHERE service_date = '{day}'")
    for r in test.toLocalIterator():
        m = r["minutes_ahead"]
        keys = quantile_keys(r["route_id"], r["direction_id"], r["service_date"], r["hour"])
        chosen = pick_quantiles([groups.get(k, {}) for k in keys], m)
        if m not in by_minutes:
            by_minutes[m] = [0] * (len(CHECK) + 1)
        by_day[day][0] += 1
        by_minutes[m][0] += 1
        for i, p in enumerate(CHECK):
            if r["error"] < chosen["quantiles"][PERCENTS.index(p)]:
                by_day[day][i + 1] += 1
                by_minutes[m][i + 1] += 1

print("share of errors below each quantile, quantiles from the days before only")
print("held out          n" + "".join(f"{p:6}%" for p in CHECK))
total = [0] * (len(CHECK) + 1)
for day, counts in by_day.items():
    print(f"{day} {counts[0]:8}" + "".join(f"{c / counts[0]:7.1%}" for c in counts[1:]))
    for i in range(len(counts)):
        total[i] += counts[i]
print(f"all days   {total[0]:8}" + "".join(f"{c / total[0]:7.1%}" for c in total[1:]))
print("minutes ahead")
for m in sorted(by_minutes):
    counts = by_minutes[m]
    print(f"{m:10} {counts[0]:8}" + "".join(f"{c / counts[0]:7.1%}" for c in counts[1:]))
