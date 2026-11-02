# Spark SQL batch job: quantiles of the prediction errors in the lake, by route, direction, day type and hour.

from pyspark.sql import SparkSession

from busbuffer.static_gtfs import read_table

# from then on about half of Metro's responses are a minute old, which makes the actual times late
FEED_CHANGE = "2026-10-20 13:20:00"

spark = (
    SparkSession.builder.appName("error quantiles")
    .master("spark://spark-master:7077")
    .config("spark.cassandra.connection.host", "cassandra")
    .config("spark.sql.session.timeZone", "America/Chicago")
    .getOrCreate()
)

directions = [(r["trip_id"], int(r["direction_id"])) for r in read_table("trips")]
spark.createDataFrame(directions, "trip_id string, direction_id int").createOrReplaceTempView("trips")
spark.read.parquet("/project/data/lake/prediction_errors").createOrReplaceTempView("lake")

errors = spark.sql(f"""
    SELECT e.route_id, t.direction_id, e.minutes_ahead, e.error, hour(e.predicted_at) AS hour,
           CASE dayofweek(e.service_date)
               WHEN 1 THEN 'sunday'
               WHEN 7 THEN 'saturday'
               ELSE 'weekday'
           END AS day_type
    FROM lake e JOIN trips t ON e.trip_id = t.trip_id
    WHERE e.actual < '{FEED_CHANGE}'
""")
errors.createOrReplaceTempView("errors")

quantiles = spark.sql("""
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
    GROUP BY GROUPING SETS (
        (route_id, direction_id, day_type, hour, minutes_ahead),
        (route_id, day_type, minutes_ahead),
        (route_id, minutes_ahead),
        (minutes_ahead)
    )
""")
(
    quantiles.write.format("org.apache.spark.sql.cassandra")
    .mode("append")
    .options(keyspace="busbuffer", table="error_quantiles")
    .save()
)
