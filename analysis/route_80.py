# Prediction error by minutes ahead before Metro's feed change: all routes, then route 80 against the rest.

from pyspark.sql import SparkSession

FEED_CHANGE = "2026-10-20 13:20:00"  # same cutoff as error_quantiles.py

spark = (
    SparkSession.builder.appName("route 80")
    .master("spark://spark-master:7077")
    .config("spark.sql.session.timeZone", "America/Chicago")
    .getOrCreate()
)
spark.read.parquet("/project/data/lake/prediction_errors").createOrReplaceTempView("lake")

spark.sql(f"""
    SELECT minutes_ahead, count(*) AS n,
           percentile(error, array(0.1, 0.5, 0.9)) AS p10_median_p90,
           avg(CASE WHEN abs(error) <= 60 THEN 1 ELSE 0 END) AS within_1_min
    FROM lake
    WHERE actual < '{FEED_CHANGE}'
    GROUP BY minutes_ahead
    ORDER BY minutes_ahead
""").show(truncate=False)

# weekdays 7 am to 7 pm, when route 80 runs most, for both sides
spark.sql(f"""
    SELECT minutes_ahead, route_id = '80' AS route_80, count(*) AS n,
           percentile(error, array(0.1, 0.5, 0.9)) AS p10_median_p90,
           avg(CASE WHEN abs(error) <= 60 THEN 1 ELSE 0 END) AS within_1_min
    FROM lake
    WHERE actual < '{FEED_CHANGE}'
        AND dayofweek(service_date) BETWEEN 2 AND 6
        AND hour(predicted_at) BETWEEN 7 AND 18
    GROUP BY minutes_ahead, route_id = '80'
    ORDER BY minutes_ahead, route_80
""").show(truncate=False)
