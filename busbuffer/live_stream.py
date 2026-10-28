# Spark job: the newest arrival prediction for each stop of each trip, kept in Cassandra for the planner.

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, explode, from_json, row_number, udf

from busbuffer.static_gtfs import service_date, trip_starts

STOP = "struct<stop_sequence: int, stop_id: string, arrival: long>"
MESSAGE = f"feed_ts long, trip_id string, route_id string, stops array<{STOP}>"


def save_predictions(batch, batch_id):
    # a batch can hold several polls of the same trip, keep the newest
    newest = Window.partitionBy("trip_id", "stop_sequence").orderBy(col("feed_ts").desc())
    ranked = batch.withColumn("rank", row_number().over(newest))
    latest = ranked.filter("rank = 1")
    latest = latest.drop("rank")
    for table in ["live_predictions_by_stop", "live_predictions_by_trip"]:
        (
            latest.write.format("org.apache.spark.sql.cassandra")
            .mode("append")
            .options(keyspace="busbuffer", table=table)
            .save()
        )


starts = trip_starts()

spark = (
    SparkSession.builder.appName("live predictions")
    .master("spark://spark-master:7077")
    .config("spark.cores.max", 2)
    .config("spark.cassandra.connection.host", "cassandra")
    .config("spark.sql.shuffle.partitions", 4)
    .getOrCreate()
)


@udf("date")
def trip_service_date(trip_id, arrival):
    return service_date(arrival, starts[trip_id])


messages = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "metro.trip_updates")
    .load()
)
values = messages.select(col("value").cast("string"))
polls = values.select(from_json("value", MESSAGE).alias("poll"))
stops = polls.select("poll.trip_id", "poll.route_id", "poll.feed_ts", explode("poll.stops").alias("stop"))
stops = stops.select("trip_id", "route_id", "feed_ts", "stop.*")
# first stops only have a departure time, and NO_DATA stops have none
stops = stops.filter(col("arrival").isNotNull())
stops = stops.withColumn("service_date", trip_service_date("trip_id", "arrival"))
stops = stops.withColumn("arrival", col("arrival").cast("timestamp"))
stops = stops.withColumn("feed_ts", col("feed_ts").cast("timestamp"))

query = (
    stops.writeStream.foreachBatch(save_predictions)
    .option("checkpointLocation", "/project/data/checkpoints/live_predictions")
    .start()
)
query.awaitTermination()
