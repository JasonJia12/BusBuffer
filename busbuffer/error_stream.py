# Spark job: arrival prediction errors from the trip messages in Kafka, saved as Parquet and in Cassandra.

import pickle

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json

from busbuffer.prediction_errors import TIMEOUT, update_trip
from busbuffer.static_gtfs import service_date, trip_starts

STOP = "struct<stop_sequence: int, arrival: long>"
MESSAGE = f"feed_ts long, trip_id string, route_id string, stops array<{STOP}>"
TRIP = "trip_id string, route_id string, service_date date"
ERRORS = f"{TRIP}, stop_sequence int, minutes_ahead int, lead int, predicted long, actual double"
STATE = "trip binary"
WATERMARK = "10 minutes"


def trip_errors(key, batches, state):
    if state.hasTimedOut:
        state.remove()
        return

    trip = None
    if state.exists:
        trip = pickle.loads(state.get[0])
    rows = pd.concat(batches)
    errors = []
    # groupby sorts by feed_ts, so the polls go in time order
    for feed_ts, poll in rows.groupby("feed_ts"):
        stops = {}
        for seq, arrival in zip(poll["stop_sequence"], poll["arrival"]):
            # pandas turns missing arrivals into NaN
            stops[seq] = None if pd.isna(arrival) else int(arrival)
        trip, passed = update_trip(trip, int(feed_ts), stops)
        errors.extend(passed)

    state.update((pickle.dumps(trip),))
    state.setTimeoutTimestamp((trip["feed_ts"] + TIMEOUT) * 1000)
    if errors:
        out = pd.DataFrame(errors)
        out["trip_id"] = key[0]
        out["route_id"] = key[1]
        start = starts[key[0]]
        out["service_date"] = [service_date(p, start) for p in out["predicted"]]
        yield out


def save_errors(batch, batch_id):
    batch.persist()
    # a retried batch overwrites its own folder, so the lake gets no duplicates
    batch.write.mode("overwrite").parquet(f"/project/data/lake/prediction_errors/batch_id={batch_id}")
    (
        batch.write.format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(keyspace="busbuffer", table="prediction_errors")
        .save()
    )
    batch.unpersist()


starts = trip_starts()

spark = (
    SparkSession.builder.appName("prediction errors")
    .master("spark://spark-master:7077")
    .config("spark.cores.max", 4)
    .config("spark.cassandra.connection.host", "cassandra")
    # the default of 200 is far too many for a few hundred trips
    .config("spark.sql.shuffle.partitions", 4)
    .getOrCreate()
)

messages = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "metro.trip_updates")
    .option("startingOffsets", "earliest")
    .load()
)
values = messages.select(col("value").cast("string"))
polls = values.select(from_json("value", MESSAGE).alias("poll"))
stops = polls.select("poll.trip_id", "poll.route_id", "poll.feed_ts", explode("poll.stops").alias("stop"))
stops = stops.select("trip_id", "route_id", "feed_ts", "stop.stop_sequence", "stop.arrival")
stops = stops.withColumn("event_time", col("feed_ts").cast("timestamp"))
stops = stops.withWatermark("event_time", WATERMARK)

trips = stops.groupBy("trip_id", "route_id")
errors = trips.applyInPandasWithState(trip_errors, ERRORS, STATE, "append", "EventTimeTimeout")
errors = errors.withColumn("predicted_at", (col("predicted") - col("lead")).cast("timestamp"))
errors = errors.withColumn("error", col("actual") - col("predicted"))
errors = errors.withColumn("predicted", col("predicted").cast("timestamp"))
errors = errors.withColumn("actual", col("actual").cast("timestamp"))

query = (
    errors.writeStream.foreachBatch(save_errors)
    .option("checkpointLocation", "/project/data/checkpoints/prediction_errors")
    .trigger(processingTime="1 minute")
    .start()
)
query.awaitTermination()
