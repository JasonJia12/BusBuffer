# Spark job: arrival prediction errors from the trip messages in Kafka, saved as Parquet.

import pickle

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_json

from busbuffer.prediction_errors import update_trip

MESSAGE = "feed_ts long, trip_id string, stops array<struct<stop_sequence: int, arrival: long>>"
ERRORS = "trip_id string, stop_sequence int, minutes_ahead int, lead int, predicted long, actual double"
STATE = "trip binary"
WATERMARK = "10 minutes"
TIMEOUT = 3600  # seconds


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
        yield out


spark = SparkSession.builder.appName("prediction errors").master("spark://spark-master:7077").getOrCreate()
# the default of 200 is far too many for a few hundred trips
spark.conf.set("spark.sql.shuffle.partitions", 4)

messages = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:19092")
    .option("subscribe", "metro.trip_updates")
    .option("startingOffsets", "earliest")
    .load()
)
values = messages.select(col("value").cast("string"))
polls = values.select(from_json("value", MESSAGE).alias("poll"))
stops = polls.select("poll.trip_id", "poll.feed_ts", explode("poll.stops").alias("stop"))
stops = stops.select("trip_id", "feed_ts", "stop.stop_sequence", "stop.arrival")
stops = stops.withColumn("event_time", col("feed_ts").cast("timestamp"))
stops = stops.withWatermark("event_time", WATERMARK)

trips = stops.groupBy("trip_id")
errors = trips.applyInPandasWithState(trip_errors, ERRORS, STATE, "append", "EventTimeTimeout")

# absolute paths
query = (
    errors.writeStream.format("parquet")
    .option("path", "/project/data/lake/prediction_errors")
    .option("checkpointLocation", "/project/data/checkpoints/prediction_errors")
    .trigger(processingTime="1 minute")
    .start()
)
query.awaitTermination()
