# BusBuffer

A personal project for CS 544. It records Madison Metro's real-time arrival predictions, works out when each bus really got to each stop, measures how wrong the predictions were, and uses that in a planner that tells me the latest time I can leave and still make class. It uses Docker Compose, Kafka, Spark Structured Streaming, Spark SQL and Cassandra.

## Problem

I take route 80 to class. When I go by the arrival time the app shows, I often end up waiting longer than it said, and then I'm late for class. But I don't want to leave really early and just stand at the stop, especially in winter. So I wanted to know two things: how much I can trust the app when it says a bus is 10 minutes away, and the latest time I can leave and still get to class on time.

## How it works

The recorder (`recorder/record.py`) polls Metro's GTFS-Realtime trips and vehicles feeds every 30 s, which is about how often the feed changes, and the alerts feed every 5 min. It saves every changed response as gzipped protobuf under `data/raw`. It only uses the standard library and doesn't depend on the rest of the stack, so it ran on my laptop from Oct 8 to Nov 4, 2026, and nothing gets lost when Kafka or Spark are down or I change the code. That's about 110 MB a day.

The producer (`busbuffer/producer.py`) parses the recorded trips files, drops detoured trips, and sends one message per trip per poll to the Kafka topic `metro.trip_updates`, which has 4 partitions. The key is the trip_id, so all polls of a trip go to the same partition and stay in time order, which the error job needs. In live mode it follows new files as the recorder writes them. In replay mode it sends old files with their original timestamps, sped up by a factor, so the whole pipeline can be run again over everything recorded.

The vehicles feed doesn't say which stop a bus is at, so the actual arrival time comes from the trips feed: once a bus passes a stop, the stop drops off the front of the trip's list, and I take the time halfway between the last poll that listed it and the first one that didn't. I checked this against GPS on Oct 9, 7-10 am (`analysis/arrival_times.py`). On the 4,946 stops that had both, GPS minus this time had a median of 1 s and 71% were within 30 s, so the times are good to about 30 s. More about the feeds is in `docs/feed_notes.md`.

The prediction error job (`busbuffer/error_stream.py`) is a Spark Structured Streaming job that reads the topic, groups by trip_id and keeps state for each trip with `applyInPandasWithState`. The logic is a plain Python function, `update_trip` in `busbuffer/prediction_errors.py`, with unit tests. For each stop it saves one prediction for each of 1, 2, 3, 5, 7, 10, 15, 20 and 30 minutes ahead (for 10 it's the first prediction made 7 to 10 minutes before its predicted arrival), and when the stop drops off the front of the list it outputs error = actual - predicted for each of them. Positive means the bus came later than predicted. It uses the feed's time as event time with a 10-minute watermark, and a 1-hour event-time timeout removes finished trips from the state.

Each micro-batch is written to Parquet in `data/lake/prediction_errors`, one folder per batch id that gets overwritten if Spark retries the batch, and to Cassandra, where writes are upserts by primary key. Because of that, the job can restart from its checkpoint after a crash without duplicates. I killed the driver with `kill -9` in the middle of a batch, and after the restart the output was identical. On a replay of everything recorded up to Oct 28 (4.4 million messages), the counts and sums of the errors matched running `update_trip` directly on the files.

The live predictions job (`busbuffer/live_stream.py`) is stateless. It keeps the newest arrival prediction for each stop of each trip in Cassandra, with a 2-hour TTL so old predictions clean themselves up.

The quantile job (`busbuffer/error_quantiles.py`) is a Spark SQL batch job over the Parquet lake. It computes the 5%, 10%, ..., 95% quantiles of the error with `percentile_approx`, and the count, using GROUPING SETS to get four group sizes in one pass: route + direction + day type + hour, route + day type, route, and all routes.

Each Cassandra table (`cassandra/schema.cql`) is made for one query. `prediction_errors` is partitioned by route and service date. `live_predictions_by_stop` answers which trips are coming to my stop, and `live_predictions_by_trip` when a trip gets to my destination. Predicted times change every poll, so there they are normal columns. If they were clustering columns, every poll would leave a new row. `error_quantiles` is keyed by the group, so the planner reads at most four small partitions.

## Planner

`busbuffer/planner.py` looks at each bus that gets to my stop and then my destination within the next 30 minutes (errors were only measured up to 30 minutes ahead). It takes the feed's predicted arrival at both stops and the quantiles for that route, direction, day type and hour. If that group has fewer than 50 errors it uses route + day type instead, then route, then all routes.

To catch the bus with 90% confidence, I should be at the stop by the predicted arrival plus the 10% quantile, since only 10% of buses came earlier than that. 80% and 95% use the 20% and 5% quantiles. The leave-by time is that minus my walk to the stop. The chance of making class is how often the bus got to the destination by class start minus my walk from the stop, read from the destination's quantiles with straight lines between them (below the 5% quantile it says 5%, above the 95% one 95%). It recommends the latest bus I can still leave for whose chance of making class is at least the confidence.

The dashboard (`busbuffer/dashboard.py`, Streamlit) reads the live predictions and quantiles from Cassandra. I pick the two stops, when class starts, my walking times and the confidence, and it shows the leave-by time and a row for each bus, updated every 30 s. The choices are kept in the page's URL, so I can bookmark my usual trip. A second tab shows the middle 80% and the median of the errors by minutes ahead for one route next to all routes.

An example with the real route 80 weekday quantiles: at 9:10 the app says the bus gets to my stop at 9:20 and to my destination at 9:28. At 10 minutes ahead the 10% quantile is -65 s, so I should be at the stop by 9:18:55, and with a 5-minute walk leave by 9:13:55. Class starts at 9:38 and I walk 5 minutes from the stop, so the bus has to get there by 9:33, 300 s after the prediction. At 18 minutes ahead (the 20-minute quantiles) that's between the 90% quantile (256.5 s) and the 95% one (328 s), so the chance of making class is about 93%, and 80% of the time the bus gets there between 9:26:52 and 9:32:17.

## Results

All numbers are from Oct 8 to Oct 20, 13:20, for the reason at the end of this section. Error is actual minus predicted, and "within 1 min" means the bus came less than a minute early or late.

Across all routes (460k to 494k errors for each minutes-ahead value):

- 1 minute ahead: median +12 s, 80% between -18 and +97 s, 82.6% within 1 min.
- 5 minutes: median +18.5 s, 80% between -56.5 and +140.5 s, 63.0% within 1 min.
- 10 minutes: median +22.5 s, 80% between -82.5 and +183 s, 49.8% within 1 min.
- 30 minutes: median +48 s, 80% between -89 and +285 s, 40.2% within 1 min.

Buses usually come a bit later than predicted, and the further ahead the prediction, the wider the range. When the app says 10 minutes, only half the buses come within a minute of it.

I also compared route 80 with the other routes, for predictions made on weekdays between 7 am and 7 pm (`analysis/route_80.py`, about 30k vs 260k errors for each minutes-ahead value). At 10 minutes ahead route 80's median was +42 s vs +21 s, and 80% were between -57 and +208 s vs -88.5 and +185 s. At 1 minute ahead, 72.1% vs 82.8% were within a minute. Route 80 comes about 20 s later than predicted compared to the other routes, but it's early less often. With 90% confidence at 10 minutes ahead, the planner tells me to be at the stop about half a minute later than it would on another route.

To check the planner's confidence, for each day from Oct 11 to Oct 20 I built the quantiles only from the days before it, picked each error's group the same way the planner does, and counted how many errors fell below each quantile (`analysis/calibration.py`). Over 3,224,951 errors, 5.4% were below the 5% quantile, 10.3% below the 10% and 20.3% below the 20%, which means being at the stop by the 90% time would have caught the bus 89.7% of the time (79.7% at 80%, 94.6% at 95%). By day that was between 88.1% and 90.6%. For the chance of making class the upper quantiles matter: 79.7% of errors were below the 80% quantile, 89.4% below the 90% and 94.3% below the 95% (50.3% below the median). Oct 15 was the worst day for this side: buses were later than usual, and only 87.1% of errors were below the 90% quantile. Errors on the same trip and day are related, so the day-to-day range says more about the spread than the 3.2 million count.

On Oct 20 at 13:20 Metro's trips feed changed, and this was the biggest problem in the project. Since then 19-55% of the responses each day are about a minute old and don't mark detoured trips. With those responses, the actual times came out about 22 s late compared to GPS. The producer skips them now, but the full responses often come two at a time, 30 s apart, and then not for about 3.5 minutes, which is too far apart to tell when a bus passed a stop as well as before. Errors after the change aren't comparable to the earlier ones, so none of the results or quantiles use them. I kept my laptop recording until Nov 4, so more than half of the days I recorded (about 15 of 28) can't be used for any of the results. That data is still how I found the change, and the live planner runs on the new feed. I had also planned from the start to compare the Oct 24 home football game with normal Saturdays, but it's after the change, so I couldn't.

## Limitations

- Actual times are only good to about 30 s, and the last two stops of a trip never get a time because the whole trip leaves the feed first.
- Detoured trips are left out (13% of trips on Oct 8), which removes almost all of routes O, J and S.
- The quantiles come from Oct 8-20 only and don't update.
- On Nov 2 I changed what happens when polls are more than 2 minutes apart: the stops passed in between get no error, but the trip keeps its other saved predictions (before, the trip started over). The Oct 8-20 lake was built before that. The two only differ on days with recording gaps, by at most 3% of the errors.
- The planner looks at the origin and the destination separately, has no transfers, uses walking times as typed in, and only covers buses with a live prediction up to 30 minutes ahead. A trip's first stop only has a departure prediction, so it can't be the origin.
- Everything runs on one laptop (one Kafka broker, one Spark worker, one Cassandra node), and the recording has gaps when it slept. They are logged in `data/recorder.log` and left as gaps.

## Data

Realtime feeds and static GTFS from Madison Metro: https://www.cityofmadison.com/metro/business/information-for-developers. Data provided under license granted by City of Madison, WI, Metro Transit. The recordings and the GTFS zip are not in the repo.

## Running it

Python 3.11, Kafka 3.9.2, Spark 3.5.9 and Cassandra 5.0.9. Docker needs about 10 GB of memory to run both streaming jobs.

```
conda create -n busbuffer python=3.11
conda activate busbuffer
python -m pip install -r requirements.txt
mkdir -p data/gtfs
curl -o data/gtfs/mmt_gtfs.zip https://transitdata.cityofmadison.com/GTFS/mmt_gtfs.zip
```

The recorder needs a line `USER_AGENT_CONTACT=<your email>` in `.env`. It keeps the Mac awake while it runs:

```
./recorder/run.sh
```

Start Kafka, Spark and Cassandra, and create the tables:

```
docker compose up -d --build
docker compose exec -T cassandra cqlsh < cassandra/schema.cql
```

Start the two streaming jobs one after the other (their package downloads clash). The live job starts at the newest messages in Kafka.

```
docker compose exec spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 busbuffer/error_stream.py
docker compose exec spark-master /opt/spark/bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.9,com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 busbuffer/live_stream.py
```

Send recorded files to Kafka (start and end in local time, then the speed factor), or follow new files. After the producer has been stopped, replay from the last file sent up to now before going live, so each trip's messages stay in time order.

```
python -m busbuffer.producer replay 2026-10-08T00:00 2026-10-20T13:20 2000
python -m busbuffer.producer live
```

Build the quantiles, then the calibration check and the route 80 comparison. They need the errors from Oct 8-20, which come from my recordings, and those aren't in the repo. The quantile job only keeps errors from before the feed change, so on a new clone the quantiles stay empty and the planner doesn't work. The tests and the pipeline on live data do.

```
docker compose exec -T spark-master /opt/spark/bin/spark-submit --packages com.datastax.spark:spark-cassandra-connector_2.12:3.5.1 busbuffer/error_quantiles.py
docker compose exec -T spark-master /opt/spark/bin/spark-submit analysis/calibration.py
docker compose exec -T spark-master /opt/spark/bin/spark-submit analysis/route_80.py
```

The dashboard needs the live predictions job running and the quantiles built:

```
python -m streamlit run busbuffer/dashboard.py
```

Tests:

```
python -m pytest tests
```
