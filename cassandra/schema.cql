CREATE KEYSPACE IF NOT EXISTS busbuffer
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- errors for one route on one day, by minutes ahead and time
CREATE TABLE IF NOT EXISTS busbuffer.prediction_errors (
    route_id text,
    service_date date,
    minutes_ahead int,
    predicted_at timestamp,
    trip_id text,
    stop_sequence int,
    lead int,
    predicted timestamp,
    actual timestamp,
    error double,
    PRIMARY KEY ((route_id, service_date), minutes_ahead, predicted_at, trip_id, stop_sequence)
);

-- planner: which trips are coming to this stop (some route 81 trips pass a stop twice)
CREATE TABLE IF NOT EXISTS busbuffer.live_predictions_by_stop (
    stop_id text,
    trip_id text,
    service_date date,
    route_id text,
    stop_sequence int,
    arrival timestamp,
    feed_ts timestamp,
    PRIMARY KEY ((stop_id), trip_id, stop_sequence)
) WITH default_time_to_live = 7200;

-- planner: when does this trip get to each of its stops
CREATE TABLE IF NOT EXISTS busbuffer.live_predictions_by_trip (
    service_date date,
    trip_id text,
    stop_sequence int,
    stop_id text,
    route_id text,
    arrival timestamp,
    feed_ts timestamp,
    PRIMARY KEY ((service_date, trip_id), stop_sequence)
) WITH default_time_to_live = 7200;
