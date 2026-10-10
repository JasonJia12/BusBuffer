# Feed notes

Notes from looking at my recordings. Numbers are from Oct 8, 2026 (whole day, polling every 30 s). There are some gaps in the recording and it's when I accidently closed the laptop.

Feeds: trips and vehicles from https://metromap.cityofmadison.com/gtfsrt/, polled every 30 s, and the static GTFS zip from https://transitdata.cityofmadison.com/GTFS/mmt_gtfs.zip.

## Refresh

The feed header timestamp changes about every 30 s, and about 6% of my polls got the exact same response as the poll before. So polling every 30 s is about right, and any actual time I get from this data is only accurate to around 30 s.

## Trips feed

At the busiest point (7:24 am) one response had 265 trips and about 6,700 stop predictions. Late evening it's around 90 trips.

Each stop has stop_sequence, stop_id and either an arrival time or a departure time, never both. Departure times are basically only at the first stop of a trip (99%), so a normal stop only has a predicted arrival. There is no delay, uncertainty or direction_id. Direction has to come from trips.txt.

Stops the bus already passed disappear from the trip (1,438 of the 1,457 trips I checked). Predictions go up to about 90 minutes ahead.

## Vehicles feed

Up to 131 buses at once. Each has lat/lon, a timestamp and a vehicle id, and 87% have a trip_id. There is no current_stop_sequence, current_status or stop_id at all. Each bus updates about every 30 s.

## Static GTFS

Version S072_202608240858, valid Aug 16 to Dec 5, 2026, so it covers the whole project. 29 routes, about 14,000 trips, 1,681 stops (all with coordinates). Rapid Route A has route_id A. 17% of stop_times are timepoints, and some times are past 24:00.

Every trip_id I saw in the realtime feeds (7-8 am on Oct 9) is in trips.txt, and stop_sequence/stop_id match the schedule except on detoured trips.

## Detours

Some trips are detoured. The feed marks them with modified_trip, and some of them don't have a trip_id. On Oct 8 that was 215 of 1,673 trips (13%): almost every trip on routes O, J and S, and about half of G and L. I'm leaving detoured trips out, which means O, J and S are basically not in the analysis.

## When did the bus really get to a stop?

The vehicles feed doesn't say which stop a bus is at, so I can't read it from there directly. Two ways that could work: the time a stop disappears from the trip in the trips feed, or the time the bus GPS gets close to the stop. I'll try both on some data and compare them next.
