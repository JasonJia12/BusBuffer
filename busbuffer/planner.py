# Planner: when to leave for a bus and the chance of getting to class on time, from the error quantiles.

MIN_N = 50
# error_quantiles has the 5%, 10%, ... 95% quantiles
PERCENTS = list(range(5, 100, 5))


def stop_times(rows, origin, destination):
    # a stop the bus has passed keeps its old row until the TTL, so only the newest poll counts
    newest = max(r["feed_ts"] for r in rows)
    at_origin = None
    for r in rows:
        if r["feed_ts"] < newest:
            continue
        if at_origin is None and r["stop_id"] == origin:
            at_origin = r["arrival"]
        elif at_origin is not None and r["stop_id"] == destination:
            return at_origin, r["arrival"]
    return None


def quantile_keys(route_id, direction_id, service_date, hour):
    if service_date.weekday() == 5:
        day_type = "saturday"
    elif service_date.weekday() == 6:
        day_type = "sunday"
    else:
        day_type = "weekday"
    return [
        (route_id, direction_id, day_type, hour),
        (route_id, -1, day_type, -1),
        (route_id, -1, "all", -1),
        ("all", -1, "all", -1),
    ]


def pick_quantiles(groups, m):
    for group in groups:
        if m in group and group[m]["n"] >= MIN_N:
            return group[m]


def percent_below(quantiles, error):
    # small groups can repeat a quantile, then the highest percent of the repeated value counts
    if error < quantiles[0]:
        return 5
    if error >= quantiles[-1]:
        return 95
    for i in range(len(quantiles) - 1):
        low = quantiles[i]
        high = quantiles[i + 1]
        if error < high:
            return PERCENTS[i] + 5 * (error - low) / (high - low)


def plan_trip(at_origin, at_destination, q_origin, q_destination, walk_to, deadline, confidence):
    # only 100 - confidence percent of buses get to the stop earlier than this
    be_at_stop = at_origin + q_origin[PERCENTS.index(100 - confidence)]
    on_time = percent_below(q_destination, deadline - at_destination)
    return {
        "be_at_stop": be_at_stop,
        "leave_by": be_at_stop - walk_to,
        "on_time": on_time,
        "arrive_from": at_destination + q_destination[PERCENTS.index(10)],
        "arrive_to": at_destination + q_destination[PERCENTS.index(90)],
    }


def recommend(plans, now, confidence):
    best = None
    for plan in plans:
        if plan["leave_by"] <= now or plan["on_time"] < confidence:
            continue
        if best is None or plan["leave_by"] > best["leave_by"]:
            best = plan
    return best
