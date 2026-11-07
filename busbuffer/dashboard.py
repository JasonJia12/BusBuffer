# Streamlit page: when to leave for the bus from the live predictions, and how far off the predictions were.

import time
from datetime import datetime

import altair as alt
import pandas as pd
import streamlit as st
from cassandra.cluster import Cluster
from cassandra.query import dict_factory

from busbuffer.planner import pick_quantiles, plan_trip, quantile_keys, recommend, stop_times
from busbuffer.prediction_errors import minutes_ahead
from busbuffer.static_gtfs import TZ, read_table

BOUND = {"0": "northbound", "90": "eastbound", "180": "southbound", "270": "westbound"}


@st.cache_resource
def connect():
    session = Cluster(["localhost"]).connect("busbuffer")
    session.row_factory = dict_factory
    return session


@st.cache_data
def stop_names():
    names = {}
    for r in read_table("stops"):
        # stations only group the stops at a transfer point, buses don't stop at them
        if r["location_type"] == "0":
            names[r["stop_id"]] = f"{r['stop_name']}, {BOUND[r['cardinal_direction']]} (#{r['stop_code']})"
    return names


@st.cache_data
def later_stops():
    trips = {}
    for r in read_table("stop_times"):
        if r["trip_id"] not in trips:
            trips[r["trip_id"]] = []
        trips[r["trip_id"]].append(r["stop_id"])
    later = {}
    # the 14k trips only have 180 different lists of stops
    for stops in set(tuple(s) for s in trips.values()):
        for i in range(len(stops) - 1):
            if stops[i] not in later:
                later[stops[i]] = set()
            later[stops[i]].update(stops[i + 1:])
    return later


@st.cache_data
def trip_directions():
    return {r["trip_id"]: int(r["direction_id"]) for r in read_table("trips")}


def clock(ts):
    return datetime.fromtimestamp(ts, TZ).strftime("%H:%M:%S")


def group_name(q):
    if q["route_id"] == "all":
        return "all routes"
    if q["day_type"] == "all":
        return f"route {q['route_id']}"
    if q["hour"] == -1:
        return f"route {q['route_id']}, {q['day_type']}"
    return f"route {q['route_id']}, direction {q['direction_id']}, {q['day_type']}, {q['hour']}:00"


def read_quantiles(route_id, direction_id, service_date, hour):
    groups = []
    for key in quantile_keys(route_id, direction_id, service_date, hour):
        rows = session.execute("""
            SELECT * FROM error_quantiles
            WHERE route_id = %s AND direction_id = %s AND day_type = %s AND hour = %s
        """, key)
        groups.append({r["minutes_ahead"]: r for r in rows})
    return groups


@st.fragment(run_every=30)
def show_plans(origin, destination, deadline, walk_to, confidence):
    now = time.time()
    hour = datetime.now(TZ).hour
    buses = session.execute("""
        SELECT trip_id, service_date, route_id, toUnixTimestamp(feed_ts) / 1000 AS feed_ts
        FROM live_predictions_by_stop WHERE stop_id = %s
    """, [origin])
    plans = {}
    newest = 0
    for bus in buses:
        newest = max(newest, bus["feed_ts"])
        rows = session.execute("""
            SELECT stop_id, toUnixTimestamp(arrival) / 1000 AS arrival,
                   toUnixTimestamp(feed_ts) / 1000 AS feed_ts
            FROM live_predictions_by_trip WHERE service_date = %s AND trip_id = %s
        """, [bus["service_date"], bus["trip_id"]])
        # stop_times loops over the rows twice, a ResultSet can only be looped over once
        times = stop_times(list(rows), origin, destination)
        if times is None:
            continue
        at_origin, at_destination = times
        m_origin = minutes_ahead(at_origin - now)
        m_destination = minutes_ahead(at_destination - now)
        # errors were only measured up to 30 minutes ahead
        if m_origin is None or m_destination is None:
            continue
        direction_id = directions[bus["trip_id"]]
        groups = read_quantiles(bus["route_id"], direction_id, bus["service_date"].date(), hour)
        origin_group = pick_quantiles(groups, m_origin)
        destination_group = pick_quantiles(groups, m_destination)
        q_origin = origin_group["quantiles"]
        q_destination = destination_group["quantiles"]
        plan = plan_trip(at_origin, at_destination, q_origin, q_destination, walk_to, deadline, confidence)
        plan["route"] = bus["route_id"]
        plan["at_origin"] = at_origin
        plan["group"] = group_name(origin_group)
        plan["n"] = origin_group["n"]
        # some route 81 trips pass a stop twice
        plans[bus["trip_id"]] = plan

    if not plans:
        st.write("No bus with a live prediction gets from this stop to that one in the next 30 minutes.")
        return
    best = recommend(plans.values(), now, confidence)
    if best is None:
        st.write(f"None of these buses gets you to class on time with {confidence}% confidence.")
    else:
        st.subheader(f"Leave by {clock(best['leave_by'])}")
        st.write(
            f"Take route {best['route']}. The app says it gets to your stop at {clock(best['at_origin'])}, "
            f"be there by {clock(best['be_at_stop'])}. Chance of making class: {best['on_time']:.0f}%."
        )

    table = []
    for plan in sorted(plans.values(), key=lambda p: p["at_origin"]):
        table.append({
            "Route": plan["route"],
            "App says": clock(plan["at_origin"]),
            "Be at stop by": clock(plan["be_at_stop"]),
            "Leave by": clock(plan["leave_by"]),
            "At destination": f"{clock(plan['arrive_from'])} - {clock(plan['arrive_to'])}",
            "On time": f"{plan['on_time']:.0f}%",
            "Errors from": plan["group"],
            "n": plan["n"],
        })
    st.dataframe(table, hide_index=True)
    st.caption(
        f"Live predictions from {clock(newest)}, checked every 30 s. Only {100 - confidence}% of buses "
        "got to a stop before the be-at-stop time, and 80% got to the destination in the window shown."
    )


st.set_page_config(page_title="BusBuffer", layout="wide")
session = connect()
names = stop_names()
later = later_stops()
directions = trip_directions()

with st.sidebar:
    origins = sorted(later, key=names.get)
    origin = st.selectbox("From", origins, index=None, format_func=names.get, key="from", bind="query-params")
    after = []
    if origin is not None:
        after = sorted(later[origin], key=names.get)
    destination = st.selectbox("To", after, index=None, format_func=names.get, key="to", bind="query-params")
    class_start = st.time_input("Class starts", value=None, step=300, key="class", bind="query-params")
    walk_to = st.number_input("Walk to the stop (min)", 0, 60, 5, key="walk_to", bind="query-params")
    walk_from = st.number_input("Walk from the stop (min)", 0, 60, 5, key="walk_from", bind="query-params")
    confidence = st.radio("Confidence (%)", [80, 90, 95], index=1, key="confidence", bind="query-params")

st.title("BusBuffer")
planner_tab, errors_tab = st.tabs(["When to leave", "How far off the app is"])

with planner_tab:
    if origin is None or destination is None or class_start is None:
        st.write("Pick your stops and when class starts on the left.")
    else:
        class_ts = datetime.combine(datetime.now(TZ).date(), class_start, TZ).timestamp()
        show_plans(origin, destination, class_ts - walk_from * 60, walk_to * 60, confidence)

with errors_tab:
    routes = sorted(r["route_id"] for r in read_table("routes"))
    route = st.selectbox("Route", routes, index=routes.index("80"))
    rows = []
    for route_id, label in [("all", "All routes"), (route, f"Route {route}")]:
        found = session.execute("""
            SELECT minutes_ahead, quantiles, n FROM error_quantiles
            WHERE route_id = %s AND direction_id = -1 AND day_type = 'all' AND hour = -1
        """, [route_id])
        for r in found:
            rows.append({
                "Route": label,
                "Minutes ahead": r["minutes_ahead"],
                "p10": r["quantiles"][1],
                "Median": r["quantiles"][9],
                "p90": r["quantiles"][17],
                "n": r["n"],
            })
    table = pd.DataFrame(rows)
    color = alt.Color("Route", scale=alt.Scale(range=["#2a78d6", "#eb6834"]))
    x = alt.X("Minutes ahead:O", title="Minutes ahead the app said", axis=alt.Axis(labelAngle=0))
    y = alt.Y("p10", title="Seconds later than the app said")
    tooltip = ["Route", "Minutes ahead", "p10", "Median", "p90", "n"]
    base = alt.Chart(table).encode(x=x, xOffset="Route", color=color, tooltip=tooltip)
    bars = base.mark_bar(size=14, opacity=0.35).encode(y=y, y2="p90")
    ticks = base.mark_tick(size=14, thickness=3).encode(y="Median")
    st.altair_chart(bars + ticks)
    st.caption(
        "Error = when the bus got to the stop minus when the app said it would, from Oct 8 to Oct 20, 2026, "
        "before Metro's feed changed. The bar holds the middle 80% of buses and the tick is the median."
    )

st.caption(
    "Data provided under license granted by City of Madison, WI, Metro Transit. "
    "A student project, not an official Metro tool."
)
