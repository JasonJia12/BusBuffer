# AI edited; see AI_transcript_full.md for details.
from busbuffer.planner import percent_below, pick_quantiles, plan_trip, recommend, stop_times

QUANTILES = list(range(-90, 100, 10))


def test_percent_below():
    assert percent_below(QUANTILES, 0) == 50
    assert percent_below(QUANTILES, 5) == 52.5
    assert percent_below(QUANTILES, -200) == 5
    assert percent_below(QUANTILES, 200) == 95


def test_percent_below_with_a_repeated_quantile():
    quantiles = [-10] * 5 + list(range(0, 140, 10))
    assert percent_below(quantiles, -10) == 25
    assert percent_below(quantiles, -5) == 27.5


def test_plan_trip():
    plan = plan_trip(1000, 2000, QUANTILES, QUANTILES, 300, 2050, 90)
    assert plan == {"be_at_stop": 920, "leave_by": 620, "on_time": 75, "arrive_from": 1920, "arrive_to": 2080}


def test_small_group_falls_back_to_a_wider_one():
    groups = [{10: {"n": 30}}, {5: {"n": 400}}, {10: {"n": 900}}, {10: {"n": 50000}}]
    assert pick_quantiles(groups, 10) == {"n": 900}


def test_stop_times_skips_a_trip_that_passed_the_origin():
    rows = [
        {"stop_id": "O", "arrival": 1100, "feed_ts": 1000},
        {"stop_id": "X", "arrival": 1200, "feed_ts": 1030},
        {"stop_id": "D", "arrival": 1300, "feed_ts": 1030},
    ]
    assert stop_times(rows, [], "O", "D") is None
    rows[0]["feed_ts"] = 1030
    assert stop_times(rows, [], "O", "D") == (1100, 1300)
    assert stop_times(rows, [], "D", "O") is None


def test_stop_times_stays_on_the_bus_into_its_next_trip():
    rows = [
        {"stop_id": "O", "arrival": 1100, "feed_ts": 1000},
        {"stop_id": "X", "arrival": 1200, "feed_ts": 1000},
    ]
    next_rows = [
        {"stop_id": "D", "arrival": 1600, "feed_ts": 1000},
        {"stop_id": "O", "arrival": 2500, "feed_ts": 1000},
    ]
    assert stop_times(rows, next_rows, "O", "D") == (1100, 1600)
    next_rows[0]["feed_ts"] = 970
    assert stop_times(rows, next_rows, "O", "D") is None
    next_rows[0]["feed_ts"] = 1000
    rows[0]["feed_ts"] = 970
    assert stop_times(rows, next_rows, "O", "D") is None


def test_recommend_the_latest_bus_likely_to_be_on_time():
    plans = [
        {"leave_by": 500, "on_time": 95},
        {"leave_by": 700, "on_time": 95},
        {"leave_by": 900, "on_time": 60},
    ]
    assert recommend(plans, 600, 90) == plans[1]
