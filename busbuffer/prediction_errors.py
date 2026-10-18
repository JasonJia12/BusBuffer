# Arrival prediction errors, worked out one trip at a time from the trips feed.

MINUTES_AHEAD = [1, 2, 3, 5, 7, 10, 15, 20, 30]
MAX_GAP = 120  # seconds


def minutes_ahead(lead):
    for m in MINUTES_AHEAD:
        if 0 < lead <= m * 60:
            return m
    return None


def update_trip(state, feed_ts, stops):
    if state is None or feed_ts - state["feed_ts"] > MAX_GAP:
        state = {"feed_ts": feed_ts, "listed": set(), "passed": 0, "saved": {}}

    errors = []
    actual = (state["feed_ts"] + feed_ts) / 2
    for seq in sorted(state["listed"] - set(stops)):
        # only stops dropping off the front were passed, the far end of the list comes and goes
        if seq < min(stops):
            for m, (lead, predicted) in state["saved"].pop(seq, {}).items():
                errors.append({"stop_sequence": seq, "minutes_ahead": m, "lead": lead,
                               "predicted": predicted, "actual": actual})
            state["passed"] = max(state["passed"], seq)

    for seq, predicted in stops.items():
        # passed stops sometimes show up again
        if predicted is None or seq <= state["passed"]:
            continue
        lead = predicted - feed_ts
        m = minutes_ahead(lead)
        saved = state["saved"].setdefault(seq, {})
        if m and m not in saved:
            saved[m] = (lead, predicted)

    state["feed_ts"] = feed_ts
    state["listed"] = set(stops)
    return state, errors
