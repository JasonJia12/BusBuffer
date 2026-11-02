from busbuffer.prediction_errors import minutes_ahead, update_trip


def test_minutes_ahead():
    assert minutes_ahead(0) is None
    assert minutes_ahead(60) == 1
    assert minutes_ahead(61) == 2
    assert minutes_ahead(1800) == 30
    assert minutes_ahead(1801) is None


def test_error_when_stop_is_passed():
    state, _ = update_trip(None, 1000, {1: None, 2: 1100, 3: 1500})
    state, _ = update_trip(state, 1030, {1: None, 2: 1110, 3: 1500})
    state, errors = update_trip(state, 1060, {2: 1100, 3: 1500})
    assert errors == []
    state, errors = update_trip(state, 1090, {3: 1520})
    assert errors == [
        {"stop_sequence": 2, "minutes_ahead": 2, "lead": 100, "predicted": 1100, "actual": 1075},
        {"stop_sequence": 2, "minutes_ahead": 1, "lead": 40, "predicted": 1100, "actual": 1075},
    ]


def test_stop_falling_off_the_end_keeps_its_prediction():
    state, _ = update_trip(None, 1000, {1: 1100, 2: 1300})
    state, _ = update_trip(state, 1030, {1: 1100})
    state, _ = update_trip(state, 1060, {2: 1300})
    state, errors = update_trip(state, 1090, {3: 1500})
    assert errors == [{"stop_sequence": 2, "minutes_ahead": 5, "lead": 300, "predicted": 1300, "actual": 1075}]


def test_passed_stop_that_comes_back_counts_once():
    state, _ = update_trip(None, 1000, {1: 1020, 2: 1200})
    state, errors = update_trip(state, 1030, {2: 1200})
    assert len(errors) == 1
    state, _ = update_trip(state, 1060, {1: 1070, 2: 1200})
    state, errors = update_trip(state, 1090, {2: 1200})
    assert errors == []


def test_stop_passed_in_a_long_gap_gets_no_error():
    state, _ = update_trip(None, 1000, {1: 1050, 2: 1500})
    state, errors = update_trip(state, 1300, {2: 1500})
    assert errors == []
    state, errors = update_trip(state, 1330, {3: 1700})
    assert errors == [
        {"stop_sequence": 2, "minutes_ahead": 10, "lead": 500, "predicted": 1500, "actual": 1315},
        {"stop_sequence": 2, "minutes_ahead": 5, "lead": 200, "predicted": 1500, "actual": 1315},
    ]


def test_trip_starts_over_the_next_day():
    state, _ = update_trip(None, 1000, {1: 1020, 2: 1200})
    state, _ = update_trip(state, 1030, {2: 1200})
    state, _ = update_trip(state, 87400, {1: 87420, 2: 87600})
    state, errors = update_trip(state, 87430, {2: 87600})
    assert len(errors) == 1
