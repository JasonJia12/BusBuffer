from datetime import date, datetime

from busbuffer.static_gtfs import TZ, service_date


def ts(text):
    return datetime.fromisoformat(text).replace(tzinfo=TZ).timestamp()


def test_daytime_trip():
    assert service_date(ts("2026-10-09T08:40"), 8 * 3600) == date(2026, 10, 9)


def test_trip_after_midnight_belongs_to_the_day_before():
    assert service_date(ts("2026-10-10T01:45"), 25 * 3600 + 30 * 60) == date(2026, 10, 9)
    assert service_date(ts("2026-10-10T00:20"), 23 * 3600 + 50 * 60) == date(2026, 10, 9)


def test_early_bus_on_the_first_trip_of_the_day():
    assert service_date(ts("2026-10-10T04:33"), 4 * 3600 + 37 * 60) == date(2026, 10, 10)
