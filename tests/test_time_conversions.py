from datetime import datetime

from dsgrid.time.time_conversions import (
    convert_datetime_to_day_type,
    convert_datetime_to_season,
    interpret_datetime,
)


def test_convert_datetime_to_day_type():
    assert convert_datetime_to_day_type(datetime(2020, 12, 27)) == "weekend"
    assert convert_datetime_to_day_type(datetime(2020, 12, 28)) == "weekday"


def test_convert_datetime_to_season():
    assert convert_datetime_to_season(datetime(2020, 1, 5)) == "winter"
    assert convert_datetime_to_season(datetime(2020, 4, 5)) == "spring"
    assert convert_datetime_to_season(datetime(2020, 7, 5)) == "summer"
    assert convert_datetime_to_season(datetime(2020, 11, 5)) == "autumn"
    assert convert_datetime_to_season(datetime(2020, 12, 30)) == "winter"


def test_interpret_datetime():
    """Should return formatted datetime string"""
    timestamps_seconds = (
        "2020-01-01 05:04:03",
        "2020-01-01T05:04:03",
        "2020-01-01T05:04:03Z",
    )
    for timestamp in timestamps_seconds:
        assert interpret_datetime(timestamp) == datetime(2020, 1, 1, 5, 4, 3)

    timestamps_microseconds = (
        "2020-01-01 05:04:03.001",
        "2020-01-01T05:04:03.001",
    )
    for timestamp in timestamps_microseconds:
        assert interpret_datetime(timestamp) == datetime(2020, 1, 1, 5, 4, 3, 1000)
