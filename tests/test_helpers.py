"""Tests for the shared helpers in :mod:`tests._helpers`."""

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from tests._helpers import collect as _collect, order_by as _order_by, perform_interval_op


def _utc(values):
    utc = ZoneInfo("UTC")
    return [v.replace(tzinfo=utc) if v.tzinfo is None else v.astimezone(utc) for v in values]


def test_perform_interval_op_adds_new_column(time_dataframe):
    shifted = perform_interval_op(time_dataframe, "timestamp", "+", 3600, "SECONDS", "timestamp2")
    # The original column is preserved and the shifted values land in a new column.
    assert "timestamp" in shifted.columns
    res = [
        r.timestamp2
        for r in _collect(_order_by(shifted.select("timestamp2").distinct(), "timestamp2"))
    ]
    assert _utc(res) == [
        datetime(2020, 1, 1, 1, tzinfo=ZoneInfo("UTC")),
        datetime(2020, 1, 1, 2, tzinfo=ZoneInfo("UTC")),
    ]


def test_perform_interval_op_subtracts(time_dataframe):
    shifted = perform_interval_op(time_dataframe, "timestamp", "-", 3600, "SECONDS", "timestamp2")
    res = [
        r.timestamp2
        for r in _collect(_order_by(shifted.select("timestamp2").distinct(), "timestamp2"))
    ]
    assert _utc(res) == [
        datetime(2019, 12, 31, 23, tzinfo=ZoneInfo("UTC")),
        datetime(2020, 1, 1, 0, tzinfo=ZoneInfo("UTC")),
    ]


def test_perform_interval_op_in_place_preserves_position(time_dataframe):
    cols_before = list(time_dataframe.columns)
    shifted = perform_interval_op(time_dataframe, "timestamp", "+", 3600, "SECONDS", "timestamp")
    # Shifting in place keeps the column name and its position; no extra column.
    assert list(shifted.columns) == cols_before


def test_perform_interval_op_rejects_bad_op(time_dataframe):
    with pytest.raises(ValueError, match="Unsupported interval op"):
        perform_interval_op(time_dataframe, "timestamp", "*", 3600, "SECONDS", "timestamp2")
