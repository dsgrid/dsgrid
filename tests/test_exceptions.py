"""Tests for dsgrid.exceptions."""

import traceback

import pytest

from dsgrid.exceptions import DSGInvalidParameter, error_context


def test_error_context_adds_note_and_preserves_exception():
    original = DSGInvalidParameter("Data file does not exist: load_data.parquet")
    with pytest.raises(DSGInvalidParameter) as exc_info:
        with error_context("registering dataset my_dataset"):
            raise original

    assert exc_info.value is original
    assert str(exc_info.value) == "Data file does not exist: load_data.parquet"
    assert exc_info.value.__notes__ == ["While registering dataset my_dataset"]
    formatted = "".join(traceback.format_exception(exc_info.value)).splitlines()
    assert formatted[-1] == "While registering dataset my_dataset"


def test_error_context_nested_notes_ordered_inner_to_outer():
    with pytest.raises(ValueError) as exc_info:
        with error_context("processing query q1"):
            with error_context("processing dataset d1"):
                msg = "bad value"
                raise ValueError(msg)

    assert exc_info.value.__notes__ == [
        "While processing dataset d1",
        "While processing query q1",
    ]


def test_error_context_no_exception():
    with error_context("doing nothing"):
        result = 1 + 1
    assert result == 2
