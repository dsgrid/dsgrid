"""Tests for :class:`dsgrid.query.report_peak_load.PeakLoadReport`.

The input tables here are small enough to check by eye. Each test states the
expected report rows literally rather than recomputing them, so a reader can
confirm the peak values and the time steps at which they occur without running
anything.

The time column holds an integer index rather than a timestamp. The report is
indifferent to the time column's type -- it only carries the column through a
join -- and a naive timestamp does not round-trip through Parquet identically
on both backends, because Spark stores it as an instant relative to the session
time zone.
"""

from pathlib import Path
from typing import Any, cast

import pytest

from dsgrid.dataset.models import PivotedTableFormatModel, StackedTableFormatModel
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidQuery
from dsgrid.ibis.io import read_dataframe, write_dataframe
from dsgrid.ibis.session import create_dataframe_from_dicts
from dsgrid.query.models import (
    DatasetModel,
    ProjectQueryModel,
    ProjectQueryParamsModel,
    QueryResultParamsModel,
    StandaloneDatasetModel,
)
from dsgrid.query.query_context import QueryContext
from dsgrid.query.report_peak_load import PeakLoadInputModel, PeakLoadReport

from tests._helpers import collect as _collect


class StubQueryContext:
    """Supplies the only QueryContext method that PeakLoadReport.generate calls."""

    def __init__(self, time_columns: set[str], metric_columns: set[str]) -> None:
        self._columns = {
            DimensionType.TIME: time_columns,
            DimensionType.METRIC: metric_columns,
        }

    def get_dimension_column_names(
        self, dimension_type: DimensionType, dataset_id: str | None = None
    ) -> set[str]:
        return self._columns[dimension_type]


# Two geographies over three time steps. CO peaks at 5.0 on time step 1, NM at
# 9.0 on time step 0. Each peak is unique within its group, so each geography
# contributes exactly one report row. NM repeats 2.0 on time steps 1 and 2: the
# report joins on the value column, so a duplicate *non-peak* value must not
# fan out.
LOAD_ROWS: list[dict[str, Any]] = [
    {"time_index": 0, "geography": "CO", "metric": "electricity", "value": 1.0},
    {"time_index": 1, "geography": "CO", "metric": "electricity", "value": 5.0},
    {"time_index": 2, "geography": "CO", "metric": "electricity", "value": 3.0},
    {"time_index": 0, "geography": "NM", "metric": "electricity", "value": 9.0},
    {"time_index": 1, "geography": "NM", "metric": "electricity", "value": 2.0},
    {"time_index": 2, "geography": "NM", "metric": "electricity", "value": 2.0},
]


def _run_report(
    tmp_path: Path,
    rows: list[dict[str, Any]],
    group_by_columns: list[str],
    time_columns: set[str] | None = None,
):
    """Write ``rows`` to Parquet, run the report, and return the report table."""
    load_data_file = tmp_path / "table.parquet"
    write_dataframe(create_dataframe_from_dicts(rows), load_data_file)
    context = StubQueryContext(
        time_columns={"time_index"} if time_columns is None else time_columns,
        metric_columns={"metric"},
    )
    output_file = PeakLoadReport().generate(
        load_data_file,
        tmp_path,
        cast(QueryContext, context),
        PeakLoadInputModel(group_by_columns=group_by_columns),
    )
    return read_dataframe(output_file)


def _as_tuples(table) -> list[tuple[Any, ...]]:
    return [(r.geography, r.metric, r.value, r.time_index) for r in _collect(table)]


def _with_value(rows: list[dict[str, Any]], geography: str, time_index: int, value: float):
    """Copy ``rows``, overwriting the value of one row identified by its dimensions."""
    updated = [dict(row) for row in rows]
    matches = [
        row for row in updated if row["geography"] == geography and row["time_index"] == time_index
    ]
    assert len(matches) == 1, f"expected one row for {geography=} {time_index=}"
    matches[0]["value"] = value
    return updated


def test_peak_load_report_reports_peak_value_and_its_time_step(tmp_path):
    """CO peaks at 5.0 on time step 1; NM peaks at 9.0 on time step 0."""
    table = _run_report(tmp_path, LOAD_ROWS, ["geography"])
    assert sorted(_as_tuples(table)) == [
        ("CO", "electricity", 5.0, 1),
        ("NM", "electricity", 9.0, 0),
    ]


def test_peak_load_report_column_order(tmp_path):
    """Group-by columns, then the metric column, then the value, then time."""
    table = _run_report(tmp_path, LOAD_ROWS, ["geography"])
    assert table.columns == ("geography", "metric", "value", "time_index")


def test_peak_load_report_metric_column_not_duplicated(tmp_path):
    """generate() appends the metric column only when the caller omits it."""
    table = _run_report(tmp_path, LOAD_ROWS, ["geography", "metric"])
    assert table.columns == ("geography", "metric", "value", "time_index")
    assert sorted(_as_tuples(table)) == [
        ("CO", "electricity", 5.0, 1),
        ("NM", "electricity", 9.0, 0),
    ]


def test_peak_load_report_emits_every_tied_peak(tmp_path):
    """A group whose peak occurs twice yields one row per peak time step.

    The report recovers the time step by joining on the peak value, so it cannot
    pick a winner among ties. CO now hits 5.0 on both time step 1 and time step 2.
    """
    rows = _with_value(LOAD_ROWS, geography="CO", time_index=2, value=5.0)
    table = _run_report(tmp_path, rows, ["geography"])
    assert sorted(_as_tuples(table)) == [
        ("CO", "electricity", 5.0, 1),
        ("CO", "electricity", 5.0, 2),
        ("NM", "electricity", 9.0, 0),
    ]


def test_peak_load_report_rejects_missing_time_column(tmp_path):
    with pytest.raises(Exception, match="expected time column"):
        _run_report(tmp_path, LOAD_ROWS, ["geography"], time_columns={"not_a_column"})


def _make_query(table_format) -> ProjectQueryModel:
    return ProjectQueryModel(
        name="peak-load",
        project=ProjectQueryParamsModel(
            project_id="test",
            dataset=DatasetModel(
                dataset_id="derived",
                source_datasets=[StandaloneDatasetModel(dataset_id="source")],
            ),
        ),
        result=QueryResultParamsModel(table_format=table_format),
    )


def test_check_query_accepts_stacked_format():
    PeakLoadReport().check_query(_make_query(StackedTableFormatModel()))


def test_check_query_rejects_pivoted_format():
    query = _make_query(PivotedTableFormatModel(pivoted_dimension_type=DimensionType.METRIC))
    with pytest.raises(DSGInvalidQuery, match="requires the value format to be stacked"):
        PeakLoadReport().check_query(query)
