import logging
import os

import pytest

from dsgrid.config.dimension_mapping_base import DimensionMappingType
from dsgrid.common import VALUE_COLUMN
from dsgrid.spark.functions import (
    aggregate_single_value,
    is_dataframe_empty,
)
from dsgrid.spark.types import use_duckdb
from dsgrid.utils.dataset import (
    apply_scaling_factor,
    is_noop_mapping,
    remove_invalid_null_timestamps,
    repartition_if_needed_by_mapping,
    unpivot_dataframe,
)
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.utils.spark import create_dataframe_from_dicts


@pytest.fixture(scope="module")
def dataframes():
    df = create_dataframe_from_dicts(
        [
            {
                "county": "Jefferson",
                "sector": "com",
                "com_elec": 2.1,
                "res_elec": None,
                "common_elec": 7.8,
            },
            {
                "county": "Boulder",
                "sector": "com",
                "com_elec": 3.5,
                "res_elec": None,
                "common_elec": 6.8,
            },
            {
                "county": "Denver",
                "sector": "res",
                "com_elec": None,
                "res_elec": 4.2,
                "common_elec": 5.8,
            },
            {
                "county": "Adams",
                "sector": "res",
                "com_elec": None,
                "res_elec": 1.3,
                "common_elec": 4.8,
            },
        ]
    )
    records = create_dataframe_from_dicts(
        [
            {"from_id": "res_elec", "to_id": "all_electricity", "from_fraction": 1.0},
            {"from_id": "com_elec", "to_id": "all_electricity", "from_fraction": 1.0},
            {"from_id": "common_elec", "to_id": "all_electricity", "from_fraction": 1.0},
        ]
    )
    pivoted_columns = {"com_elec", "res_elec", "common_elec"}
    yield df, records, pivoted_columns


@pytest.fixture(scope="module")
def pivoted_dataframe_with_time():
    df = create_dataframe_from_dicts(
        [
            {
                "time_index": 0,
                "county": "Jefferson",
                "cooling": 2.1,
                "heating": 1.3,
            },
            {
                "time_index": 1,
                "county": "Jefferson",
                "cooling": 2.2,
                "heating": 1.4,
            },
            {
                "time_index": 3,
                "county": "Jefferson",
                "cooling": 2.3,
                "heating": 1.5,
            },
            {
                "time_index": 0,
                "county": "Boulder",
                "cooling": 1.1,
                "heating": None,
            },
            {
                "time_index": 1,
                "county": "Boulder",
                "cooling": 1.2,
                "heating": None,
            },
            {
                "time_index": 3,
                "county": "Boulder",
                "cooling": 1.3,
                "heating": None,
            },
        ]
    )
    yield df.cache(), ["time_index"], ["cooling", "heating"]
    df.unpersist()


def test_is_noop_mapping_true():
    df = create_dataframe_from_dicts(
        [
            {
                "from_id": "elec_cooling",
                "to_id": "elec_cooling",
                "from_fraction": 1.0,
            },
            {
                "from_id": "elec_heating",
                "to_id": "elec_heating",
                "from_fraction": 1.0,
            },
        ]
    )
    assert is_noop_mapping(df)


def test_is_noop_mapping_false():
    for records in (
        [
            {
                "from_id": "elec_cooling",
                "to_id": "elec_cooling",
                "from_fraction": 1.0,
            },
            {
                "from_id": "electricity_heating",
                "to_id": "elec_heating",
                "from_fraction": 1.0,
            },
        ],
        [
            {
                "from_id": "elec_cooling",
                "to_id": "electricity_cooling",
                "from_fraction": 1.0,
            },
            {
                "from_id": "elec_heating",
                "to_id": "elec_heating",
                "from_fraction": 1.0,
            },
        ],
        [
            {
                "from_id": "elec_cooling",
                "to_id": "elec_cooling",
                "from_fraction": 2.0,
            },
            {
                "from_id": "elec_heating",
                "to_id": "elec_heating",
                "from_fraction": 1.0,
            },
        ],
        [
            {
                "from_id": "elec_cooling",
                "to_id": "elec_cooling",
                "from_fraction": 1.0,
            },
            {
                "from_id": "elec_heating",
                "to_id": "elec_heating",
                "from_fraction": 2.0,
            },
        ],
        [
            # NULLs are ignored
            {
                "from_id": "elec_cooling",
                "to_id": "elec_cooling",
                "from_fraction": 1.0,
            },
            {
                "from_id": "elec_cooling",
                "to_id": None,
                "from_fraction": 1.0,
            },
        ],
        [
            # NULLs are ignored
            {
                "from_id": "elec_cooling",
                "to_id": "elec_cooling",
                "from_fraction": 1.0,
            },
            {
                "from_id": None,
                "to_id": "elec_cooling",
                "from_fraction": 1.0,
            },
        ],
    ):
        df = create_dataframe_from_dicts(records)
        assert not is_noop_mapping(df)


def test_remove_invalid_null_timestamps():
    df = create_dataframe_from_dicts(
        [
            # No nulls
            {"timestamp": 1, "county": "Jefferson", "subsector": "warehouse", "value": 4},
            {"timestamp": 2, "county": "Jefferson", "subsector": "warehouse", "value": 5},
            # Nulls and valid values
            {"timestamp": None, "county": "Boulder", "subsector": "large_office", "value": 0},
            {"timestamp": 1, "county": "Boulder", "subsector": "large_office", "value": 4},
            {"timestamp": 2, "county": "Boulder", "subsector": "large_office", "value": 5},
            # Only nulls
            {"timestamp": None, "county": "Adams", "subsector": "retail_stripmall", "value": 0},
            {"timestamp": None, "county": "Denver", "subsector": "hospital", "value": 0},
        ]
    )
    stacked = ["county", "subsector"]
    time_col = "timestamp"
    result = remove_invalid_null_timestamps(df, {time_col}, stacked)
    assert result.count() == 6
    assert result.filter("county == 'Boulder'").count() == 2
    assert is_dataframe_empty(result.filter(f"county == 'Boulder' and {time_col} is NULL"))


def test_apply_scaling_factor():
    df = create_dataframe_from_dicts(
        [
            {"a": 1, "b": 2, "bystander": 1, "scaling_factor": 5},
            {"a": 2, "b": 3, "bystander": 1, "scaling_factor": 6},
            {"a": 3, "b": 4, "bystander": 1, "scaling_factor": None},
        ],
    )
    df2 = apply_scaling_factor(df, ("a", "b"))
    expected_sum_a = 1 * 5 + 2 * 6 + 3
    expected_sum_b = 2 * 5 + 3 * 6 + 4
    expected_sum_bystander = 1 + 1 + 1

    assert aggregate_single_value(df2, "sum", "a") == expected_sum_a
    assert aggregate_single_value(df2, "sum", "b") == expected_sum_b
    assert aggregate_single_value(df2, "sum", "bystander") == expected_sum_bystander


@pytest.mark.skipif(use_duckdb(), reason="This feature is not used with DuckDB.")
def test_repartition_if_needed_by_mapping(tmp_path, caplog, dataframes):
    df = dataframes[0]
    context = ScratchDirContext(tmp_path)
    with caplog.at_level(logging.INFO):
        df = repartition_if_needed_by_mapping(
            df,
            DimensionMappingType.ONE_TO_MANY_DISAGGREGATION,
            context,
        )
        assert "Completed repartition" in caplog.text


@pytest.mark.skipif(use_duckdb(), reason="This feature is not used with DuckDB.")
def test_repartition_if_needed_by_mapping_override(tmp_path, caplog, dataframes):
    df = dataframes[0]
    context = ScratchDirContext(tmp_path)
    os.environ["DSGRID_SKIP_MAPPING_SKEW_REPARTITION"] = "true"
    try:
        with caplog.at_level(logging.INFO):
            df = repartition_if_needed_by_mapping(
                df,
                DimensionMappingType.ONE_TO_MANY_DISAGGREGATION,
                context,
            )
            assert "DSGRID_SKIP_MAPPING_SKEW_REPARTITION is true" in caplog.text
    finally:
        os.environ.pop("DSGRID_SKIP_MAPPING_SKEW_REPARTITION")


@pytest.mark.skipif(use_duckdb(), reason="This feature is not used with DuckDB.")
def test_repartition_if_needed_by_mapping_not_needed(tmp_path, caplog, dataframes):
    df = dataframes[0]
    context = ScratchDirContext(tmp_path)
    with caplog.at_level(logging.DEBUG):
        df = repartition_if_needed_by_mapping(
            df,
            DimensionMappingType.ONE_TO_ONE,
            context,
        )
        assert "Repartition is not needed" in caplog.text
        assert "Completed repartition" not in caplog.text


def test_unpivot(pivoted_dataframe_with_time):
    df, time_columns, value_columns = pivoted_dataframe_with_time
    unpivoted = unpivot_dataframe(df, value_columns, "end_use", time_columns)
    expected_columns = [*time_columns, "county", "end_use", VALUE_COLUMN]
    assert unpivoted.columns == expected_columns
    null_data = unpivoted.filter("county = 'Boulder' and end_use = 'heating'").collect()
    assert len(null_data) == 1
    assert null_data[0].time_index is None
    assert null_data[0][VALUE_COLUMN] is None
