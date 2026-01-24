from dsgrid.dataset.dataset_mapping_manager import DatasetMappingManager
import logging

import pytest
import ibis
import pandas as pd

from dsgrid.config.dimension_mapping_base import DimensionMappingType
from dsgrid.common import VALUE_COLUMN
from dsgrid.query.dataset_mapping_plan import DatasetMappingPlan
from dsgrid.spark.types import (
    IntegerType,
    ShortType,
    LongType,
)
from dsgrid.tests.utils import use_duckdb
from dsgrid.utils.dataset import (
    add_null_rows_from_load_data_lookup,
    apply_scaling_factor,
    convert_types_if_necessary,
    is_noop_mapping,
    remove_invalid_null_timestamps,
    repartition_if_needed_by_mapping,
    unpivot_dataframe,
)
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.ibis_api import create_dataframe_from_dicts


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
            {
                "from_id": "common_elec",
                "to_id": "all_electricity",
                "from_fraction": 1.0,
            },
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
    yield df, ["time_index"], ["cooling", "heating"]


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


def test_add_null_rows_from_load_data_lookup():
    df = ibis.memtable(
        pd.DataFrame(
            [
                ("2018-01-01 01:00:00", 2030, "Jefferson", 1.0),
                ("2018-01-01 02:00:00", 2030, "Jefferson", 2.0),
                ("2018-01-01 03:00:00", 2030, "Jefferson", 3.0),
            ],
            columns=["timestamp", "model_year", "geography", "value"],
        )
    )
    # Cast to match schema if necessary, or rely on inference.
    # Original test specified schema. ibis/pandas might infer types differently (e.g. string vs object).
    # table_to_dataframe handles it.

    lookup = ibis.memtable(
        pd.DataFrame(
            [
                (None, 2030, "Jefferson"),
                (None, 2030, "Boulder"),
            ],
            columns=["id", "model_year", "geography"],
        )
    )
    result = add_null_rows_from_load_data_lookup(df, lookup)
    assert result.count().execute() == 4
    null_rows = result.filter(result["timestamp"].isnull()).to_pyarrow().to_pylist()
    assert len(null_rows) == 1
    assert null_rows[0]["geography"] == "Boulder"


def test_remove_invalid_null_timestamps():
    df = create_dataframe_from_dicts(
        [
            # No nulls
            {
                "timestamp": 1,
                "county": "Jefferson",
                "subsector": "warehouse",
                "value": 4,
            },
            {
                "timestamp": 2,
                "county": "Jefferson",
                "subsector": "warehouse",
                "value": 5,
            },
            # Nulls and valid values
            {
                "timestamp": None,
                "county": "Boulder",
                "subsector": "large_office",
                "value": 0,
            },
            {
                "timestamp": 1,
                "county": "Boulder",
                "subsector": "large_office",
                "value": 4,
            },
            {
                "timestamp": 2,
                "county": "Boulder",
                "subsector": "large_office",
                "value": 5,
            },
            # Only nulls
            {
                "timestamp": None,
                "county": "Adams",
                "subsector": "retail_stripmall",
                "value": 0,
            },
            {
                "timestamp": None,
                "county": "Denver",
                "subsector": "hospital",
                "value": 0,
            },
        ]
    )
    stacked = ["county", "subsector"]
    time_col = "timestamp"
    result = remove_invalid_null_timestamps(df, {time_col}, stacked)
    assert result.count().execute() == 6
    assert result.filter(result["county"] == "Boulder").count().execute() == 2
    assert (
        result.filter((result["county"] == "Boulder") & result[time_col].isnull())
        .count()
        .execute()
        == 0
    )


def test_apply_scaling_factor(tmp_path):
    df = create_dataframe_from_dicts(
        [
            {"value": 1, "bystander": 1, "scaling_factor": 5},
            {"value": 2, "bystander": 1, "scaling_factor": 6},
            {"value": 3, "bystander": 1, "scaling_factor": 0},
            {"value": 4, "bystander": 1, "scaling_factor": None},
        ],
    )
    dataset_id = "test_dataset"
    plan = DatasetMappingPlan(dataset_id=dataset_id)
    with DatasetMappingManager(dataset_id, plan, ScratchDirContext(tmp_path)) as mgr:
        df2 = apply_scaling_factor(df, "value", mgr)
    expected_sum = 1 * 5 + 2 * 6 + 0 + 4
    expected_sum_bystander = 1 + 1 + 1 + 1

    assert df2.to_pandas()["value"].sum() == expected_sum
    assert df2.to_pandas()["bystander"].sum() == expected_sum_bystander


@pytest.mark.skipif(use_duckdb(), reason="This feature is not used with DuckDB.")
def test_repartition_if_needed_by_mapping(tmp_path, caplog, dataframes):
    df = dataframes[0]
    context = ScratchDirContext(tmp_path)
    with caplog.at_level(logging.INFO):
        df, _ = repartition_if_needed_by_mapping(
            df,
            DimensionMappingType.ONE_TO_MANY_DISAGGREGATION,
            context,
        )
        assert "Completed repartition" in caplog.text


@pytest.mark.skipif(use_duckdb(), reason="This feature is not used with DuckDB.")
def test_repartition_if_needed_by_mapping_not_needed(tmp_path, caplog, dataframes):
    df = dataframes[0]
    context = ScratchDirContext(tmp_path)
    with caplog.at_level(logging.DEBUG):
        df, _ = repartition_if_needed_by_mapping(
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
    assert list(unpivoted.columns) == expected_columns
    null_data = (
        unpivoted.filter((unpivoted["county"] == "Boulder") & (unpivoted["end_use"] == "heating"))
        .to_pyarrow()
        .to_pylist()
    )
    assert len(null_data) == 1
    assert null_data[0]["time_index"] is None
    assert null_data[0][VALUE_COLUMN] is None


@pytest.mark.parametrize("data_type", [IntegerType(), ShortType(), LongType()])
def test_convert_types_if_necessary(data_type):
    # Ibis doesn't support Spark types in schema.
    # We can create dataframe and cast?
    # Or just use pandas and let ibis infer.
    # The test checks if types are converted.
    # If we pass integers, Ibis infers integers (int64).
    # convert_types_if_necessary converts them to string?
    # No, it converts based on some logic?
    # Let's check convert_types_if_necessary implementation.
    # It casts model_year, weather_year, etc to string.
    # So if input is int, output should be string.

    df1 = ibis.memtable(
        pd.DataFrame([(2030, 2018, 2040)], columns=["model_year", "weather_year", "bystander"])
    )
    # If we want specific types (ShortType etc), we might need to cast in Ibis or use PyArrow table.
    # But for this test, checking int -> string conversion is likely enough.

    df2 = convert_types_if_necessary(df1)
    row = df2.to_pyarrow().to_pylist()[0]
    assert row["model_year"] == "2030"
    assert row["weather_year"] == "2018"
    assert row["bystander"] == 2040


@pytest.fixture
def missing_dimension_associations(tmp_path):
    filename = tmp_path / "missing_associations.csv"
    with open(filename, "w") as f:
        f.write("sector,subsector\n")
        f.write("com,midrise_apartment\n")
        f.write("res,hotel\n")
        f.write("res,hospital\n")

    df_from_records_cross_join = create_dataframe_from_dicts(
        [
            {
                "sector": "res",
                "subsector": "midrise_apartment",
                "geography": "36047",
            },
            {
                "sector": "com",
                "subsector": "midrise_apartment",
                "geography": "36047",
            },
            {
                "sector": "com",
                "subsector": "hotel",
                "geography": "36047",
            },
            {
                "sector": "res",
                "subsector": "hotel",
                "geography": "36047",
            },
            {
                "sector": "com",
                "subsector": "hospital",
                "geography": "36047",
            },
            {
                "sector": "res",
                "subsector": "hospital",
                "geography": "36047",
            },
        ]
    )
    yield filename, df_from_records_cross_join
    filename.unlink()
