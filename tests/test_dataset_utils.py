from dsgrid.dataset.dataset_mapping_manager import DatasetMappingManager
import logging

import pytest

from dsgrid.config.dimension_mapping_base import DimensionMappingType
from dsgrid.common import VALUE_COLUMN
from dsgrid.query.dataset_mapping_plan import DatasetMappingPlan
from dsgrid.spark.functions import (
    aggregate_single_value,
    cache,
    get_spark_session,
    is_dataframe_empty,
    unpersist,
)
from dsgrid.spark.types import (
    StructField,
    StructType,
    DoubleType,
    IntegerType,
    ShortType,
    LongType,
    StringType,
    use_duckdb,
)
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
    yield cache(df), ["time_index"], ["cooling", "heating"]
    unpersist(df)


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
    spark = get_spark_session()
    df = spark.createDataFrame(
        [
            ("2018-01-01 01:00:00", 2030, "Jefferson", 1.0),
            ("2018-01-01 02:00:00", 2030, "Jefferson", 2.0),
            ("2018-01-01 03:00:00", 2030, "Jefferson", 3.0),
        ],
        StructType(
            [
                StructField("timestamp", StringType(), True),
                StructField("model_year", IntegerType(), False),
                StructField("geography", StringType(), False),
                StructField("value", DoubleType(), True),
            ],
        ),
    )
    lookup = spark.createDataFrame(
        [
            (None, 2030, "Jefferson"),
            (None, 2030, "Boulder"),
        ],
        StructType(
            [
                StructField("id", IntegerType(), True),
                StructField("model_year", IntegerType(), False),
                StructField("geography", StringType(), False),
            ],
        ),
    )
    result = add_null_rows_from_load_data_lookup(df, lookup)
    assert result.count() == 4
    null_rows = result.filter("timestamp is NULL").collect()
    assert len(null_rows) == 1
    assert null_rows[0].geography == "Boulder"


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
    assert result.count() == 6
    assert result.filter("county == 'Boulder'").count() == 2
    assert is_dataframe_empty(result.filter(f"county == 'Boulder' and {time_col} is NULL"))


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

    assert aggregate_single_value(df2, "sum", "value") == expected_sum
    assert aggregate_single_value(df2, "sum", "bystander") == expected_sum_bystander


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
    assert unpivoted.columns == expected_columns
    null_data = unpivoted.filter("county = 'Boulder' and end_use = 'heating'").collect()
    assert len(null_data) == 1
    assert null_data[0].time_index is None
    assert null_data[0][VALUE_COLUMN] is None


@pytest.mark.parametrize("data_type", [IntegerType(), ShortType(), LongType()])
def test_convert_types_if_necessary(data_type):
    schema = StructType(
        [
            StructField("model_year", data_type, False),
            StructField("weather_year", data_type, False),
            StructField("bystander", IntegerType(), False),
        ]
    )
    df1 = get_spark_session().createDataFrame([(2030, 2018, 2040)], schema)
    df2 = convert_types_if_necessary(df1)
    row = df2.collect()[0]
    assert row.model_year == "2030"
    assert row.weather_year == "2018"
    assert row.bystander == 2040


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
