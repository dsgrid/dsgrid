import math

import pyspark.sql.functions as F
import pytest

from dsgrid.utils.dataset import (
    map_and_reduce_pivoted_dimension,
    is_noop_mapping,
    remove_invalid_null_timestamps,
)
from dsgrid.utils.spark import get_spark_session


@pytest.fixture(scope="module")
def dataframes():
    spark = get_spark_session()
    df = spark.createDataFrame(
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
    records = spark.createDataFrame(
        [
            {"from_id": "res_elec", "to_id": "all_electricity", "from_fraction": 1.0},
            {"from_id": "com_elec", "to_id": "all_electricity", "from_fraction": 1.0},
            {"from_id": "common_elec", "to_id": "all_electricity", "from_fraction": 1.0},
        ]
    )
    pivoted_columns = {"com_elec", "res_elec", "common_elec"}
    yield df, records, pivoted_columns


def test_map_and_reduce_pivoted_dimension_sum_rename(dataframes):
    df, records, pivoted_columns = dataframes
    operation = "sum"
    res, new_pivoted, dropped = map_and_reduce_pivoted_dimension(
        df, records, pivoted_columns, operation, rename=True
    )
    assert new_pivoted == ["all_electricity_sum"]
    assert sorted(dropped) == sorted(pivoted_columns)
    expected = 2.1 + 7.8 + 3.5 + 6.8 + 4.2 + 5.8 + 1.3 + 4.8
    assert math.isclose(
        res.select("all_electricity_sum")
        .agg(F.sum("all_electricity_sum").alias("sum_elec"))
        .collect()[0]
        .sum_elec,
        expected,
    )


def test_map_and_reduce_pivoted_dimension_sum_no_rename(dataframes):
    df, records, pivoted_columns = dataframes
    operation = "sum"
    res, new_pivoted, dropped = map_and_reduce_pivoted_dimension(
        df, records, pivoted_columns, operation, rename=False
    )
    assert new_pivoted == ["all_electricity"]
    assert sorted(dropped) == sorted(pivoted_columns)
    expected = 2.1 + 7.8 + 3.5 + 6.8 + 4.2 + 5.8 + 1.3 + 4.8
    assert math.isclose(
        res.select("all_electricity")
        .agg(F.sum("all_electricity").alias("sum_elec"))
        .collect()[0]
        .sum_elec,
        expected,
    )


def test_map_and_reduce_pivoted_dimension_max(dataframes):
    df, records, pivoted_columns = dataframes
    operation = "max"
    res, new_pivoted, dropped = map_and_reduce_pivoted_dimension(
        df, records, pivoted_columns, operation, rename=False
    )
    assert new_pivoted == ["all_electricity"]
    assert sorted(dropped) == sorted(pivoted_columns)
    assert res.filter("county == 'Jefferson'").collect()[0].all_electricity == 7.8
    assert res.filter("county == 'Boulder'").collect()[0].all_electricity == 6.8
    assert res.filter("county == 'Denver'").collect()[0].all_electricity == 5.8
    assert res.filter("county == 'Adams'").collect()[0].all_electricity == 4.8


def test_map_and_reduce_pivoted_dimension_min(dataframes):
    df, records, pivoted_columns = dataframes
    operation = "min"
    res, new_pivoted, dropped = map_and_reduce_pivoted_dimension(
        df, records, pivoted_columns, operation, rename=False
    )
    assert new_pivoted == ["all_electricity"]
    assert sorted(dropped) == sorted(pivoted_columns)
    assert res.filter("county == 'Jefferson'").collect()[0].all_electricity == 2.1
    assert res.filter("county == 'Boulder'").collect()[0].all_electricity == 3.5
    assert res.filter("county == 'Denver'").collect()[0].all_electricity == 4.2
    assert res.filter("county == 'Adams'").collect()[0].all_electricity == 1.3


def test_is_noop_mapping_true():
    spark = get_spark_session()
    df = spark.createDataFrame(
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
    spark = get_spark_session()
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
                "to_id": "elect_cooling",
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
                "to_id": "elect_cooling",
                "from_fraction": 1.0,
            },
            {
                "from_id": "elec_heating",
                "to_id": "elec_heating",
                "from_fraction": 2.0,
            },
        ],
    ):
        df = spark.createDataFrame(records)
        assert not is_noop_mapping(df)


def test_remove_invalid_null_timestamps():
    spark = get_spark_session()
    df = spark.createDataFrame(
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
    assert result.filter(f"county == 'Boulder' and {time_col} is NULL").rdd.isEmpty()


# TODO: enable this when we decide if and how to handle NULL values in mean operations.
# def test_map_and_reduce_pivoted_dimension_mean(dataframes):
#    df, records, pivoted_columns = dataframes
#    operation = "mean"
#    res, new_pivoted, dropped = map_and_reduce_pivoted_dimension(
#        df, records, pivoted_columns, operation, rename=False
#    )
#    assert new_pivoted == ["all_electricity"]
#    assert sorted(dropped) == sorted(pivoted_columns)
#    assert math.isclose(res.filter("county == 'Jefferson'").collect()[0].all_electricity, (2.1 + 7.8) / 2)
#    assert math.isclose(res.filter("county == 'Boulder'").collect()[0].all_electricity, (3.5 + 6.8) / 2)
#    assert math.isclose(res.filter("county == 'Denver'").collect()[0].all_electricity, (4.2 + 5.8) / 2)
#    assert math.isclose(res.filter("county == 'Adams'").collect()[0].all_electricity, (1.3 + 4.8) / 2)
