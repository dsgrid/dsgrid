import math

import pyspark.sql.functions as F
import pytest

from dsgrid.utils.dataset import map_and_reduce_pivoted_dimension
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
