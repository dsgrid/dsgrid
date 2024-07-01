import logging
import os

import pyspark.sql.functions as F
import pytest
from pyspark.sql.types import StructType, IntegerType

from dsgrid.config.dimension_mapping_base import DimensionMappingType
from dsgrid.utils.dataset import (
    apply_scaling_factor,
    is_noop_mapping,
    remove_invalid_null_timestamps,
    repartition_if_needed_by_mapping,
)
from dsgrid.utils.scratch_dir_context import ScratchDirContext
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


def test_apply_scaling_factor():
    spark = get_spark_session()
    schema = (
        StructType()
        .add("a", IntegerType(), False)
        .add("b", IntegerType(), False)
        .add("bystander", IntegerType(), False)
        .add("scaling_factor", IntegerType(), True)
    )
    df = spark.createDataFrame(
        [
            {"a": 1, "b": 2, "bystander": 1, "scaling_factor": 5},
            {"a": 2, "b": 3, "bystander": 1, "scaling_factor": 6},
            {"a": 3, "b": 4, "bystander": 1, "scaling_factor": None},
        ],
        schema=schema,
    )
    df2 = apply_scaling_factor(df, ("a", "b"))
    assert df2.select("a").agg(F.sum("a").alias("sum_a")).collect()[0].sum_a == 1 * 5 + 2 * 6 + 3
    assert df2.select("b").agg(F.sum("b").alias("sum_b")).collect()[0].sum_b == 2 * 5 + 3 * 6 + 4
    assert df2.select("bystander").agg(F.sum("bystander").alias("c")).collect()[0].c == 1 + 1 + 1


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
