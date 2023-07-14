import pyspark.sql.functions as F
import pytest

from dsgrid.units.energy import (
    KWH,
    MWH,
    GWH,
    TWH,
    MBTU,
    THERM,
    KWH_PER_MBTU,
    KWH_PER_THERM,
    THERMS_PER_MBTU,
    to_kwh,
    to_mwh,
    to_gwh,
    to_twh,
    to_therm,
    to_mbtu,
)
from dsgrid.utils.spark import get_spark_session


@pytest.fixture(scope="module")
def dataframes():
    spark = get_spark_session()
    df = spark.createDataFrame(
        [
            {
                "fans": 1,
                "cooling": 2,
                "dryer": 3,
                "ev_l1l2": 4,
                "ng_heating": 5,
                "p_heating": 6,
            },
        ]
    )
    records = spark.createDataFrame(
        [
            {"id": "fans", "name": "Fans", "fuel_id": "electricity", "unit": KWH},
            {"id": "cooling", "name": "Space Cooling", "fuel_id": "electricity", "unit": MWH},
            {"id": "dryer", "name": "Dryer", "fuel_id": "electricity", "unit": GWH},
            {"id": "ev_l1l2", "name": "EV L1/L2", "fuel_id": "electricity", "unit": TWH},
            {"id": "ng_heating", "name": "NG - Heating", "fuel_id": "natural_gas", "unit": THERM},
            {"id": "p_heating", "name": "Propane - Heating", "fuel_id": "propane", "unit": MBTU},
        ]
    )
    yield df.cache(), records.cache()
    df.unpersist()
    records.unpersist()


def test_to_kwh(dataframes):
    df, records = dataframes
    row = _convert_units(df, records, to_kwh)
    assert row["fans"] == 1
    assert row["cooling"] == 2 * 1_000
    assert row["dryer"] == 3 * 1_000_000
    assert row["ev_l1l2"] == 4 * 1_000_000_000
    assert row["ng_heating"] == 5 * KWH_PER_THERM
    assert row["p_heating"] == 6 * KWH_PER_MBTU


def test_to_mwh(dataframes):
    df, records = dataframes
    row = _convert_units(df, records, to_mwh)
    assert row["fans"] == 1 / 1_000
    assert row["cooling"] == 2
    assert row["dryer"] == 3 * 1_000
    assert row["ev_l1l2"] == 4 * 1_000_000
    assert row["ng_heating"] == 5 * KWH_PER_THERM / 1_000
    assert row["p_heating"] == 6 * KWH_PER_MBTU / 1_000


def test_to_gwh(dataframes):
    df, records = dataframes
    row = _convert_units(df, records, to_gwh)
    assert row["fans"] == 1 / 1_000_000
    assert row["cooling"] == 2 / 1_000
    assert row["dryer"] == 3
    assert row["ev_l1l2"] == 4 * 1_000
    assert row["ng_heating"] == 5 * KWH_PER_THERM / 1_000_000
    assert row["p_heating"] == 6 * KWH_PER_MBTU / 1_000_000


def test_to_twh(dataframes):
    df, records = dataframes
    row = _convert_units(df, records, to_twh)
    assert row["fans"] == 1 / 1_000_000_000
    assert row["cooling"] == 2 / 1_000_000
    assert row["dryer"] == 3 / 1_000
    assert row["ev_l1l2"] == 4
    assert row["ng_heating"] == 5 * KWH_PER_THERM / 1_000_000_000
    assert row["p_heating"] == 6 * KWH_PER_MBTU / 1_000_000_000


def test_to_therm(dataframes):
    df, records = dataframes
    row = _convert_units(df, records, to_therm)
    assert row["fans"] == 1 / KWH_PER_THERM
    assert row["cooling"] == 2 / KWH_PER_THERM / 1_000
    assert row["dryer"] == 3 / KWH_PER_THERM / 1_000_000
    assert row["ev_l1l2"] == 4 / KWH_PER_THERM / 1_000_000_000
    assert row["ng_heating"] == 5
    assert row["p_heating"] == 6 * THERMS_PER_MBTU


def test_to_mbtu(dataframes):
    df, records = dataframes
    row = _convert_units(df, records, to_mbtu)
    assert row["fans"] == 1 / KWH_PER_MBTU
    assert row["cooling"] == 2 / KWH_PER_MBTU / 1_000
    assert row["dryer"] == 3 / KWH_PER_MBTU / 1_000_000
    assert row["ev_l1l2"] == 4 / KWH_PER_MBTU / 1_000_000_000
    assert row["ng_heating"] == 5 / THERMS_PER_MBTU
    assert row["p_heating"] == 6


def _convert_units(df, records, conversion_func):
    unit_col = "unit"
    for column in df.columns:
        unit_val = records.filter(f"id='{column}'").select(unit_col).collect()[0][unit_col]
        tdf = df.withColumn(unit_col, F.lit(unit_val))
        df = tdf.withColumn(column, conversion_func(unit_col, column)).drop(unit_col)
    return df.collect()[0]
