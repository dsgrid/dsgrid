import math

import pyspark.sql.functions as F
import pytest

from dsgrid.common import VALUE_COLUMN
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
    from_any_to_any,
)
from dsgrid.utils.spark import get_spark_session


@pytest.fixture(scope="module")
def records_dataframe():
    spark = get_spark_session()
    records = spark.createDataFrame(
        [
            {"id": "fans", "name": "Fans", "fuel_id": "electricity", "unit": KWH},
            {"id": "cooling", "name": "Space Cooling", "fuel_id": "electricity", "unit": MWH},
            {"id": "dryer", "name": "Dryer", "fuel_id": "electricity", "unit": GWH},
            {"id": "ev_l1l2", "name": "EV L1/L2", "fuel_id": "electricity", "unit": TWH},
            {"id": "ng_heating", "name": "NG - Heating", "fuel_id": "natural_gas", "unit": THERM},
            {"id": "p_heating", "name": "Propane - Heating", "fuel_id": "propane", "unit": MBTU},
            {"id": "unitless", "name": "unitless", "fuel_id": "electricity", "unit": ""},
        ]
    )
    yield records.cache()
    records.unpersist()


@pytest.fixture(scope="module")
def pivoted_dataframes(records_dataframe):
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
                "unitless": 7,
            },
        ]
    )
    yield df.cache(), records_dataframe
    df.unpersist()


@pytest.fixture(scope="module")
def unpivoted_dataframes(records_dataframe):
    spark = get_spark_session()
    df = spark.createDataFrame(
        [
            {
                "timestamp": 1,
                "metric": "fans",
                "value": 1,
            },
            {
                "timestamp": 1,
                "metric": "cooling",
                "value": 2,
            },
            {
                "timestamp": 1,
                "metric": "dryer",
                "value": 3,
            },
            {
                "timestamp": 1,
                "metric": "ev_l1l2",
                "value": 4,
            },
            {
                "timestamp": 1,
                "metric": "ng_heating",
                "value": 5,
            },
            {
                "timestamp": 1,
                "metric": "p_heating",
                "value": 6,
            },
            {
                "timestamp": 1,
                "metric": "unitless",
                "value": 7,
            },
        ]
    )
    yield df.cache(), records_dataframe
    df.unpersist()


def test_to_kwh(pivoted_dataframes):
    df, records = pivoted_dataframes
    row = _convert_units(df, records, to_kwh)
    assert row["fans"] == 1
    assert row["cooling"] == 2 * 1_000
    assert row["dryer"] == 3 * 1_000_000
    assert row["ev_l1l2"] == 4 * 1_000_000_000
    assert row["ng_heating"] == 5 * KWH_PER_THERM
    assert row["p_heating"] == 6 * KWH_PER_MBTU
    assert row["unitless"] == 7


def test_to_mwh(pivoted_dataframes):
    df, records = pivoted_dataframes
    row = _convert_units(df, records, to_mwh)
    assert row["fans"] == 1 / 1_000
    assert row["cooling"] == 2
    assert row["dryer"] == 3 * 1_000
    assert row["ev_l1l2"] == 4 * 1_000_000
    assert row["ng_heating"] == 5 * KWH_PER_THERM / 1_000
    assert row["p_heating"] == 6 * KWH_PER_MBTU / 1_000
    assert row["unitless"] == 7


def test_to_gwh(pivoted_dataframes):
    df, records = pivoted_dataframes
    row = _convert_units(df, records, to_gwh)
    assert row["fans"] == 1 / 1_000_000
    assert row["cooling"] == 2 / 1_000
    assert row["dryer"] == 3
    assert row["ev_l1l2"] == 4 * 1_000
    assert row["ng_heating"] == 5 * KWH_PER_THERM / 1_000_000
    assert row["p_heating"] == 6 * KWH_PER_MBTU / 1_000_000
    assert row["unitless"] == 7


def test_to_twh(pivoted_dataframes):
    df, records = pivoted_dataframes
    row = _convert_units(df, records, to_twh)
    assert row["fans"] == 1 / 1_000_000_000
    assert row["cooling"] == 2 / 1_000_000
    assert row["dryer"] == 3 / 1_000
    assert row["ev_l1l2"] == 4
    assert row["ng_heating"] == 5 * KWH_PER_THERM / 1_000_000_000
    assert row["p_heating"] == 6 * KWH_PER_MBTU / 1_000_000_000
    assert row["unitless"] == 7


def test_to_therm(pivoted_dataframes):
    df, records = pivoted_dataframes
    row = _convert_units(df, records, to_therm)
    assert row["fans"] == 1 / KWH_PER_THERM
    assert row["cooling"] == 2 / KWH_PER_THERM / 1_000
    assert row["dryer"] == 3 / KWH_PER_THERM / 1_000_000
    assert row["ev_l1l2"] == 4 / KWH_PER_THERM / 1_000_000_000
    assert row["ng_heating"] == 5
    assert row["p_heating"] == 6 * THERMS_PER_MBTU
    assert row["unitless"] == 7


def test_to_mbtu(pivoted_dataframes):
    df, records = pivoted_dataframes
    row = _convert_units(df, records, to_mbtu)
    assert row["fans"] == 1 / KWH_PER_MBTU
    assert row["cooling"] == 2 / KWH_PER_MBTU / 1_000
    assert row["dryer"] == 3 / KWH_PER_MBTU / 1_000_000
    assert row["ev_l1l2"] == 4 / KWH_PER_MBTU / 1_000_000_000
    assert row["ng_heating"] == 5 / THERMS_PER_MBTU
    assert row["p_heating"] == 6
    assert row["unitless"] == 7


@pytest.mark.parametrize("to_unit", [KWH, MWH, GWH, TWH, THERM, MBTU])
def test_from_any_to_any(unpivoted_dataframes, to_unit):
    df, records = unpivoted_dataframes
    df_with_units = (
        df.join(records, on=df["metric"] == records["id"])
        .withColumnRenamed("unit", "from_unit")
        .withColumn("to_unit", F.lit(to_unit))
        .select("metric", "timestamp", "from_unit", "to_unit", VALUE_COLUMN)
    )
    res = df_with_units.withColumn(
        VALUE_COLUMN, from_any_to_any("from_unit", "to_unit", VALUE_COLUMN)
    )

    fans = res.filter("metric == 'fans'").collect()[0][VALUE_COLUMN]
    cooling = res.filter("metric == 'cooling'").collect()[0][VALUE_COLUMN]
    dryer = res.filter("metric == 'dryer'").collect()[0][VALUE_COLUMN]
    ev_l1l2 = res.filter("metric == 'ev_l1l2'").collect()[0][VALUE_COLUMN]
    ng_heating = res.filter("metric == 'ng_heating'").collect()[0][VALUE_COLUMN]
    p_heating = res.filter("metric == 'p_heating'").collect()[0][VALUE_COLUMN]
    unitless = res.filter("metric == 'unitless'").collect()[0][VALUE_COLUMN]
    assert unitless == 7

    match to_unit:
        case "kWh":
            assert fans == 1
            assert cooling == 2 * 1_000
            assert dryer == 3 * 1_000_000
            assert ev_l1l2 == 4 * 1_000_000_000
            assert ng_heating == 5 * KWH_PER_THERM
            assert p_heating == 6 * KWH_PER_MBTU
        case "MWh":
            assert math.isclose(fans, 1 / 1_000)
            assert cooling == 2
            assert dryer == 3 * 1_000
            assert ev_l1l2 == 4 * 1_000_000
            assert math.isclose(ng_heating, 5 / 1_000 * KWH_PER_THERM)
            assert math.isclose(p_heating, 6 / 1_000 * KWH_PER_MBTU)
        case "GWh":
            assert math.isclose(fans, 1 / 1_000_000)
            assert math.isclose(cooling, 2 / 1_000)
            assert dryer == 3
            assert ev_l1l2 == 4 * 1_000
            assert math.isclose(ng_heating, 5 / 1_000_000 * KWH_PER_THERM)
            assert math.isclose(p_heating, 6 / 1_000_000 * KWH_PER_MBTU)
        case "TWh":
            assert math.isclose(fans, 1 / 1_000_000_000)
            assert math.isclose(cooling, 2 / 1_000_000)
            assert math.isclose(dryer, 3 / 1_000)
            assert ev_l1l2 == 4
            assert math.isclose(ng_heating, 5 / 1_000_000_000 * KWH_PER_THERM)
            assert math.isclose(p_heating, 6 / 1_000_000_000 * KWH_PER_MBTU)
        case "therm":
            assert math.isclose(fans, 1 / KWH_PER_THERM)
            assert math.isclose(cooling, 2 * 1_000 / KWH_PER_THERM)
            assert math.isclose(dryer, 3 * 1_000_000 / KWH_PER_THERM)
            assert math.isclose(ev_l1l2, 4 * 1_000_000_000 / KWH_PER_THERM)
            assert ng_heating == 5
            assert math.isclose(p_heating, 6 * THERMS_PER_MBTU)
        case "MBtu":
            assert math.isclose(fans, 1 / KWH_PER_MBTU)
            assert math.isclose(cooling, 2 * 1_000 / KWH_PER_MBTU)
            assert math.isclose(dryer, 3 * 1_000_000 / KWH_PER_MBTU)
            assert math.isclose(ev_l1l2, 4 * 1_000_000_000 / KWH_PER_MBTU)
            assert math.isclose(ng_heating, 5 / THERMS_PER_MBTU)
            assert p_heating == 6
        case _:
            assert False, to_unit


def _convert_units(df, records, conversion_func):
    unit_col = "unit"
    for column in df.columns:
        unit_val = records.filter(f"id='{column}'").select(unit_col).collect()[0][unit_col]
        tdf = df.withColumn(unit_col, F.lit(unit_val))
        df = tdf.withColumn(column, conversion_func(unit_col, column)).drop(unit_col)
    return df.collect()[0]
