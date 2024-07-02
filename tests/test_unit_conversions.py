import math

import pytest

from dsgrid.common import VALUE_COLUMN
from dsgrid.units.energy import (
    KWH,
    MWH,
    GWH,
    TWH,
    MBTU,
    THERM,
    KILO_TO_MEGA,
    KILO_TO_GIGA,
    KILO_TO_TERA,
    MEGA_TO_KILO,
    MEGA_TO_GIGA,
    MEGA_TO_TERA,
    GIGA_TO_KILO,
    GIGA_TO_MEGA,
    GIGA_TO_TERA,
    TERA_TO_KILO,
    TERA_TO_MEGA,
    TERA_TO_GIGA,
    THERM_TO_KWH,
    THERM_TO_MWH,
    THERM_TO_GWH,
    THERM_TO_TWH,
    MWH_TO_THERM,
    GWH_TO_THERM,
    TWH_TO_THERM,
    MBTU_TO_KWH,
    MBTU_TO_MWH,
    MBTU_TO_GWH,
    MBTU_TO_TWH,
    to_kwh,
    to_mwh,
    to_gwh,
    to_twh,
    to_therm,
    to_mbtu,
    from_any_to_any,
)
from dsgrid.spark.types import F, use_duckdb
from dsgrid.utils.spark import create_dataframe_from_dicts, get_spark_session


KWH_VAL = 1234.5
MWH_VAL = KWH_VAL / 1_000
GWH_VAL = KWH_VAL / 1_000_000
TWH_VAL = KWH_VAL / 1_000_000_000
THERM_VAL = KWH_VAL / THERM_TO_KWH
MBTU_VAL = KWH_VAL / MBTU_TO_KWH
UNIT_COLUMNS = ("fans", "cooling", "dryer", "ev_l1l2", "ng_heating", "p_heating")


@pytest.fixture(scope="module")
def records_dataframe():
    data = [
        {"id": "fans", "name": "Fans", "fuel_id": "electricity", "unit": KWH},
        {"id": "cooling", "name": "Space Cooling", "fuel_id": "electricity", "unit": MWH},
        {"id": "dryer", "name": "Dryer", "fuel_id": "electricity", "unit": GWH},
        {"id": "ev_l1l2", "name": "EV L1/L2", "fuel_id": "electricity", "unit": TWH},
        {"id": "ng_heating", "name": "NG - Heating", "fuel_id": "natural_gas", "unit": THERM},
        {"id": "p_heating", "name": "Propane - Heating", "fuel_id": "propane", "unit": MBTU},
        {"id": "unitless", "name": "unitless", "fuel_id": "electricity", "unit": ""},
    ]
    records = create_dataframe_from_dicts(data)
    if not use_duckdb():
        records.cache()
    yield records
    if not use_duckdb():
        records.unpersist()


@pytest.fixture(scope="module")
def pivoted_dataframes(records_dataframe):
    data = [
        {
            "fans": KWH_VAL,
            "cooling": MWH_VAL,
            "dryer": GWH_VAL,
            "ev_l1l2": TWH_VAL,
            "ng_heating": THERM_VAL,
            "p_heating": MBTU_VAL,
            "unitless": KWH_VAL,
        },
    ]
    df = create_dataframe_from_dicts(data)
    if not use_duckdb():
        df.cache()
    yield df, records_dataframe
    if not use_duckdb():
        df.unpersist()


@pytest.fixture(scope="module")
def unpivoted_dataframes(records_dataframe):
    data = [
        {
            "timestamp": 1,
            "metric": "fans",
            "value": KWH_VAL,
        },
        {
            "timestamp": 1,
            "metric": "cooling",
            "value": MWH_VAL,
        },
        {
            "timestamp": 1,
            "metric": "dryer",
            "value": GWH_VAL,
        },
        {
            "timestamp": 1,
            "metric": "ev_l1l2",
            "value": TWH_VAL,
        },
        {
            "timestamp": 1,
            "metric": "ng_heating",
            "value": THERM_VAL,
        },
        {
            "timestamp": 1,
            "metric": "p_heating",
            "value": MBTU_VAL,
        },
        {
            "timestamp": 1,
            "metric": "unitless",
            "value": KWH_VAL,
        },
    ]
    df = create_dataframe_from_dicts(data)
    if not use_duckdb():
        df.cache()
    yield df, records_dataframe
    if not use_duckdb():
        df.unpersist()


def test_constants():
    assert KILO_TO_MEGA == 1 / 1_000
    assert KILO_TO_GIGA == 1 / 1_000_000
    assert KILO_TO_TERA == 1 / 1_000_000_000
    assert MEGA_TO_KILO == 1_000
    assert MEGA_TO_GIGA == 1 / 1_000
    assert MEGA_TO_TERA == 1 / 1_000_000
    assert GIGA_TO_KILO == 1_000_000
    assert GIGA_TO_MEGA == 1_000
    assert GIGA_TO_TERA == 1 / 1_000
    assert TERA_TO_KILO == 1_000_000_000
    assert TERA_TO_MEGA == 1_000_000
    assert TERA_TO_GIGA == 1_000
    assert math.isclose(THERM_TO_MWH, THERM_TO_KWH / 1_000)
    assert math.isclose(THERM_TO_GWH, THERM_TO_KWH / 1_000_000)
    assert math.isclose(THERM_TO_TWH, THERM_TO_KWH / 1_000_000_000)
    assert math.isclose(MWH_TO_THERM, 1 * 1_000 / THERM_TO_KWH)
    assert math.isclose(GWH_TO_THERM, 1 * 1_000_000 / THERM_TO_KWH)
    assert math.isclose(TWH_TO_THERM, 1 * 1_000_000_000 / THERM_TO_KWH)
    assert math.isclose(MBTU_TO_MWH, MBTU_TO_KWH / 1_000)
    assert math.isclose(MBTU_TO_GWH, MBTU_TO_KWH / 1_000_000)
    assert math.isclose(MBTU_TO_TWH, MBTU_TO_KWH / 1_000_000_000)


def check_column_values(row, expected_val):
    for col in UNIT_COLUMNS:
        assert math.isclose(row[col], expected_val)
    assert row["unitless"] == KWH_VAL


@pytest.mark.parametrize(
    "inputs",
    (
        (to_kwh, KWH_VAL),
        (to_mwh, MWH_VAL),
        (to_gwh, GWH_VAL),
        (to_twh, TWH_VAL),
        (to_therm, THERM_VAL),
        (to_mbtu, MBTU_VAL),
    ),
)
def test_to_units(pivoted_dataframes, inputs):
    df, records = pivoted_dataframes
    func, expected_val = inputs
    row = _convert_units(df, records, func)
    check_column_values(row, expected_val)


@pytest.mark.parametrize("to_unit", [KWH, MWH, GWH, TWH, THERM, MBTU])
def test_from_any_to_any(unpivoted_dataframes, to_unit):
    df, records = unpivoted_dataframes
    df_with_units = (
        df.join(records, on=df.metric == records.id)
        .withColumnRenamed("unit", "from_unit")
        .withColumn("to_unit", F.lit(to_unit))
        .select("metric", "timestamp", "from_unit", "to_unit", VALUE_COLUMN)
    )
    res = df_with_units.withColumn(
        VALUE_COLUMN, from_any_to_any("from_unit", "to_unit", VALUE_COLUMN)
    )

    unitless = res.filter("metric == 'unitless'").collect()[0][VALUE_COLUMN]
    assert unitless == KWH_VAL

    match to_unit:
        case "kWh":
            expected_val = KWH_VAL
        case "MWh":
            expected_val = MWH_VAL
        case "GWh":
            expected_val = GWH_VAL
        case "TWh":
            expected_val = TWH_VAL
        case "therm":
            expected_val = THERM_VAL
        case "MBtu":
            expected_val = MBTU_VAL
        case _:
            assert False, to_unit

    for col in UNIT_COLUMNS:
        val = res.filter(f"metric == '{col}'").collect()[0][VALUE_COLUMN]
        assert math.isclose(val, expected_val)


def _convert_units(df, records, conversion_func):
    unit_col = "unit"
    for column in df.columns:
        unit_val = records.filter(f"id='{column}'").select(unit_col).collect()[0][unit_col]
        tdf = df.withColumn(unit_col, F.lit(unit_val))
        df = tdf.withColumn(column, conversion_func(unit_col, column)).drop(unit_col)
    return df.collect()[0]
