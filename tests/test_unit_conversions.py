import math

import pytest
import ibis

import dsgrid.units.energy as energy
import dsgrid.units.power as power
from dsgrid.common import VALUE_COLUMN
from dsgrid.units.constants import (
    GIGA_TO_KILO,
    GIGA_TO_MEGA,
    GIGA_TO_TERA,
    GW,
    GWH,
    GWH_TO_THERM,
    KILO_TO_GIGA,
    KILO_TO_MEGA,
    KILO_TO_TERA,
    KW,
    KWH,
    MBTU,
    MBTU_TO_GWH,
    MBTU_TO_KWH,
    MBTU_TO_MWH,
    MBTU_TO_TWH,
    MEGA_TO_GIGA,
    MEGA_TO_KILO,
    MEGA_TO_TERA,
    MW,
    MWH,
    MWH_TO_THERM,
    TERA_TO_GIGA,
    TERA_TO_KILO,
    TERA_TO_MEGA,
    THERM,
    THERM_TO_GWH,
    THERM_TO_KWH,
    THERM_TO_MWH,
    THERM_TO_TWH,
    TW,
    TWH,
    TWH_TO_THERM,
)
from dsgrid.ibis_api import create_dataframe_from_dicts


KWH_VAL = 1234.5
MWH_VAL = KWH_VAL / 1_000
GWH_VAL = KWH_VAL / 1_000_000
TWH_VAL = KWH_VAL / 1_000_000_000
KW_VAL = 1234.5
MW_VAL = KW_VAL / 1_000
GW_VAL = KW_VAL / 1_000_000
TW_VAL = KW_VAL / 1_000_000_000
THERM_VAL = KWH_VAL / THERM_TO_KWH
MBTU_VAL = KWH_VAL / MBTU_TO_KWH
UNIT_COLUMNS_ENERGY = ("fans", "cooling", "dryer", "ev_l1l2", "ng_heating", "p_heating")
UNIT_COLUMNS_POWER = ("fans", "cooling", "dryer", "ev_l1l2")


@pytest.fixture(scope="module")
def records_dataframe_energy():
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
    yield records


@pytest.fixture(scope="module")
def records_dataframe_power():
    data = [
        {"id": "fans", "name": "Fans", "unit": KW},
        {"id": "cooling", "name": "Space Cooling", "unit": MW},
        {"id": "dryer", "name": "Dryer", "unit": GW},
        {"id": "ev_l1l2", "name": "EV L1/L2", "unit": TW},
        {"id": "unitless", "name": "unitless", "unit": ""},
    ]
    records = create_dataframe_from_dicts(data)
    yield records


@pytest.fixture(scope="module")
def pivoted_dataframes(records_dataframe_energy):
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
    yield df, records_dataframe_energy


@pytest.fixture(scope="module")
def unpivoted_dataframes_energy(records_dataframe_energy):
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
    yield df, records_dataframe_energy


@pytest.fixture(scope="module")
def unpivoted_dataframes_power(records_dataframe_power):
    data = [
        {
            "timestamp": 1,
            "metric": "fans",
            "value": KW_VAL,
        },
        {
            "timestamp": 1,
            "metric": "cooling",
            "value": MW_VAL,
        },
        {
            "timestamp": 1,
            "metric": "dryer",
            "value": GW_VAL,
        },
        {
            "timestamp": 1,
            "metric": "ev_l1l2",
            "value": TW_VAL,
        },
        {
            "timestamp": 1,
            "metric": "unitless",
            "value": KW_VAL,
        },
    ]
    df = create_dataframe_from_dicts(data)
    yield df, records_dataframe_power


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


def check_column_values(row, expected_val, unit_columns):
    for col in unit_columns:
        assert math.isclose(row[col], expected_val)
    assert row["unitless"] == KWH_VAL


@pytest.mark.parametrize(
    "inputs",
    (
        (energy.to_kwh, KWH_VAL),
        (energy.to_mwh, MWH_VAL),
        (energy.to_gwh, GWH_VAL),
        (energy.to_twh, TWH_VAL),
        (energy.to_therm, THERM_VAL),
        (energy.to_mbtu, MBTU_VAL),
    ),
)
def test_to_units(pivoted_dataframes, inputs):
    df, records = pivoted_dataframes
    func, expected_val = inputs
    row = _convert_units(df, records, func)
    check_column_values(row, expected_val, UNIT_COLUMNS_ENERGY)


@pytest.mark.parametrize("to_unit", [KWH, MWH, GWH, TWH, THERM, MBTU])
def test_from_any_to_any_energy(unpivoted_dataframes_energy, to_unit):
    df, records = unpivoted_dataframes_energy

    df_with_units = df.join(records, df["metric"] == records["id"]).select(
        df["metric"],
        df["timestamp"],
        records["unit"].name("from_unit"),
        ibis.literal(to_unit).name("to_unit"),
        df[VALUE_COLUMN],
    )

    res = df_with_units.mutate(
        **{
            VALUE_COLUMN: energy.from_any_to_any(
                df_with_units["from_unit"], df_with_units["to_unit"], df_with_units[VALUE_COLUMN]
            )
        }
    )

    unitless_row = res.filter(res["metric"] == "unitless").to_pyarrow().to_pylist()[0]
    assert unitless_row[VALUE_COLUMN] == KWH_VAL

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

    for col in UNIT_COLUMNS_ENERGY:
        val = res.filter(res["metric"] == col).to_pyarrow().to_pylist()[0][VALUE_COLUMN]
        assert math.isclose(val, expected_val)


@pytest.mark.parametrize("to_unit", [KW, MW, GW, TW])
def test_from_any_to_any_power(unpivoted_dataframes_power, to_unit):
    df, records = unpivoted_dataframes_power
    df_with_units = df.join(records, df["metric"] == records["id"]).select(
        df["metric"],
        df["timestamp"],
        records["unit"].name("from_unit"),
        ibis.literal(to_unit).name("to_unit"),
        df[VALUE_COLUMN],
    )
    res = df_with_units.mutate(
        **{
            VALUE_COLUMN: power.from_any_to_any(
                df_with_units["from_unit"], df_with_units["to_unit"], df_with_units[VALUE_COLUMN]
            )
        }
    )

    unitless_row = res.filter(res["metric"] == "unitless").to_pyarrow().to_pylist()[0]
    assert unitless_row[VALUE_COLUMN] == KWH_VAL

    match to_unit:
        case "kW":
            expected_val = KW_VAL
        case "MW":
            expected_val = MW_VAL
        case "GW":
            expected_val = GW_VAL
        case "TW":
            expected_val = TW_VAL
        case _:
            assert False, to_unit

    for col in UNIT_COLUMNS_POWER:
        val = res.filter(res["metric"] == col).to_pyarrow().to_pylist()[0][VALUE_COLUMN]
        assert math.isclose(val, expected_val)


def _convert_units(df, records, conversion_func):
    unit_col = "unit"
    for column in df.columns:
        rows = records.filter(records["id"] == column).select(unit_col).to_pyarrow().to_pylist()
        if not rows:
            continue  # Or handle as error? Original code assumed it exists.
        unit_val = rows[0][unit_col]

        # In Ibis, we can't chain withColumn/drop easily in a loop if we are modifying columns in place without materializing?
        # Actually ibis expressions are immutable.
        # But we want to apply conversion to `column`.

        # We need to apply logic: new_col = conversion_func(unit_val, old_col)
        # Note conversion_func likely returns an expression involving the inputs.

        # conversion_func signature in tests is like: energy.to_kwh(unit_col_name, val_col_name)
        # But here we pass literal unit_val string?
        # energy.to_kwh implementation handles string literals?
        # Let's check dsgrid/units/energy.py if possible, but assuming it constructs an expression.
        # The previous code did:
        # tdf = df.withColumn(unit_col, F.lit(unit_val))
        # df = tdf.withColumn(column, conversion_func(unit_col, column)).drop(unit_col)

        # We can just do:
        # new_val = conversion_func(ibis.literal(unit_val), df[column])
        # df = df.mutate(**{column: new_val})

        # Wait, conversion_func expects column names or expressions?
        # Spark F.lit returns a Column. ibis.literal returns an expression.

        new_val = conversion_func(ibis.literal(unit_val), df[column])
        df = df.mutate(**{column: new_val})

    return df.to_pyarrow().to_pylist()[0]
