"""Tests for dsgrid.units.

Covers the unit constants module and the only production unit-conversion
entry point, :func:`dsgrid.units.convert.convert_units_unpivoted`.
"""

import math

import pytest

from dsgrid.ibis.functions import cache, unpersist
from dsgrid.ibis.session import create_dataframe_from_dicts
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
from dsgrid.units.convert import convert_units_unpivoted

from tests._helpers import collect as _collect


# --- Test data --------------------------------------------------------------

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


@pytest.fixture(scope="module")
def energy_from_records():
    """Source records: each metric ID carries its native unit."""
    data = [
        {"id": "fans", "unit": KWH},
        {"id": "cooling", "unit": MWH},
        {"id": "dryer", "unit": GWH},
        {"id": "ev_l1l2", "unit": TWH},
        {"id": "ng_heating", "unit": THERM},
        {"id": "p_heating", "unit": MBTU},
    ]
    records = create_dataframe_from_dicts(data)
    records = cache(records)
    yield records
    unpersist(records)


@pytest.fixture(scope="module")
def power_from_records():
    data = [
        {"id": "fans", "unit": KW},
        {"id": "cooling", "unit": MW},
        {"id": "dryer", "unit": GW},
        {"id": "ev_l1l2", "unit": TW},
    ]
    records = create_dataframe_from_dicts(data)
    records = cache(records)
    yield records
    unpersist(records)


@pytest.fixture(scope="module")
def energy_unpivoted_df():
    """An unpivoted load_data table whose values match each metric's native unit."""
    data = [
        {"timestamp": 1, "metric": "fans", "value": KWH_VAL},
        {"timestamp": 1, "metric": "cooling", "value": MWH_VAL},
        {"timestamp": 1, "metric": "dryer", "value": GWH_VAL},
        {"timestamp": 1, "metric": "ev_l1l2", "value": TWH_VAL},
        {"timestamp": 1, "metric": "ng_heating", "value": THERM_VAL},
        {"timestamp": 1, "metric": "p_heating", "value": MBTU_VAL},
    ]
    df = create_dataframe_from_dicts(data)
    df = cache(df)
    yield df
    unpersist(df)


@pytest.fixture(scope="module")
def power_unpivoted_df():
    data = [
        {"timestamp": 1, "metric": "fans", "value": KW_VAL},
        {"timestamp": 1, "metric": "cooling", "value": MW_VAL},
        {"timestamp": 1, "metric": "dryer", "value": GW_VAL},
        {"timestamp": 1, "metric": "ev_l1l2", "value": TW_VAL},
    ]
    df = create_dataframe_from_dicts(data)
    df = cache(df)
    yield df
    unpersist(df)


# --- Constants --------------------------------------------------------------


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


# --- convert_units_unpivoted -----------------------------------------------


def _to_unit_records(to_unit: str, ids: list[str]):
    """Build a to_unit_records table where every id maps to ``to_unit``."""
    return create_dataframe_from_dicts([{"id": i, "unit": to_unit} for i in ids])


def _expected(to_unit: str, is_power: bool) -> float:
    """Return the expected per-row value after conversion to ``to_unit``."""
    if is_power:
        return {KW: KW_VAL, MW: MW_VAL, GW: GW_VAL, TW: TW_VAL}[to_unit]
    return {
        KWH: KWH_VAL,
        MWH: MWH_VAL,
        GWH: GWH_VAL,
        TWH: TWH_VAL,
        THERM: THERM_VAL,
        MBTU: MBTU_VAL,
    }[to_unit]


@pytest.mark.parametrize("to_unit", [KWH, MWH, GWH, TWH, THERM, MBTU])
def test_convert_units_unpivoted_energy_without_mapping(
    energy_unpivoted_df, energy_from_records, to_unit
):
    """No from_to_records: from_records carry the units directly."""
    ids = ["fans", "cooling", "dryer", "ev_l1l2", "ng_heating", "p_heating"]
    to_records = _to_unit_records(to_unit, ids)
    result = convert_units_unpivoted(
        energy_unpivoted_df,
        metric_column="metric",
        from_records=energy_from_records,
        from_to_records=None,
        to_unit_records=to_records,
    )
    rows = _collect(result)
    assert len(rows) == len(ids)
    expected_val = _expected(to_unit, is_power=False)
    for row in rows:
        assert math.isclose(row.value, expected_val), f"row={row} to_unit={to_unit}"


@pytest.mark.parametrize("to_unit", [KW, MW, GW, TW])
def test_convert_units_unpivoted_power_without_mapping(
    power_unpivoted_df, power_from_records, to_unit
):
    ids = ["fans", "cooling", "dryer", "ev_l1l2"]
    to_records = _to_unit_records(to_unit, ids)
    result = convert_units_unpivoted(
        power_unpivoted_df,
        metric_column="metric",
        from_records=power_from_records,
        from_to_records=None,
        to_unit_records=to_records,
    )
    rows = _collect(result)
    assert len(rows) == len(ids)
    expected_val = _expected(to_unit, is_power=True)
    for row in rows:
        assert math.isclose(row.value, expected_val), f"row={row} to_unit={to_unit}"


def test_convert_units_unpivoted_returns_input_when_units_match(
    energy_unpivoted_df, energy_from_records
):
    """Early-exit path: when from-units and to-units already match, return df."""
    # to_unit_records has the same (id, unit) mapping as from_records, so every
    # row's source unit matches its target unit.
    same_records = energy_from_records
    result = convert_units_unpivoted(
        energy_unpivoted_df,
        metric_column="metric",
        from_records=energy_from_records,
        from_to_records=None,
        to_unit_records=same_records,
    )
    # Input is returned unchanged; row count and value column are preserved.
    rows = _collect(result)
    assert len(rows) == 6
    expected = {
        "fans": KWH_VAL,
        "cooling": MWH_VAL,
        "dryer": GWH_VAL,
        "ev_l1l2": TWH_VAL,
        "ng_heating": THERM_VAL,
        "p_heating": MBTU_VAL,
    }
    for row in rows:
        assert math.isclose(row.value, expected[row.metric])


def test_convert_units_unpivoted_with_mapping(energy_from_records):
    """from_to_records path: each source ID is renamed to a target ID and converted to kWh.

    The function assumes the df's metric column already holds *target* IDs at
    call time (mapping happens upstream of convert_units_unpivoted). The
    from_to_records argument is what lets the conversion machinery look up
    each target ID's original from_unit.
    """
    # 1:1 mapping. df.metric holds the to_ids.
    df = create_dataframe_from_dicts(
        [
            {"timestamp": 1, "metric": "fans_out", "value": KWH_VAL},
            {"timestamp": 1, "metric": "cooling_out", "value": MWH_VAL},
            {"timestamp": 1, "metric": "dryer_out", "value": GWH_VAL},
            {"timestamp": 1, "metric": "ev_l1l2_out", "value": TWH_VAL},
            {"timestamp": 1, "metric": "ng_heating_out", "value": THERM_VAL},
            {"timestamp": 1, "metric": "p_heating_out", "value": MBTU_VAL},
        ]
    )
    from_to_records = create_dataframe_from_dicts(
        [
            {"from_id": "fans", "to_id": "fans_out"},
            {"from_id": "cooling", "to_id": "cooling_out"},
            {"from_id": "dryer", "to_id": "dryer_out"},
            {"from_id": "ev_l1l2", "to_id": "ev_l1l2_out"},
            {"from_id": "ng_heating", "to_id": "ng_heating_out"},
            {"from_id": "p_heating", "to_id": "p_heating_out"},
        ]
    )
    to_records = create_dataframe_from_dicts(
        [
            {"id": f"{i}_out", "unit": KWH}
            for i in ["fans", "cooling", "dryer", "ev_l1l2", "ng_heating", "p_heating"]
        ]
    )
    result = convert_units_unpivoted(
        df,
        metric_column="metric",
        from_records=energy_from_records,
        from_to_records=from_to_records,
        to_unit_records=to_records,
    )
    rows = _collect(result)
    assert len(rows) == 6
    for row in rows:
        # Every row's original value, expressed in its native unit, is the same
        # KWH_VAL; after conversion to kWh they should all equal KWH_VAL.
        assert math.isclose(row.value, KWH_VAL), f"row={row}"


def test_convert_units_unpivoted_mixed_units_raises(energy_unpivoted_df, energy_from_records):
    """Mixing energy and power target units raises ValueError."""
    to_records = create_dataframe_from_dicts(
        [
            {"id": "fans", "unit": KWH},
            {"id": "cooling", "unit": KW},  # power mixed in with energy
        ]
    )
    with pytest.raises(ValueError, match="Unsupported unit conversion"):
        convert_units_unpivoted(
            energy_unpivoted_df,
            metric_column="metric",
            from_records=energy_from_records,
            from_to_records=None,
            to_unit_records=to_records,
        )
