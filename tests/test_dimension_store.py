
import datetime

import pytest

from dsgrid.dimension.base import *
from .data.dimension_models.minimal.models import *
from dsgrid.dimension.store import DimensionStore, deserialize_row
from dsgrid.exceptions import *
from dsgrid.utils.files import load_data


# Use one store for all tests. It won't be mutated after load.
store = DimensionStore.load(
    MODEL_MAPPINGS,
    dimension_mappings=DIMENSION_MAPPINGS,
)


def test_dimension_store_basics():
    assert store.list_dimension_types()[:2] == [CensusDivision, CensusRegion]
    states = store.list_records(State)
    assert len(states) == 56
    assert states[0].name == "Alaska"
    record = store.get_record(State, "AK")
    assert record == states[0]
    assert isinstance(deserialize_row(State, record), State)
    assert store.has_record(State, "AK")
    assert store.list_records(State)[0].name == "Alaska"
    assert not store.has_record(State, "ZZ")
    assert store.list_dimension_types(base_class=TimeDimension) == [DayType, Season]


def test_dimension_store_invalid_types():
    @dataclass
    class Unknown:
        a: str

    assert not store.has_record(Unknown, "ZZ")
    with pytest.raises(DSGInvalidDimension):
        assert not store.get_record(Unknown, "ZZ")
    with pytest.raises(DSGInvalidDimension):
        assert not store.get_record(State, "ZZ")


def test_dimension_mapping():
    key = store.get_dimension_mapping_key(County, State)
    assert key == "state"
    from_df = store.get_dataframe(County)
    assert len(from_df.collect()) == len(from_df.select(key).collect())
    to_df = store.get_dataframe(State).withColumnRenamed("name", "state_name") \
        .withColumnRenamed("id", "state_id")
    df = from_df.join(to_df, from_df.state == to_df.state_id) \
        .filter("state_id = 'CO'") \
        .filter("name = 'Adams County'")
    assert df.count() == 1

    with pytest.raises(DSGInvalidDimensionMapping):
        store.get_dimension_mapping_key(Season, State)
