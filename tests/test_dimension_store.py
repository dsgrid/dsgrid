
import datetime

from dsgrid.dimension.base import *
from .data.dimension_models.minimal.models import *
from dsgrid.dimension.store import DimensionStore, deserialize_record
from dsgrid.utils.files import load_data


# Use one store for all tests. It won't be mutated after load.
store = DimensionStore.load(
    MODEL_MAPPINGS,
    one_to_many=ONE_TO_MANY,
    one_to_one=ONE_TO_ONE,
    programmatic_one_to_one=PROGRAMMATIC_ONE_TO_ONE,
)


def test_dimension_store_basics():
    assert store.list_dimension_types()[:2] == [CensusDivision, CensusRegion]
    states = store.list_records(State)
    assert len(states) == 56
    assert states[0].name == "Alaska"
    assert store.get_record(State, "AK") == states[0]
    assert store.has_record(State, "AK")
    assert not store.has_record(State, "ZZ")
    assert store.list_dimension_types(base_class=TimeDimension) == [DayType, Season]


def test_one_to_many_mapping():
    conn = store.to_sqlite3()
    query = "select * from County where County.state = 'CO'"
    result = conn.execute(query)
    co_counties1 = [deserialize_record(County, x) for x in result]
    co_counties2 = store.map_one_to_many(State, County, "CO")
    assert co_counties1 == co_counties2


def test_one_to_one_mapping():
    conn = store.to_sqlite3()
    query = "select * from County where name = 'Adams County' and County.state = 'CO'"
    result = conn.execute(query)
    counties = result.fetchall()
    assert len(counties) == 1
    county = deserialize_record(County, counties[0])
    state = store.get_record(State, "CO")
    assert store.map_one_to_one(County, State, county.id) == state


def test_date_season_mapping():
    seasons = {
        datetime.datetime(year=2020, month=2, day=5): "winter",
        datetime.datetime(year=2020, month=5, day=10): "spring",
        datetime.datetime(year=2020, month=7, day=15): "summer",
        datetime.datetime(year=2020, month=11, day=20): "autumn",
    }
    for timestamp, season in seasons.items():
        result = store.map_programmatic_one_to_one(datetime.datetime, Season, timestamp)
        assert result.id == season


def test_date_day_type_mapping():
    days = {
        datetime.datetime(year=2020, month=12, day=11): "weekday",
        datetime.datetime(year=2020, month=12, day=12): "weekend",
    }
    for day, day_type in days.items():
        result = store.map_programmatic_one_to_one(datetime.datetime, DayType, day)
        assert result.id == day_type


def test_dimension_store__to_sqlite3():
    conn = store.to_sqlite3()
    states = conn.execute("select * from State")
    states = [deserialize_record(State, x) for x in states]
    assert list(states)[1].name == "Alabama"

    query = "SELECT County.* FROM State JOIN County ON County.State = State.id " \
            "WHERE County.timezone = 'MST' AND State.id = 'TX'"
    counties = conn.execute(query)
    assert len([deserialize_record(County, x) for x in counties]) == 1
