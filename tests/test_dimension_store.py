import datetime
import logging

import pytest
from pydantic import BaseModel

from dsgrid.time.types import DayType, Season
from dsgrid.config.dimensions import TimeDimensionModel
from .data.dimension_models.minimal.models import *
from dsgrid.dimension.standard import County, State, EndUse, CensusDivision, CensusRegion, Time
from dsgrid.dimension.store import DimensionStore
from dsgrid.exceptions import *
from dsgrid.project import Project
from dsgrid.utils.files import load_data

logger = logging.getLogger(__name__)

store_configured = True
skip_message = 'Test requires Project.load("test").project_dimension_store'
try:
    # Use one store for all tests. It won't be mutated after load.
    project = Project.load("test")
    store = project.project_dimension_store
except:
    store_configured = False
    logger.error('Unable to access Project.load("test").project_dimension_store')


@pytest.mark.skipif(not store_configured, reason=skip_message)
def test_dimension_store():
    assert store.list_dimension_classes()[:2] == [CensusDivision, CensusRegion]
    assert store.list_dimension_classes(base_class=TimeDimensionModel) == [Time]


@pytest.mark.skipif(not store_configured, reason=skip_message)
def test_dimension_records():
    record_store = store.record_store
    states = record_store.list_records(State)
    assert len(states) == 56
    assert states[0].name == "Alaska"
    record = record_store.get_record(State, "AK")
    assert record == states[0]
    assert record_store.has_record(State, "AK")
    assert record_store.list_records(State)[0].name == "Alaska"
    assert not record_store.has_record(State, "ZZ")


@pytest.mark.skipif(not store_configured, reason=skip_message)
def test_dimension_store_invalid_types():
    class Unknown(BaseModel):
        a: str

    record_store = store.record_store
    assert not record_store.has_record(Unknown, "ZZ")
    with pytest.raises(DSGInvalidDimension):
        assert not record_store.get_record(Unknown, "ZZ")
    with pytest.raises(DSGInvalidDimension):
        assert not record_store.get_record(State, "ZZ")


# @pytest.mark.skipif(not store_configured, reason = skip_message)
# def test_dimension_mapping():
#    key = store.get_dimension_mapping_key(County, State)
#    assert key == "state"
#    from_df = store.get_dataframe(County)
#    assert len(from_df.collect()) == len(from_df.select(key).collect())
#    to_df = store.get_dataframe(State).withColumnRenamed("name", "state_name") \
#        .withColumnRenamed("id", "state_id")
#    df = from_df.join(to_df, from_df.state == to_df.state_id) \
#        .filter("state_id = 'CO'") \
#        .filter("name = 'Adams County'")
#    assert df.count() == 1
#
#    with pytest.raises(DSGInvalidDimensionMapping):
#        store.get_dimension_mapping_key(Season, State)
