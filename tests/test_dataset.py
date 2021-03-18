
import os
from pathlib import Path

from dsgrid.analysis.dataset import Dataset
from dsgrid.dimension.store import DimensionStore
from dsgrid.utils.files import load_data
from .data.dimension_models.minimal.models import *


# Use one store for all tests. It won't be mutated after load.
#store = DimensionStore.load(PROJECT_CONFIG_FILE)

#def test_aggregate_load_by_state():
#    store = DimensionStore.load(PROJECT_CONFIG_FILE)
#    dataset = Dataset.load(store)
#    df = dataset.aggregate_sector_sums_by_dimension(County, State)
#    assert "state" in df.columns
#    assert "sum((sum(fans) * scale_factor))" in df.columns
#    # For now just ensure this doesn't fail.
#    df.count()
