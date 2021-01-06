
import os
from pathlib import Path

from dsgrid.analysis.sector_dataset import SectorDataset
from dsgrid.dimension.store import DimensionStore
from .data.dimension_models.minimal.models import *

# Use one store for all tests. It won't be mutated after load.
store = DimensionStore.load(
    MODEL_MAPPINGS,
    dimension_mappings=DIMENSION_MAPPINGS,
)

# TODO: need to make a small dataset for use in tests
DATA_DIR = os.path.join(str(Path.home()), "dsgrid-data/output/industrial")


def test_aggregate_load_by_state():
    dataset = SectorDataset.load(DATA_DIR, store)
    df = dataset.aggregate_sector_sums_by_dimension(County, State)
    assert "state" in df.columns
    assert "sum((sum(conventional_boiler_use) * scale_factor))" in df.columns
    # For now just ensure this doesn't fail.
    df.count()
