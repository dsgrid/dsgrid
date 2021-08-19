"""Example script to programmatically generate dimenions.toml
"""
from dsgrid.config.dimension_config import DimensionModel
from dsgrid.config.dimensions_config import DimensionsConfigModel
from dsgrid.data_models import serialize_model, serialize_user_model
from dsgrid.utils.files import dump_data

from dsgrid.tests.common import PROJECT_REPO

print(PROJECT_REPO)
import os

os.chdir(f"{PROJECT_REPO}/dsgrid_project/")
print(list(os.listdir()))

dimensions = []
dimX = DimensionModel(
    name="US Counties 2020",
    type="geography",
    class_name="County",
    module="dsgrid.dimension.standard",
    description="US Census Counties 2020",
    file=f"dimensions/counties.csv",
)
dimensions.append(dimX)
config = DimensionsConfigModel(dimensions=[dimX])
x = serialize_user_model(config)
dump_data(x, "examples_dimensions.toml")
