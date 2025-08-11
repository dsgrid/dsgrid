"""Example script to programmatically generate dimenions.json5"""

import os

from dsgrid.config.dimension_config import DimensionModel
from dsgrid.config.dimensions_config import DimensionsConfigModel
from dsgrid.data_models import serialize_user_model
from dsgrid.utils.files import dump_data

from dsgrid.tests.common import TEST_PROJECT_REPO


print(TEST_PROJECT_REPO)
os.chdir(f"{TEST_PROJECT_REPO}/dsgrid_project/")
print(list(os.listdir()))

dimensions = []
dimX = DimensionModel(
    name="US Counties 2020",
    type="geography",
    class_name="County",
    module="dsgrid.dimension.standard",
    description="US Census Counties 2020",
    file="dimensions/counties.csv",
)
dimensions.append(dimX)
config = DimensionsConfigModel(dimensions=[dimX])
x = serialize_user_model(config)
dump_data(x, "examples_dimensions.json5")
