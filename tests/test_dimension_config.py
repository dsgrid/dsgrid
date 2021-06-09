import os

import pytest

from dsgrid.config.dimensions_config import DimensionsConfigModel
from dsgrid.utils.files import load_data
from tests.data.dimension_models.minimal.models import DIMENSION_CONFIG_FILE


@pytest.fixture
def config_as_dict():
    orig = os.getcwd()
    os.chdir(os.path.dirname(DIMENSION_CONFIG_FILE))
    yield load_data(DIMENSION_CONFIG_FILE)
    os.chdir(orig)


def test_dimensions_config_good(config_as_dict):
    model = DimensionsConfigModel(**config_as_dict)
    assert isinstance(model, DimensionsConfigModel)


def test_dimensions_config_invalid_dimension_filename(config_as_dict):
    for dimension in config_as_dict["dimensions"]:
        if dimension["type"] != "time":
            dimension["file"] += "_bad"
            break
    with pytest.raises(ValueError):
        DimensionsConfigModel(**config_as_dict)


# def test_dimensions_config_invalid_dimension_name_to_class(config_as_dict):
#     for dimension in config_as_dict["dimensions"]:
#         if dimension["name"] == "County":
#             dimension["name"] = "InvalidCounty"
#             break
#     with pytest.raises(ValueError):
#         DimensionsConfigModel(**config_as_dict)


def test_dimensions_config_invalid_dimension_class(config_as_dict):
    for dimension in config_as_dict["dimensions"]:
        dimension["class"] = "InvalidClassName"
        break
    with pytest.raises(ValueError):
        DimensionsConfigModel(**config_as_dict)


def test_dimensions_config_invalid_dimension_name(config_as_dict):
    config_as_dict["dimensions"][0]["name"] = ""
    with pytest.raises(ValueError):
        DimensionsConfigModel(**config_as_dict)
