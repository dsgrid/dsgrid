
import copy

import pytest

from dsgrid.utils.files import load_data
from dsgrid.config.project_config import ProjectConfig, load_project_config
from tests.data.dimension_models.minimal.models import PROJECT_CONFIG_FILE


@pytest.fixture
def config_as_dict():
    return load_data(PROJECT_CONFIG_FILE)


def test_good_project_config():
    config = load_project_config(PROJECT_CONFIG_FILE)
    assert isinstance(config, ProjectConfig)


def test_project_duplicate_id(config_as_dict):
    for dimension in config_as_dict["dimensions"]["project_dimensions"]:
        if dimension["name"] == "County":
            dimension["name"] = "InvalidCounty"
            break
    with pytest.raises(ValueError):
        ProjectConfig(**config_as_dict)


def test_project_config_invalid_dimension_name(config_as_dict):
    config_as_dict["dimensions"]["project_dimensions"][0]["name"] = ""
    with pytest.raises(ValueError):
        ProjectConfig(**config_as_dict)


def test_project_config_invalid_dimension_filename(config_as_dict):
    for dimension in config_as_dict["dimensions"]["project_dimensions"]:
        if dimension["type"] != "time":
            dimension["file"] += "_bad"
            dimension["name"] = ""
            break
    with pytest.raises(ValueError):
        ProjectConfig(**config_as_dict)


def test_project_config_invalid_dimension_name_to_class(config_as_dict):
    for dimension in config_as_dict["dimensions"]["project_dimensions"]:
        if dimension["name"] == "County":
            dimension["name"] = "InvalidCounty"
            break
    with pytest.raises(ValueError):
        ProjectConfig(**config_as_dict)


def test_project_config_invalid_dimension_class(config_as_dict):
    for dimension in config_as_dict["dimensions"]["project_dimensions"]:
        if dimension["name"] == "County":
            dimension["class_name"] = "InvalidCounty"
            break
    with pytest.raises(ValueError):
        ProjectConfig(**config_as_dict)


def test_project_config_missing_dimension(config_as_dict):
    for i, dimension in enumerate(config_as_dict["dimensions"]["project_dimensions"]):
        if dimension["name"] == "County":
            break
    config_as_dict["dimensions"]["project_dimensions"].pop(i)
    with pytest.raises(ValueError):
        ProjectConfig(**config_as_dict)


def test_project_duplicate_dimension(config_as_dict):
    first = config_as_dict["dimensions"]["project_dimensions"][0]
    config_as_dict["dimensions"]["project_dimensions"].append(first)
    with pytest.raises(ValueError):
        ProjectConfig(**config_as_dict)


def test_dataset_invalid_path(config_as_dict):
    config_as_dict["input_datasets"]["datasets"][0]["path"] += "bad"
    with pytest.raises(ValueError):
        ProjectConfig(**config_as_dict)
