import pytest

from dsgrid.exceptions import DSGValueNotRegistered
from dsgrid.utils.files import load_data
from dsgrid.config.project_config import ProjectConfigModel
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.tests.common import TEST_REGISTRY
from tests.data.dimension_models.minimal.models import PROJECT_CONFIG_FILE


@pytest.fixture
def config_as_dict():
    return load_data(PROJECT_CONFIG_FILE)


@pytest.fixture
def dimension_manager():
    registry = RegistryManager.load(TEST_REGISTRY, offline_mode=True)
    return registry.dimension_manager


def test_good_project_config(config_as_dict):
    model = ProjectConfigModel(**config_as_dict)
    assert isinstance(model, ProjectConfigModel)


def test_project_invalid_id(config_as_dict, dimension_manager):
    dimensions = config_as_dict["dimensions"]["base_dimension_references"]
    first = dimensions[0]
    first["dimension_id"] = "invalid"
    model = ProjectConfigModel(**config_as_dict)
    with pytest.raises(DSGValueNotRegistered):
        dimension_manager.load_dimensions(model.dimensions.base_dimension_references)


def test_project_config_missing_dimension(config_as_dict):
    for i, dimension in enumerate(config_as_dict["dimensions"]["base_dimension_references"]):
        if dimension["dimension_id"].startswith("county"):
            break
    config_as_dict["dimensions"]["base_dimension_references"].pop(i)
    with pytest.raises(ValueError):
        ProjectConfigModel(**config_as_dict)


def test_project_duplicate_dimension(config_as_dict, dimension_manager):
    first = config_as_dict["dimensions"]["base_dimension_references"][0]
    config_as_dict["dimensions"]["base_dimension_references"].append(first)
    with pytest.raises(ValueError):
        ProjectConfigModel(**config_as_dict)


def test_project_duplicate_type(config_as_dict, dimension_manager):
    index = None
    for i, dim in enumerate(config_as_dict["dimensions"]["supplemental_dimension_references"]):
        if dim["dimension_id"].startswith("us_states"):
            index = i
            config_as_dict["dimensions"]["base_dimension_references"].append(dim)
    assert index is not None
    config_as_dict["dimensions"]["supplemental_dimension_references"].pop(index)

    with pytest.raises(ValueError):
        ProjectConfigModel(**config_as_dict)
