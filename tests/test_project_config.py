import pytest

from dsgrid.exceptions import DSGValueNotRegistered
from dsgrid.utils.files import load_data
from dsgrid.config.project_config import ProjectConfig, ProjectConfigModel
from dsgrid.registry.registry_manager import RegistryManager, get_registry_path
from tests.data.dimension_models.minimal.models import PROJECT_CONFIG_FILE


@pytest.fixture
def config_as_dict():
    return load_data(PROJECT_CONFIG_FILE)


@pytest.fixture
def dimension_manager():
    registry = RegistryManager.load(get_registry_path(), offline_mode=True)
    return registry.dimension_manager


def test_good_project_config(dimension_manager):
    config = ProjectConfig.load(PROJECT_CONFIG_FILE, dimension_manager)
    assert isinstance(config, ProjectConfig)


def test_project_invalid_id(config_as_dict, dimension_manager):
    dimensions = config_as_dict["dimensions"]["base_dimensions"]
    first = dimensions[0]
    first["dimension_id"] = "invalid"
    model = ProjectConfigModel(**config_as_dict)
    with pytest.raises(DSGValueNotRegistered):
        dimension_manager.load_dimensions(model.dimensions.base_dimensions)


def test_project_config_missing_dimension(config_as_dict):
    for i, dimension in enumerate(config_as_dict["dimensions"]["base_dimensions"]):
        if dimension["dimension_id"].startswith("county"):
            break
    config_as_dict["dimensions"]["base_dimensions"].pop(i)
    with pytest.raises(ValueError):
        ProjectConfigModel(**config_as_dict)


def test_project_duplicate_dimension(config_as_dict, dimension_manager):
    first = config_as_dict["dimensions"]["base_dimensions"][0]
    config_as_dict["dimensions"]["base_dimensions"].append(first)
    with pytest.raises(ValueError):
        ProjectConfigModel(**config_as_dict)


def test_project_duplicate_type(config_as_dict, dimension_manager):
    index = None
    duplicate = None
    for i, dim in enumerate(config_as_dict["dimensions"]["supplemental_dimensions"]):
        if dim["dimension_id"].startswith("us_states"):
            index = i
            config_as_dict["dimensions"]["base_dimensions"].append(dim)
    assert index is not None
    config_as_dict["dimensions"]["supplemental_dimensions"].pop(index)

    with pytest.raises(ValueError):
        ProjectConfigModel(**config_as_dict)
