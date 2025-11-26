import copy
import shutil

import pytest

from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGValueNotRegistered
from dsgrid.utils.files import load_data
from dsgrid.config.project_config import (
    ProjectConfigModel,
    ProjectDimensionNamesModel,
    RequiredDimensionsModel,
    RequiredDimensionRecordsModel,
)
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.utils.id_remappings import (
    map_dimension_ids_to_names,
    map_dimension_names_to_ids,
    map_dimension_mapping_names_to_ids,
    replace_dimension_names_with_current_ids,
    replace_dimension_mapping_names_with_current_ids,
)
from tests.data.dimension_models.minimal.models import PROJECT_CONFIG_FILE


@pytest.fixture(scope="module")
def config_as_dict(cached_registry, tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("dsgrid")
    conn = cached_registry
    with RegistryManager.load(conn, offline_mode=True) as mgr:
        dim_map = map_dimension_names_to_ids(mgr.dimension_manager)
        dim_id_to_name = map_dimension_ids_to_names(mgr.dimension_manager)
        dim_mappings_map = map_dimension_mapping_names_to_ids(
            mgr.dimension_mapping_manager, dim_id_to_name
        )
        project_config_file = tmp_path / "project.json5"
        shutil.copyfile(PROJECT_CONFIG_FILE, project_config_file)
        replace_dimension_names_with_current_ids(project_config_file, dim_map)
        replace_dimension_mapping_names_with_current_ids(project_config_file, dim_mappings_map)
        yield load_data(project_config_file), mgr.dimension_manager


def test_good_project_config(config_as_dict):
    config = config_as_dict[0]
    model = ProjectConfigModel(**config)
    assert isinstance(model, ProjectConfigModel)


def test_project_invalid_id(config_as_dict):
    config, dimension_manager = config_as_dict
    config = copy.copy(config)
    dimensions = config["dimensions"]["base_dimension_references"]
    first = dimensions[0]
    first["dimension_id"] = "invalid"
    model = ProjectConfigModel(**config)
    with pytest.raises(DSGValueNotRegistered):
        dimension_manager.load_dimensions(model.dimensions.base_dimension_references)


def test_project_config_missing_dimension(config_as_dict):
    config = copy.copy(config_as_dict[0])
    for i, dimension in enumerate(config["dimensions"]["base_dimension_references"]):
        if dimension["dimension_id"].startswith("county"):
            break
    config["dimensions"]["base_dimension_references"].pop(i)
    with pytest.raises(ValueError):
        ProjectConfigModel(**config)


def test_project_duplicate_dimension(config_as_dict):
    config = copy.copy(config_as_dict[0])
    first = config["dimensions"]["base_dimension_references"][0]
    config["dimensions"]["base_dimension_references"].append(first)
    with pytest.raises(ValueError):
        ProjectConfigModel(**config)


def test_project_duplicate_type(config_as_dict):
    config = copy.copy(config_as_dict[0])
    base_refs = config["dimensions"]["base_dimension_references"]
    supp_refs = config["dimensions"]["supplemental_dimension_references"]
    assert base_refs
    assert supp_refs
    base_refs.append(supp_refs[0])
    supp_refs.pop(0)

    with pytest.raises(ValueError):
        ProjectConfigModel(**config)


def test_project_dimension_names_model():
    assert not {x.value for x in DimensionType}.difference(ProjectDimensionNamesModel.model_fields)


def test_duplicate_dimension_requirements():
    single_dim_data = {"subsector": {"base": ["subsectors"]}}
    multi_dim_data = [
        {
            "subsector": {"base": ["bev_compact"]},
            "metric": {"base": ["electricity_ev_ldv_home_l1"]},
        },
    ]
    with pytest.raises(ValueError, match="dimensions cannot be defined"):
        RequiredDimensionsModel(
            single_dimensional=RequiredDimensionRecordsModel(**single_dim_data),
            multi_dimensional=[RequiredDimensionRecordsModel(**x) for x in multi_dim_data],
        )


def test_invalid_multi_dimensional_requirement_too_few():
    single_dim_data = {"subsector": {"base": ["subsectors"]}}
    multi_dim_data = [{"metric": {"base": ["electricity_ev_ldv_home_l1"]}}]
    with pytest.raises(ValueError, match="at least two"):
        RequiredDimensionsModel(
            single_dimensional=RequiredDimensionRecordsModel(**single_dim_data),
            multi_dimensional=[RequiredDimensionRecordsModel(**x) for x in multi_dim_data],
        )


def test_invalid_multi_dimensional_requirement_partial_intersection():
    single_dim_data = {"sector": {"base": ["sector1"]}}
    multi_dim_data = [
        {
            "metric": {"base": ["metric1"]},
            "subsector": {"base": ["subsector1"]},
        },
        {
            "metric": {"base": ["metric2"]},
            "subsector": {"base": ["subsector2"]},
            "geography": {"base": ["geography1"]},
        },
    ]
    with pytest.raises(ValueError, match="must have a full intersection"):
        RequiredDimensionsModel(
            single_dimensional=RequiredDimensionRecordsModel(**single_dim_data),
            multi_dimensional=[RequiredDimensionRecordsModel(**x) for x in multi_dim_data],
        )


def test_invalid_multi_dimensional_requirement_base_and_base_missing():
    multi_dim_data = [
        {
            "metric": {"base": ["metric1"], "base_missing": ["metric2"]},
            "subsector": {"base": ["subsector1"]},
        },
    ]
    with pytest.raises(ValueError, match="base and base_missing cannot both contain"):
        RequiredDimensionsModel(
            multi_dimensional=[RequiredDimensionRecordsModel(**x) for x in multi_dim_data],
        )
