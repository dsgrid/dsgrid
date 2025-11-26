import pytest

from dsgrid.config.project_config import ProjectConfig
from dsgrid.dataset.dataset_mapping_manager import DatasetMappingManager
from dsgrid.project import Project
from dsgrid.dataset.dataset import Dataset
from dsgrid.dimension.base_models import DimensionType, DimensionCategory
from dsgrid.exceptions import (
    DSGInvalidParameter,
    DSGValueNotRegistered,
    DSGInvalidDimensionMapping,
)
from dsgrid.dsgrid_rc import DsgridRuntimeConfig
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.utils.scratch_dir_context import ScratchDirContext


PROJECT_ID = "test_efs"
DATASET_ID = "test_efs_comstock"


@pytest.fixture(scope="module")
def project_config(cached_registry) -> ProjectConfig:
    conn = cached_registry
    with RegistryManager.load(conn, offline_mode=True) as mgr:
        project = mgr.project_manager.load_project(PROJECT_ID)
        yield project.config


def test_project_load(cached_registry):
    conn = cached_registry
    with RegistryManager.load(conn, offline_mode=True) as mgr:
        project = mgr.project_manager.load_project(PROJECT_ID)
        assert isinstance(project, Project)

        config = project.config
        dim = config.get_base_dimension(DimensionType.GEOGRAPHY)
        assert dim.model.dimension_type == DimensionType.GEOGRAPHY
        geo_supp_dims = config.list_supplemental_dimensions(DimensionType.GEOGRAPHY)
        assert len(geo_supp_dims) == 4
        subsector_supp_dims = config.list_supplemental_dimensions(DimensionType.SUBSECTOR)
        assert len(subsector_supp_dims) == 3
        assert config.has_base_to_supplemental_dimension_mapping_types(DimensionType.GEOGRAPHY)
        mappings = config.get_base_to_supplemental_dimension_mappings_by_types(
            DimensionType.GEOGRAPHY
        )
        assert len(mappings) == 4
        assert config.has_base_to_supplemental_dimension_mapping_types(DimensionType.SECTOR)
        assert config.has_base_to_supplemental_dimension_mapping_types(DimensionType.SUBSECTOR)
        subset_dims = config.list_dimension_names(category=DimensionCategory.SUBSET)
        assert subset_dims == ["commercial_subsectors2", "residential_subsectors"]
        config.get_dimension("residential_subsectors").get_unique_ids() == {"MidriseApartment"}

        records = project.config.get_dimension_records("all_test_efs_subsectors").collect()
        assert len(records) == 1
        assert records[0].id == "all_subsectors"

        with pytest.raises(DSGValueNotRegistered):
            project = mgr.project_manager.load_project(PROJECT_ID, version="0.0.0")
            assert isinstance(project, Project)


def test_list_base_dimensions(project_config: ProjectConfig):
    assert len(project_config.list_base_dimensions()) == len(DimensionType)
    geos = project_config.list_base_dimensions(dimension_type=DimensionType.GEOGRAPHY)
    assert len(geos) == 1
    assert geos[0].model.name == "US Counties 2010 - ComStock Only"
    geos = project_config.list_base_dimensions_with_records(dimension_type=DimensionType.GEOGRAPHY)
    assert len(geos) == 1
    assert geos[0].model.name == "US Counties 2010 - ComStock Only"


def test_get_base_dimension(project_config: ProjectConfig):
    geo = project_config.get_base_dimension(DimensionType.GEOGRAPHY)
    assert geo.model.name == "US Counties 2010 - ComStock Only"
    assert (
        project_config.get_base_dimension(
            DimensionType.GEOGRAPHY, dimension_name="US Counties 2010 - ComStock Only"
        ).model.name
        == "US Counties 2010 - ComStock Only"
    )
    with pytest.raises(DSGValueNotRegistered):
        project_config.get_base_dimension(DimensionType.GEOGRAPHY, dimension_name="US States")


def test_get_base_time_dimension(project_config: ProjectConfig):
    dim = project_config.get_base_dimension(DimensionType.TIME)
    assert dim.model.name == "Time-2012-EST-hourly-periodBeginning-noDST-noLeapDayAdjustment-total"


def test_get_base_dimension_and_version(project_config: ProjectConfig):
    dim, version = project_config.get_base_dimension_and_version(DimensionType.GEOGRAPHY)
    assert dim.model.name == "US Counties 2010 - ComStock Only"
    assert version == "1.0.0"
    dim, version = project_config.get_base_dimension_and_version(
        DimensionType.GEOGRAPHY, dimension_name="US Counties 2010 - ComStock Only"
    )
    assert dim.model.name == "US Counties 2010 - ComStock Only"
    assert version == "1.0.0"

    with pytest.raises(DSGValueNotRegistered):
        project_config.get_base_dimension_and_version(
            DimensionType.GEOGRAPHY, dimension_name="US States"
        )


def test_get_dimension(project_config: ProjectConfig):
    assert (
        project_config.get_dimension("US Counties 2010 - ComStock Only").model.name
        == "US Counties 2010 - ComStock Only"
    )
    with pytest.raises(DSGValueNotRegistered):
        project_config.get_dimension("invalid")


def test_get_time_dimension(project_config: ProjectConfig):
    assert (
        project_config.get_base_time_dimension().model.name
        == "Time-2012-EST-hourly-periodBeginning-noDST-noLeapDayAdjustment-total"
    )
    with pytest.raises(DSGValueNotRegistered):
        project_config.get_time_dimension("invalid")
    with pytest.raises(DSGInvalidParameter):
        project_config.get_time_dimension("US Counties 2010 - ComStock Only")


def test_get_dimension_with_records(project_config: ProjectConfig):
    assert (
        project_config.get_dimension_with_records("US Counties 2010 - ComStock Only").model.name
        == "US Counties 2010 - ComStock Only"
    )
    with pytest.raises(DSGInvalidParameter):
        project_config.get_dimension_with_records(
            "Time-2012-EST-hourly-periodBeginning-noDST-noLeapDayAdjustment-total"
        )


def test_get_dimension_records(project_config: ProjectConfig):
    assert project_config.get_dimension_records("US Counties 2010 - ComStock Only").count() == 8
    with pytest.raises(DSGInvalidParameter):
        project_config.get_dimension_records(
            "Time-2012-EST-hourly-periodBeginning-noDST-noLeapDayAdjustment-total"
        )


def test_get_base_to_supplemental_config(project_config: ProjectConfig):
    base_dim = project_config.get_dimension_with_records("US Counties 2010 - ComStock Only")
    supp_dim = project_config.get_dimension_with_records("US States")
    mapping = project_config.get_base_to_supplemental_config(base_dim, supp_dim)
    assert mapping.model.from_dimension.dimension_id == base_dim.model.dimension_id
    assert mapping.model.to_dimension.dimension_id == supp_dim.model.dimension_id

    subsector_dim = project_config.get_dimension_with_records("Commercial Subsectors")
    with pytest.raises(DSGValueNotRegistered):
        project_config.get_base_to_supplemental_config(base_dim, subsector_dim)

    with pytest.raises(DSGInvalidParameter):
        project_config.get_base_to_supplemental_config(base_dim, base_dim)


def test_get_base_dimension_by_id(project_config: ProjectConfig):
    county = project_config.get_dimension("US Counties 2010 - ComStock Only")
    assert (
        project_config.get_base_dimension_by_id(county.model.dimension_id).model.name
        == "US Counties 2010 - ComStock Only"
    )
    with pytest.raises(DSGValueNotRegistered):
        project_config.get_base_dimension_by_id("invalid")


def test_get_base_dimension_records_by_id(project_config: ProjectConfig):
    county = project_config.get_dimension("US Counties 2010 - ComStock Only")
    assert project_config.get_base_dimension_records_by_id(county.model.dimension_id).count() == 8
    assert (
        project_config.get_base_dimension_by_id(county.model.dimension_id).model.name
        == "US Counties 2010 - ComStock Only"
    )
    time_dim = project_config.get_base_dimension(DimensionType.TIME)
    with pytest.raises(DSGInvalidParameter):
        project_config.get_base_dimension_records_by_id(time_dim.model.dimension_id)


def test_dataset_load(cached_registry, scratch_dir_context):
    conn = cached_registry
    with RegistryManager.load(conn, offline_mode=True) as mgr:
        project = mgr.project_manager.load_project(PROJECT_ID)
        project.load_dataset(DATASET_ID)
        dataset = project.get_dataset(DATASET_ID)

        assert isinstance(dataset, Dataset)
        query_names = sorted(project.config.list_dimension_names_by_type(DimensionType.GEOGRAPHY))
        assert query_names == [
            "US Census Divisions",
            "US Census Regions",
            "US Counties 2010 - ComStock Only",
            "US States",
            "all_test_efs_geographies",
        ]
        records = project.config.get_dimension_records("US States")
        assert records.filter("id = 'CO'").count() > 0


def test_dimension_map_and_reduce_in_dataset(cached_registry):
    conn = cached_registry
    with RegistryManager.load(conn, offline_mode=True) as registry_mgr:
        project = registry_mgr.project_manager.load_project(PROJECT_ID)
        project.load_dataset(DATASET_ID)
        dataset = project.get_dataset(DATASET_ID)

        load_data_df = dataset._handler._load_data
        load_data_lookup_df = dataset._handler._load_data_lookup
        plan = dataset.handler.build_default_dataset_mapping_plan()
        context = ScratchDirContext(DsgridRuntimeConfig.load().get_scratch_dir())
        with DatasetMappingManager(DATASET_ID, plan, context) as mapping_mgr:
            mapped_load_data = dataset._handler._remap_dimension_columns(load_data_df, mapping_mgr)
        with DatasetMappingManager(DATASET_ID, plan, context) as mapping_mgr:
            mapped_load_data_lookup = dataset.handler._remap_dimension_columns(
                load_data_lookup_df, mapping_mgr
            )

        # [1] check that mapped tables contain all to_id records from mappings
        for ref in dataset._handler._mapping_references:
            column = ref.from_dimension_type.value
            mapping_config = dataset._handler._dimension_mapping_mgr.get_by_id(ref.mapping_id)
            to_records = mapping_config.get_unique_to_ids()  # set

            if column in mapped_load_data.columns:
                table_type = "load_data"
                table = mapped_load_data
            else:
                assert column in mapped_load_data_lookup.columns
                table_type = "load_data_lookup"
                table = mapped_load_data_lookup
            diff = set(
                [row[column] for row in table.select(column).distinct().collect()]
            ).symmetric_difference(to_records)
            if diff:
                raise DSGInvalidDimensionMapping(
                    "Mapped %s is incorrect, check %s mapping: %s or mapping logic in 'dataset_schema_handler_base._map_and_reduce_dimension()' \n%s"
                    % (table_type, column, ref.mapping_id, diff)
                )

        # [2] check that fraction is correctly applied and reduced
        # [2A] load_data_lookup
        assert "fraction" in mapped_load_data_lookup.columns

        # * this check is specific to the actual from_fraction values specified in the mapping *
        data_filters = "subsector=='Warehouse' and model_year=='2050'"
        fraction = [
            row.fraction
            for row in mapped_load_data_lookup.filter(data_filters)
            .select("fraction")
            .distinct()
            .collect()
        ]
        assert len(fraction) == 1
        assert fraction[0] == (0.9 * 1.3)
