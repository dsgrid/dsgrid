import os

import pytest
from pyspark.sql import SparkSession

from dsgrid.project import Project
from dsgrid.dataset import Dataset
from dsgrid.config.association_tables import AssociationTableConfig
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGValueNotRegistered
from dsgrid.tests.common import TEST_REGISTRY


PROJECT_ID = "test_efs"
DATASET_ID = "test_efs_comstock"


def test_project_load():
    project = Project.load(PROJECT_ID, offline_mode=True, registry_path=TEST_REGISTRY)
    assert isinstance(project, Project)
    project = Project.load(
        PROJECT_ID, version="1.0.0", offline_mode=True, registry_path=TEST_REGISTRY
    )
    assert isinstance(project, Project)

    config = project.config
    dim = config.get_base_dimension(DimensionType.GEOGRAPHY)
    assert dim.model.dimension_type == DimensionType.GEOGRAPHY
    supp_dims = config.get_supplemental_dimensions(DimensionType.GEOGRAPHY)
    assert len(supp_dims) == 3
    assert config.has_base_to_supplemental_dimension_mapping_types(DimensionType.GEOGRAPHY)
    mappings = config.get_base_to_supplemental_dimension_mappings_by_types(DimensionType.GEOGRAPHY)
    assert len(mappings) == 3
    assert not config.has_base_to_supplemental_dimension_mapping_types(DimensionType.SECTOR)

    with pytest.raises(DSGValueNotRegistered):
        project = Project.load(
            PROJECT_ID, version="0.0.0", offline_mode=True, registry_path=TEST_REGISTRY
        )
        assert isinstance(project, Project)


def test_dataset_load():
    project = Project.load(PROJECT_ID, offline_mode=True, registry_path=TEST_REGISTRY)
    project.load_dataset(DATASET_ID)
    dataset = project.get_dataset(DATASET_ID)
    assert isinstance(dataset, Dataset)
    spark = SparkSession.getActiveSession()
    data = spark.sql(f"select * from {DATASET_ID}__load_data")
    assert "timestamp" in data.columns
    assert "fans" in data.columns
    lookup = spark.sql(f"select * from {DATASET_ID}__load_data_lookup")
    assert "subsector" in lookup.columns
    assert "id" in lookup.columns

    project.unload_dataset(DATASET_ID)
    assert spark.sql("show tables").rdd.isEmpty()


# def test_aggregate_load_by_state():
#    store = DimensionStore.load(PROJECT_CONFIG_FILE)
#    dataset = Dataset.load(store)
#    df = dataset.aggregate_sector_sums_by_dimension(County, State)
#    assert "state" in df.columns
#    assert "sum((sum(fans) * scale_factor))" in df.columns
#    # For now just ensure this doesn't fail.
#    df.count()
