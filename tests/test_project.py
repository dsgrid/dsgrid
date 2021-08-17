import os

import pytest
from pyspark.sql import SparkSession

from dsgrid.project import Project
from dsgrid.dataset import Dataset
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
