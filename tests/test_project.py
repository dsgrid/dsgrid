import pytest
from pyspark.sql import SparkSession

from dsgrid.project import Project
from dsgrid.dataset import Dataset
from dsgrid.exceptions import DSGValueNotRegistered


def test_project_load():
    project = Project.load("test", offline_mode=True)
    assert isinstance(project, Project)
    project = Project.load("test", version="1.0.0", offline_mode=True)
    assert isinstance(project, Project)
    with pytest.raises(DSGValueNotRegistered):
        project = Project.load("test", version="0.0.0", offline_mode=True)
        assert isinstance(project, Project)


def test_dataset_load():
    project = Project.load("test", offline_mode=True)
    dataset_id = "efs_comstock"
    project.load_dataset(dataset_id)
    dataset = project.get_dataset(dataset_id)
    assert isinstance(dataset, Dataset)
    spark = SparkSession.getActiveSession()
    data = spark.sql("select * from efs_comstock__load_data")
    assert "timestamp" in data.columns
    assert "fans" in data.columns
    lookup = spark.sql("select * from efs_comstock__load_data_lookup")
    assert "subsector" in lookup.columns
    assert "id" in lookup.columns

    project.unload_dataset(dataset_id)
    assert spark.sql("show tables").rdd.isEmpty()


# def test_aggregate_load_by_state():
#    store = DimensionStore.load(PROJECT_CONFIG_FILE)
#    dataset = Dataset.load(store)
#    df = dataset.aggregate_sector_sums_by_dimension(County, State)
#    assert "state" in df.columns
#    assert "sum((sum(fans) * scale_factor))" in df.columns
#    # For now just ensure this doesn't fail.
#    df.count()
