from pyspark.sql import SparkSession

from dsgrid.project import Project
from dsgrid.config.project_config import ProjectConfig
from dsgrid.dataset import Dataset


def test_project_load():
    project = Project.load("test")
    assert isinstance(project, Project)


def test_dataset_load():
    project = Project.load("test")
    dataset_id = "comstock"
    project.load_dataset(dataset_id)
    dataset = project.get_dataset(dataset_id)
    assert isinstance(dataset, Dataset)
    spark = SparkSession.getActiveSession()
    data = spark.sql("select * from comstock__load_data")
    assert "timestamp" in data.columns
    assert "fans" in data.columns
    lookup = spark.sql("select * from comstock__load_data_lookup")
    assert "subsector" in lookup.columns
    assert "data_id" in lookup.columns

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
