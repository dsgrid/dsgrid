
import os
from pathlib import Path

from dsgrid.analysis.datasets import Datasets
from dsgrid.dimension.store import DimensionStore
from .data.dimension_models.minimal.models import *

store = DimensionStore.load(PROJECT_CONFIG_FILE)


def test_datasets__load():
    datasets = Datasets.load(store)
    assert isinstance(datasets, Datasets)
    spark = store.spark
    # Just make sure that we can run a query.
    assert spark.sql("select * from State where id='CO'").count() == 1
