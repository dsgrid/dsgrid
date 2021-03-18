"""TBD"""

from dsgrid.analysis.dataset import Dataset


class Datasets:
    """Manages datasets in the project config"""
    def __init__(self, store, datasets):
        self._store = store
        self._record_store = store.record_store  # DimensionRecords
        self._datasets = datasets
        self._spark = store.spark

    @classmethod
    def load(cls, store):
        """Load datasets from a DimensionStore."""
        record_store = store.record_store
        for dim_class in record_store.iter_dimension_classes():
            df = record_store.get_dataframe(dim_class)
            df.createOrReplaceTempView(dim_class.__name__)

        datasets = {}
        for input_dataset in store.iter_datasets():
            dataset = Dataset.load(input_dataset, store)
            datasets[input_dataset.dataset_id] = dataset

        return cls(store, datasets)

    def get_dataset(self, dataset_id):
        return self._datasets[dataset_id]

    def make_tables(self):
        for dim_class in self._record_store.iter_dimension_classes():
            df = self._record_store.get_dataframe(dim_class)
            df.createOrReplaceTempView(dim_class.__name__)
        self._spark.sql("show tables").show()
