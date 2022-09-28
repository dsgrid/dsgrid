from dsgrid.query.models import AggregationModel
from .dataset_table_base import DatasetTableBase


class LongTable(DatasetTableBase):
    """Implements behavior for datasets in long format."""

    def __init__(self, df, dimension_records, dimension_mapping_records):
        super().__init__(df)
        self._dimension_records = dimension_records
        self._dimension_mapping_records = dimension_mapping_records

    def process_aggregations(self, aggregations: AggregationModel):
        assert False
