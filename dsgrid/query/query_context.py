import logging
from collections import defaultdict

from dsgrid.exceptions import DSGInvalidParameter
from .models import QueryBaseModel


logger = logging.getLogger(__name__)


class QueryContext:
    """Maintains context of the query as it is processed through the stack."""

    def __init__(self, model: QueryBaseModel):
        self._model = model
        self._record_ids_by_dimension_type = {}  # DimensionType to DataFrame
        self._metric_columns = None
        self._required_dimension_mappings = defaultdict(list)  # aggregation_name to [query_name]
        self._metric_reductions = {}  # query_name to (dimension_type, records)

    @property
    def metric_columns(self):
        return self._metric_columns

    @metric_columns.setter
    def metric_columns(self, columns):
        self._metric_columns = columns

    @property
    def model(self):
        return self._model

    def get_record_ids_by_dimension_type(self, dimension_type):
        return self._record_ids_by_dimension_type[dimension_type]

    def set_record_ids_by_dimension_type(self, dimension_type, record_ids):
        self._record_ids_by_dimension_type[dimension_type] = record_ids

    def iter_record_ids_by_dimension_type(self):
        return self._record_ids_by_dimension_type.items()

    @property
    def required_dimension_mappings(self):
        return self._required_dimension_mappings

    @required_dimension_mappings.setter
    def required_dimension_mappings(self, mappings):
        for key, query_names in mappings.items():
            existing = self._required_dimension_mappings[key]
            self._required_dimension_mappings[key] = list(set(existing + query_names))

    def add_required_dimension_mapping(self, aggregation_name: str, query_name: str):
        self._required_dimension_mappings[aggregation_name].append(query_name)

    def get_required_dimension_mappings(self, aggregation_name: str):
        return self._required_dimension_mappings.get(aggregation_name, [])

    def add_metric_reduction_records(self, query_name: str, dimension_type, records):
        self._metric_reductions[query_name] = (dimension_type, records)

    def get_metric_reduction_records(self, query_name: str):
        val = self._metric_reductions.get(query_name)
        if val is None:
            raise DSGInvalidParameter(f"query_name={query_name} is not stored")
        return val[0], val[1]
