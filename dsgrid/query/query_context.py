import logging

from dsgrid.dimension.base_models import DimensionType

from .models import QueryBaseModel, DatasetMetadataModel, TableFormatType


logger = logging.getLogger(__name__)


class QueryContext:
    """Maintains context of the query as it is processed through the stack."""

    def __init__(self, model: QueryBaseModel):
        self._model = model
        self._record_ids_by_dimension_type = {}  # DimensionType to DataFrame
        self._metadata = DatasetMetadataModel()
        self._dataset_metadata = {}  # dataset_id to DatasetMetadataModel

    @property
    def metadata(self):
        return self._metadata

    @metadata.setter
    def metadata(self, val):
        self._metadata = val

    @property
    def model(self):
        return self._model

    def consolidate_dataset_metadata(self):
        for dim_type in DimensionType:
            field = dim_type.value
            getattr(self._metadata.dimensions, field).clear()
            for dataset_metadata in self._dataset_metadata.values():
                getattr(self._metadata.dimensions, field).update(
                    getattr(dataset_metadata.dimensions, field)
                )

        self._metadata.pivoted.columns.clear()
        for dataset_metadata in self._dataset_metadata.values():
            if dataset_metadata.table_format_type == TableFormatType.PIVOTED:
                self.add_pivoted_columns(dataset_metadata.pivoted.columns)
                if self.get_pivoted_dimension_type() is None:
                    self.set_pivoted_dimension_type(dataset_metadata.pivoted.dimension_type)
                    self.set_table_format_type(TableFormatType.PIVOTED)
                elif dataset_metadata.pivoted.dimension_type != self.get_pivoted_dimension_type():
                    # TODO: Remove this check when we support transforming to long format.
                    raise Exception(
                        "Datasets have different pivoted dimension types: "
                        f"{dataset_metadata.pivoted.dimension_type} {self.get_pivoted_dimension_type()}"
                    )

    def get_pivoted_columns(self, dataset_id=None):
        if dataset_id is None:
            return self._metadata.pivoted.columns
        return self._dataset_metadata[dataset_id].pivoted.columns

    def set_pivoted_columns(self, columns, dataset_id=None):
        if dataset_id is None:
            self._metadata.pivoted.columns = columns
        else:
            self._dataset_metadata[dataset_id].pivoted.columns = columns
        logger.info("Set pivoted columns dataset_id=%s: %s", dataset_id, columns)

    def add_pivoted_columns(self, columns, dataset_id=None):
        if dataset_id is None:
            self._metadata.pivoted.columns.update(columns)
        else:
            self._dataset_metadata[dataset_id].pivoted.columns.update(columns)

    def get_pivoted_dimension_type(self, dataset_id=None):
        if dataset_id is None:
            return self._metadata.pivoted.dimension_type
        return self._dataset_metadata[dataset_id].pivoted.dimension_type

    def set_pivoted_dimension_type(self, val, dataset_id=None):
        if dataset_id is None:
            self._metadata.pivoted.dimension_type = val
        else:
            self._dataset_metadata[dataset_id].pivoted.dimension_type = val

    def get_table_format_type(self, dataset_id=None):
        if dataset_id is None:
            return self._metadata.table_format_type
        return self._dataset_metadata[dataset_id].table_format_type

    def set_table_format_type(self, val, dataset_id=None):
        if dataset_id is None:
            self._metadata.table_format_type = val
        else:
            self._dataset_metadata[dataset_id].table_format_type = val

    def get_dimension_query_names(self, dimension_type: DimensionType):
        return getattr(self._metadata.dimensions, dimension_type.value)

    def get_all_dimension_query_names(self):
        names = set()
        for dimension_type in DimensionType:
            names.update(getattr(self._metadata.dimensions, dimension_type.value))
        return names

    def add_dataset_metadata(self, dataset_id):
        self._dataset_metadata[dataset_id] = DatasetMetadataModel()

    def add_dimension_query_name(
        self, dimension_type: DimensionType, dimension_query_name, dataset_id=None
    ):
        container = self._get_dimension_query_name_container(dimension_type, dataset_id=dataset_id)
        if dimension_query_name not in container:
            container.add(dimension_query_name)
            logger.info(
                "Added dimension query name for %s: %s dataset_id=%s",
                dimension_type,
                dimension_query_name,
                dataset_id,
            )

    def remove_dimension_query_name(
        self, dimension_type: DimensionType, dimension_query_name, dataset_id=None
    ):
        container = self._get_dimension_query_name_container(dimension_type, dataset_id=dataset_id)
        container.remove(dimension_query_name)
        logger.info(
            "Removed dimension query name for %s: %s", dimension_type, dimension_query_name
        )

    def replace_dimension_query_names(
        self, dimension_type: DimensionType, dimension_query_names, dataset_id=None
    ):
        field = dimension_type.value
        if dataset_id is None:
            setattr(self._metadata.dimensions, field, dimension_query_names)
        else:
            setattr(self._dataset_metadata[dataset_id].dimensions, field, dimension_query_names)
        logger.info(
            "Replaced dimension for %s: %s dataset_id=%s",
            dimension_type,
            dimension_query_names,
            dataset_id,
        )

    def _get_dimension_query_name_container(self, dimension_type: DimensionType, dataset_id=None):
        field = dimension_type.value
        if dataset_id is None:
            return getattr(self._metadata.dimensions, field)
        return getattr(self._dataset_metadata[dataset_id].dimensions, field)

    def get_record_ids_by_dimension_type(self, dimension_type):
        return self._record_ids_by_dimension_type[dimension_type]

    def set_record_ids_by_dimension_type(self, dimension_type, record_ids):
        self._record_ids_by_dimension_type[dimension_type] = record_ids

    def iter_record_ids_by_dimension_type(self):
        return self._record_ids_by_dimension_type.items()