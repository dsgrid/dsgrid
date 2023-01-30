import logging
from pathlib import Path

from dsgrid.dimension.base_models import DimensionType
from dsgrid.utils.spark import get_spark_session
from .models import ProjectQueryModel, DatasetMetadataModel, TableFormatType


logger = logging.getLogger(__name__)


class QueryContext:
    """Maintains context of the query as it is processed through the stack."""

    def __init__(self, model: ProjectQueryModel):
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
                    # TODO #202: Remove this check when we support transforming to long format.
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

    def get_dimension_query_names(self, dimension_type: DimensionType, dataset_id=None):
        return self._get_dimension_query_name_container(dimension_type, dataset_id=dataset_id)

    def get_all_dimension_query_names(self):
        names = set()
        for dimension_type in DimensionType:
            names.update(getattr(self._metadata.dimensions, dimension_type.value))
        return names

    def set_dataset_metadata(
        self,
        dataset_id,
        pivoted_columns,
        pivoted_dimension_type,
        table_format_type,
        project_config,
    ):
        self.init_dataset_metadata(dataset_id)
        self.set_pivoted_columns(pivoted_columns, dataset_id=dataset_id)
        self.set_pivoted_dimension_type(pivoted_dimension_type, dataset_id=dataset_id)
        self.set_table_format_type(table_format_type, dataset_id=dataset_id)
        for dim_type, name in project_config.get_base_dimension_to_query_name_mapping().items():
            self.add_dimension_query_name(dim_type, name, dataset_id=dataset_id)

    def get_dataset_metadata(self, dataset_id):
        return self._dataset_metadata[dataset_id]

    def init_dataset_metadata(self, dataset_id):
        self._dataset_metadata[dataset_id] = DatasetMetadataModel()

    def serialize_dataset_metadata_to_file(self, dataset_id, filename: Path):
        filename.write_text(self._dataset_metadata[dataset_id].json(indent=2))

    def set_dataset_metadata_from_file(self, dataset_id, filename: Path):
        assert dataset_id not in self._dataset_metadata, dataset_id
        self._dataset_metadata[dataset_id] = DatasetMetadataModel.from_file(filename)

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

    def get_record_ids(self):
        spark = get_spark_session()
        return {k: spark.createDataFrame(v) for k, v in self._record_ids_by_dimension_type.items()}

    def set_record_ids_by_dimension_type(self, dimension_type, record_ids):
        # Can't keep the dataframes in memory because of spark restarts.
        self._record_ids_by_dimension_type[dimension_type] = record_ids.collect()
