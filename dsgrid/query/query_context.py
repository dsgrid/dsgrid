import logging
from pathlib import Path

from dsgrid.dimension.base_models import DimensionType
from dsgrid.query.models import ColumnType, DimensionMetadataModel
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
            main_metadata = self._metadata.dimensions.get_metadata(dim_type)
            main_metadata.clear()
            keys = set()
            for dataset_metadata in self._dataset_metadata.values():
                for metadata in dataset_metadata.dimensions.get_metadata(dim_type):
                    key = metadata.make_key()
                    if key not in keys:
                        main_metadata.append(metadata)
                        keys.add(key)

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
                        f"{dataset_metadata.pivoted.dimension_type=} "
                        f"{self.get_pivoted_dimension_type()=}"
                    )

    def get_pivoted_columns(self, dataset_id=None):
        return self._get_metadata(dataset_id).pivoted.columns

    def set_pivoted_columns(self, columns, dataset_id=None):
        self._get_metadata(dataset_id).pivoted.columns = columns
        logger.info("Set pivoted columns dataset_id=%s: %s", dataset_id, columns)

    def add_pivoted_columns(self, columns, dataset_id=None):
        self._get_metadata(dataset_id).pivoted.columns.update(columns)

    def get_pivoted_dimension_type(self, dataset_id=None):
        return self._get_metadata(dataset_id).pivoted.dimension_type

    def set_pivoted_dimension_type(self, val, dataset_id=None):
        self._get_metadata(dataset_id).pivoted.dimension_type = val

    def get_table_format_type(self, dataset_id=None):
        return self._get_metadata(dataset_id).table_format_type

    def set_table_format_type(self, val, dataset_id=None):
        self._get_metadata(dataset_id).table_format_type = val

    def get_dimension_column_names(self, dimension_type: DimensionType, dataset_id=None):
        return self._get_metadata(dataset_id).dimensions.get_column_names(dimension_type)

    def get_dimension_query_names(self, dimension_type: DimensionType, dataset_id=None):
        return self._get_metadata(dataset_id).dimensions.get_dimension_query_names(dimension_type)

    def get_all_dimension_query_names(self):
        names = set()
        for dimension_type in DimensionType:
            names.update(self._metadata.dimensions.get_dimension_query_names(dimension_type))
        return names

    def set_dataset_metadata(
        self,
        dataset_id,
        pivoted_columns,
        column_type,
        pivoted_dimension_type,
        table_format_type,
        project_config,
    ):
        self.init_dataset_metadata(dataset_id)
        self.set_pivoted_columns(pivoted_columns, dataset_id=dataset_id)
        self.set_pivoted_dimension_type(pivoted_dimension_type, dataset_id=dataset_id)
        self.set_table_format_type(table_format_type, dataset_id=dataset_id)
        for dim_type, name in project_config.get_base_dimension_to_query_name_mapping().items():
            match (column_type, dim_type):
                case (ColumnType.DIMENSION_QUERY_NAMES, _):
                    column_names = [name]
                case (ColumnType.DIMENSION_TYPES, DimensionType.TIME):
                    # This uses the project dimension because the dataset is being mapped.
                    time_columns = project_config.get_load_data_time_columns(name)
                    column_names = time_columns
                case (ColumnType.DIMENSION_TYPES, _):
                    column_names = [dim_type.value]
                case _:
                    raise NotImplementedError(f"Bug: need to support {column_type=}")
            self.add_dimension_metadata(
                dim_type,
                DimensionMetadataModel(dimension_query_name=name, column_names=column_names),
                dataset_id=dataset_id,
            )

    def init_dataset_metadata(self, dataset_id):
        self._dataset_metadata[dataset_id] = DatasetMetadataModel()

    def serialize_dataset_metadata_to_file(self, dataset_id, filename: Path):
        filename.write_text(self._dataset_metadata[dataset_id].json(indent=2))

    def set_dataset_metadata_from_file(self, dataset_id, filename: Path):
        assert dataset_id not in self._dataset_metadata, dataset_id
        self._dataset_metadata[dataset_id] = DatasetMetadataModel.from_file(filename)

    def add_dimension_metadata(
        self,
        dimension_type: DimensionType,
        dimension_metadata: DimensionMetadataModel,
        dataset_id=None,
    ):
        self._get_metadata(dataset_id).dimensions.add_metadata(dimension_type, dimension_metadata)
        logger.debug(
            "Added dimension query name for %s: %s dataset_id=%s",
            dimension_type,
            dimension_metadata,
            dataset_id,
        )

    def get_dimension_column_names_by_query_name(
        self,
        dimension_type: DimensionType,
        query_name: str,
        dataset_id=None,
    ) -> str:
        """Return the load data column name for the dimension."""
        for metadata in self.get_dimension_metadata(dimension_type, dataset_id=dataset_id):
            if metadata.dimension_query_name == query_name:
                return metadata.column_names
        raise Exception(f"No base dimension match: {dimension_type=} {query_name=}")

    def get_dimension_metadata(
        self,
        dimension_type: DimensionType,
        dataset_id=None,
    ):
        return self._get_metadata(dataset_id).dimensions.get_metadata(dimension_type)

    def replace_dimension_metadata(
        self, dimension_type: DimensionType, dimension_metadata, dataset_id=None
    ):
        self._get_metadata(dataset_id).dimensions.replace_metadata(
            dimension_type, dimension_metadata
        )
        logger.info(
            "Replaced dimension for %s: %s dataset_id=%s",
            dimension_type,
            dimension_metadata,
            dataset_id,
        )

    def _get_metadata(self, dataset_id):
        return self._metadata if dataset_id is None else self._dataset_metadata[dataset_id]

    def get_record_ids(self):
        spark = get_spark_session()
        return {k: spark.createDataFrame(v) for k, v in self._record_ids_by_dimension_type.items()}

    def try_get_record_ids_by_dimension_type(self, dimension_type):
        records = self._record_ids_by_dimension_type.get(dimension_type)
        if records is None:
            return records

        spark = get_spark_session()
        return spark.createDataFrame(records)

    def set_record_ids_by_dimension_type(self, dimension_type, record_ids):
        # Can't keep the dataframes in memory because of spark restarts.
        self._record_ids_by_dimension_type[dimension_type] = record_ids.collect()
