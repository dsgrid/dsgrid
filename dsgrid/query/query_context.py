import logging
from pathlib import Path

from pyspark.sql import DataFrame

from dsgrid.dataset.models import (
    TableFormatType,
    UnpivotedTableFormatModel,
)
from dsgrid.common import VALUE_COLUMN
from dsgrid.dataset.models import PivotedTableFormatModel
from dsgrid.dimension.base_models import DimensionType
from dsgrid.config.project_config import ProjectConfig
from dsgrid.utils.spark import get_spark_session
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from .models import ColumnType, DatasetMetadataModel, DimensionMetadataModel, ProjectQueryModel


logger = logging.getLogger(__name__)


class QueryContext:
    """Maintains context of the query as it is processed through the stack."""

    def __init__(self, model: ProjectQueryModel, scratch_dir_context: ScratchDirContext):
        self._model = model
        self._record_ids_by_dimension_type: dict[DimensionType, DataFrame] = {}
        self._metadata = DatasetMetadataModel(table_format=self.model.result.table_format)
        self._dataset_metadata: dict[str, DatasetMetadataModel] = {}
        self._scratch_dir_context = scratch_dir_context

    @property
    def metadata(self):
        return self._metadata

    @metadata.setter
    def metadata(self, val):
        self._metadata = val

    @property
    def model(self):
        return self._model

    @property
    def scratch_dir_context(self) -> ScratchDirContext:
        """Return the context for managing scratch directories."""
        return self._scratch_dir_context

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

    def get_value_columns(self) -> set[str]:
        """Return the value columns in the final dataset."""
        match self.get_table_format_type():
            case TableFormatType.PIVOTED:
                return self.get_pivoted_columns()
            case TableFormatType.UNPIVOTED:
                return {VALUE_COLUMN}
            case _:
                msg = str(self.get_table_format_type())
                raise NotImplementedError(msg)

    def get_pivoted_columns(self) -> set[str]:
        if self.get_table_format_type() != TableFormatType.PIVOTED:
            raise Exception("Bug: get_pivoted_columns is only supported on a pivoted table")
        metadata = self._get_metadata()
        return self.get_dimension_column_names(metadata.table_format.pivoted_dimension_type)

    def get_pivoted_dimension_type(self):
        if self.get_table_format_type() != TableFormatType.PIVOTED:
            msg = "Bug: get_pivoted_dimension_type is only supported on a pivoted table"
            raise Exception(msg)
        metadata = self._get_metadata()
        return metadata.table_format.pivoted_dimension_type

    def get_table_format_type(self, dataset_id=None) -> TableFormatType:
        val = self._get_metadata(dataset_id).table_format.format_type
        if not isinstance(val, TableFormatType):
            val = TableFormatType(val)
        return val

    def set_table_format_type(self, val: TableFormatType):
        if not isinstance(val, TableFormatType):
            val = TableFormatType(val)
        self._metadata.table_format.format_type = val

    def get_dimension_column_names(
        self, dimension_type: DimensionType, dataset_id=None
    ) -> set[str]:
        return self._get_metadata(dataset_id).dimensions.get_column_names(dimension_type)

    def get_dimension_query_names(
        self, dimension_type: DimensionType, dataset_id=None
    ) -> set[str]:
        return self._get_metadata(dataset_id).dimensions.get_dimension_query_names(dimension_type)

    def get_all_dimension_query_names(self):
        names = set()
        for dimension_type in DimensionType:
            names.update(self._metadata.dimensions.get_dimension_query_names(dimension_type))
        return names

    def set_dataset_metadata(
        self,
        dataset_id: str,
        column_type: ColumnType,
        project_config: ProjectConfig,
    ):
        table_format = UnpivotedTableFormatModel()
        self._dataset_metadata[dataset_id] = DatasetMetadataModel(table_format=table_format)
        for dim_type, name in project_config.get_base_dimension_to_query_name_mapping().items():
            match (column_type, dim_type):
                case (ColumnType.DIMENSION_TYPES, DimensionType.TIME):
                    # This uses the project dimension because the dataset is being mapped.
                    time_columns = project_config.get_load_data_time_columns(name)
                    column_names = time_columns
                case (ColumnType.DIMENSION_QUERY_NAMES, _):
                    column_names = [name]
                case (ColumnType.DIMENSION_TYPES, _):
                    column_names = [dim_type.value]
                case _:
                    msg = f"Bug: need to support {column_type=} {dim_type=}"
                    raise NotImplementedError(msg)
            self.add_dimension_metadata(
                dim_type,
                DimensionMetadataModel(dimension_query_name=name, column_names=column_names),
                dataset_id=dataset_id,
            )

    def convert_to_pivoted(self) -> str:
        assert isinstance(self.model.result.table_format, PivotedTableFormatModel)
        pivoted_dimension_type = self.model.result.table_format.pivoted_dimension_type
        self.set_table_format_type(TableFormatType.PIVOTED)
        columns = self.get_dimension_column_names(pivoted_dimension_type)
        query_names = self.get_dimension_query_names(pivoted_dimension_type)
        if len(columns) != 1 or len(query_names) != 1:
            # This is checked in the query model and so this should never happen.
            msg = (
                "Bug: The pivoted dimension can only have 1 column and 1 query name: "
                f"{columns=} {query_names=}"
            )
            raise Exception(msg)
        return next(iter(columns))

    def serialize_dataset_metadata_to_file(self, dataset_id, filename: Path):
        filename.write_text(self._dataset_metadata[dataset_id].model_dump_json(indent=2))

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
        raise Exception(f"No dimension match: {dimension_type=} {query_name=}")

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

    def _get_metadata(self, dataset_id=None):
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
