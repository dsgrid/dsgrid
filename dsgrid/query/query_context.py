import logging
from pathlib import Path

from pyspark.sql import DataFrame

from dsgrid.dataset.models import (
    TableFormatType,
    PivotedTableFormatModel,
    UnpivotedTableFormatModel,
)
from dsgrid.common import VALUE_COLUMN
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidQuery
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

        if isinstance(self._metadata.table_format, PivotedTableFormatModel):
            self._consolidate_pivoted_metadata()

    def _consolidate_pivoted_metadata(self):
        for dataset_metadata in self._dataset_metadata.values():
            if dataset_metadata.get_table_format_type() != TableFormatType.PIVOTED:
                raise Exception(f"Bug: dataset is not pivoted: {dataset_metadata}")
            if (
                dataset_metadata.table_format.pivoted_dimension_type
                != self.get_pivoted_dimension_type()
            ):
                raise DSGInvalidQuery(
                    "Datasets have different pivoted dimension types: "
                    f"{dataset_metadata.table_format.pivoted_dimension_type=} "
                    f"{self.get_pivoted_dimension_type()=}. "
                    "Please set the output format to 'unpivoted'."
                )

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

    def get_pivoted_columns(self, dataset_id=None) -> set[str]:
        metadata = self._get_metadata(dataset_id)
        if isinstance(metadata.table_format, UnpivotedTableFormatModel):
            return set()
        return self.get_dimension_column_names(
            metadata.table_format.pivoted_dimension_type, dataset_id=dataset_id
        )

    def get_pivoted_dimension_type(self, dataset_id=None):
        metadata = self._get_metadata(dataset_id=dataset_id)
        if isinstance(metadata.table_format, UnpivotedTableFormatModel):
            return None
        return metadata.table_format.pivoted_dimension_type

    def get_table_format_type(self, dataset_id=None) -> TableFormatType:
        val = self._get_metadata(dataset_id).table_format.format_type
        if not isinstance(val, TableFormatType):
            val = TableFormatType(val)
        return val

    def set_table_format_type(self, val: TableFormatType, dataset_id=None):
        if not isinstance(val, TableFormatType):
            val = TableFormatType(val)
        self._get_metadata(dataset_id).table_format.format_type = val

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
        column_type,
        table_format_type,
        project_config,
        pivoted_columns=None,
        pivoted_dimension_type=None,
    ):
        if table_format_type == TableFormatType.PIVOTED and (
            not pivoted_columns or not pivoted_dimension_type
        ):
            raise Exception(
                f"Bug: {table_format_type=} {pivoted_columns=} {pivoted_dimension_type=}"
            )
        match table_format_type:
            case TableFormatType.PIVOTED:
                table_format = PivotedTableFormatModel(
                    pivoted_dimension_type=pivoted_dimension_type
                )
            case TableFormatType.UNPIVOTED:
                table_format = UnpivotedTableFormatModel()
            case _:
                raise NotImplementedError(str(table_format_type))
        self._dataset_metadata[dataset_id] = DatasetMetadataModel(table_format=table_format)
        for dim_type, name in project_config.get_base_dimension_to_query_name_mapping().items():
            is_pivoted_dim_type = (
                table_format_type == TableFormatType.PIVOTED and dim_type == pivoted_dimension_type
            )
            match (table_format_type, column_type, dim_type, is_pivoted_dim_type):
                case (_, ColumnType.DIMENSION_TYPES, DimensionType.TIME, False):
                    # This uses the project dimension because the dataset is being mapped.
                    time_columns = project_config.get_load_data_time_columns(name)
                    column_names = time_columns
                case (
                    TableFormatType.PIVOTED,
                    ColumnType.DIMENSION_QUERY_NAMES | ColumnType.DIMENSION_TYPES,
                    _,
                    True,
                ):
                    column_names = list(pivoted_columns)
                    assert pivoted_columns.issubset(
                        project_config.get_dimension(name).get_unique_ids()
                    )
                case (
                    TableFormatType.PIVOTED | TableFormatType.UNPIVOTED,
                    ColumnType.DIMENSION_QUERY_NAMES,
                    _,
                    False,
                ):
                    column_names = [name]
                case (
                    TableFormatType.PIVOTED | TableFormatType.UNPIVOTED,
                    ColumnType.DIMENSION_TYPES,
                    _,
                    False,
                ):
                    column_names = [dim_type.value]
                case _:
                    raise NotImplementedError(
                        f"Bug: need to support {table_format_type=} {column_type=} {dim_type=} "
                        f"{is_pivoted_dim_type=}"
                    )
            self.add_dimension_metadata(
                dim_type,
                DimensionMetadataModel(dimension_query_name=name, column_names=column_names),
                dataset_id=dataset_id,
            )

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
