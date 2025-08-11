import logging
from contextlib import contextmanager
from pathlib import Path
from typing import Generator

from dsgrid.dataset.models import (
    TableFormatType,
    UnpivotedTableFormatModel,
)
from dsgrid.common import VALUE_COLUMN
from dsgrid.dataset.models import PivotedTableFormatModel
from dsgrid.dimension.base_models import DimensionType
from dsgrid.config.project_config import DatasetBaseDimensionNamesModel
from dsgrid.dataset.dataset_mapping_manager import DatasetMappingManager
from dsgrid.query.dataset_mapping_plan import DatasetMappingPlan, MapOperationCheckpoint
from dsgrid.spark.functions import drop_temp_tables_and_views
from dsgrid.spark.types import DataFrame
from dsgrid.utils.spark import get_spark_session
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from .models import ColumnType, DatasetMetadataModel, DimensionMetadataModel, QueryBaseModel


logger = logging.getLogger(__name__)


class QueryContext:
    """Maintains context of the query as it is processed through the stack."""

    def __init__(
        self,
        model: QueryBaseModel,
        base_dimension_names: DatasetBaseDimensionNamesModel,
        scratch_dir_context: ScratchDirContext,
        checkpoint: MapOperationCheckpoint | None = None,
    ) -> None:
        self._model = model
        self._record_ids_by_dimension_type: dict[DimensionType, list[tuple[str]]] = {}
        self._metadata = DatasetMetadataModel(
            table_format=self.model.result.table_format,
            base_dimension_names=base_dimension_names,
        )
        self._dataset_metadata: dict[str, DatasetMetadataModel] = {}
        self._scratch_dir_context = scratch_dir_context
        self._checkpoint = checkpoint

    @property
    def metadata(self) -> DatasetMetadataModel:
        return self._metadata

    @metadata.setter
    def metadata(self, val: DatasetMetadataModel) -> None:
        self._metadata = val

    @property
    def model(self) -> QueryBaseModel:
        return self._model

    @property
    def base_dimension_names(self) -> DatasetBaseDimensionNamesModel:
        return self._metadata.base_dimension_names

    @property
    def scratch_dir_context(self) -> ScratchDirContext:
        """Return the context for managing scratch directories."""
        return self._scratch_dir_context

    def consolidate_dataset_metadata(self) -> None:
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

    def finalize(self) -> None:
        """Perform cleanup."""
        drop_temp_tables_and_views()

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
            msg = "Bug: get_pivoted_columns is only supported on a pivoted table"
            raise Exception(msg)
        metadata = self._get_metadata()
        assert isinstance(metadata.table_format, PivotedTableFormatModel)
        return self.get_dimension_column_names(metadata.table_format.pivoted_dimension_type)

    def get_pivoted_dimension_type(self) -> DimensionType:
        if self.get_table_format_type() != TableFormatType.PIVOTED:
            msg = "Bug: get_pivoted_dimension_type is only supported on a pivoted table"
            raise Exception(msg)
        metadata = self._get_metadata()
        assert isinstance(metadata.table_format, PivotedTableFormatModel)
        return metadata.table_format.pivoted_dimension_type

    def get_table_format_type(self, dataset_id: str | None = None) -> TableFormatType:
        val = self._get_metadata(dataset_id).table_format.format_type
        if not isinstance(val, TableFormatType):
            val = TableFormatType(val)
        return val

    def set_table_format_type(self, val: TableFormatType) -> None:
        if not isinstance(val, TableFormatType):
            val = TableFormatType(val)
        self._metadata.table_format.format_type = val

    def get_dimension_column_names(
        self, dimension_type: DimensionType, dataset_id: str | None = None
    ) -> set[str]:
        """Return the load data column names for the dimension."""
        return self._get_metadata(dataset_id).dimensions.get_column_names(dimension_type)

    def get_all_dimension_column_names(
        self, dataset_id: str | None = None, exclude: set[DimensionType] | None = None
    ) -> set[str]:
        names = set()
        for dimension_type in DimensionType:
            if exclude is not None and dimension_type in exclude:
                continue
            names.update(self.get_dimension_column_names(dimension_type, dataset_id=dataset_id))
        return names

    def get_dimension_names(
        self, dimension_type: DimensionType, dataset_id: str | None = None
    ) -> set[str]:
        return self._get_metadata(dataset_id).dimensions.get_dimension_names(dimension_type)

    def get_all_dimension_names(
        self, dataset_id: str | None = None, exclude: set[DimensionType] | None = None
    ) -> set[str]:
        names = set()
        for dimension_type in DimensionType:
            if exclude is not None and dimension_type in exclude:
                continue
            names.update(self.get_dimension_names(dimension_type, dataset_id=dataset_id))
        return names

    def set_dataset_metadata(
        self,
        dataset_id: str,
        column_type: ColumnType,
        mapped_time_columns: list[str],
    ) -> None:
        table_format = UnpivotedTableFormatModel()
        self._dataset_metadata[dataset_id] = DatasetMetadataModel(table_format=table_format)
        base_dimension_names = self.base_dimension_names
        for dim_type in DimensionType:
            name = getattr(base_dimension_names, dim_type.value)
            assert name is not None
            match (column_type, dim_type):
                case (ColumnType.DIMENSION_TYPES, DimensionType.TIME):
                    column_names = mapped_time_columns
                case (ColumnType.DIMENSION_NAMES, _):
                    column_names = [name]
                case (ColumnType.DIMENSION_TYPES, _):
                    column_names = [dim_type.value]
                case _:
                    msg = f"Bug: need to support {column_type=} {dim_type=}"
                    raise NotImplementedError(msg)
            self.add_dimension_metadata(
                dim_type,
                DimensionMetadataModel(dimension_name=name, column_names=column_names),
                dataset_id=dataset_id,
            )

    def convert_to_pivoted(self) -> str:
        assert isinstance(self.model.result.table_format, PivotedTableFormatModel)
        pivoted_dimension_type = self.model.result.table_format.pivoted_dimension_type
        self.set_table_format_type(TableFormatType.PIVOTED)
        columns = self.get_dimension_column_names(pivoted_dimension_type)
        names = self.get_dimension_names(pivoted_dimension_type)
        if len(columns) != 1 or len(names) != 1:
            # This is checked in the query model and so this should never happen.
            msg = (
                "Bug: The pivoted dimension can only have 1 column and 1 name: "
                f"{columns=} {names=}"
            )
            raise Exception(msg)
        return next(iter(columns))

    def serialize_dataset_metadata_to_file(self, dataset_id: str, filename: Path) -> None:
        filename.write_text(self._dataset_metadata[dataset_id].model_dump_json(indent=2))

    def set_dataset_metadata_from_file(self, dataset_id: str, filename: Path) -> None:
        assert dataset_id not in self._dataset_metadata, dataset_id
        self._dataset_metadata[dataset_id] = DatasetMetadataModel.from_file(filename)

    def add_dimension_metadata(
        self,
        dimension_type: DimensionType,
        dimension_metadata: DimensionMetadataModel,
        dataset_id=None,
    ) -> None:
        self._get_metadata(dataset_id).dimensions.add_metadata(dimension_type, dimension_metadata)
        logger.debug(
            "Added dimension name for %s: %s dataset_id=%s",
            dimension_type,
            dimension_metadata,
            dataset_id,
        )

    def get_dimension_column_names_by_name(
        self,
        dimension_type: DimensionType,
        name: str,
        dataset_id: str | None = None,
    ) -> list[str]:
        """Return the load data column names for the dimension."""
        for metadata in self.get_dimension_metadata(dimension_type, dataset_id=dataset_id):
            if metadata.dimension_name == name:
                return metadata.column_names
        msg = f"No dimension match: {dimension_type=} {name=}"
        raise Exception(msg)

    def get_dimension_metadata(
        self,
        dimension_type: DimensionType,
        dataset_id: str | None = None,
    ) -> list[DimensionMetadataModel]:
        return self._get_metadata(dataset_id).dimensions.get_metadata(dimension_type)

    def replace_dimension_metadata(
        self,
        dimension_type: DimensionType,
        dimension_metadata: list[DimensionMetadataModel],
        dataset_id: str | None = None,
    ) -> None:
        self._get_metadata(dataset_id).dimensions.replace_metadata(
            dimension_type, dimension_metadata
        )
        logger.debug(
            "Replaced dimension for %s: %s dataset_id=%s",
            dimension_type,
            dimension_metadata,
            dataset_id,
        )

    def _get_metadata(self, dataset_id: str | None = None) -> DatasetMetadataModel:
        return self._metadata if dataset_id is None else self._dataset_metadata[dataset_id]

    def get_record_ids(self) -> dict[DimensionType, DataFrame]:
        spark = get_spark_session()
        return {
            k: spark.createDataFrame(v, ["id"])
            for k, v in self._record_ids_by_dimension_type.items()
        }

    def try_get_record_ids_by_dimension_type(self, dim_type: DimensionType) -> DataFrame | None:
        records = self._record_ids_by_dimension_type.get(dim_type)
        if records is None:
            return records

        spark = get_spark_session()
        return spark.createDataFrame(records, [dim_type.value])

    def set_record_ids_by_dimension_type(
        self, dim_type: DimensionType, record_ids: DataFrame
    ) -> None:
        # Can't keep the dataframes in memory because of spark restarts.
        self._record_ids_by_dimension_type[dim_type] = [(x.id,) for x in record_ids.collect()]

    @contextmanager
    def dataset_mapping_manager(
        self, dataset_id: str, plan: DatasetMappingPlan
    ) -> Generator[DatasetMappingManager, None, None]:
        """Start a mapping manager for a dataset."""
        checkpoint = (
            self._checkpoint
            if self._checkpoint is not None and self._checkpoint.dataset_id == dataset_id
            else None
        )
        with DatasetMappingManager(dataset_id, plan, self._scratch_dir_context, checkpoint) as mgr:
            yield mgr
