from sqlalchemy import Connection

from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.dataset.models import TableFormat
from dsgrid.dataset.dataset_schema_handler_two_table import TwoTableDatasetSchemaHandler
from dsgrid.dataset.dataset_schema_handler_one_table import OneTableDatasetSchemaHandler
from dsgrid.registry.data_store_interface import DataStoreInterface
from dsgrid.registry.dimension_registry_manager import DimensionRegistryManager
from dsgrid.registry.dimension_mapping_registry_manager import DimensionMappingRegistryManager
from dsgrid.config.dimension_mapping_base import DimensionMappingReferenceModel
from dsgrid.utils.scratch_dir_context import ScratchDirContext


def make_dataset_schema_handler(
    conn: Connection | None,
    config: DatasetConfig,
    dimension_mgr: DimensionRegistryManager,
    dimension_mapping_mgr: DimensionMappingRegistryManager,
    store: DataStoreInterface | None = None,
    mapping_references: list[DimensionMappingReferenceModel] | None = None,
    scratch_dir_context: ScratchDirContext | None = None,
):
    match config.get_table_format():
        case TableFormat.TWO_TABLE:
            return TwoTableDatasetSchemaHandler.load(
                config,
                conn,
                dimension_mgr,
                dimension_mapping_mgr,
                store=store,
                mapping_references=mapping_references,
                scratch_dir_context=scratch_dir_context,
            )
        case TableFormat.ONE_TABLE:
            return OneTableDatasetSchemaHandler.load(
                config,
                conn,
                dimension_mgr,
                dimension_mapping_mgr,
                store=store,
                mapping_references=mapping_references,
                scratch_dir_context=scratch_dir_context,
            )
        case _:
            msg = f"Unsupported table format: {config.get_table_format()}"
            raise NotImplementedError(msg)
