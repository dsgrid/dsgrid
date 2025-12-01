from sqlalchemy import Connection

from dsgrid.config.dataset_config import DataSchemaType, DatasetConfig
from dsgrid.dataset.dataset_schema_handler_standard import StandardDatasetSchemaHandler
from dsgrid.dataset.dataset_schema_handler_one_table import OneTableDatasetSchemaHandler
from dsgrid.registry.data_store_interface import DataStoreInterface
from dsgrid.registry.dimension_registry_manager import DimensionRegistryManager
from dsgrid.registry.dimension_mapping_registry_manager import DimensionMappingRegistryManager
from dsgrid.config.dimension_mapping_base import DimensionMappingReferenceModel


def make_dataset_schema_handler(
    conn: Connection | None,
    config: DatasetConfig,
    dimension_mgr: DimensionRegistryManager,
    dimension_mapping_mgr: DimensionMappingRegistryManager,
    store: DataStoreInterface | None = None,
    mapping_references: list[DimensionMappingReferenceModel] | None = None,
):
    match config.get_data_schema_type():
        case DataSchemaType.STANDARD:
            return StandardDatasetSchemaHandler.load(
                config,
                conn,
                dimension_mgr,
                dimension_mapping_mgr,
                store=store,
                mapping_references=mapping_references,
            )
        case DataSchemaType.ONE_TABLE:
            return OneTableDatasetSchemaHandler.load(
                config,
                conn,
                dimension_mgr,
                dimension_mapping_mgr,
                store=store,
                mapping_references=mapping_references,
            )
        case _:
            msg = f"{config.model.table_schema.data_schema.data_schema_type=}"
            raise NotImplementedError(msg)
