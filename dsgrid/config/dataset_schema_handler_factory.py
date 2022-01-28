from .dataset_config import DataSchemaType
from .dataset_schema_handler_standard import StandardDatasetSchemaHandler
from .dataset_schema_handler_one_table import OneTableDatasetSchemaHandler


def make_dataset_schema_handler(config, dimension_mgr, dimension_mapping_mgr):
    if config.model.data_schema_type == DataSchemaType.STANDARD:
        return StandardDatasetSchemaHandler.load(config, dimension_mgr, dimension_mapping_mgr)
    elif config.model.data_schema_type == DataSchemaType.ONE_TABLE:
        return OneTableDatasetSchemaHandler.load(config, dimension_mgr, dimension_mapping_mgr)
    else:
        assert False, config.model.data_schema_type
