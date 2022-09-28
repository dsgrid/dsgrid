from dsgrid.dataset.dataset_schema_handler_standard import StandardDatasetSchemaHandler
from dsgrid.dataset.dataset_schema_handler_one_table import OneTableDatasetSchemaHandler
from .dataset_config import DataSchemaType


def make_dataset_schema_handler(
    config, dimension_mgr, dimension_mapping_mgr, mapping_references=None, project_time_dim=None
):
    if config.model.data_schema_type == DataSchemaType.STANDARD:
        return StandardDatasetSchemaHandler.load(
            config,
            dimension_mgr,
            dimension_mapping_mgr,
            mapping_references=mapping_references,
            project_time_dim=project_time_dim,
        )
    elif config.model.data_schema_type == DataSchemaType.ONE_TABLE:
        return OneTableDatasetSchemaHandler.load(
            config,
            dimension_mgr,
            dimension_mapping_mgr,
            mapping_references=mapping_references,
            project_time_dim=project_time_dim,
        )
    else:
        assert False, config.model.data_schema_type
