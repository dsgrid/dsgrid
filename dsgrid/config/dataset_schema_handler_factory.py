from dsgrid.dataset.dataset_schema_handler_standard import StandardDatasetSchemaHandler
from dsgrid.dataset.dataset_schema_handler_one_table import OneTableDatasetSchemaHandler
from .dataset_config import DataSchemaType


def make_dataset_schema_handler(
    config, dimension_mgr, dimension_mapping_mgr, mapping_references=None, project_time_dim=None
):
    match config.get_data_schema_type():
        case DataSchemaType.STANDARD:
            return StandardDatasetSchemaHandler.load(
                config,
                dimension_mgr,
                dimension_mapping_mgr,
                mapping_references=mapping_references,
                project_time_dim=project_time_dim,
            )
        case DataSchemaType.ONE_TABLE:
            return OneTableDatasetSchemaHandler.load(
                config,
                dimension_mgr,
                dimension_mapping_mgr,
                mapping_references=mapping_references,
                project_time_dim=project_time_dim,
            )
        case _:
            raise NotImplementedError(f"{config.model.data_schema.data_schema_type=}")
