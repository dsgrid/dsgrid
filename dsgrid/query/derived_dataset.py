import logging
from pathlib import Path

import json5

from dsgrid.config.dataset_config import (
    DataClassificationType,
    DataSchemaType,
    InputDatasetType,
    ColumnType,
)
from dsgrid.data_models import serialize_model
from dsgrid.utils.spark import read_dataframe, get_unique_values
from dsgrid.dimension.base_models import DimensionType
from dsgrid.query.models import ProjectQueryModel, DatasetMetadataModel
from dsgrid.query.query_submitter import QuerySubmitterBase
from dsgrid.registry.dataset_registry import DatasetRegistry
from dsgrid.registry.registry_manager import RegistryManager


logger = logging.getLogger(__name__)


def create_derived_dataset_confg_from_query(
    query_path: Path, dst_path: Path, registry_manager: RegistryManager
):
    """Create a DatasetConfigModel and dimensions from a query result.

    Parameters
    ----------
    query_path : Path
        Output directory from a query.
    dst_path : Path
        Directory in which to create the dataset config files.
    registry_manager : RegistryManager

    Returns
    -------
    bool
        Returns True if the operation is successful.
    """
    metadata_file = QuerySubmitterBase.metadata_filename(query_path)
    query_file = QuerySubmitterBase.query_filename(query_path)
    table_file = QuerySubmitterBase.table_filename(query_path)
    if not metadata_file.exists() or not query_file.exists():
        logger.error("%s is not a valid query result directory", query_path)
        return False

    query = ProjectQueryModel.from_file(query_file)
    if not does_query_support_a_derived_dataset(query):
        return False

    metadata = DatasetMetadataModel.from_file(metadata_file)
    project = registry_manager.project_manager.load_project(
        query.project.project_id, version=query.project.version
    )
    # TODO: should there be a warning if the current project version is later?
    dimensions = []
    dimension_references = []
    for dim_type in DimensionType:
        dims = getattr(metadata.dimensions, dim_type.value)
        assert len(dims) == 1, dims
        dim = project.config.get_dimension(next(iter(dims)))
        df = read_dataframe(table_file)
        if metadata.pivoted.dimension_type == dim_type:
            is_valid = bool(metadata.pivoted.columns.difference(df.columns))
            table_records = metadata.pivoted.columns
        elif dim_type == DimensionType.TIME:
            is_valid = True  # TODO: how can time be different? This was already mapped to project
            table_records = None
        else:
            is_valid = is_dimension_valid_for_dataset(dim, df)
            table_records = get_unique_values(df, dim.model.dimension_query_name)
        if is_valid:
            dim_ref = project.config.get_dimension_reference(dim.model.dimension_id)
            dimension_references.append(serialize_model(dim_ref))
        else:
            records = dim.get_records_dataframe()
            records = records.filter(records.id.isin(table_records))
            # TODO: AWS #186 - not an issue if registry is in a database instead of files
            filename = dst_path / f"{dim.model.dimension_query_name}_records.csv"
            # Use pandas because spark creates a directory.
            records.toPandas().to_csv(filename, index=False)
            # Use dictionaries to avoid validation.
            new_dim = {
                "type": dim.model.dimension_type.value,
                "name": dim.model.name,
                "display_name": dim.model.display_name,
                "module": dim.model.module,
                "class_name": dim.model.class_name,
                "description": dim.model.description,
                "filename": filename.name,
            }
            dimensions.append(new_dim)

    config = {
        "dataset_id": query.project.dataset.dataset_id,
        "dataset_type": InputDatasetType.MODELED.value,
        "data_source": "",
        "data_schema_type": DataSchemaType.ONE_TABLE.value,
        "data_schema": {
            "load_data_column_dimension": metadata.pivoted.dimension_type.value,
            "column_type": ColumnType.DIMENSION_QUERY_NAMES.value,
        },
        "dataset_version": "1.0.0",
        "description": "",
        "origin_creator": "",
        "origin_organization": "",
        "origin_date": "",
        "origin_project": "",
        "origin_version": "",
        "source": "",
        "data_classification": DataClassificationType.MODERATE.value,  # TODO
        "use_project_geography_time_zone": True,
        "dimensions": dimensions,
        "dimension_references": dimension_references,
    }
    config_file = dst_path / DatasetRegistry.config_filename()
    config_file.write_text(json5.dumps(config, indent=2))
    logger.info(
        "Created %s with default information. "
        "Re-used %s project dimensions and generated %s new dimensions. "
        "Examine %s, fill out the remaining fields, and review the generated dimension values "
        "before submitting the dataset.",
        dst_path,
        len(dimension_references),
        len(dimensions),
        config_file,
    )
    return True


def does_query_support_a_derived_dataset(query: ProjectQueryModel):
    """Return True if a derived dataset can be created from a query.

    Returns
    -------
    bool
    """
    is_valid = True
    if query.project.dataset.params.dimension_filters:
        is_valid = False
        logger.error(
            "Cannot create a derived dataset from a query with filtered dataset dimensions"
        )
    if query.result.supplemental_columns:
        is_valid = False
        logger.error("Cannot create a derived dataset from a query with supplemental_columns")
    if query.result.replace_ids_with_names:
        is_valid = False
        logger.error("Cannot create a derived dataset from a query with replace_ids_with_names")
    if query.result.dimension_filters:
        is_valid = False
        logger.error(
            "Cannot create a derived dataset from a query with filtered result dimensions"
        )

    return is_valid


def is_dimension_valid_for_dataset(dim_config, df):
    """Return True if the dimension records are symmetric with the load data table.

    Parameters
    ----------
    dim_config: DimensionConfig
    df : pyspark.sql.DataFrame
        Load data DataFrame

    Returns
    -------
    bool
    """
    records = dim_config.get_records_dataframe()
    dim_values = get_unique_values(records, "id")
    table_values = get_unique_values(df, dim_config.model.dimension_query_name)
    diff = dim_values.symmetric_difference(table_values)
    return not bool(diff)
