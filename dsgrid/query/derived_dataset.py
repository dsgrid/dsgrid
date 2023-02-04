from dsgrid.config.dimensions import DimensionModel
import logging
from pathlib import Path

import json5

from dsgrid.config.dataset_config import (
    DataClassificationType,
    DataSchemaType,
    InputDatasetType,
)
from dsgrid.data_models import serialize_model
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.query.models import ProjectQueryModel, DatasetMetadataModel, ColumnType
from dsgrid.query.query_submitter import QuerySubmitterBase
from dsgrid.registry.dataset_registry import DatasetRegistry
from dsgrid.registry.dimension_registry import DimensionRegistry
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.utils.files import dump_data
from dsgrid.utils.spark import read_dataframe, get_unique_values


logger = logging.getLogger(__name__)


def create_derived_dataset_config_from_query(
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
    if not metadata_file.exists() or not query_file.exists() or not table_file.exists():
        logger.error("%s is not a valid query result directory", query_path)
        return False

    query = ProjectQueryModel.from_file(query_file)
    if not does_query_support_a_derived_dataset(query):
        return False

    metadata = DatasetMetadataModel.from_file(metadata_file)
    project = registry_manager.project_manager.load_project(
        query.project.project_id, version=query.project.version
    )
    new_supplemental_dims_path = dst_path / "new_supplemental_dimensions"
    df = read_dataframe(table_file)
    # TODO: should there be a warning if the current project version is later?
    dimensions = []
    dimension_references = []
    dimension_mapping_references = []
    base_dim_query_names = project.config.get_base_dimension_query_names()
    for dim_type in DimensionType:
        dimension_query_names = getattr(metadata.dimensions, dim_type.value)
        assert len(dimension_query_names) == 1, dimension_query_names
        dim_query_name = next(iter(dimension_query_names))
        dim = project.config.get_dimension(dim_query_name)
        if dim_type == DimensionType.TIME:
            is_valid = _does_time_dimension_match(dim, df)
            if not is_valid:
                logger.warning(
                    "The dataset does not match the project's time dimension. "
                    "If this is expected, add a new time dimension to the dataset config file "
                    "and create an appropriate dimension mapping."
                )
                continue
            unique_data_records = None
        else:
            if metadata.pivoted.dimension_type == dim_type:
                unique_data_records = metadata.pivoted.columns
            else:
                unique_data_records = _get_unique_data_records(
                    df, dim.model, query.result.column_type
                )
            is_valid = _is_dimension_valid_for_dataset(dim, unique_data_records)
        if is_valid:
            dimension_references.append(_get_dimension_reference(dim.model, project.config))
            if dim.model.dimension_query_name not in base_dim_query_names:
                dimension_mapping_references.append(
                    _get_dimension_mapping_reference(dim.model, project.config)
                )
        else:
            supp_dim = _get_matching_supplemental_dimension(
                project.config, dim_type, unique_data_records
            )
            if supp_dim is None:
                assert dim_query_name in base_dim_query_names, dim_query_name
                _make_new_supplemental_dimension(
                    dim, unique_data_records, new_supplemental_dims_path
                )
            else:
                dimension_references.append(_get_dimension_reference(supp_dim, project.config))
                dimension_mapping_references.append(
                    _get_dimension_mapping_reference(supp_dim, project.config)
                )

    if dimension_mapping_references:
        _make_dimension_mapping_references_file(dimension_mapping_references, dst_path)

    _make_dataset_config(
        query.project.dataset.dataset_id,
        metadata.pivoted.dimension_type.value,
        dimensions,
        dimension_references,
        dst_path,
    )
    return True


def does_query_support_a_derived_dataset(query: ProjectQueryModel):
    """Return True if a derived dataset can be created from a query.

    Returns
    -------
    bool
    """
    is_valid = True
    if query.result.column_type != ColumnType.DIMENSION_TYPES:
        is_valid = False
        logger.error(
            "Cannot create a derived dataset from a query with column_type = %s. It must be %s.",
            query.result.column_type.value,
            ColumnType.DIMENSION_TYPES.value,
        )
    if query.result.supplemental_columns:
        is_valid = False
        logger.error("Cannot create a derived dataset from a query with supplemental_columns")
    if query.result.replace_ids_with_names:
        is_valid = False
        logger.error("Cannot create a derived dataset from a query with replace_ids_with_names")

    return is_valid


def _does_time_dimension_match(dim_config, df):
    try:
        dim_config.check_dataset_time_consistency(df, dim_config.get_timestamp_load_data_columns())
    except DSGInvalidDataset:
        return False
    return True


def _is_dimension_valid_for_dataset(dim_config, unique_data_records):
    records = dim_config.get_records_dataframe()
    dim_values = get_unique_values(records, "id")
    diff = dim_values.symmetric_difference(unique_data_records)
    return not bool(diff)


def _get_matching_supplemental_dimension(project_config, dimension_type, unique_data_records):
    for dim_config in project_config.list_supplemental_dimensions(dimension_type):
        if _is_dimension_valid_for_dataset(dim_config, unique_data_records):
            return dim_config.model

    return None


def _make_dataset_config(
    dataset_id,
    pivoted_dim_type,
    dimensions,
    dimension_references,
    path: Path,
    data_classification=DataClassificationType.MODERATE.value,
):
    # Use dictionaries to avoid validation.
    config = {
        "dataset_id": dataset_id,
        "dataset_type": InputDatasetType.MODELED.value,
        "data_source": "",
        "data_schema_type": DataSchemaType.ONE_TABLE.value,
        "data_schema": {
            "load_data_column_dimension": pivoted_dim_type,
        },
        "dataset_version": "1.0.0",
        "description": "",
        "origin_creator": "",
        "origin_organization": "",
        "origin_date": "",
        "origin_project": "",
        "origin_version": "",
        "source": "",
        "data_classification": data_classification,
        "use_project_geography_time_zone": True,
        "dimensions": dimensions,
        "dimension_references": dimension_references,
    }
    config_file = path / DatasetRegistry.config_filename()
    config_file.write_text(json5.dumps(config, indent=2))
    logger.info(
        "Created %s with default information. "
        "Re-used %s project dimensions and generated %s new dimensions. "
        "Examine %s, fill out the remaining fields, and review the generated dimension values "
        "before registering and submitting the dataset to the project.",
        path,
        len(dimension_references),
        len(dimensions),
        config_file,
    )


def _make_new_supplemental_dimension(orig_dim_config, unique_data_records, path: Path):
    # This assumes that the new supplemental dimension is a subset of project's base dimension.
    new_dim_path = path / orig_dim_config.model.dimension_type.value
    new_dim_path.mkdir(parents=True)
    orig_records = orig_dim_config.get_records_dataframe()
    records = orig_records.filter(orig_records.id.isin(unique_data_records))
    # TODO: AWS #186 - not an issue if registry is in a database instead of files
    filename = new_dim_path / "records.csv"
    # Use pandas because spark creates a directory.
    records.toPandas().to_csv(filename, index=False)
    # Use dictionaries to avoid validation.
    new_dim = {
        "type": orig_dim_config.model.dimension_type.value,
        "name": "",
        "display_name": "",
        "module": orig_dim_config.model.module,
        "class_name": orig_dim_config.model.class_name,
        "description": "",
        "filename": filename.name,
    }
    dump_data(new_dim, new_dim_path / DimensionRegistry.config_filename(), indent=2)
    logger.warning(
        "The derived dataset does not match any project dimension for dimension "
        "type %s. Consider creating a new supplemental dimension out of the files in %s",
        orig_dim_config.model.dimension_type.value,
        new_dim_path,
    )


def _make_dimension_mapping_references_file(dimension_mapping_references, path: Path):
    dim_mapping_ref_filename = path / "dimension_mapping_references.json5"
    dim_mapping_ref_filename.write_text(
        json5.dumps({"references": dimension_mapping_references}, indent=2)
    )
    logger.info(
        "Wrote dimension mapping references file %s with %s references. "
        "Specify that file when submitting the dataset to the project.",
        dim_mapping_ref_filename,
        len(dimension_mapping_references),
    )


def _get_unique_data_records(df, dim_model: DimensionModel, column_type: ColumnType):
    match column_type:
        case ColumnType.DIMENSION_QUERY_NAMES:
            column = dim_model.dimension_query_name
        case ColumnType.DIMENSION_TYPES:
            column = dim_model.dimension_type.value
        case _:
            raise Exception(f"BUG: unhandled column type: {column_type}")

    return get_unique_values(df, column)


def _get_dimension_reference(dim_model: DimensionModel, project_config):
    dim_ref = project_config.get_dimension_reference(dim_model.dimension_id)
    return serialize_model(dim_ref)


def _get_dimension_mapping_reference(dim_model: DimensionModel, project_config):
    key, _ = project_config.get_base_to_supplemental_config(dim_model.dimension_query_name)
    # Use dictionaries to avoid validation and be consistent with dimension definition.
    return {
        "mapping_id": key.id,
        "from_dimension_type": dim_model.dimension_type.value,
        "to_dimension_type": dim_model.dimension_type.value,
        "version": str(key.version),
    }
