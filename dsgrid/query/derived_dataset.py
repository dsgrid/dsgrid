import logging
from pathlib import Path

import chronify
import json5

from dsgrid.chronify import create_store
from dsgrid.common import VALUE_COLUMN
from dsgrid.config.dataset_config import (
    DataClassificationType,
    DataSchemaType,
    InputDatasetType,
)
from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.config.dimension_config import DimensionBaseConfigWithFiles, DimensionConfig
from dsgrid.config.dimensions import DimensionModel
from dsgrid.config.project_config import ProjectConfig
from dsgrid.config.time_dimension_base_config import TimeDimensionBaseConfig
from dsgrid.dataset.models import TableFormatType
from dsgrid.dimension.base_models import DimensionType, DimensionCategory
from dsgrid.dsgrid_rc import DsgridRuntimeConfig
from dsgrid.exceptions import DSGInvalidDataset
from dsgrid.query.models import ProjectQueryModel, DatasetMetadataModel, ColumnType
from dsgrid.query.query_submitter import QuerySubmitterBase
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.spark.functions import make_temp_view_name
from dsgrid.spark.types import DataFrame
from dsgrid.utils.files import dump_data
from dsgrid.utils.scratch_dir_context import ScratchDirContext
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
    format_type = metadata.get_table_format_type()
    table_format = {"format_type": format_type.value}
    if format_type == TableFormatType.PIVOTED:
        table_format["pivoted_dimension_type"] = metadata.table_format.pivoted_dimension_type.value

    project = registry_manager.project_manager.load_project(
        query.project.project_id, version=query.project.version
    )
    new_supplemental_dims_path = dst_path / "new_supplemental_dimensions"
    df = read_dataframe(table_file)
    # TODO: should there be a warning if the current project version is later?

    # This code blocks compares the dimension records in the dataframe against the project's base
    # and supplemental dimensions.
    # If the records match an existing dimension, add a reference to that dimension in the
    # dataset config.
    # If the records don't match an existing dimension, create a new supplemental dimension and
    # base-to-supplemental mapping that the user will need to register.
    dimension_references = []
    dimension_mapping_references = []
    base_dim_query_names = set(
        project.config.list_dimension_names(category=DimensionCategory.BASE)
    )
    num_new_supplemental_dimensions = 0
    for dim_type in DimensionType:
        dimension_names = metadata.dimensions.get_dimension_names(dim_type)
        assert len(dimension_names) == 1, dimension_names
        dim_query_name = next(iter(dimension_names))
        if dim_type == DimensionType.TIME:
            time_dim = project.config.get_time_dimension(dim_query_name)
            is_valid = _does_time_dimension_match(time_dim, df, table_file)
            if not is_valid:
                logger.warning(
                    "The dataset does not match the project's time dimension. "
                    "If this is expected, add a new time dimension to the dataset config file "
                    "and create an appropriate dimension mapping."
                )
            continue

        dim = project.config.get_dimension_with_records(dim_query_name)
        if (
            format_type == TableFormatType.PIVOTED
            and metadata.table_format.pivoted_dimension_type == dim_type
        ):
            unique_data_records = metadata.dimensions.get_column_names(dim_type)
        else:
            unique_data_records = _get_unique_data_records(df, dim.model, query.result.column_type)
        is_valid = _is_dimension_valid_for_dataset(dim, unique_data_records)

        if is_valid:
            dimension_references.append(_get_dimension_reference(dim, project.config))
            if dim_query_name not in base_dim_query_names:
                dimension_mapping_references.append(
                    _get_supplemental_dimension_mapping_reference(dim, project.config, metadata)
                )
        else:
            subset_dim_ref = project.config.get_matching_subset_dimension(
                dim_type, unique_data_records
            )
            if subset_dim_ref is not None:
                dimension_references.append(subset_dim_ref.serialize())
                continue

            supp_dim = _get_matching_supplemental_dimension(
                project.config, dim_type, unique_data_records
            )
            if supp_dim is None:
                assert dim_query_name in base_dim_query_names, dim_query_name
                _make_new_supplemental_dimension(
                    dim, unique_data_records, new_supplemental_dims_path
                )
                num_new_supplemental_dimensions += 1
            else:
                dimension_references.append(_get_dimension_reference(supp_dim, project.config))
                dimension_mapping_references.append(
                    _get_supplemental_dimension_mapping_reference(
                        supp_dim, project.config, metadata
                    )
                )

    if dimension_mapping_references:
        _make_dimension_mapping_references_file(dimension_mapping_references, dst_path)

    _make_dataset_config(
        query.project.dataset.dataset_id,
        table_format,
        dimension_references,
        dst_path,
        num_new_supplemental_dimensions,
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
    if query.result.replace_ids_with_names:
        is_valid = False
        logger.error("Cannot create a derived dataset from a query with replace_ids_with_names")

    return is_valid


def _does_time_dimension_match(dim_config: TimeDimensionBaseConfig, df: DataFrame, df_path: Path):
    try:
        if dim_config.supports_chronify():
            _check_time_dimension_with_chronify(dim_config, df, df_path)
        else:
            dim_config.check_dataset_time_consistency(df, dim_config.get_load_data_time_columns())
    except DSGInvalidDataset:
        return False
    return True


def _check_time_dimension_with_chronify(
    dim_config: TimeDimensionBaseConfig, df: DataFrame, df_path: Path
):
    scratch_dir = DsgridRuntimeConfig.load().get_scratch_dir()
    with ScratchDirContext(scratch_dir) as scratch_dir_context:
        time_cols = dim_config.get_load_data_time_columns()
        time_array_id_columns = [
            x
            for x in df.columns
            # If there are multiple weather years:
            #   - that are continuous, weather year needs to be excluded (one overall range).
            #   - that are not continuous, weather year needs to be included and chronify
            #     needs additional support. TODO: issue #340
            if x != DimensionType.WEATHER_YEAR.value
            and x in set(df.columns).difference(time_cols).difference({VALUE_COLUMN})
        ]
        schema = chronify.TableSchema(
            name=make_temp_view_name(),
            time_config=dim_config.to_chronify(),
            time_array_id_columns=time_array_id_columns,
            value_column=VALUE_COLUMN,
        )
        store_file = scratch_dir_context.get_temp_filename(suffix=".db")
        with create_store(store_file) as store:
            # This performs all of the checks.
            store.create_view_from_parquet(df_path, schema)
            store.drop_view(schema.name)


def _is_dimension_valid_for_dataset(
    dim_config: DimensionBaseConfigWithFiles, unique_data_records: DataFrame
):
    records = dim_config.get_records_dataframe()
    dim_values = get_unique_values(records, "id")
    diff = dim_values.symmetric_difference(unique_data_records)
    if not diff:
        return True

    return False


def _get_matching_supplemental_dimension(
    project_config: ProjectConfig,
    dimension_type: DimensionType,
    unique_data_records: DataFrame,
) -> DimensionBaseConfigWithFiles | None:
    for dim_config in project_config.list_supplemental_dimensions(dimension_type):
        if _is_dimension_valid_for_dataset(dim_config, unique_data_records):
            return dim_config

    return None


def _make_dataset_config(
    dataset_id,
    table_format: dict[str, str],
    dimension_references,
    path: Path,
    num_new_supplemental_dimensions,
    data_classification=DataClassificationType.MODERATE.value,
):
    # Use dictionaries instead of DatasetConfigModel to avoid validation, which isn't possible
    # here.
    config = {
        "dataset_id": dataset_id,
        "dataset_type": InputDatasetType.MODELED.value,
        "data_schema": {
            "data_schema_type": DataSchemaType.ONE_TABLE.value,
            "table_format": table_format,
        },
        "version": "1.0.0",
        "description": "",
        "origin_creator": "",
        "origin_organization": "",
        "origin_date": "",
        "origin_project": "",
        "origin_version": "",
        "data_source": "",
        "source": "",
        "data_classification": data_classification,
        "use_project_geography_time_zone": True,
        "dimensions": [],
        "dimension_references": dimension_references,
    }
    config_file = path / DatasetConfig.config_filename()
    config_file.write_text(json5.dumps(config, indent=2))
    if num_new_supplemental_dimensions > 0:
        logger.info(
            "Generated %s new supplemental dimensions. Review the records and fill out "
            "the remaining fields, and then register them.",
            num_new_supplemental_dimensions,
        )
    logger.info(
        "Created %s with default information. Re-used %s project dimensions. "
        "Examine %s, fill out the remaining fields, and register any new dimensions "
        "before registering and submitting the dataset to the project.",
        path,
        len(dimension_references),
        config_file,
    )


def _make_new_supplemental_dimension(orig_dim_config, unique_data_records, path: Path):
    project_record_ids = orig_dim_config.get_unique_ids()
    if not unique_data_records.issubset(project_record_ids):
        diff = project_record_ids.difference(unique_data_records)
        if diff:
            msg = (
                f"The derived dataset records do not include some project base dimension "
                f"records. Dimension type = {orig_dim_config.model.dimension_type} {diff=}"
            )
            raise DSGInvalidDataset(msg)
        assert unique_data_records.issuperset(project_record_ids)
        diff = unique_data_records.difference(project_record_ids)
        msg = (
            f"The derived dataset records is a superset of the project base dimension "
            f"records. Dimension type = {orig_dim_config.model.dimension_type} {diff=}"
        )
        raise DSGInvalidDataset(msg)

    new_dim_path = path / orig_dim_config.model.dimension_type.value
    new_dim_path.mkdir(parents=True)
    orig_records = orig_dim_config.get_records_dataframe()
    records = orig_records.filter(orig_records.id.isin(unique_data_records))
    # TODO: AWS #186 - not an issue if registry is in a database instead of files
    filename = new_dim_path / "records.csv"
    # Use pandas because spark creates a directory.
    records.toPandas().to_csv(filename, index=False)
    # Use dictionaries instead of DimensionModel to avoid running the Pydantic validators.
    # Some won't work, like loading the records. Others, like file_hash, shouldn't get set yet.
    new_dim = {
        "type": orig_dim_config.model.dimension_type.value,
        "name": "",
        "module": orig_dim_config.model.module,
        "class_name": orig_dim_config.model.class_name,
        "description": "",
        "filename": filename.name,
    }
    dump_data(new_dim, new_dim_path / DimensionConfig.config_filename(), indent=2)
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
        case ColumnType.DIMENSION_NAMES:
            column = dim_model.name
        case ColumnType.DIMENSION_TYPES:
            column = dim_model.dimension_type.value
        case _:
            msg = f"BUG: unhandled: {column_type=}"
            raise NotImplementedError(msg)

    return get_unique_values(df, column)


def _get_dimension_reference(dim: DimensionBaseConfigWithFiles, project_config: ProjectConfig):
    dim_ref = project_config.get_dimension_reference(dim.model.dimension_id)
    return dim_ref.serialize()


def _get_supplemental_dimension_mapping_reference(
    supp_dim: DimensionBaseConfigWithFiles,
    project_config: ProjectConfig,
    metadata: DatasetMetadataModel,
):
    base_dim_name = getattr(metadata.base_dimension_names, supp_dim.model.dimension_type.value)
    base_dim = project_config.get_dimension_with_records(base_dim_name)
    mapping_config = project_config.get_base_to_supplemental_config(base_dim, supp_dim)
    # Use dictionaries to avoid validation and be consistent with dimension definition.
    return {
        "mapping_id": mapping_config.model.mapping_id,
        "from_dimension_type": base_dim.model.dimension_type.value,
        "to_dimension_type": supp_dim.model.dimension_type.value,
        "version": str(mapping_config.model.version),
    }
