import logging
from pathlib import Path
from typing import Iterable

from chronify.utils.path_utils import check_overwrite
from pydantic import Field

from dsgrid.config.dataset_config import (
    get_unique_dimension_record_ids,
    make_unvalidated_dataset_config,
)
from dsgrid.config.file_schema import Column
from dsgrid.data_models import DSGBaseModel
from dsgrid.dataset.models import TableFormat
from dsgrid.config.project_config import ProjectConfig
from dsgrid.dimension.base_models import DimensionType
from dsgrid.dimension.time import TimeDimensionType
from dsgrid.exceptions import DSGInvalidParameter
from dsgrid.registry.dimension_registry_manager import DimensionRegistryManager
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.utils.files import dump_data, load_data
from dsgrid.config.dimensions import DimensionReferenceModel
from dsgrid.config.dimension_config import DimensionBaseConfigWithFiles


logger = logging.getLogger(__name__)


class DataFileColumns(DSGBaseModel):
    """User-declared columns for the data files scanned by ``generate-config``.

    The CLI reads raw on-disk files before a dataset config exists, so dsgrid
    cannot derive column types from a registered schema. Users declare columns
    here per file role to give the readers an authoritative schema.

    - ``one_table`` layouts only need ``load_data``.
    - ``two_table`` layouts may declare ``load_data`` and/or ``load_data_lookup``.

    Columns omitted from these lists keep whatever type the backend's default
    reader infers. Declared columns are cast to the requested type after read,
    so the same schema applies uniformly to CSV, JSON, and Parquet inputs.
    """

    load_data: list[Column] = Field(
        default_factory=list,
        description="Columns for the load_data file.",
    )
    load_data_lookup: list[Column] = Field(
        default_factory=list,
        description="Columns for the load_data_lookup file (two_table only).",
    )


def load_data_file_columns(path: Path) -> DataFileColumns:
    """Load a ``--schema-file`` JSON5 document into a :class:`DataFileColumns`.

    Parameters
    ----------
    path : Path
        JSON5 file shaped like ``{"load_data": [...], "load_data_lookup": [...]}``.
        Each list element is a :class:`Column` (``name`` and ``data_type``).

    Returns
    -------
    DataFileColumns

    Raises
    ------
    DSGInvalidParameter
        If the file cannot be parsed into the model.
    """
    try:
        return DataFileColumns.model_validate(load_data(path))
    except Exception as exc:
        msg = f"Failed to parse schema file {path}: {exc}"
        raise DSGInvalidParameter(msg) from exc


def generate_config_from_dataset(
    registry_manager: RegistryManager,
    dataset_id: str,
    dataset_path: Path,
    table_format: TableFormat,
    metric_type: str,
    pivoted_dimension_type: DimensionType | None = None,
    time_type: TimeDimensionType | None = None,
    time_columns: set[str] | None = None,
    output_directory: Path | None = None,
    project_id: str | None = None,
    overwrite: bool = False,
    no_prompts: bool = False,
    data_file_columns: DataFileColumns | None = None,
):
    """Generate dataset config files from a dataset table.

    Fill out the dimension record files based on the unique values in the dataset.

    Look for matches for dimensions in the registry, checking for project base dimensions
    first. Prompt the user for confirmation unless --no-prompts is set. If --no-prompts is
    set, the first match is automatically accepted.

    Parameters
    ----------
    data_file_columns : DataFileColumns | None
        Optional user-declared columns for the input files. See
        :class:`DataFileColumns`. When omitted, dsgrid relies on each
        backend's default reader, which can disagree across CSV and JSON (e.g.
        DuckDB's CSV reader returns all columns as VARCHAR while its JSON
        reader infers types). Pass this when the input files include a shared
        join key whose type would otherwise be inferred inconsistently.
    """
    project_config = (
        None if project_id is None else registry_manager.project_manager.get_by_id(project_id)
    )
    output_dir = (output_directory or Path()) / dataset_id
    check_overwrite(output_dir, overwrite)
    output_dir.mkdir()
    dimensions_dir = output_dir / "dimensions"
    dimensions_dir.mkdir()
    dataset_file = output_dir / "dataset.json5"
    time_cols = time_columns or {"timestamp"}

    load_data_columns = (
        list(data_file_columns.load_data) if data_file_columns is not None else None
    )
    load_data_lookup_columns = (
        list(data_file_columns.load_data_lookup) if data_file_columns is not None else None
    )

    dimension_references: list[DimensionReferenceModel] = []
    for dim_type, ids in get_unique_dimension_record_ids(
        dataset_path,
        table_format,
        pivoted_dimension_type,
        time_cols,
        load_data_columns=load_data_columns,
        load_data_lookup_columns=load_data_lookup_columns,
    ).items():
        ref, checked_project_dim_ids = find_matching_project_base_dimension(
            project_config, ids, dim_type, no_prompts=no_prompts
        )
        if ref is None:
            ref = find_matching_registry_dimensions(
                registry_manager.dimension_manager,
                ids,
                dim_type,
                checked_project_dim_ids,
                no_prompts=no_prompts,
            )
        if ref is None:
            write_dimension_records(ids, dimensions_dir / f"{dim_type.value}.csv")
        else:
            dimension_references.append(ref)

    config = make_unvalidated_dataset_config(
        dataset_id,
        metric_type,
        dimension_references=dimension_references,
        time_type=time_type,
    )
    dump_data(config, dataset_file, indent=2)
    logger.info("Wrote dataset config to %s", dataset_file)


def write_dimension_records(ids: Iterable[str], filename: Path) -> None:
    with open(filename, "w", encoding="utf-8") as f:
        header = ["id", "name"]
        f.write(",".join(header))
        f.write("\n")
        for id_ in ids:
            str_id = str(id_)
            values = [str_id, str_id.title().replace("_", " ")]
            f.write(",".join(values))
            f.write("\n")
    logger.info("Wrote dimension records to %s", filename)


def find_matching_project_base_dimension(
    project_config: ProjectConfig | None,
    sorted_record_ids: list[str],
    dimension_type: DimensionType,
    no_prompts: bool = False,
) -> tuple[DimensionReferenceModel | None, set[str]]:
    """Find matching base dimensions for a dataset in a project."""
    checked_project_dim_ids: set[str] = set()
    if project_config is None:
        return None, checked_project_dim_ids

    if dimension_type == DimensionType.TIME:
        return None, checked_project_dim_ids

    for dim in project_config.list_base_dimensions_with_records(dimension_type=dimension_type):
        project_records = sorted(dim.get_unique_ids())
        checked_project_dim_ids.add(dim.model.dimension_id)
        if sorted_record_ids == project_records and (
            no_prompts or get_user_input_on_dimension_match(dim, "project base dimension")
        ):
            return make_dimension_ref(dim), checked_project_dim_ids

    return None, checked_project_dim_ids


def find_matching_registry_dimensions(
    dimension_manager: DimensionRegistryManager,
    ids: list[str],
    dimension_type: DimensionType,
    checked_project_dim_ids: set[str],
    no_prompts: bool = False,
) -> DimensionReferenceModel | None:
    for dim in dimension_manager.find_matching_dimensions(ids, dimension_type):
        if dim.model.dimension_id not in checked_project_dim_ids and (
            no_prompts or get_user_input_on_dimension_match(dim, "dimension from the registry")
        ):
            return make_dimension_ref(dim)
    return None


def get_user_input_on_dimension_match(dim: DimensionBaseConfigWithFiles, tag: str) -> bool:
    value = input(
        f"Found a {tag} with matching records:\n"
        f"  Dimension type: {dim.model.dimension_type.value}\n"
        f"  Name: {dim.model.name}\n"
        f"  Description: {dim.model.description}\n"
        f"  Dimension ID: {dim.model.dimension_id}\n"
        "Do you want to use it? (y/n) >>> "
    )
    return value.lower().strip() == "y"


def make_dimension_ref(dim: DimensionBaseConfigWithFiles) -> DimensionReferenceModel:
    return DimensionReferenceModel(
        dimension_id=dim.model.dimension_id,
        type=dim.model.dimension_type,
        version=dim.model.version,
    )
