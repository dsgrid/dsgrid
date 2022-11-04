"""Contains data models to control registration of test projects and datasets."""

from pathlib import Path

from pydantic import validator

from dsgrid.data_models import DSGBaseModel
from dsgrid.dimension.base_models import DimensionType
from dsgrid.utils.files import load_data


class DatasetRegistrationModel(DSGBaseModel):

    dataset_id: str
    dataset_path: str | Path
    config_file: str | Path
    dimension_mapping_file: str | Path | None
    dimension_mapping_references_file: str | Path | None
    fix_dimension_uuids: bool = False
    fix_dimension_mapping_uuids: bool = False
    register_dataset: bool = True
    submit_to_project: bool = True
    autogen_reverse_supplemental_mappings: set[str | DimensionType] = set()

    @validator(
        "dataset_path",
        "config_file",
        "dimension_mapping_file",
        "dimension_mapping_references_file",
    )
    def fix_path(cls, val):
        return Path(val) if isinstance(val, str) else val

    @validator("autogen_reverse_supplemental_mappings")
    def fix_autogen_reverse_supplemental_mappings(cls, val):
        return {DimensionType(x) for x in val}


class ProjectRegistrationModel(DSGBaseModel):

    project_id: str
    config_file: str | Path
    datasets: list[DatasetRegistrationModel]
    register_project: bool = True

    @validator("config_file")
    def fix_path(cls, val):
        return Path(val) if isinstance(val, str) else val


class RegistrationModel(DSGBaseModel):

    create_registry: bool
    registry_path: str | Path
    projects: list[ProjectRegistrationModel]

    @validator("registry_path")
    def fix_path(cls, val):
        return Path(val) if isinstance(val, str) else val


def create_registration(input_file: Path):
    """Create registration inputs."""
    return RegistrationModel(**load_data(input_file))
