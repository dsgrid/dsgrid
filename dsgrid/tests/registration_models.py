"""Contains data models to control registration of test projects and datasets."""

from pathlib import Path

from pydantic import field_validator

from dsgrid.data_models import DSGBaseModel
from dsgrid.dimension.base_models import DimensionType
from dsgrid.registry.registry_database import DatabaseConnection
from dsgrid.utils.files import load_data


class DatasetRegistrationModel(DSGBaseModel):

    dataset_id: str
    dataset_path: str | Path
    config_file: str | Path
    dimension_mapping_file: str | Path | None = None
    dimension_mapping_references_file: str | Path | None = None
    replace_dimension_names_with_ids: bool = False
    replace_dimension_mapping_names_with_ids: bool = False
    register_dataset: bool = True
    submit_to_project: bool = True
    autogen_reverse_supplemental_mappings: set[str | DimensionType] = set()

    @field_validator(
        "dataset_path",
        "config_file",
        "dimension_mapping_file",
        "dimension_mapping_references_file",
    )
    @classmethod
    def fix_path(cls, val):
        return Path(val) if isinstance(val, str) else val

    @field_validator("autogen_reverse_supplemental_mappings")
    @classmethod
    def fix_autogen_reverse_supplemental_mappings(cls, val):
        return {DimensionType(x) for x in val}


class ProjectRegistrationModel(DSGBaseModel):

    project_id: str
    config_file: str | Path
    datasets: list[DatasetRegistrationModel]
    register_project: bool = True

    @field_validator("config_file")
    @classmethod
    def fix_path(cls, val):
        return Path(val) if isinstance(val, str) else val


class RegistrationModel(DSGBaseModel):

    create_registry: bool
    conn: DatabaseConnection
    data_path: str | Path
    projects: list[ProjectRegistrationModel]

    @field_validator("data_path")
    @classmethod
    def fix_path(cls, val):
        return Path(val) if isinstance(val, str) else val


def create_registration(input_file: Path):
    """Create registration inputs."""
    return RegistrationModel(**load_data(input_file))
