from pathlib import Path

from pydantic import validator

from dsgrid.data_models import DSGBaseModel
from dsgrid.utils.files import load_data


class DatasetRegistrationModel(DSGBaseModel):

    dataset_id: str
    dataset_path: str | Path
    config_file: str | Path
    dimension_mapping_file: str | Path | None
    fix_dimension_uuids: bool = False
    fix_dimension_mapping_uuids: bool = False
    register_dataset: bool = True
    submit_to_project: bool = True

    @validator("dataset_path", "config_file", "dimension_mapping_file")
    def fix_path(cls, val):
        return Path(val) if isinstance(val, str) else val


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
