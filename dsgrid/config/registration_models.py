"""Contains data models to control bulk registration of projects and datasets."""

from pathlib import Path
from typing import Any, Iterable

from pydantic import Field, ValidationInfo, field_validator, model_validator

from dsgrid.data_models import DSGBaseModel
from dsgrid.dimension.base_models import DimensionType
from dsgrid.utils.files import load_data


class ProjectRegistrationModel(DSGBaseModel):
    """Defines a project to be registered."""

    project_id: str = Field(description="Project ID")
    config_file: Path = Field(description="Path to project.json5")
    log_message: str | None = Field(
        default=None,
        description="Log message to use when registering the project. Defaults to an auto-generated message.",
    )

    @model_validator(mode="before")
    @classmethod
    def fix_paths(cls, data: dict[str, Any]) -> dict[str, Any]:
        _fix_paths(data, ("config_file",))
        return data

    @field_validator("log_message")
    def fix_log_message(cls, log_message: str | None, info: ValidationInfo) -> str | None:
        if log_message is None and "project_id" in info.data:
            log_message = f"Register project {info.data['project_id']}"
        return log_message


class DatasetRegistrationModel(DSGBaseModel):
    """Defines a dataset to be registered."""

    dataset_id: str = Field(description="Dataset ID")
    dataset_path: Path = Field(
        description="Directory containing load_data/load_data_lookup.parquet"
    )
    config_file: Path = Field(description="Path to dataset.json5")
    replace_dimension_names_with_ids: bool = Field(
        description="Replace the dimension entries with IDs of dimensions in the database "
        "with matching names. Typically only useful for tests.",
        default=False,
    )
    log_message: str | None = Field(
        default=None,
        description="Log message to use when registering the dataset. Defaults to an auto-generated message.",
    )

    @field_validator("log_message")
    def fix_log_message(cls, log_message: str | None, info: ValidationInfo) -> str | None:
        if log_message is None and "dataset_id" in info.data:
            log_message = f"Register dataset {info.data['dataset_id']}"
        return log_message

    @model_validator(mode="before")
    @classmethod
    def fix_paths(cls, data: dict[str, Any]) -> dict[str, Any]:
        _fix_paths(
            data,
            (
                "dataset_path",
                "config_file",
                "dimension_mapping_file",
                "dimension_mapping_references_file",
            ),
        )
        return data


class DatasetSubmissionModel(DSGBaseModel):
    """Defines how a dataset should be submitted to a project."""

    dataset_id: str
    project_id: str
    dimension_mapping_file: Path | None = Field(
        description="Path to file containing mappings of dataset-to-project dimensions",
        default=None,
    )
    dimension_mapping_references_file: Path | None = Field(
        description="Path to file containing references to mappings of dataset-to-project dimensions",
        default=None,
    )
    replace_dimension_mapping_names_with_ids: bool = Field(
        description="Replace the dimension mapping entries with IDs of dimension mappings "
        "in the database with matching names. Typically only useful for tests.",
        default=False,
    )
    autogen_reverse_supplemental_mappings: set[DimensionType] = Field(
        description="Dimensions on which to attempt create reverse mappings from supplemental dimensions.",
        default=set(),
    )
    log_message: str | None = Field(
        default=None,
        description="Log message to use when submitting the dataset. Defaults to an auto-generated message.",
    )

    @model_validator(mode="before")
    @classmethod
    def fix_autogen_reverse_supplemental_mappings(cls, data: dict[str, Any]) -> dict[str, Any]:
        if "autogen_reverse_supplemental_mappings" in data:
            data["autogen_reverse_supplemental_mappings"] = {
                DimensionType(x) for x in data["autogen_reverse_supplemental_mappings"]
            }
        return data

    @field_validator("log_message")
    def fix_log_message(cls, log_message: str | None, info: ValidationInfo) -> str | None:
        if log_message is None and "dataset_id" in info.data:
            log_message = (
                f"Submit dataset {info.data['dataset_id']} to project {info.data['project_id']}"
            )
        return log_message


class SubmittedDatasetsJournal(DSGBaseModel):
    """Defines a dataset that was successfully submitted to a project."""

    dataset_id: str
    project_id: str


class RegistrationJournal(DSGBaseModel):
    """Defines projects and datasets that were succesfully registered."""

    registered_projects: list[str] = []
    registered_datasets: list[str] = []
    submitted_datasets: list[SubmittedDatasetsJournal] = []

    def add_dataset(self, dataset_id: str) -> None:
        assert dataset_id not in self.registered_datasets, dataset_id
        self.registered_datasets.append(dataset_id)

    def add_project(self, project_id: str) -> None:
        assert project_id not in self.registered_projects, project_id
        self.registered_projects.append(project_id)

    def add_submitted_dataset(self, dataset_id: str, project_id: str) -> None:
        entry = SubmittedDatasetsJournal(dataset_id=dataset_id, project_id=project_id)
        assert entry not in self.submitted_datasets, entry
        self.submitted_datasets.append(entry)

    def has_entries(self) -> bool:
        return (
            bool(self.registered_projects)
            or bool(self.registered_datasets)
            or bool(self.submitted_datasets)
        )


class RegistrationModel(DSGBaseModel):
    """Defines a list of projects and datasets to be registered."""

    projects: list[ProjectRegistrationModel] = Field(description="List of projects to register.")
    datasets: list[DatasetRegistrationModel] = Field(description="List of datasets to register.")
    dataset_submissions: list[DatasetSubmissionModel] = Field(
        description="List of datasets to be submitted to projects."
    )

    def filter_by_journal(self, journal: RegistrationJournal) -> "RegistrationModel":
        """Return a new instance of RegistrationModel by filtering an existing instance with
        a journal.
        """
        projects = list(
            filter(lambda x: x.project_id not in journal.registered_projects, self.projects)
        )
        datasets = list(
            filter(lambda x: x.dataset_id not in journal.registered_datasets, self.datasets)
        )
        dataset_submissions = list(
            filter(
                lambda x: SubmittedDatasetsJournal(
                    dataset_id=x.dataset_id, project_id=x.project_id
                )
                not in journal.submitted_datasets,
                self.dataset_submissions,
            )
        )
        return RegistrationModel(
            projects=projects,
            datasets=datasets,
            dataset_submissions=dataset_submissions,
        )


def _fix_paths(data: dict[str, Any], fields: Iterable[str]) -> None:
    for field in fields:
        val = data.get(field)
        if isinstance(val, str):
            data[field] = Path(val)


def create_registration(input_file: Path):
    """Create registration inputs."""
    return RegistrationModel(**load_data(input_file))
