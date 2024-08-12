"""Contains data models to control registration of test projects and datasets."""

from collections.abc import Iterable
from pathlib import Path
from typing import Any, Optional

from pydantic import Field, model_validator
from typing_extensions import Annotated

from dsgrid.data_models import DSGBaseModel
from dsgrid.dimension.base_models import DimensionType
from dsgrid.utils.files import load_data


class DatasetRegistrationModel(DSGBaseModel):
    """Defines a dataset to be registered and optionally submitted to a project."""

    dataset_id: Annotated[str, Field(description="Dataset ID")]
    dataset_path: Annotated[
        Path, Field(description="Directory containing load_data/load_data_lookup.parquet")
    ]
    config_file: Annotated[Path, Field(description="Path to dataset.json5")]
    dimension_mapping_file: Annotated[
        Optional[Path],
        Field(
            description="Path to file containing mappings of dataset-to-project dimensions",
            default=None,
        ),
    ]
    dimension_mapping_references_file: Annotated[
        Optional[Path],
        Field(
            description="Path to file containing references to mappings of dataset-to-project dimensions",
            default=None,
        ),
    ]
    replace_dimension_names_with_ids: Annotated[
        bool,
        Field(
            description="Replace the dimension entries with IDs of dimensions in the database "
            "with matching names. Typically only useful for tests.",
            default=False,
        ),
    ]
    replace_dimension_mapping_names_with_ids: Annotated[
        bool,
        Field(
            description="Replace the dimension mapping entries with IDs of dimension mappings "
            "in the database with matching names. Typically only useful for tests.",
            default=False,
        ),
    ]
    register_dataset: Annotated[
        bool, Field(description="If True, register the dataset", default=True)
    ]
    submit_to_project: Annotated[
        bool, Field(description="If True, submit the dataset to the project", default=True)
    ]
    autogen_reverse_supplemental_mappings: Annotated[
        set[DimensionType],
        Field(
            description="Dimensions on which to attempt create reverse mappings from supplemental dimensions.",
            default=set(),
        ),
    ]

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

    @model_validator(mode="before")
    @classmethod
    def fix_autogen_reverse_supplemental_mappings(cls, data: dict[str, Any]) -> dict[str, Any]:
        if "autogen_reverse_supplemental_mappings" in data:
            data["autogen_reverse_supplemental_mappings"] = {
                DimensionType(x) for x in data["autogen_reverse_supplemental_mappings"]
            }
        return data


class ProjectRegistrationModel(DSGBaseModel):
    """Defines a project to be registered."""

    project_id: Annotated[str, Field(description="Project ID")]
    config_file: Annotated[Path, Field(description="Path to project.json5")]
    datasets: Annotated[
        list[DatasetRegistrationModel],
        Field(description="List of datasets to register and optionally submit to the project"),
    ]
    register_project: Annotated[
        bool, Field(description="If True, register the project.", default=True)
    ]

    @model_validator(mode="before")
    @classmethod
    def fix_paths(cls, data: dict[str, Any]) -> dict[str, Any]:
        _fix_paths(data, ("config_file",))
        return data


class RegistrationModel(DSGBaseModel):
    """Defines a list of projects and datasets to be registered."""

    projects: Annotated[
        list[ProjectRegistrationModel],
        Field(description="Defines all projects and datasets to register."),
    ]


def _fix_paths(data: dict[str, Any], fields: Iterable[str]) -> None:
    for field in fields:
        val = data.get(field)
        if isinstance(val, str):
            data[field] = Path(val)


def create_registration(input_file: Path):
    """Create registration inputs."""
    return RegistrationModel(**load_data(input_file))
