"""Defines dataset dimension requirements for a project."""

from pydantic import conlist, Field
from typing_extensions import Annotated

from dsgrid.config.project_config import RequiredDimensionsModel, InputDatasetModel
from dsgrid.data_models import DSGBaseModel


class InputDatasetDimensionRequirementsModel(DSGBaseModel):
    """Defines dataset dimension requirements."""

    dataset_id: str
    required_dimensions: Annotated[
        RequiredDimensionsModel,
        Field(
            title="required_dimensions",
            description="Defines required record IDs that must exist for each dimension.",
        ),
    ]


class InputDatasetDimensionRequirementsListModel(DSGBaseModel):
    """Defines a list of dataset dimension requirements."""

    datasets: Annotated[
        conlist(InputDatasetDimensionRequirementsModel, min_length=1),
        Field(description="List of dataset dimension requirements"),
    ]


class InputDatasetListModel(DSGBaseModel):
    datasets: Annotated[
        conlist(InputDatasetModel, min_length=1),
        Field(
            title="datasets",
            description="List of input datasets for the project.",
        ),
    ]
