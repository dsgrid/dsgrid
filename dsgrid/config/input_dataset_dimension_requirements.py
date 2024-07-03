"""Defines dataset dimension requirements for a project."""

from pydantic import conlist

from dsgrid.config.project_config import RequiredDimensionsModel
from dsgrid.data_models import DSGBaseModel


class InputDatasetDimensionRequirementsModel(DSGBaseModel):
    """Defines dataset dimension requirements."""

    dataset_id: str
    required_dimensions: RequiredDimensionsModel


class InputDatasetDimensionRequirementsListModel(DSGBaseModel):
    """Defines a list of dataset dimension requirements."""

    datasets: conlist(InputDatasetDimensionRequirementsModel, min_length=1)
