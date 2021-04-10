"""
# ******************************************
# RUNNING LIST OF PROJECT CONFIG TODOS
# ******************************************

- we need to establish relationships across project-dimensions (to/from or
    some kind of mapper)
- need to establish dsgrid types standard relationship mappers
- need to use said mapper to run compatibility checks
- need to better establish expected fields/types in the dimension dataclasses
    in dsgrid.dimension.standard
- enforce supplemental dimensions to have a from/to mapping to the project
    dimension of the same type?
- I think we need to add the association table to
    dimensions.associations.project_dimensions in the config

- Add registry details
- need to generate input data config

"""
from dsgrid.utils.utilities import check_uniqueness
import os
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Union
import importlib
from pathlib import Path

from pydantic.dataclasses import dataclass
from pydantic.fields import Field
from pydantic.class_validators import root_validator, validator

from dsgrid.exceptions import DSGInvalidField, DSGValueNotStored
from dsgrid.data_models import DSGBaseModel
from dsgrid.dimension.models import (
    DimensionReferenceModel,
    DimensionModel,
    DimensionType,
    TimeDimensionModel,
    handle_dimension_union,
    DimensionUnionModel,
)
from dsgrid.registry.dimension_registry_manager import DimensionRegistryManager

# from dsgrid.dimension.time import (
#     LeapDayAdjustmentType, Period, TimeValueMeasurement, TimeFrequency,
#     TimezoneType
#     )

LOAD_DATA_FILENAME = "load_data.parquet"
LOAD_DATA_LOOKUP_FILENAME = "load_data_lookup.parquet"


class DimensionsModel(DSGBaseModel):
    """Contains dimensions defined by a dataset"""

    project_dimensions: List[DimensionReferenceModel] = Field(
        title="project_dimensions",
        description="dimensions defined by the project",
    )
    supplemental_dimensions: Optional[List[DimensionReferenceModel]] = Field(
        title="supplemental_dimensions",
        description="supplemental dimensions",
        default=[],
    )

    @validator("project_dimensions")
    def check_project_dimension(cls, val):
        """Validate project_dimensions types"""
        dimension_types = [i.dimension_type for i in val]
        required_dim_types = list(DimensionType)
        # validate required dimensions for project_dimensions
        for i in required_dim_types:
            if i not in dimension_types:
                raise ValueError(
                    f"Required project dimension {i} is not in project ",
                    "config project.project_dimensions",
                )
        check_uniqueness(dimension_types, "project_dimension")
        return val

    # @root_validator
    # def check_dimension_names(cls, values: dict) -> dict:
    #    """Validate dimension names"""
    #    check_uniqueness(
    #        (ii.name for i in values for ii in values[i]),
    #        "dimension name",
    #    )
    #    return values

    # @root_validator
    # def check_unique_classes(cls, values: dict) -> dict:
    #    """Validate dimension classes are unique"""
    #    check_uniqueness(
    #        (getattr(ii, "cls") for i in values for ii in values[i]),
    #        "dimension cls",
    #    )
    #    return values

    # @root_validator
    # def check_files(cls, values: dict) -> dict:
    #    """Validate dimension files across all dimensions"""
    #    check_uniqueness(
    #        (ii.filename for i in values for ii in values[i] if isinstance(ii, Dimension)),
    #        "dimension record filename",
    #    )
    #    return values

    @root_validator
    def check_dimension_mappings(cls, values: dict) -> dict:
        """validates that a
        check that keys exist in both jsons
        check that all from_keys have a match in the to_keys json

        """
        # supplemental_mapping = {
        #   x.name: x.cls for x in values["supplemental_dimensions"]}
        # Should already have been checked.
        # assert len(supplemental_mapping) == \
        #   len(values["supplemental_dimensions"])
        # for dim in values["project_dimensions"]:
        #    mappings = getattr(dim, "mappings", [])
        #    # TODO: other mapping types
        #    for mapping in (
        #       x for x in mappings if isinstance(x, DimensionDirectMapping)):
        #        to_dim = supplemental_mapping.get(mapping.to_dimension)
        #        if to_dim is None:
        #            raise ValueError(
        #               f"dimension {mapping.to_dimension} is not stored in"
        #               f"supplemental_dimensions"
        #            )
        #        mapping.to_dimension = to_dim

        return values


class InputDatasetModel(DSGBaseModel):
    """Defines an input dataset"""

    dataset_id: str = Field(
        title="dataset_id",
        description="dataset ID",
    )
    dataset_type: str = Field(  # TODO this needs to be ENUM
        title="dataset_type", description="Dataset Type"
    )
    # TODO this model_sector must be validated in the dataset_config
    model_sector: str = Field(
        title="model_sector",
        description="model sector",
    )
    # TODO: is this needed?
    # sectors: List[str] = Field(
    #    title="sectors",
    #    description="sectors used in the project",
    # )


class InputDatasetsModel(DSGBaseModel):
    """Defines all input datasets for a project"""

    # TODO: incorrect
    benchmark: List[str] = Field(
        title="benchmark",
        default=[],
        description="benchmark",
    )
    # TODO: incorrect
    historical: List[str] = Field(
        title="historical",
        default=[],
        description="historical",
    )
    datasets: List[InputDatasetModel] = Field(
        title="datasets",
        description="project input datasets",
    )

    # TODO:
    #   - Check for unique dataset IDs
    #   - check model_name, model_sector, sectors are all expected and align
    #   with the dimension records


class ProjectConfigModel(DSGBaseModel):
    """Represents project configurations"""

    project_id: str = Field(
        title="project_id",
        description="project identifier",
    )
    name: str = Field(
        title="name",
        description="project name",
    )
    input_datasets: InputDatasetsModel = Field(
        title="input_datasets",
        description="input datasets for the project",
    )
    dimensions: DimensionsModel = Field(
        title="dimensions",
        description="dimensions",
    )
    registration: Optional[Dict] = Field(
        title="registration",
        description="registration information",
    )
    description: str = Field(
        title="description",
        description="describe project in details",
    )

    @validator("project_id")
    def check_project_id_handle(cls, project_id):
        """Check for valid characteris in project id"""
        # TODO: any other invalid character for the project_id?
        # TODO: may want to check for pre-existing project_id
        #       (e.g., LA100 Run 1 vs. LA100 Run 0 kind of thing)
        if "-" in project_id:
            raise ValueError('invalid character "-" in project id')
        return project_id


class ProjectConfig:
    """Provides an interface to a ProjectConfigModel."""

    def __init__(self, model):
        self._model = model

    @classmethod
    def load(cls, config_file, dimension_manager):
        if not os.path.exists(config_file):
            raise DSGValueNotStored(f"{config_file} does not exist. Check the version.")
        model = ProjectConfigModel.load(config_file)
        config = cls(model)
        dimension_manager.replace_dimension_references(config.project_dimensions)
        dimension_manager.replace_dimension_references(config.supplemental_dimensions)
        return config

    def get_dataset(self, dataset_id):
        """Return a dataset by ID."""
        for dataset in self._model.input_datasets.datasets:
            if dataset.dataset_id == dataset_id:
                return dataset

        raise DSGInvalidField(f"no dataset with dataset_id={dataset_id}")

    def has_dataset(self, dataset_id):
        """Return True if the dataset_id is present in the configuration."""
        for _id in self.iter_dataset_ids():
            if _id == dataset_id:
                return True

        # TODO DT: what about benchmark and historical?
        return False

    def iter_datasets(self):
        for dataset in self._model.input_datasets.datasets:
            yield dataset

    def iter_dataset_ids(self):
        for dataset in self._model.input_datasets.datasets:
            yield dataset.dataset_id

    @property
    def model(self):
        return self._model

    @property
    def project_dimensions(self):
        return self._model.dimensions.project_dimensions

    @property
    def supplemental_dimensions(self):
        return self._model.dimensions.supplemental_dimensions
