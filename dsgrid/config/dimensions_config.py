import logging
from typing import List, Union

from pydantic import Field
from pydantic import validator

from .config_base import ConfigBase
from .dimensions import (
    TimeDimensionModel,
    AnnualTimeDimensionModel,
    RepresentativePeriodTimeDimensionModel,
    DimensionModel,
    handle_dimension_union,
)
from dsgrid.data_models import DSGBaseModel
from dsgrid.registry.common import make_registry_id, check_config_id_loose
from dsgrid.utils.utilities import check_uniqueness

logger = logging.getLogger(__name__)


class DimensionsConfigModel(DSGBaseModel):
    """Represents multiple dimension models.

    Used when registering multiple dimensions in one command.
    """

    dimensions: List = Field(
        title="dimensions",
        description="Dimensions for submission to the dimension registry",
    )
    # Pydantic doesn't cleanly handle this list of a Union of types.
    # They will eventually fix it. Refer to https://github.com/samuelcolvin/pydantic/issues/619
    # We tried to implement workarounds but always eventually hit cases where Pydantic
    # would try to construct the wrong type and raise ValueError.
    # Until they implement the feature we will use an untyped list and handle each of our types
    # manually.
        #Union[
        #    DimensionModel,
        #    TimeDimensionModel,
        #    AnnualTimeDimensionModel,
        #    RepresentativePeriodTimeDimensionModel,
        #]

    @validator("dimensions")
    def check_files(cls, values: dict) -> dict:
        """Validate dimension files are unique across all dimensions"""
        check_uniqueness(
            (x.filename for x in values if isinstance(x, DimensionModel)),
            "dimension record filename",
        )
        return values

    @validator("dimensions")
    def check_names(cls, values: dict) -> dict:
        """Validate dimension names are unique across all dimensions."""
        check_uniqueness(
            [dim.name for dim in values],
            "dimension record name",
        )
        return values

    @validator("dimensions", pre=True, each_item=True, always=True)
    def handle_dimension_union(cls, values):
        return handle_dimension_union(values)


class DimensionsConfig(ConfigBase):
    """Provides an interface to a DimensionsConfigModel."""

    @staticmethod
    def config_filename():
        return "dimensions.toml"

    @property
    def config_id(self):
        assert False, "not correct for this class"

    @staticmethod
    def model_class():
        return DimensionsConfigModel

    def assign_ids(self):
        """Assign unique IDs to each mapping in the config"""
        logger.info("Dimension record ID assignment:")
        for dim in self.model.dimensions:
            # assign id, made from dimension.name and a UUID
            dimension_id = make_registry_id([dim.name.lower().replace(" ", "_")])
            check_config_id_loose(dimension_id, "Dimension")
            dim.dimension_id = dimension_id
