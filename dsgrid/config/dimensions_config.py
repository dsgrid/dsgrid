import logging
from pathlib import Path

from pydantic import field_validator, Field

from dsgrid.data_models import DSGBaseModel
from dsgrid.utils.utilities import check_uniqueness
from .config_base import ConfigBase
from .dimensions import DimensionModel, DimensionsListModel

logger = logging.getLogger(__name__)


class DimensionsConfigModel(DSGBaseModel):
    """Represents multiple dimension models.

    Used when registering multiple dimensions in one command.
    """

    dimensions: DimensionsListModel = Field(
        title="dimensions",
        description="Dimensions for submission to the dimension registry",
    )

    @field_validator("dimensions")
    @classmethod
    def check_files(cls, values: dict) -> dict:
        """Validate dimension files are unique across all dimensions"""
        check_uniqueness(
            (x.filename for x in values if isinstance(x, DimensionModel) and x.filename),
            "dimension record filename",
        )
        return values

    @field_validator("dimensions")
    @classmethod
    def check_names(cls, values: dict) -> dict:
        """Validate dimension names are unique across all dimensions."""
        check_uniqueness(
            [dim.name for dim in values],
            "dimension record name",
        )
        return values


class DimensionsConfig(ConfigBase):
    """Provides an interface to a DimensionsConfigModel."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._src_dir = None

    @staticmethod
    def config_filename():
        return "dimensions.json5"

    @property
    def config_id(self):
        assert False, "not correct for this class"

    @staticmethod
    def model_class():
        return DimensionsConfigModel

    @classmethod
    def load(cls, config_filename: Path, *args, **kwargs):
        return super().load(config_filename, *args, **kwargs)

    @classmethod
    def load_from_model(cls, model):
        return cls(model)
