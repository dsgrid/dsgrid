import abc
import logging
import os
import shutil
from pathlib import Path

from .config_base import ConfigWithDataFilesBase
from .dimensions import (
    TimeDimensionModel,
    DimensionModel,
    DimensionType,
)
from dsgrid.data_models import serialize_model
from dsgrid.exceptions import DSGInvalidOperation
from dsgrid.utils.files import dump_data, load_data

logger = logging.getLogger(__name__)


class DimensionBaseConfig(ConfigWithDataFilesBase, abc.ABC):
    """Base class for dimension configs"""

    @staticmethod
    def config_filename():
        return "dimension.toml"

    @property
    def config_id(self):
        return self.model.dimension_id


class DimensionConfig(DimensionBaseConfig):
    """Provides an interface to a DimensionModel."""

    @staticmethod
    def model_class():
        return DimensionModel


class TimeDimensionConfig(DimensionBaseConfig):
    """Provides an interface to a TimeDimensionModel."""

    @staticmethod
    def model_class():
        return TimeDimensionModel

    def serialize(self, path, force=False):
        model_data = serialize_model(self.model)
        filename = path / self.config_filename()
        if filename.exists() and not force:
            raise DSGInvalidOperation(f"{filename} exists. Set force=True to overwrite.")
        dump_data(model_data, filename)
        return filename


def get_dimension_config(model, src_dir):
    if isinstance(model, TimeDimensionModel):
        return TimeDimensionConfig(model)
    elif isinstance(model, DimensionModel):
        config = DimensionConfig(model)
        config.src_dir = src_dir
        return config
    assert False, type(model)


def load_dimension_config(filename):
    data = load_data(filename)
    if data["type"] == DimensionType.TIME:
        return TimeDimensionConfig.load(filename)
    return DimensionConfig.load(filename)
