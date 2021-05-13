import abc
import logging
import os
import shutil
from pathlib import Path

from .config_base import ConfigBase
from .dimensions import (
    TimeDimensionModel,
    DimensionModel,
    DimensionType,
)
from dsgrid.data_models import serialize_model
from dsgrid.utils.files import dump_data, load_data

logger = logging.getLogger(__name__)


class DimensionBaseConfig(ConfigBase, abc.ABC):
    """Base class for dimension configs"""

    @staticmethod
    def config_filename():
        return "dimension.toml"

    @property
    def config_id(self):
        return self.model.dimension_id


class DimensionConfig(DimensionBaseConfig):
    """Provides an interface to a DimensionModel."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._src_dir = None

    @classmethod
    def load(cls, config_file):
        config = cls._load(config_file)
        config.src_dir = os.path.dirname(config_file)
        return config

    @staticmethod
    def model_class():
        return DimensionModel

    def serialize(self, path):
        model_data = serialize_model(self.model)
        orig_file = getattr(self.model, "filename")

        # Leading directories from the original are not relevant in the registry.
        dst_record_file = path / os.path.basename(orig_file)
        shutil.copyfile(self._src_dir / self.model.filename, dst_record_file)
        # We have to make this change in the serialized dict instead of
        # model because Pydantic will fail the assignment due to not being
        # able to find the path.
        model_data["file"] = os.path.basename(self.model.filename)

        filename = path / self.config_filename()
        dump_data(model_data, filename)
        return filename

    @property
    def src_dir(self):
        return self._src_dir

    @src_dir.setter
    def src_dir(self, src_dir):
        self._src_dir = Path(src_dir)


class TimeDimensionConfig(DimensionBaseConfig):
    """Provides an interface to a TimeDimensionModel."""

    @staticmethod
    def model_class():
        return TimeDimensionModel

    def serialize(self, path):
        model_data = self.model.dict(by_alias=True)
        filename = path / self.config_filename()
        dump_data(model_data, filename)
        return filename


def get_dimension_config(model, src_dir):
    if isinstance(model, TimeDimensionModel):
        return TimeDimensionConfig(model)
    elif isinstance(model, DimensionModel):
        config = DimensionConfig(model)
        config.src_dir = src_dir
        return config
    assert False, str(type(model))


def load_dimension_config(filename):
    data = load_data(filename)
    if data["type"] == DimensionType.TIME:
        return TimeDimensionConfig.load(filename)
    return DimensionConfig.load(filename)
