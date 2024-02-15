import abc
import logging
from typing import Union

from .config_base import ConfigBase, ConfigWithRecordFileBase
from .dimensions import DimensionModel

logger = logging.getLogger(__name__)


class DimensionBaseConfigWithFiles(ConfigWithRecordFileBase, abc.ABC):
    """Base class for dimension configs"""

    @staticmethod
    def config_filename():
        return "dimension.json5"

    @property
    def config_id(self):
        return self.model.dimension_id


class DimensionBaseConfigWithoutFiles(ConfigBase, abc.ABC):
    """Base class for dimension configs"""

    @staticmethod
    def config_filename():
        return "dimension.json5"

    @property
    def config_id(self):
        return self.model.dimension_id


class DimensionConfig(DimensionBaseConfigWithFiles):
    """Provides an interface to a DimensionModel."""

    @staticmethod
    def model_class():
        return DimensionModel

    def get_unique_ids(self):
        """Return the unique IDs in a dimension's records.

        Returns
        -------
        set
            set of str

        """
        return {x.id for x in self.model.records}


DimensionBaseConfig = Union[DimensionBaseConfigWithFiles, DimensionBaseConfigWithoutFiles]
