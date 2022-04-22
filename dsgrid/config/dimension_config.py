import abc
import logging

from .config_base import ConfigBase, ConfigWithDataFilesBase
from .dimensions import DimensionModel

logger = logging.getLogger(__name__)


class DimensionBaseConfigWithFiles(ConfigWithDataFilesBase, abc.ABC):
    """Base class for dimension configs"""

    @staticmethod
    def config_filename():
        return "dimension.toml"

    @property
    def config_id(self):
        return self.model.dimension_id

    @staticmethod
    def data_file_fields():
        return ["filename"]

    @staticmethod
    def data_files_fields():
        return []


class DimensionBaseConfigWithoutFiles(ConfigBase, abc.ABC):
    """Base class for dimension configs"""

    @staticmethod
    def config_filename():
        return "dimension.toml"

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
