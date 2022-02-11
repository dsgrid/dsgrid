import abc
import logging
import os
import shutil
from collections import namedtuple
from datetime import datetime, timedelta
from pathlib import Path

import pytz
import pandas as pd
import pyspark.sql.functions as F

from .config_base import ConfigBase, ConfigWithDataFilesBase
from .dimensions import DimensionModel
from dsgrid.data_models import serialize_model, ExtendedJSONEncoder
from dsgrid.exceptions import DSGInvalidDataset, DSGInvalidOperation
from dsgrid.utils.files import dump_data, load_data

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
