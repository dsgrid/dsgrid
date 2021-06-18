import abc
import datetime
import logging
import os
import shutil
from pathlib import Path

import pytz

from .config_base import ConfigWithDataFilesBase
from .dimensions import (
    TimeDimensionModel,
    DimensionModel,
    DimensionType,
    TimeRange,
)
from dsgrid.data_models import serialize_model, ExtendedJSONEncoder
from dsgrid.dimension.time import TimezoneType
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
        model_data = serialize_model(self.model, exclude={"dimension_cls"})
        filename = path / self.config_filename()
        if filename.exists() and not force:
            raise DSGInvalidOperation(f"{filename} exists. Set force=True to overwrite.")
        dump_data(model_data, filename)
        return filename

    def get_time_ranges(self):
        ranges = []
        tz = self.get_tzinfo()
        for time_range in self.model.ranges:
            start = time_range.start.replace(tzinfo=tz)
            end = time_range.end.replace(tzinfo=tz)
            ranges.append(TimeRange(start=start, end=end))

        return ranges

    def get_tzinfo(self):
        """Return a tzinfo instance for this dimension.

        Returns
        -------
        tzinfo

        """
        if self.model.timezone == TimezoneType.EST:
            if self.model.includes_dst:
                return pytz.timezone("US/Eastern")
            else:
                return datetime.timezone(datetime.timedelta(hours=-5))
        elif self.model.timezone == TimezoneType.CST:
            if self.model.includes_dst:
                return pytz.timezone("US/Central")
            else:
                return datetime.timezone(datetime.timedelta(hours=-6))
        elif self.model.timezone == TimezoneType.MST:
            if self.model.includes_dst:
                return pytz.timezone("US/Mountain")
            else:
                return datetime.timezone(datetime.timedelta(hours=-7))
        elif self.model.timezone == TimezoneType.PST:
            if self.model.includes_dst:
                return pytz.timezone("US/Pacific")
            else:
                return datetime.timezone(datetime.timedelta(hours=-8))
        else:
            assert False, tz_type


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
