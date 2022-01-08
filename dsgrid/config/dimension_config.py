import abc
import datetime
import logging
import os
import shutil
from pathlib import Path

import pytz
import pandas as pd

from .config_base import ConfigBase, ConfigWithDataFilesBase
from .dimensions import (
    TimeDimensionModel,
    DimensionModel,
    DimensionType,
    AnnualTimeDimensionModel,
    RepresentativePeriodTimeDimensionModel,
)
from dsgrid.data_models import serialize_model, ExtendedJSONEncoder
from dsgrid.dimension.time import (
    DatetimeRange,
    AnnualTimeRange,
    TimeZone,
    TimeDimensionType,
    make_time_range,
)
from dsgrid.exceptions import DSGInvalidOperation
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


class TimeDimensionBaseConfig(DimensionBaseConfigWithoutFiles):
    """Base class for all time dimension configs"""


class TimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to a TimeDimensionModel."""

    @staticmethod
    def model_class():
        return TimeDimensionModel

    def get_time_ranges(self):
        """Return time ranges with timezone applied.

        Returns
        -------
        list
            list of DatetimeRange

        """
        ranges = []
        tz = self.get_tzinfo()
        for time_range in self.model.ranges:

            start = datetime.datetime.strptime(time_range.start, self.model.str_format)
            start = pd.Timestamp(start, tz=tz)
            end = datetime.datetime.strptime(time_range.end, self.model.str_format)
            end = pd.Timestamp(end, tz=tz)
            ranges.append(
                make_time_range(
                    start=start,
                    end=end,
                    frequency=self.model.frequency,
                    leap_day_adjustment=self.model.leap_day_adjustment,
                )
            )

        return ranges

    def list_time_range(self, time_range: DatetimeRange):
        """Return a list of datetimes for a time range.

        Parameters
        ----------
        time_range : DatetimeRange

        Returns
        -------
        list
            list of datetime

        """
        return time_range.list_time_range()

    def get_tzinfo(self):
        """Return a tzinfo instance for the range specified for this dimension.

        Returns
        -------
        tzinfo

        """
        # assert self.model.ranges_timezone is not TimeZone.LOCAL
        return self.model.ranges_timezone.tz

    def get_data_tzinfo(self):
        """Return the tzinfo for this dimension as it appears in the data.

        Returns
        -------
        tzinfo

        """
        assert self.model.timezone is not TimeZone.LOCAL
        return self.model.timezone.tz


class AnnualTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to an AnnualTimeDimensionModel."""

    @staticmethod
    def model_class():
        return AnnualTimeDimensionModel


class RepresentativePeriodTimeDimensionConfig(TimeDimensionBaseConfig):
    """Provides an interface to an RepresentativePeriodTimeDimensionModel."""

    @staticmethod
    def model_class():
        return RepresentativePeriodTimeDimensionModel


def get_dimension_config(model, src_dir):
    if isinstance(model, TimeDimensionModel):
        return TimeDimensionConfig(model)
    if isinstance(model, AnnualTimeDimensionModel):
        return AnnualTimeDimensionConfig(model)
    if isinstance(model, RepresentativePeriodTimeDimensionModel):
        return RepresentativePeriodTimeDimensionConfig(model)
    elif isinstance(model, DimensionModel):
        config = DimensionConfig(model)
        config.src_dir = src_dir
        return config
    assert False, type(model)


def load_dimension_config(filename):
    """Loads a dimension config file before the exact type is known.

    Parameters
    ----------
    filename : Path

    Returns
    -------
    DimensionBaseConfig

    """
    data = load_data(filename)
    if data["type"] == DimensionType.TIME.value:
        if data["time_type"] == TimeDimensionType.DATETIME.value:
            return TimeDimensionConfig.load(filename)
        elif data["time_type"] == TimeDimensionType.ANNUAL.value:
            return AnnualTimeDimensionConfig.load(filename)
        elif data["time_type"] == TimeDimensionType.REPRESENTATIVE_PERIOD.value:
            return RepresentativePeriodTimeDimensionConfig.load(filename)
        else:
            raise ValueError(f"time_type={data['time_type']} not supported")

    return DimensionConfig.load(filename)
