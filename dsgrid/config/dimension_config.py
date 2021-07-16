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
)
from dsgrid.data_models import serialize_model, ExtendedJSONEncoder
from dsgrid.dimension.time import DatetimeRange, TimezoneType
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

    TIMEZONE_TO_TZINFO = {
        TimezoneType.UTC: datetime.timezone(datetime.timedelta()),
        TimezoneType.HST: datetime.timedelta(hours=-10),
        TimezoneType.AST: datetime.timezone(datetime.timedelta(hours=-9)),
        TimezoneType.APT: pytz.timezone("US/Alaska"),
        TimezoneType.PST: datetime.timezone(datetime.timedelta(hours=-8)),
        TimezoneType.PPT: pytz.timezone("US/Pacific"),
        TimezoneType.MST: datetime.timezone(datetime.timedelta(hours=-7)),
        TimezoneType.MPT: pytz.timezone("US/Mountain"),
        TimezoneType.CST: datetime.timezone(datetime.timedelta(hours=-6)),
        TimezoneType.CPT: pytz.timezone("US/Central"),
        TimezoneType.EST: datetime.timezone(datetime.timedelta(hours=-5)),
        TimezoneType.EPT: pytz.timezone("US/Eastern"),
        # TimezoneType.LOCAL: None,  # TODO: needs handling: DSGRID-171
    }

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
            end = datetime.datetime.strptime(time_range.end, self.model.str_format)
            ranges.append(
                DatetimeRange(
                    start=start.replace(tzinfo=tz),
                    end=end.replace(tzinfo=tz),
                    frequency=self.model.frequency,
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
        return time_range.list_time_range(self.model.period, self.model.leap_day_adjustment)

    def get_tzinfo(self):
        """Return a tzinfo instance for this dimension.

        Returns
        -------
        tzinfo

        """
        assert self.model.timezone in self.TIMEZONE_TO_TZINFO, self.model.timezone
        return self.TIMEZONE_TO_TZINFO[self.model.timezone]


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
