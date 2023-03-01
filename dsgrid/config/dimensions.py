import abc
import csv
import importlib
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, List, Optional

from pydantic import validator, root_validator
from pydantic import Field

from dsgrid.data_models import DSGBaseModel
from dsgrid.dimension.base_models import DimensionType
from dsgrid.dimension.time import (
    LeapDayAdjustmentType,
    TimeInvervalType,
    MeasurementType,
    TimeZone,
    TimeDimensionType,
    RepresentativePeriodFormat,
)
from dsgrid.registry.common import REGEX_VALID_REGISTRY_NAME, REGEX_VALID_REGISTRY_DISPLAY_NAME
from dsgrid.utils.files import compute_file_hash


logger = logging.getLogger(__name__)


class DimensionBaseModel(DSGBaseModel):
    """Common attributes for all dimensions"""

    name: str = Field(
        title="name",
        description="Dimension name",
        note="Dimension names should be descriptive, memorable, identifiable, and reusable for "
        "other datasets and projects",
        notes=(
            "Only alphanumeric characters and dashes are supported (no underscores or spaces).",
            "The :meth:`~dsgrid.config.dimensions.check_name` validator is used to enforce valid"
            " dimension names.",
        ),
        updateable=False,
    )
    display_name: str = Field(
        title="display_name",
        description="Display name. Source for auto-generated dimension_query_name.",
        note="Dimension display names should be singular noun phrases that are concise and "
        "distinguish the dimension across all dimensions within a project, inclusive of dataset "
        "dimensions, project base dimensions, and project supplemental dimensions. This uniqueness "
        "requirement applies unless the dimension is trivial, that is, contains only a single "
        "record. For trivial dimensions, if the dimension represents a single slice of the data, "
        "e.g., all electricity use or model year 2018, then the convention is to list that "
        "'record' name as the display name, e.g.,'Electricity Use' or '2018'.",
        notes=(
            "Only alphanumeric characters, underscores, and spaces are supported (no "
            "special characters).",
            "The :meth:`~dsgrid.config.dimensions.check_display_name` validator is used to "
            "enforce valid dimension display names.",
        ),
    )
    dimension_query_name: Optional[str] = Field(
        title="dimension_query_name",
        description="Auto-generated query name for SQL queries.",
    )
    dimension_type: DimensionType = Field(
        title="dimension_type",
        alias="type",
        description="Type of the dimension",
        options=DimensionType.format_for_docs(),
    )
    dimension_id: Optional[str] = Field(
        title="dimension_id",
        alias="id",
        description="Unique identifier, generated by dsgrid",
        dsg_internal=True,
        updateable=False,
    )
    module: Optional[str] = Field(
        title="module",
        description="Python module with the dimension class",
        default="dsgrid.dimension.standard",
    )
    class_name: str = Field(
        title="class_name",
        description="Dimension record model class name",
        alias="class",
        notes=(
            "The dimension class defines the expected and allowable fields (and their data types)"
            " for the dimension records file.",
            "All dimension records must have a 'id' and 'name' field."
            "Some dimension classes support additional fields that can be used for mapping,"
            " querying, display, etc.",
            "dsgrid in online-mode only supports dimension classes defined in the"
            " :mod:`dsgrid.dimension.standard` module. If dsgrid does not currently support a"
            " dimension class that you require, please contact the dsgrid-coordination team to"
            " request a new class feature",
        ),
    )
    cls: Optional[Any] = Field(
        title="cls",
        description="Dimension record model class",
        alias="dimension_class",
        dsg_internal=True,
    )
    description: str = Field(
        title="description",
        description="A description of the dimension records that is helpful, memorable, and "
        "identifiable",
        notes=(
            "The description will get stored in the dimension record registry and may be used"
            " when searching the registry.",
        ),
    )

    @validator("name")
    def check_name(cls, name):
        if name == "":
            raise ValueError(f'Empty name field for dimension: "{cls}"')

        if REGEX_VALID_REGISTRY_NAME.search(name) is None:
            raise ValueError(f"dimension name={name} does not meet the requirements")

        # TODO: improve validation for allowable dimension record names.
        prohibited_names = [x.value.replace("_", "") for x in DimensionType] + [
            "county",
            "counties",
            "year",
            "hourly",
            "comstock",
            "resstock",
            "tempo",
            "model",
            "source",
            "data-source",
            "dimension",
        ]
        prohibited_names = prohibited_names + [x + "s" for x in prohibited_names]
        if name.lower().replace(" ", "-") in prohibited_names:
            raise ValueError(
                f"""
                 Dimension name '{name}' is not descriptive enough for a dimension record name.
                 Please be more descriptive in your naming.
                 Hint: try adding a vintage, or other distinguishable text that will be this dimension memorable,
                 identifiable, and reusable for other datasets and projects.
                 e.g., 'time-2012-est-houlry-periodending-nodst-noleapdayadjustment-mean' is a good descriptive name.
                 """
            )
        return name

    @validator("display_name")
    def check_display_name(cls, display_name):
        if display_name == "":
            raise ValueError(f'Empty name field for dimension: "{cls}"')
        if REGEX_VALID_REGISTRY_DISPLAY_NAME.search(display_name) is None:
            raise ValueError(f"display_name={display_name} does not meet the requirements")
        return display_name

    @validator("dimension_query_name")
    def check_query_name(cls, dimension_query_name, values):
        if "display_name" not in values:
            return dimension_query_name

        generated_query_name = values["display_name"].lower().replace(" ", "_").replace("-", "_")

        if dimension_query_name is not None and dimension_query_name != generated_query_name:
            raise ValueError(
                f"dimension_query_name cannot be set by the user: {dimension_query_name}"
            )

        return generated_query_name

    @validator("module", always=True)
    def check_module(cls, module):
        if not module.startswith("dsgrid"):
            raise ValueError("Only dsgrid modules are supported as a dimension module.")
        return module

    @validator("class_name", always=True)
    def get_dimension_class_name(cls, class_name, values):
        """Set class_name based on inputs."""
        if "name" not in values:
            # An error occurred with name. Ignore everything else.
            return class_name

        if "module" not in values:
            # An error occurred with module. Ignore everything else.
            return class_name

        mod = importlib.import_module(values["module"])
        cls_name = class_name or values["name"]
        if not hasattr(mod, cls_name):
            if class_name is None:
                msg = (
                    f'There is no class "{cls_name}" in module: {mod}.'
                    "\nIf you are using a unique dimension name, you must "
                    "specify the dimension class."
                )
            else:
                msg = f"dimension class {class_name} not in {mod}"
            raise ValueError(msg)

        return cls_name

    @validator("cls", always=True)
    def get_dimension_class(cls, dim_class, values):
        # Return if an error has already occurred
        for req in ("name", "module", "class_name"):
            if values.get(req) is None:
                return dim_class

        if dim_class is not None:
            raise ValueError(f"cls={dim_class} should not be set")

        return getattr(
            importlib.import_module(values["module"]),
            values["class_name"],
        )


class DimensionModel(DimensionBaseModel):
    """Defines a non-time dimension"""

    filename: str = Field(
        title="filename",
        alias="file",
        description="Filename containing dimension records",
    )
    file_hash: Optional[str] = Field(
        title="file_hash",
        description="Hash of the contents of the file",
        dsg_internal=True,
    )
    records: Optional[List] = Field(
        title="records",
        description="Dimension records in filename that get loaded at runtime",
        dsg_internal=True,
        default=[],
    )

    @validator("filename")
    def check_file(cls, filename):
        """Validate that dimension file exists and has no errors"""
        if not os.path.isfile(filename):
            raise ValueError(f"file {filename} does not exist")

        return filename

    @validator("file_hash")
    def compute_file_hash(cls, file_hash, values):
        if "filename" not in values:
            # TODO #194
            # We are getting here for Time. That shouldn't be happening.
            # This seems to work, but something is broken.
            return None
        return file_hash or compute_file_hash(values["filename"])

    @validator("records", always=True)
    def add_records(cls, records, values):
        """Add records from the file."""
        prereqs = ("name", "filename", "cls")
        for req in prereqs:
            if values.get(req) is None:
                return records

        filename = Path(values["filename"])
        dim_class = values["cls"]
        assert not str(filename).startswith("s3://"), "records must exist in the local filesystem"

        if records:
            raise ValueError("records should not be defined in the dimension config")

        records = []
        if filename.name.endswith(".csv"):
            with open(filename, encoding="utf") as f_in:
                ids = set()
                reader = csv.DictReader(f_in)
                for row in reader:
                    record = dim_class(**row)
                    if record.id in ids:
                        raise ValueError(f"{record.id} is listed multiple times")
                    ids.add(record.id)
                    records.append(record)
        else:
            raise ValueError(f"only CSV is supported: {filename}")

        return records

    def dict(self, *args, **kwargs):
        return super().dict(*args, **self._handle_kwargs(**kwargs))

    def json(self, *args, **kwargs):
        return super().json(*args, **self._handle_kwargs(**kwargs))

    @staticmethod
    def _handle_kwargs(**kwargs):
        exclude = {"cls", "records"}
        if "exclude" in kwargs and kwargs["exclude"] is not None:
            kwargs["exclude"].union(exclude)
        else:
            kwargs["exclude"] = exclude
        return kwargs


class TimeRangeModel(DSGBaseModel):
    """Defines a continuous range of time."""

    # This uses str instead of datetime because this object doesn't have the ability
    # to serialize/deserialize by itself (no str-format).
    # We use the DatetimeRange object during processing.
    start: str = Field(
        title="start",
        description="First timestamp in the data",
    )
    end: str = Field(
        title="end",
        description="Last timestamp in the data (inclusive)",
    )


class MonthRangeModel(DSGBaseModel):
    """Defines a continuous range of time."""

    # This uses str instead of datetime because this object doesn't have the ability
    # to serialize/deserialize by itself (no str-format).
    # We use the DatetimeRange object during processing.
    start: int = Field(
        title="start",
        description="First month in the data (January is 1, December is 12)",
    )
    end: int = Field(
        title="end",
        description="Last month in the data (inclusive)",
    )


class TimeDimensionBaseModel(DimensionBaseModel, abc.ABC):
    """Defines a base model common to all time dimensions."""

    time_type: TimeDimensionType = Field(
        title="time_type",
        default=TimeDimensionType.DATETIME,
        description="""
        Type of time dimension:
            datetime, annual, representative_period, noop
        """,
        options=TimeDimensionType.format_for_docs(),
    )

    def dict(self, *args, **kwargs):
        return super().dict(*args, **self._handle_kwargs(**kwargs))

    def json(self, *args, **kwargs):
        return super().json(*args, **self._handle_kwargs(**kwargs))

    @staticmethod
    def _handle_kwargs(**kwargs):
        exclude = {"cls"}
        if "exclude" in kwargs and kwargs["exclude"] is not None:
            kwargs["exclude"].union(exclude)
        else:
            kwargs["exclude"] = exclude
        return kwargs

    @abc.abstractmethod
    def is_time_zone_required_in_geography(self):
        """Returns True if the geography dimension records must contain a time_zone column."""


class DateTimeDimensionModel(TimeDimensionBaseModel):
    """Defines a time dimension where timestamps translate to datetime objects."""

    measurement_type: MeasurementType = Field(
        title="measurement_type",
        default=MeasurementType.TOTAL,
        description="""
        The type of measurement represented by a value associated with a timestamp:
            mean, min, max, measured, total
        """,
        options=MeasurementType.format_for_docs(),
    )
    str_format: Optional[str] = Field(
        title="str_format",
        default="%Y-%m-%d %H:%M:%s",
        description="Timestamp string format",
        notes=(
            "The string format is used to parse the timestamps provided in the time ranges."
            "Cheatsheet reference: `<https://strftime.org/>`_.",
        ),
    )
    frequency: timedelta = Field(
        title="frequency",
        description="Resolution of the timestamps",
        notes=(
            "Reference: `Datetime timedelta objects"
            " <https://docs.python.org/3/library/datetime.html#timedelta-objects>`_",
        ),
    )
    ranges: List[TimeRangeModel] = Field(
        title="time_ranges",
        description="Defines the continuous ranges of time in the data, inclusive of start and end time.",
    )
    leap_day_adjustment: Optional[LeapDayAdjustmentType] = Field(
        title="leap_day_adjustment",
        description="Leap day adjustment method applied to time data",
        default=LeapDayAdjustmentType.NONE,
        optional=True,
        options=LeapDayAdjustmentType.format_descriptions_for_docs(),
        notes=(
            "The dsgrid default is None, i.e., no adjustment made to leap years.",
            "Adjustments are made to leap years only.",
        ),
    )
    time_interval_type: TimeInvervalType = Field(
        title="time_interval",
        description="The range of time that the value associated with a timestamp represents, e.g., period-beginning",
        options=TimeInvervalType.format_descriptions_for_docs(),
    )
    timezone: TimeZone = Field(
        title="timezone",
        description="""
        Time zone of data:
            UTC,
            HawaiiAleutianStandard,
            AlaskaStandard, AlaskaPrevailing,
            PacificStandard, PacificPrevailing,
            MountainStandard, MountainPrevailing,
            CentralStandard, CentralPrevailing,
            EasternStandard, EasternPrevailing,
        """,
        options=TimeZone.format_descriptions_for_docs(),
    )

    @root_validator(pre=False, skip_on_failure=True)
    def check_time_type_and_class_consistency(cls, values):
        return _check_time_type_and_class_consistency(values)

    @root_validator(pre=False, skip_on_failure=True)
    def check_frequency(cls, values):
        if values["frequency"] in [timedelta(days=365), timedelta(days=366)]:
            raise ValueError(
                f'frequency={values["frequency"]}, 365 or 366 days not allowed, '
                "use class=AnnualTime, time_type=annual to specify a year series."
            )
        return values

    @validator("ranges")
    def check_times(cls, ranges, values):
        if "str_format" not in values or "frequency" not in values:
            return ranges
        return _check_time_ranges(ranges, values["str_format"], values["frequency"])

    def is_time_zone_required_in_geography(self):
        return self.timezone == TimeZone.NONE


class AnnualTimeDimensionModel(TimeDimensionBaseModel):
    """Defines an annual time dimension where timestamps are years."""

    time_type: TimeDimensionType = Field(default=TimeDimensionType.ANNUAL)
    measurement_type: MeasurementType = Field(
        title="measurement_type",
        default=MeasurementType.TOTAL,
        description="""
        The type of measurement represented by a value associated with a timestamp:
            mean, min, max, measured, total
        """,
        options=MeasurementType.format_for_docs(),
    )
    str_format: Optional[str] = Field(
        title="str_format",
        default="%Y",
        description="Timestamp string format",
        notes=(
            "The string format is used to parse the timestamps provided in the time ranges."
            "Cheatsheet reference: `<https://strftime.org/>`_.",
        ),
    )
    frequency: int = Field(
        title="frequency",
        default=1,
        description="Resolution of the timestamps",
        notes=(
            "Frequency of data reporting, in years",
        ),
    ) # HERE -- list_time_ranges (generally mash up what's done for DateTimeDimensionModel into annual_time)
    ranges: List[TimeRangeModel] = Field(
        title="time_ranges",
        description="Defines the contiguous ranges of time in the data, inclusive of start and end time.",
    )
    include_leap_day: bool = Field(
        title="include_leap_day",
        default=False,
        description="Whether annual time includes leap day.",
    )

    @root_validator(pre=False)
    def check_time_type_and_class_consistency(cls, values):
        return _check_time_type_and_class_consistency(values)

    @validator("ranges")
    def check_times(cls, ranges, values):
        if "str_format" not in values:
            return ranges
        return _check_time_ranges(ranges, values["str_format"], timedelta(days=365))

    def is_time_zone_required_in_geography(self):
        return False


class RepresentativePeriodTimeDimensionModel(TimeDimensionBaseModel):
    """Defines a representative time dimension."""

    measurement_type: MeasurementType = Field(
        title="measurement_type",
        default=MeasurementType.TOTAL,
        description="""
        The type of measurement represented by a value associated with a timestamp:
            mean, min, max, measured, total
        """,
        options=MeasurementType.format_for_docs(),
    )
    format: RepresentativePeriodFormat = Field(
        title="format",
        description="Format of the timestamps in the load data",
    )
    ranges: List[MonthRangeModel] = Field(
        title="ranges",
        description="Defines the continuous ranges of time in the data, inclusive of start and end time.",
    )
    time_interval_type: TimeInvervalType = Field(
        title="time_interval",
        description="The range of time that the value associated with a timestamp represents",
        options=TimeInvervalType.format_descriptions_for_docs(),
    )

    def is_time_zone_required_in_geography(self):
        return True


class NoOpTimeDimensionModel(TimeDimensionBaseModel):
    """Defines a NoOp time dimension."""

    time_type: TimeDimensionType = Field(default=TimeDimensionType.NOOP)

    @root_validator(pre=False)
    def check_time_type_and_class_consistency(cls, values):
        return _check_time_type_and_class_consistency(values)

    def is_time_zone_required_in_geography(self):
        return False


class DimensionReferenceModel(DSGBaseModel):
    """Reference to a dimension stored in the registry"""

    dimension_type: DimensionType = Field(
        title="dimension_type",
        alias="type",
        description="Type of the dimension",
        options=DimensionType.format_for_docs(),
    )
    dimension_id: str = Field(
        title="dimension_id",
        description="Unique ID of the dimension in the registry",
        notes=(
            "The dimension ID is generated by dsgrid when a dimension is registered and it is a"
            " concatenation of the user-provided name and an auto-generated UUID.",
            "Only alphanumerics and dashes are supported.",
        ),
    )
    version: str = Field(
        title="version",
        description="Version of the dimension",
        requirements=(
            "The version string must be in semver format (e.g., '1.0.0') and it must be "
            " a valid/existing version in the registry.",
        ),
        # TODO: add notes about warnings for outdated versions DSGRID-189 & DSGRID-148
    )


class DimensionReferenceByNameModel(DSGBaseModel):
    """Reference to a dimension that has yet to be registered."""

    dimension_type: DimensionType = Field(
        title="dimension_type",
        alias="type",
        description="Type of the dimension",
        options=DimensionType.format_for_docs(),
    )
    name: str = Field(
        title="name",
        description="Dimension name",
    )


def handle_dimension_union(value):
    if isinstance(value, DimensionBaseModel):
        return value

    # NOTE: Errors inside DimensionModel or DateTimeDimensionModel will be duplicated by Pydantic
    if value["type"] == DimensionType.TIME.value:
        if value["time_type"] == TimeDimensionType.DATETIME.value:
            val = DateTimeDimensionModel(**value)
        elif value["time_type"] == TimeDimensionType.ANNUAL.value:
            val = AnnualTimeDimensionModel(**value)
        elif value["time_type"] == TimeDimensionType.REPRESENTATIVE_PERIOD.value:
            val = RepresentativePeriodTimeDimensionModel(**value)
        elif value["time_type"] == TimeDimensionType.NOOP.value:
            val = NoOpTimeDimensionModel(**value)
        else:
            options = [x.value for x in TimeDimensionType]
            raise ValueError(f"{value['time_type']} not supported, valid options: {options}")
    else:
        val = DimensionModel(**value)
    return val


def _check_time_ranges(ranges: list, str_format: str, frequency: timedelta):
    assert isinstance(frequency, timedelta)
    for time_range in ranges:
        # Make sure start and end time parse.
        start = datetime.strptime(time_range.start, str_format)
        end = datetime.strptime(time_range.end, str_format)
        if str_format == "%Y":
            if frequency != timedelta(days=365):
                raise ValueError(f"str_format={str_format} is inconsistent with {frequency}")
        # There may be other special cases to handle.
        elif (end - start) % frequency != timedelta(0):
            raise ValueError(f"time range {time_range} is inconsistent with {frequency}")

    return ranges


# TODO: modify as model works with more time_type schema
def _check_time_type_and_class_consistency(values):
    if (
        (values["class_name"] == "Time" and values["time_type"] == TimeDimensionType.DATETIME)
        or (
            values["class_name"] == "AnnualTime"
            and values["time_type"] == TimeDimensionType.ANNUAL
        )
        or (values["class_name"] == "NoOpTime" and values["time_type"] == TimeDimensionType.NOOP)
    ):
        pass
    else:
        raise ValueError(
            f'time_type={values["time_type"].value} does not match class_name={values["class_name"]}. \n'
            " * For class=Time, use time_type=datetime. \n"
            " * For class=AnnualTime, use time_type=annual. \n"
            " * For class=NoOpTime, use time_type=noop. "
        )
    return values


class DimensionCommonModel(DSGBaseModel):
    """Common attributes for all dimensions"""

    name: str
    display_name: str
    dimension_query_name: str
    dimension_type: DimensionType
    dimension_id: str
    class_name: str
    description: str


class ProjectDimensionModel(DimensionCommonModel):
    """Common attributes for all dimensions that are assigned to a project"""

    is_base: bool


def create_dimension_common_model(model):
    """Constructs an instance of DimensionBaseModel from subclasses in order to give the API
    one common model for all dimensions. Avoids the complexity of dealing with
    DimensionBaseModel validators.
    """
    fields = set(DimensionCommonModel.__fields__)
    data = {x: getattr(model, x) for x in type(model).__fields__ if x in fields}
    return DimensionCommonModel(**data)


def create_project_dimension_model(model, is_base):
    data = create_dimension_common_model(model).dict()
    data["is_base"] = is_base
    return ProjectDimensionModel(**data)
