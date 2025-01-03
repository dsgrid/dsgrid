import abc
import csv
import importlib
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Optional, Union, Literal
import copy

from pydantic import field_serializer, field_validator, model_validator, Field, ValidationInfo
from pydantic.functional_validators import BeforeValidator
from typing_extensions import Annotated

from dsgrid.data_models import DSGBaseDatabaseModel, DSGBaseModel
from dsgrid.dimension.base_models import DimensionType, DimensionCategory
from dsgrid.dimension.time import (
    TimeIntervalType,
    MeasurementType,
    TimeZone,
    TimeDimensionType,
    RepresentativePeriodFormat,
    DatetimeFormat,
)
from dsgrid.registry.common import REGEX_VALID_REGISTRY_NAME, REGEX_VALID_REGISTRY_DISPLAY_NAME
from dsgrid.utils.files import compute_file_hash
from dsgrid.utils.utilities import convert_record_dicts_to_classes


logger = logging.getLogger(__name__)


class DimensionBaseModel(DSGBaseDatabaseModel):
    """Common attributes for all dimensions"""

    name: str = Field(
        title="name",
        description="Dimension name",
        json_schema_extra={
            "note": "Dimension names should be descriptive, memorable, identifiable, and reusable for "
            "other datasets and projects",
            "notes": (
                "Only alphanumeric characters and dashes are supported (no underscores or spaces).",
                "The :meth:`~dsgrid.config.dimensions.check_name` validator is used to enforce valid"
                " dimension names.",
            ),
            "updateable": False,
        },
    )
    display_name: str = Field(
        title="display_name",
        description="Display name. Source for auto-generated dimension_query_name.",
        json_schema_extra={
            "note": "Dimension display names should be singular noun phrases that are concise and "
            "distinguish the dimension across all dimensions within a project, inclusive of dataset "
            "dimensions, project base dimensions, and project supplemental dimensions. This uniqueness "
            "requirement applies unless the dimension is trivial, that is, contains only a single "
            "record. For trivial dimensions, if the dimension represents a single slice of the data, "
            "e.g., all electricity use or model year 2018, then the convention is to list that "
            "'record' name as the display name, e.g.,'Electricity Use' or '2018'.",
            "notes": (
                "Only alphanumeric characters, underscores, and spaces are supported (no "
                "special characters).",
                "The :meth:`~dsgrid.config.dimensions.check_display_name` validator is used to "
                "enforce valid dimension display names.",
            ),
        },
    )
    dimension_query_name: Optional[str] = Field(
        default=None,
        title="dimension_query_name",
        description="Auto-generated query name for SQL queries.",
    )
    dimension_type: DimensionType = Field(
        title="dimension_type",
        alias="type",
        description="Type of the dimension",
        json_schema_extra={
            "options": DimensionType.format_for_docs(),
        },
    )
    dimension_id: Optional[str] = Field(
        default=None,
        title="dimension_id",
        description="Unique identifier, generated by dsgrid",
        json_schema_extra={
            "dsg_internal": True,
            "updateable": False,
        },
    )
    module: str = Field(
        title="module",
        description="Python module with the dimension class",
        default="dsgrid.dimension.standard",
    )
    class_name: str = Field(
        title="class_name",
        description="Dimension record model class name",
        alias="class",
        json_schema_extra={
            "notes": (
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
        },
    )
    cls: Any = Field(
        default=None,
        title="cls",
        description="Dimension record model class",
        alias="dimension_class",
        json_schema_extra={
            "dsgrid_internal": True,
        },
    )
    description: str = Field(
        title="description",
        description="A description of the dimension records that is helpful, memorable, and "
        "identifiable",
        json_schema_extra={
            "notes": (
                "The description will get stored in the dimension record registry and may be used"
                " when searching the registry.",
            ),
        },
    )
    id: Optional[int] = Field(
        default=None,
        description="Registry database ID",
        json_schema_extra={
            "dsgrid_internal": True,
        },
    )

    @field_validator("name")
    @classmethod
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
                 e.g., 'time-2012-est-hourly-periodending-nodst-noleapdayadjustment-mean' is a good descriptive name.
                 """
            )
        return name

    @field_validator("display_name")
    @classmethod
    def check_display_name(cls, display_name):
        return check_display_name(display_name)

    @field_validator("dimension_query_name")
    @classmethod
    def check_query_name(cls, dimension_query_name, info: ValidationInfo):
        if "display_name" not in info.data:
            return dimension_query_name
        return generate_dimension_query_name(dimension_query_name, info.data["display_name"])

    @field_validator("module")
    @classmethod
    def check_module(cls, module) -> "DimensionBaseModel":
        if not module.startswith("dsgrid"):
            raise ValueError("Only dsgrid modules are supported as a dimension module.")
        return module

    @field_validator("class_name")
    @classmethod
    def get_dimension_class_name(cls, class_name, info: ValidationInfo):
        """Set class_name based on inputs."""
        if "module" not in info.data:
            return class_name

        mod = importlib.import_module(info.data["module"])
        if not hasattr(mod, class_name):
            if class_name is None:
                msg = (
                    f'There is no class "{class_name}" in module: {mod}.'
                    "\nIf you are using a unique dimension name, you must "
                    "specify the dimension class."
                )
            else:
                msg = f"dimension class {class_name} not in {mod}"
            raise ValueError(msg)

        return class_name

    @field_validator("cls")
    @classmethod
    def get_dimension_class(cls, dim_class, info: ValidationInfo):
        if "module" not in info.data or "class_name" not in info.data:
            return dim_class

        if dim_class is not None:
            raise ValueError(f"cls={dim_class} should not be set")

        return getattr(
            importlib.import_module(info.data["module"]),
            info.data["class_name"],
        )


class DimensionModel(DimensionBaseModel):
    """Defines a non-time dimension"""

    filename: Optional[str] = Field(
        title="filename",
        alias="file",
        default=None,
        description="Filename containing dimension records. Only assigned for user input and "
        "output purposes. The registry database stores records in the dimension JSON document.",
    )
    file_hash: Optional[str] = Field(
        title="file_hash",
        description="Hash of the contents of the file",
        json_schema_extra={
            "dsgrid_internal": True,
        },
        default=None,
    )
    records: list = Field(
        title="records",
        description="Dimension records in filename that get loaded at runtime",
        json_schema_extra={
            "dsgrid_internal": True,
        },
        default=[],
    )

    @field_validator("filename")
    @classmethod
    def check_file(cls, filename):
        """Validate that dimension file exists and has no errors"""
        if filename is not None:
            if not os.path.isfile(filename):
                raise ValueError(f"file {filename} does not exist")
            if filename.startswith("s3://"):
                raise ValueError("records must exist in the local filesystem, not on S3")
            if not filename.endswith(".csv"):
                raise ValueError(f"only CSV is supported: {filename}")

        return filename

    @field_validator("file_hash")
    @classmethod
    def compute_file_hash(cls, file_hash, info: ValidationInfo):
        if "filename" not in info.data:
            return file_hash

        if file_hash is None:
            file_hash = compute_file_hash(info.data["filename"])
        return file_hash

    @field_validator("records")
    @classmethod
    def add_records(cls, records, info: ValidationInfo):
        """Add records from the file."""
        dim_class = info.data.get("cls")
        if "filename" not in info.data or dim_class is None:
            return records

        if records:
            if isinstance(records[0], dict):
                records = convert_record_dicts_to_classes(
                    records, dim_class, check_duplicates=["id"]
                )
            return records

        with open(info.data["filename"], encoding="utf-8-sig") as f_in:
            records = convert_record_dicts_to_classes(
                csv.DictReader(f_in), dim_class, check_duplicates=["id"]
            )
        return records

    @field_serializer("cls", "filename")
    def serialize_cls(self, val, _):
        return None


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


class IndexRangeModel(DSGBaseModel):
    """Defines a continuous range of indices."""

    start: int = Field(
        title="start",
        description="First of indices",
    )
    end: int = Field(
        title="end",
        description="Last of indices (inclusive)",
    )


class TimeDimensionBaseModel(DimensionBaseModel, abc.ABC):
    """Defines a base model common to all time dimensions."""

    time_type: TimeDimensionType = Field(
        title="time_type",
        default=TimeDimensionType.DATETIME,
        description="Type of time dimension",
        json_schema_extra={
            "options": TimeDimensionType.format_for_docs(),
        },
    )

    @field_serializer("cls")
    def serialize_cls(self, val, _):
        return None

    @abc.abstractmethod
    def is_time_zone_required_in_geography(self):
        """Returns True if the geography dimension records must contain a time_zone column."""


class AlignedTime(DSGBaseModel):
    """Data has absolute timestamps that are aligned with the same start and end
    for each geography."""

    format_type: Literal[DatetimeFormat.ALIGNED] = DatetimeFormat.ALIGNED
    timezone: TimeZone = Field(
        title="timezone",
        description="Time zone of data",
        json_schema_extra={
            "options": TimeZone.format_descriptions_for_docs(),
        },
    )


class LocalTimeAsStrings(DSGBaseModel):
    """Data has absolute timestamps formatted as strings with offsets from UTC.
    They are aligned for each geography when adjusted for time zone but staggered
    in an absolute time scale."""

    format_type: Literal[DatetimeFormat.LOCAL_AS_STRINGS] = DatetimeFormat.LOCAL_AS_STRINGS

    data_str_format: str = Field(
        title="data_str_format",
        default="yyyy-MM-dd HH:mm:ssZZZZZ",
        description="Timestamp string format (for parsing the time column of the dataframe)",
        json_schema_extra={
            "notes": (
                "The string format is used to parse the timestamps in the dataframe while in Spark, "
                "(e.g., yyyy-MM-dd HH:mm:ssZZZZZ). "
                "Cheatsheet reference: `<https://spark.apache.org/docs/latest/sql-ref-datetime-pattern.html>`_.",
            ),
        },
    )

    @field_validator("data_str_format")
    @classmethod
    def check_data_str_format(cls, data_str_format):
        raise NotImplementedError("DatetimeFormat.LOCAL_AS_STRINGS is not fully implemented.")
        dsf = data_str_format
        if (
            "x" not in dsf
            and "X" not in dsf
            and "Z" not in dsf
            and "z" not in dsf
            and "V" not in dsf
            and "O" not in dsf
        ):
            raise ValueError("data_str_format must provide time zone or zone offset.")
        return data_str_format


class DateTimeDimensionModel(TimeDimensionBaseModel):
    """Defines a time dimension where timestamps translate to datetime objects."""

    datetime_format: Union[AlignedTime, LocalTimeAsStrings] = Field(
        title="datetime_format",
        discriminator="format_type",
        description="""
        Format of the datetime used to define the data format, alignment between geography,
        and time zone information.
        """,
    )

    measurement_type: MeasurementType = Field(
        title="measurement_type",
        default=MeasurementType.TOTAL,
        description="""
        The type of measurement represented by a value associated with a timestamp:
            mean, min, max, measured, total
        """,
        json_schema_extra={
            "options": MeasurementType.format_for_docs(),
        },
    )

    str_format: str = Field(
        title="str_format",
        default="%Y-%m-%d %H:%M:%s",
        description="Timestamp string format (for parsing the time ranges)",
        json_schema_extra={
            "notes": (
                "The string format is used to parse the timestamps provided in the time ranges."
                "Cheatsheet reference: `<https://strftime.org/>`_.",
            ),
        },
    )
    frequency: timedelta = Field(
        title="frequency",
        description="Resolution of the timestamps",
        json_schema_extra={
            "notes": (
                "Reference: `Datetime timedelta objects"
                " <https://docs.python.org/3/library/datetime.html#timedelta-objects>`_",
            ),
        },
    )
    ranges: list[TimeRangeModel] = Field(
        title="time_ranges",
        description="Defines the continuous ranges of time in the data, inclusive of start and end time.",
    )
    time_interval_type: TimeIntervalType = Field(
        title="time_interval",
        description="The range of time that the value associated with a timestamp represents, e.g., period-beginning",
        json_schema_extra={
            "options": TimeIntervalType.format_descriptions_for_docs(),
        },
    )

    @model_validator(mode="before")
    @classmethod
    def handle_legacy_fields(cls, values):
        if "leap_day_adjustment" in values:
            if values["leap_day_adjustment"] != "none":
                msg = f"Unknown data_schema format: {values=}"
                raise ValueError(msg)
            values.pop("leap_day_adjustment")

        if "timezone" in values:
            values["datetime_format"] = {
                "format_type": DatetimeFormat.ALIGNED.value,
                "timezone": values["timezone"],
            }
            values.pop("timezone")

        return values

    @model_validator(mode="after")
    def check_frequency(self) -> "DateTimeDimensionModel":
        if self.frequency in [timedelta(days=365), timedelta(days=366)]:
            raise ValueError(
                f"frequency={self.frequency}, datetime config does not allow 365 or 366 days frequency, "
                "use class=AnnualTime, time_type=annual to specify a year series."
            )
        return self

    @field_validator("ranges")
    @classmethod
    def check_times(
        cls, ranges: list[TimeRangeModel], info: ValidationInfo
    ) -> list[TimeRangeModel]:
        if "str_format" not in info.data or "frequency" not in info.data:
            return ranges
        return _check_time_ranges(ranges, info.data["str_format"], info.data["frequency"])

    def is_time_zone_required_in_geography(self):
        return False


class AnnualTimeDimensionModel(TimeDimensionBaseModel):
    """Defines an annual time dimension where timestamps are years."""

    time_type: TimeDimensionType = Field(default=TimeDimensionType.ANNUAL)
    measurement_type: MeasurementType = Field(
        title="measurement_type",
        default=MeasurementType.TOTAL,
        description="""
        The type of measurement represented by a value associated with a timestamp:
            e.g., mean, total
        """,
        json_schema_extra={
            "options": MeasurementType.format_for_docs(),
        },
    )
    str_format: str = Field(
        title="str_format",
        default="%Y",
        description="Timestamp string format",
        json_schema_extra={
            "notes": (
                "The string format is used to parse the timestamps provided in the time ranges."
                "Cheatsheet reference: `<https://strftime.org/>`_.",
            ),
        },
    )
    ranges: list[TimeRangeModel] = Field(
        title="time_ranges",
        description="Defines the contiguous ranges of time in the data, inclusive of start and end time.",
    )
    include_leap_day: bool = Field(
        title="include_leap_day",
        default=False,
        description="Whether annual time includes leap day.",
    )

    @field_validator("ranges")
    @classmethod
    def check_times(
        cls, ranges: list[TimeRangeModel], info: ValidationInfo
    ) -> list[TimeRangeModel]:
        if "str_format" not in info.data or "frequency" not in info.data:
            return ranges
        return _check_time_ranges(ranges, info.data["str_format"], timedelta(days=365))

    @field_validator("measurement_type")
    @classmethod
    def check_measurement_type(cls, measurement_type: MeasurementType) -> MeasurementType:
        # This restriction exists because any other measurement type would require a frequency,
        # and that isn't part of the model definition.
        if measurement_type != MeasurementType.TOTAL:
            msg = f"Annual time currently only supports MeasurementType total: {measurement_type}"
            raise ValueError(msg)
        return measurement_type

    def is_time_zone_required_in_geography(self):
        return False


class RepresentativePeriodTimeDimensionModel(TimeDimensionBaseModel):
    """Defines a representative time dimension."""

    time_type: TimeDimensionType = Field(default=TimeDimensionType.REPRESENTATIVE_PERIOD)
    measurement_type: MeasurementType = Field(
        title="measurement_type",
        default=MeasurementType.TOTAL,
        description="""
        The type of measurement represented by a value associated with a timestamp:
            e.g., mean, total
        """,
        json_schema_extra={
            "options": MeasurementType.format_for_docs(),
        },
    )
    format: RepresentativePeriodFormat = Field(
        title="format",
        description="Format of the timestamps in the load data",
    )
    ranges: list[MonthRangeModel] = Field(
        title="ranges",
        description="Defines the continuous ranges of time in the data, inclusive of start and end time.",
    )
    time_interval_type: TimeIntervalType = Field(
        title="time_interval",
        description="The range of time that the value associated with a timestamp represents",
        json_schema_extra={
            "options": TimeIntervalType.format_descriptions_for_docs(),
        },
    )

    def is_time_zone_required_in_geography(self):
        return True


class IndexTimeDimensionModel(TimeDimensionBaseModel):
    """Defines a time dimension where timestamps are indices."""

    time_type: TimeDimensionType = Field(default=TimeDimensionType.INDEX)
    measurement_type: MeasurementType = Field(
        title="measurement_type",
        default=MeasurementType.TOTAL,
        description="""
        The type of measurement represented by a value associated with a timestamp:
            e.g., mean, total
        """,
        json_schema_extra={
            "options": MeasurementType.format_for_docs(),
        },
    )
    ranges: list[IndexRangeModel] = Field(
        title="ranges",
        description="Defines the continuous ranges of indices of the data, inclusive of start and end index.",
    )
    frequency: timedelta = Field(
        title="frequency",
        description="Resolution of the timestamps for which the ranges represent.",
        json_schema_extra={
            "notes": (
                "Reference: `Datetime timedelta objects"
                " <https://docs.python.org/3/library/datetime.html#timedelta-objects>`_",
            ),
        },
    )
    starting_timestamps: list[str] = Field(
        title="starting timestamps",
        description="Starting timestamp for for each of the ranges.",
    )
    str_format: str = Field(
        title="str_format",
        default="%Y-%m-%d %H:%M:%s",
        description="Timestamp string format",
        json_schema_extra={
            "notes": (
                "The string format is used to parse the starting timestamp provided."
                "Cheatsheet reference: `<https://strftime.org/>`_.",
            ),
        },
    )
    time_interval_type: TimeIntervalType = Field(
        title="time_interval",
        description="The range of time that the value associated with a timestamp represents, e.g., period-beginning",
        json_schema_extra={
            "options": TimeIntervalType.format_descriptions_for_docs(),
        },
    )

    @field_validator("starting_timestamps")
    @classmethod
    def check_timestamps(cls, starting_timestamps, info: ValidationInfo) -> list[str]:
        if len(starting_timestamps) != len(info.data["ranges"]):
            msg = f"{starting_timestamps=} must match the number of ranges."
            raise ValueError(msg)
        return starting_timestamps

    @field_validator("ranges")
    @classmethod
    def check_indices(cls, ranges: list[IndexRangeModel]) -> list[IndexRangeModel]:
        return _check_index_ranges(ranges)

    def is_time_zone_required_in_geography(self):
        return True


class NoOpTimeDimensionModel(TimeDimensionBaseModel):
    """Defines a NoOp time dimension."""

    time_type: TimeDimensionType = TimeDimensionType.NOOP

    def is_time_zone_required_in_geography(self):
        return False


class DimensionReferenceModel(DSGBaseModel):
    """Reference to a dimension stored in the registry"""

    dimension_type: DimensionType = Field(
        title="dimension_type",
        alias="type",
        description="Type of the dimension",
        json_schema_extra={
            "options": DimensionType.format_for_docs(),
        },
    )
    dimension_id: str = Field(
        title="dimension_id",
        description="Unique ID of the dimension in the registry",
        json_schema_extra={
            "notes": (
                "The dimension ID is generated by dsgrid when a dimension is registered.",
                "Only alphanumerics and dashes are supported.",
            ),
        },
    )
    version: str = Field(
        title="version",
        description="Version of the dimension",
        json_schema_extra={
            "requirements": (
                "The version string must be in semver format (e.g., '1.0.0') and it must be "
                " a valid/existing version in the registry.",
            ),
            # TODO: add notes about warnings for outdated versions DSGRID-189 & DSGRID-148
        },
    )


class DimensionReferenceByNameModel(DSGBaseModel):
    """Reference to a dimension that has yet to be registered."""

    dimension_type: DimensionType = Field(
        title="dimension_type",
        alias="type",
        description="Type of the dimension",
        json_schema_extra={
            "options": DimensionType.format_for_docs(),
        },
    )
    name: str = Field(
        title="name",
        description="Dimension name",
    )


def handle_dimension_union(values):
    values = copy.deepcopy(values)
    for i, value in enumerate(values):
        if isinstance(value, DimensionBaseModel):
            continue

        dim_type = value.get("type")
        if dim_type is None:
            dim_type = value["dimension_type"]
        # NOTE: Errors inside DimensionModel or DateTimeDimensionModel will be duplicated by Pydantic
        if dim_type == DimensionType.TIME.value:
            if value["time_type"] == TimeDimensionType.DATETIME.value:
                values[i] = DateTimeDimensionModel(**value)
            elif value["time_type"] == TimeDimensionType.ANNUAL.value:
                values[i] = AnnualTimeDimensionModel(**value)
            elif value["time_type"] == TimeDimensionType.REPRESENTATIVE_PERIOD.value:
                values[i] = RepresentativePeriodTimeDimensionModel(**value)
            elif value["time_type"] == TimeDimensionType.INDEX.value:
                values[i] = IndexTimeDimensionModel(**value)
            elif value["time_type"] == TimeDimensionType.NOOP.value:
                values[i] = NoOpTimeDimensionModel(**value)
            else:
                options = [x.value for x in TimeDimensionType]
                raise ValueError(f"{value['time_type']} not supported, valid options: {options}")
        else:
            values[i] = DimensionModel(**value)
    return values


DimensionsListModel = Annotated[
    list[
        Union[
            DimensionModel,
            DateTimeDimensionModel,
            AnnualTimeDimensionModel,
            RepresentativePeriodTimeDimensionModel,
            IndexTimeDimensionModel,
            NoOpTimeDimensionModel,
        ]
    ],
    BeforeValidator(handle_dimension_union),
]


def _check_time_ranges(ranges: list[TimeRangeModel], str_format: str, frequency: timedelta):
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


def _check_index_ranges(ranges: list[IndexRangeModel]):
    for range in ranges:
        if range.end <= range.start:
            raise ValueError(f"index range {range} end point must be greater than start point.")

    return ranges


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

    category: DimensionCategory


def create_dimension_common_model(model):
    """Constructs an instance of DimensionBaseModel from subclasses in order to give the API
    one common model for all dimensions. Avoids the complexity of dealing with
    DimensionBaseModel validators.
    """
    fields = set(DimensionCommonModel.model_fields)
    data = {x: getattr(model, x) for x in type(model).model_fields if x in fields}
    return DimensionCommonModel(**data)


def create_project_dimension_model(model, category: DimensionCategory):
    data = create_dimension_common_model(model).model_dump()
    data["category"] = category.value
    return ProjectDimensionModel(**data)


def check_display_name(display_name: str):
    """Check that a dimension display name meets all requirements.

    Raises
    ------
    ValueError
        Raised if the display_name is invalid.
    """
    if display_name == "":
        raise ValueError("display_name cannot be empty")
    if REGEX_VALID_REGISTRY_DISPLAY_NAME.search(display_name) is None:
        raise ValueError(f"{display_name=} does not meet the requirements")
    return display_name


def generate_dimension_query_name(dimension_query_name: str, display_name: str) -> str:
    """Generate a dimension query name from a display name.

    Raises
    ------
    ValueError
        Raised if the dimension_query_name was set by the user and does not match the generated
        name.
    """
    generated_query_name = display_name.lower().replace(" ", "_").replace("-", "_")

    if dimension_query_name is not None and dimension_query_name != generated_query_name:
        raise ValueError(f"dimension_query_name cannot be set by the user: {dimension_query_name}")

    return generated_query_name
