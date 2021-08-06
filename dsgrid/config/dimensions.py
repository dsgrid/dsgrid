import csv
import enum
import importlib
import json
import os
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

import pandas as pd
from pydantic import validator
from pydantic import Field
from pyspark.sql import DataFrame, Row, SparkSession
from semver import VersionInfo

from dsgrid.data_models import DSGBaseModel, serialize_model, ExtendedJSONEncoder
from dsgrid.dimension.base_models import DimensionType
from dsgrid.dimension.time import (
    LeapDayAdjustmentType,
    Period,
    TimeValueMeasurement,
    TimezoneType,
)
from dsgrid.registry.common import REGEX_VALID_REGISTRY_NAME
from dsgrid.utils.files import compute_file_hash, compute_hash, load_data
from dsgrid.utils.spark import create_dataframe, read_dataframe
from dsgrid.utils.versioning import handle_version_or_str


class DimensionBaseModel(DSGBaseModel):
    """Common attributes for all dimensions"""

    name: str = Field(
        title="name",
        description="Dimension name",
    )
    dimension_type: DimensionType = Field(
        title="dimension_type",
        alias="type",
        description="Type of the dimension",
    )
    dimension_id: Optional[str] = Field(
        title="dimension_id",
        alias="id",
        description="Unique identifier, generated by dsgrid",
        dsg_internal=True,
    )
    module: Optional[str] = Field(
        title="module",
        description="Dimension module",
        default="dsgrid.dimension.standard",
    )
    class_name: str = Field(
        title="class_name",
        description="Dimension record model class name",
        alias="class",
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
        "identifiable; this description will get stored in the dimension record registry",
        alias="description",
    )
    # Keep this last for validation purposes.
    model_hash: Optional[str] = Field(
        title="model_hash",
        description="Hash of the contents of the model",
        dsg_internal=True,
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
                f" Dimension name '{name}' is not descriptive enough for a dimension record name. Please be more descriptive in your naming. Hint: try adding a vintage, or other distinguishable text that will be this dimension memorable, identifiable, and reusable for other datasets and projects. e.g., 'time-2012-est-houlry-periodending-nodst-noleapdayadjustment-mean' is a good descriptive name."
            )
        return name

    @validator("class_name", always=True)
    def get_dimension_class_name(cls, class_name, values):
        """Set class_name based on inputs."""
        if "name" not in values:
            # An error occurred with name. Ignore everything else.
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
        if "name" not in values or values.get("class_name") is None:
            # An error occurred with name. Ignore everything else.
            return None

        if dim_class is not None:
            raise ValueError(f"cls={dim_class} should not be set")

        return getattr(
            importlib.import_module(values["module"]),
            values["class_name"],
        )

    @validator("model_hash")
    def compute_model_hash(cls, model_hash, values):
        # Always re-compute because the user may have changed something.
        text = ""
        for key, val in sorted(values.items()):
            if key != "dimension_id":
                text += str(val)
        return compute_hash(text.encode())


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
    # TODO: I think we may remove mappings altogether in favor of associations
    # TODO: I think we need to add the association table to
    #   dimensions.associations.base_dimensions in the config
    association_table: Optional[str] = Field(
        title="association_table",
        description="Optional table that provides mappings of foreign keys",
    )
    # TODO: some of the dimensions will enforce dimension mappings while
    #   others may not
    # TODO: I really don't think we need these mappings at this stage.
    #   I think association tables are fine.
    # one_to_many_mappings: Optional[List[OneToManyMapping]] = Field(
    #     title="one_to_many_mappings",
    #     description="Defines one-to-many mappings for this dimension",
    #     default=[],
    # )
    # many_to_one_mappings: Optional[List[ManyToOneMapping]] = Field(
    #     title="many_to_one_mappings",
    #     description="Defines many-to-one mappings for this dimension",
    #     default=[],
    # )
    # many_to_many_mappings: Optional[List[ManyToManyMapping]] = Field(
    #     title="many_to_many_mappings",
    #     description="Defines many-to-many mappings for this dimension",
    #     default=[],
    # )
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
            # TODO
            # We are getting here for Time. That shouldn't be happening.
            # This seems to work, but something is broken.
            return None
        return file_hash or compute_file_hash(values["filename"])

    # TODO: is this what we want?
    # @validator(
    #     "one_to_many_mappings",
    #     "many_to_one_mappings",
    #     "many_to_many_mappings",
    #     each_item=True,
    # )
    # def add_mapping_dimension_types(cls, val, values):
    #     """Find the dimension mappings types and add them."""
    #     module = importlib.import_module(values["module"])
    #     val.from_dimension_cls = getattr(module, val.from_dimension, None)
    #     if val.from_dimension_cls is None:
    #         raise ValueError(f"{module} does not define {val.from_dimension}")

    #     val.to_dimension_cls = getattr(module, val.to_dimension, None)
    #     if val.to_dimension_cls is None:
    #         raise ValueError(f"{module} does not define {val.to_dimension}")

    #     return val

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
            with open(filename) as f_in:
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

    def dict(self, by_alias=True, **kwargs):
        exclude = {"cls", "records"}
        if "exclude" in kwargs and kwargs["exclude"] is not None:
            kwargs["exclude"].union(exclude)
        else:
            kwargs["exclude"] = exclude
        data = super().dict(by_alias=by_alias, **kwargs)
        data["module"] = str(data["module"])
        data["dimension_class"] = None
        _convert_for_serialization(data)
        return data


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


class TimeDimensionModel(DimensionBaseModel):
    """Defines a time dimension"""

    # TODO: what is this intended purpose?
    #       originally i thought it was to interpret start/end, but
    #       the year here is unimportant because it will be based on
    #       the weather_year
    str_format: Optional[str] = Field(
        title="str_format",
        default="%Y-%m-%d %H:%M:%s-%z",
        description="Timestamp format",
    )
    ranges: List[TimeRangeModel] = Field(
        title="time_ranges",
        description="Defines the continuous ranges of time in the data.",
    )
    frequency: timedelta = Field(
        title="frequency",
        description="Resolution of the timestamps",
    )
    leap_day_adjustment: Optional[LeapDayAdjustmentType] = Field(
        title="leap_day_adjustment",
        default=None,
        description="TODO",
    )
    period: Period = Field(
        title="period",
        description="TODO",
    )
    timezone: TimezoneType = Field(
        title="timezone",
        description="Timezone of data",
    )
    # TODO: is this a project-level time dimension config?
    value_representation: TimeValueMeasurement = Field(
        title="value_representation",
        default="mean",
        description="TODO",
    )

    @validator("ranges", pre=True)
    def check_times(cls, ranges, values):
        for time_range in ranges:
            # make sure start and end time parse
            datetime.strptime(time_range["start"], values["str_format"])
            datetime.strptime(time_range["end"], values["str_format"])
            # TODO: validate consistency between start, end, frequency
        return ranges

    @validator("leap_day_adjustment")
    def check_leap_day_adjustment(cls, value):
        if value is not None:
            # TODO: DSGRID-172
            raise ValueError("leap_day_adjustment is not yet supported")
        return value

    def dict(self, by_alias=True, **kwargs):
        exclude = {"cls"}
        if "exclude" in kwargs and kwargs["exclude"] is not None:
            kwargs["exclude"].union(exclude)
        else:
            kwargs["exclude"] = exclude
        data = super().dict(by_alias=by_alias, **kwargs)
        data["module"] = str(data["module"])
        data["dimension_class"] = None
        _convert_for_serialization(data)
        return data


class DimensionReferenceModel(DSGBaseModel):
    """Reference to a dimension stored in the registry"""

    dimension_type: DimensionType = Field(
        title="dimension_type",
        alias="type",
        description="Type of the dimension",
    )
    dimension_id: str = Field(
        title="dimension_id",
        description="Unique ID of the dimension",
    )
    version: Union[str, VersionInfo] = Field(
        title="version",
        description="Version of the dimension",
    )

    @validator("version")
    def check_version(cls, version):
        return handle_version_or_str(version)


DimensionUnionModel = List[Union[DimensionModel, DimensionReferenceModel, TimeDimensionModel]]


def handle_dimension_union(value):
    """
    Validate dimension type work around for pydantic Union bug
    related to: https://github.com/samuelcolvin/pydantic/issues/619
    """
    if isinstance(value, DimensionBaseModel):
        return value

    # NOTE: Errors inside DimensionModel or TimeDimensionModel will be duplicated by Pydantic
    if value["type"] == DimensionType.TIME.value:
        val = TimeDimensionModel(**value)
    elif sorted(value.keys()) == ["dimension_id", "type", "version"]:
        val = DimensionReferenceModel(**value)
    else:
        val = DimensionModel(**value)
    return val


def _convert_for_serialization(data):
    for key, val in data.items():
        if isinstance(val, enum.Enum):
            data[key] = val.value
