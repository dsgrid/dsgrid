'''
Shared configuration models and enums shared between ProjectConfig and
DatasetConfig.

TODO: @Dthom reanme or move this where ever it makes sense.
'''

import os
from datetime import datetime
from enum import Enum
from typing import Dict, List, Optional, Union
import importlib
from pathlib import Path
import csv

from pydantic.dataclasses import dataclass
from pydantic.fields import Field
from pydantic.class_validators import root_validator, validator
from semver import VersionInfo


from dsgrid.common import LOCAL_REGISTRY
from dsgrid.dimension.base import DimensionType, DSGBaseModel
from dsgrid.dimension.time import (
    LeapDayAdjustmentType, Period, TimeValueMeasurement, TimeFrequency,
    TimezoneType
    )
from dsgrid.utils.aws import sync
from dsgrid.utils.files import load_data
from dsgrid.utils.utilities import check_uniqueness


class DimensionBase(DSGBaseModel):
    """Common attributes for all dimensions"""
    name: str = Field(
        title="name",
        description="dimension name",
    )
    dimension_type: DimensionType = Field(
        title="dimension_type",
        alias="type",
        description="type of the dimension",
    )
    module: Optional[str] = Field(
        title='module',
        description='dimension module',
        default='dsgrid.dimension.standard',
    )
    class_name: Optional[str] = Field(
        title="class_name",
        description="dimension model class name",
        alias="class",
    )
    cls: Optional[type] = Field(
        title="cls",
        description="dimension model class",
        alias="dimension_class",
    )

    @validator("name")
    def check_name(cls, name):
        if name == "":
            raise ValueError(f'Empty name field for dimension: "{cls}"')
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
                msg = 'Setting class based on name failed. ' \
                    f'There is no class "{cls_name}" in module: {mod}.' \
                    '\nIf you are using a unique dimension name, you must ' \
                    'specify the dimension class.'
            else:
                msg = f'dimension class {class_name} not in {mod}'
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


class Dimension(DimensionBase):
    """Defines a non-time dimension"""
    filename: str = Field(
        title="filename",
        alias="file",
        description="filename containing dimension records",
    )
    # TODO: I think we may remove mappings altogether in favor of associations
    # TODO: I think we need to add the association table to
    #   dimensions.associations.project_dimensions in the config
    association_table: Optional[str] = Field(
        title="association_table",
        description="optional table that provides mappings of foreign keys"
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
    records: Optional[List[Dict]] = Field(
        title="records",
        description="dimension records in filename that get loaded at runtime",
        default=[],
    )

    @validator("filename")
    def check_file(cls, filename):
        """Validate that dimension file exists and has no errors"""
        # validation for S3 paths (sync locally)
        if filename.startswith('s3://'):
            path = LOCAL_REGISTRY / filename.replace("s3://", "")
            breakpoint() # TODO DT: this doesn't look right for AWS
            #sync(filename, path)

        # Validate that filename exists
        if not os.path.isfile(filename):
            raise ValueError(f"file {filename} does not exist")

        return filename

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

        filename = values["filename"]
        dim_class = values["cls"]
        assert not filename.startswith('s3://')  # TODO: see above

        if records:
            raise ValueError(
                "records should not be defined in the project config")

        # Deserialize the model, validate, add default values, ensure id
        # uniqueness, and then store as dict that can be loaded into
        # Spark later.
        # TODO: Do we want to make sure name is unique too?
        ids = set()
        # TODO: Dan - we want to add better support for csv
        if values["filename"].endswith('.csv'):
            filerecords = csv.DictReader(open(values["filename"]))
        else:
            filerecords = load_data(values["filename"])
        for record in filerecords:
            actual = dim_class(**record)
            if actual.id in ids:
                raise ValueError(
                    f"{actual.id} is stored multiple times for "
                    f"{dim_class.__name__}"
                )
            ids.add(actual.id)
            records.append(actual.dict())

        return records


class TimeDimension(DimensionBase):
    """Defines a time dimension"""
    # TODO: what is this intended purpose?
    #       originally i thought it was to interpret start/end, but
    #       the year here is unimportant because it will be based on
    #       the weather_year
    str_format: Optional[str] = Field(
        title="str_format",
        default="%Y-%m-%d %H:%M:%s-%z",
        description="timestamp format",
    )
    # TODO: we may want a list of start and end times;
    #       can this be string or list of strings?
    start: datetime = Field(
        title="start",
        description="first timestamp in the data",
    )
    # TODO: Is this inclusive or exclusive? --> mm:I don't know what this means
    # TODO: we may want to support a list of start and end times
    end: datetime = Field(
        title="end",
        description="last timestamp in the data",
    )
    # TODO: it would be nice to have this be a func that splits nmbr from unit
    frequency: TimeFrequency = Field(
        title="frequency",
        description="resolution of the timestamps",
    )
    includes_dst: bool = Field(
        title="includes_dst",
        description="includes daylight savings time",
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
        description="timezone of data",
    )
    # TODO: is this a project-level time dimension config?
    value_representation: TimeValueMeasurement = Field(
        title="value_representation",
        default="mean",
        description="TODO",
    )

    @validator("start", "end", pre=True)
    def check_times(cls, val, values):
        # TODO: technical year doesn't matter; needs to apply the weather year
        # make sure start and end time parse
        datetime.strptime(val, values["str_format"])
        # TODO: validate consistency between start, end, frequency
        return val


class VersionUpdateType(Enum):
    # TODO: we need to find general version update types that can be mapped to
    #   major, minor and patch.
    # i.e., replace input_dataset, fix project_config,
    MAJOR = 'minor'
    MINOR = 'major'
    PATCH = 'patch'


class ConfigRegistrationModel(DSGBaseModel):
    """Registration fields required by the ProjectConfig and DatasetConfig"""
    version: Union[str, VersionInfo] = Field(
        title='version',
        description="version resulting from the registration",
    )
    submitter: str = Field(
        title="submitter",
        description="person that submitted the registration"
    )
    date: datetime = Field(
        title="date",
        description="registration date"
    )
    log_message: Optional[str] = Field(
        title="log_message",
        description="reason for the update"
    )


# ------------------------------------------------------------------------------------
# TODO: I don't have a good sense of what we are doing with all these mappings
#   at the moment


@dataclass
class DimensionMap():
    # TODO: this needs QAQC checks
    from_dimension: str
    to_dimension: str
    from_key: str
    to_key: str


class DimensionDirectMapping(DSGBaseModel):
    field: str = Field(
        title="field",
        description="field in from_dimension containing foreign_key",
    )
    to_dimension: Union[str, type] = Field(
        title="to_dimension",
        description="target Dimension for mapping, initially a str",
    )
    foreign_key: str = Field(
        title="foreign_key",
        description="key in to_dimension",
    )


class MappingBaseModel(DSGBaseModel):
    from_dimension: str = Field(
        title="from_dimension",
        description="ORM class name that defines the from dimension",
    )
    to_dimension: str = Field(
        title="to_dimension",
        description="ORM class name that defines the to dimension",
    )
    from_dimension_cls: Optional[type] = Field(
        title="from_dimension_cls",
        description="ORM class that defines the from dimension",
    )
    to_dimension_cls: Optional[type] = Field(
        title="to_dimension_cls",
        description="ORM class that defines the to dimension",
    )


class OneToManyMapping(MappingBaseModel):
    """Defines mapping of one to many."""


class ManyToOneMapping(MappingBaseModel):
    """Defines mapping of many to one."""


class ManyToManyMapping(MappingBaseModel):
    """Defines mapping of many to many."""
    filename: str = Field(
        title="file",
        alias="file",
        description="file that defines the associations",
    )

    @validator("filename")
    def check_file(cls, filename):
        """Check that association file exists."""
        if not os.path.isfile(filename):
            raise ValueError(f"file {filename} does not exist")

        return filename
