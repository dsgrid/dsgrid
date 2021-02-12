"""
# ******************************************
# RUNNING LIST OF PROJECT CONFIG TODOS
# ******************************************

- jsons(or other dim files) should not have any unique IDs
- we need to establish relationships across project-dimensions (to/from or some kind of mapper)
- need to link dsgrid.dimension types to the files provided in config to see if they align (expected columns, duplicates, nullables)
- need to establish dsgrid types standard relationship mappers
- need to use said mapper to run compatibility checks
- need to better establish expected fields/types in the dimension dataclasses in dsgrid.dimension.standard 
- enforce supplemental dimensions to have a from/to mapping to the project dimension of the same type

- need to generate input data config

"""
import abc
import os
from datetime import datetime
from enum import Enum
from typing import List, Optional, Union
import importlib
import inspect

from pydantic.dataclasses import dataclass
from pydantic import BaseModel, Field, ValidationError
from pydantic.fields import Field
from pydantic.class_validators import root_validator, validator

from dsgrid.exceptions import DSGProjectConfigError




# TODO: does this belong in dsgrid.dimensions?
class DimensionType(Enum):  # TODO: is this a duplicate of another dsg class?
    """Dimension types"""
    END_USE = "end_use"
    GEOGRAPHY = "geography"
    SECTOR = "sector"
    SUBSECTOR = "subsector"
    TIME = "time"
    WEATHER = "weather"
    MODEL_YEAR = "model_year"
    SCENARIO = "scenario"
    MODEL = "model"


# TODO: do all these time enums belong in dsgrid.timeseries.time_enums ?

class LeapDayAdjustmentType(Enum):
    """Timezone enum types"""
    # TODO: need some kind of mapping from this enum to leap day
    #       adjustment methods
    DROP_DEC31 = "drop_dec31"
    DROP_FEB29 = "drop_feb29"
    DROP_JAN1 = "drop_jan1"


class Period(Enum):
    """Time period enum types"""
    # TODO: R2PD uses a different set; do we want to align?
    # https://github.com/Smart-DS/R2PD/blob/master/R2PD/tshelpers.py#L15
    PERIOD_ENDING = "period_ending"
    PERIOD_BEGINNING = "period_beginning"
    INSTANTANEOUS = "instantaneous"


class TimeValueMeasurement(Enum):
    """Time value measurement enum types"""
    # TODO: any kind of mappings/conversions for this?
    # TODO: may want a way to alarm if input data != project data measurement
    MEAN = "mean"
    MIN = "min"
    MAX = "max"
    MEASURED = "measured"


class TimeFrequency(Enum):
    # TODO: this is incomplete; good enough for first pass
    # TODO: it would be nice if this could be 
    # TODO: do we want to support common frequency aliases, e.g.:
    # https://pandas.pydata.org/docs/user_guide/timeseries.html#timeseries-offset-aliases
    _15_MIN = "15 min"
    _1_HOUR = "1 hour"
    _1_DAY = "1 day"
    _1_WEEK = "1 week"
    _1_MONTH = "1 month"
    _1_YEAR = "1 year"


class TimezoneType(Enum):
    """Timezone enum types"""
    # TODO: TimezoneType enum is likely incomplete
    UTC = "UTC"
    PST = "PST"
    MST = "MST"
    CST = "CST"
    EST = "EST"


@dataclass
class Timezone():
    # TODO: Timezone class  is likely incomplete
    id: str
    utc_offset: int
    includes_dst: bool
    tz: str


# TODO: move this to some kind of time module
# TODO: TIME_ZONE_MAPPING is incomplete
# EXAMPLE of applying time zone attributes to TimezoneType enum
TIME_ZONE_MAPPING = {
    TimezoneType.UTC: Timezone(id="UTC", utc_offset=0, includes_dst=False, tz="Etc/GMT+0"),
    TimezoneType.PST: Timezone(id="PST", utc_offset=-8, includes_dst=False, tz="Etc/GMT+8"),
    TimezoneType.MST: Timezone(id="MST", utc_offset=-7, includes_dst=False, tz="Etc/GMT+7"),
    TimezoneType.CST: Timezone(id="CST", utc_offset=-6, includes_dst=False, tz="Etc/GMT+6"),
    TimezoneType.EST: Timezone(id="EST", utc_offset=-5, includes_dst=False, tz="Etc/GMT+5"),
}


@dataclass
class DimensionMap():
    # TODO: this needs QAQC checks
    from_dimension: str
    to_dimension: str
    from_key: str
    to_key: str


class DSGBaseModel(BaseModel, abc.ABC):  # TOD
    """Base model for all dsgrid models"""
    # TODO: for all dsgrid models or all dsgrid projects? or all dsgrid configs?

    class Config:
        title = "DSGBaseModel"
        anystr_strip_whitespace = True
        validate_assignment = True
        validate_all = True
        extra = "forbid"  # TODO: consider changing this after we get this working -->????
        use_enum_values = True


class DimensionBase(DSGBaseModel, abc.ABC):
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
        description='dimension module path',
        default=None
    )
    cls: Optional[type] = Field(
        title="cls",  # should title be cls?
        alias="class",
        description="dimesion data class",
        default=None
    )

    @root_validator(pre=True)
    def get_dimension_module(cls, values: dict) -> dict:
        """Infer dsgrid.dimension module from name if module is None"""
        # TODO: use dsgrid.dimension.standard.{dimension_type} implementation
        # dimension_type = values['type']
        if 'module' not in values.keys():
            values['module'] = f'dsgrid.dimension.standard'
        return values

    @root_validator(pre=True)  
    def get_dimension_class(cls, values: dict) -> dict:
        """Get dimension dataclass"""
        mod = importlib.import_module(values['module'])
        classes = [mod for mod, obj in inspect.getmembers(mod)]
        # if cls is set in config:
        if 'class' in values.keys():
            class_name = values['class']
            # check that cls exists in module
            if class_name not in classes:
                raise DSGProjectConfigError(
                    f'dimension class "{class_name}" not in dimension '
                    f'module "{mod}"')
        # if cls is not set in config:
        else:
            class_name = values['name']
            # check if name can be used as class name
            if class_name not in classes:
                raise DSGProjectConfigError(
                    'Setting class based on name failed. '
                    f'There is no class "{class_name}" in module: {mod}.'
                    '\nIf you are using a unique dimension name, you must '
                    'specify the dimension class.')
        values['class'] = getattr(mod, class_name) # TODO: what type is this?
        return values


class Dimension(DimensionBase):
    """Defines a dimension"""
    filename: str = Field(
        title="filename",
        alias="file",
        description="filename containing dimension records",
    )
    # TODO: some of the dimnsions with enforce dimension mappings while others may not
    mapping: Optional[List[DimensionMap]] = Field(
        title="dimension_map",
        alias="dimension_mappings",
        description="TODO"
    )

    @validator('filename')
    def validate_file(cls, filename: dict) -> dict:
        """Validate that dimension file exists and has no errors"""
        # TODO: need validation for S3 paths
        if filename.startswith('S3://'):
            raise DSGProjectConfigError(
                'dsgrid currently does not support S3 files')
        else:
            # Validate that filename exists
            if not os.path.isfile(filename):
                raise DSGProjectConfigError(
                    f"file {filename} does not exist")
        # TODO: validate that the json value has unqiue ID values
        return filename
        

class TimeDimension(DimensionBase):
    """Defines a time dimension"""
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
    # TODO: what is this intended purpose? 
    #       originally i thought it was to interpret start/end, but
    #       the year here is unimportant because it will be based on 
    #       the weather_year
    str_format: Optional[str] = Field(
        title="str_format",
        default="%Y-%m-%d %H:%M:%s-%z",
        description="timestamp format",
    )
    # TODO: it would be nice to have this be a function that splits number from unit
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

    @root_validator(pre=True)
    def validate_time_dimension(cls, values: dict) -> dict:
        # TODO: technical year doesn't matter; needs to apply the weather year
        # make sure start and end time parse
        start = datetime.strptime(values["end"], values["str_format"])
        end = datetime.strptime(values["end"], values["str_format"])
        # TODO: validate consistency between start, end, frequency
        return values


class Dimensions(DSGBaseModel):
    """Contains dimensions defined by a dataset"""
    project_dimensions: List[Union[Dimension, TimeDimension]] = Field(
        title="project_dimensions",
        description="dimensions defined by the project",
    )
    supplemental_dimensions: Optional[List[Union[Dimension, TimeDimension]]] = Field(
        title="supplemental_dimensions",
        description="supplemental dimensions",
        default=[],
    )

    @validator('project_dimensions', 'supplemental_dimensions', pre=True)
    def validate_dimension_type(cls, values):
        """
        Validate dimension type work around for pydantic Union bug
        related to: https://github.com/samuelcolvin/pydantic/issues/619
        """
        dimensions = []
        for value in values:
            if isinstance(value, Dimension):
                return value
            if not isinstance(value, dict):
                raise ValueError('value must be dict')
            dimension_type = value.get('type')
            if dimension_type == 'time':
                dimensions.append(TimeDimension(**value))
            else:
                dimensions.append(Dimension(**value))
        return dimensions

    @validator('project_dimensions')
    def validate_project_dimension(cls, values: dict) -> dict:
        """Validate project_dimensions types"""
        dimension_types = [i.dimension_type for i in values]
        required_dim_types = [i.value for i in DimensionType]
        # validate required dimensions for project_dimensions
        for i in required_dim_types:
            if i not in dimension_types:
                raise DSGProjectConfigError(
                    f'Required project dimension {i} is not in project ',
                    'config project.project_dimensions')
        # validate unique dimension types for project_dimensions
        dimension_type_set = set()
        for i in dimension_types:
            if i in dimension_type_set:
                raise DSGProjectConfigError(
                    f'Duplicate project.project_dimension exists for "{i}"')
            dimension_type_set.add(i)
        return values

    @root_validator()
    def validate_dimension_names(cls, values: dict) -> dict:
        """Validate dimension names"""
        dimensions = [ii for i in values for ii in values[i]]
        # validate dimension names are not empty
        for dimension in dimensions:
            if len(dimension.name) == 0:
                raise DSGProjectConfigError(
                    f'Empty name field for dimension: {dimension}""')
        # validate dimension names are unique
        dimension_names = [i.name for i in dimensions]
        dimension_names_set = set()
        for i in dimension_names:
            if i in dimension_names_set:
                raise DSGProjectConfigError(
                    f'Duplicate dimension name: "{i}"')
            dimension_names_set.add(i)
        return values

    @root_validator()
    def validate_unique_classes(cls, values: dict) -> dict:
        """Validate dimension classes are unique"""
        classes = [getattr(ii, 'cls') for i in values for ii in values[i]]
        class_set = set()
        for i in classes:
            if i in class_set:
                raise DSGProjectConfigError(
                    f'Duplicate dimension class: "{i}"')
            class_set.add(i)
        return values

    @root_validator()
    def validate_files(cls, values: dict) -> dict:
        """Validate dimension files across all dimensions"""
        files = [ii.filename for i in values for ii in values[i] if isinstance(ii, Dimension)]
        # Validate that dimension files are all unique
        fileset = set()
        for i in files:
            if i in fileset:
                raise DSGProjectConfigError(
                    f'Duplicate dimension name: "{i}"')
            fileset.add(i)
        return values

    # def validate_dimension_mappings(cls, values: dict) -> dict:
    #     """ validates that a 
    #     check that keys exist in both jsons
    #     check that all from_keys have a match in the to_keys json

    #     """
 

# TODO: how do we want this to interact with the project config and the dataset config?
#       i am assuming this class is for how the project defines the input dataset
class InputDataset(DSGBaseModel):
    """Defines an input dataset"""
    dataset_id: str = Field(
        title="dataset_id",
        alias="id",
        description="dataset ID",
    )
    #dataset_type: str = Field(
    #    title=,
    #    description=,
    #)
    #version: str = Field(
    #    title=,
    #    description=,
    #)
    #dimensions: List[Dimension] = Field(
    #    title=,
    #    description=,
    #)
    #metadata: Optional[Dict] = Field(
    #    title=,
    #    description=,
    #)
    model_name: str = Field(
        title="model_name",
        description="model name",
    )
    model_sector: str = Field(
        title="model_sector",
        description="model sector",
    )
    sectors: List[str] = Field(
        title="sectors",
        description="sectors used in the project",
    )


class InputDatasets(DSGBaseModel):
    """Defines all input datasets for a project"""
    # TODO: incorrect
    benchmark: List[str] = Field(
        title="benchmark",
        default=[],
        description="benchmark",
    )
    # TODO: incorrect
    historical: List[str] = Field(
        title="historical",
        default=[],
        description="historical",
    )
    datasets: List[InputDataset] = Field(
        title="datasets",
        description="project input datasets",
    )


class ProjectConfig(DSGBaseModel):
    """Represents project configurations"""
    project_id: str = Field(
        title="project_id",
        alias="id",
        description="project identifier",
    )
    name: str = Field(
        title="name",
        description="project name",
    )
    version: str = Field(
        title="version",
        description="project version",
    )
    input_datasets: InputDatasets = Field(
        title="input_datasets",
        description="input datasets for the project",
    )
    dimensions: Dimensions = Field(
        title="dimensions",
        description="dimensions",
    )
    