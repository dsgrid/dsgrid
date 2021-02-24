"""
# ******************************************
# RUNNING LIST OF PROJECT CONFIG TODOS
# ******************************************

- we need to establish relationships across project-dimensions (to/from or
    some kind of mapper)
- need to establish dsgrid types standard relationship mappers
- need to use said mapper to run compatibility checks
- need to better establish expected fields/types in the dimension dataclasses
    in dsgrid.dimension.standard
- enforce supplemental dimensions to have a from/to mapping to the project
    dimension of the same type?
- I think we need to add the association table to
    dimensions.associations.project_dimensions in the config

- Add registry details
- need to generate input data config

"""
from dsgrid.utils.utilities import run_command
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

from dsgrid.dimension.base import DimensionType, DSGBaseModel
from dsgrid.utils.files import load_data
from dsgrid.dimension.time import (
    LeapDayAdjustmentType, Period, TimeValueMeasurement, TimeFrequency,
    TimezoneType
    )

LOAD_DATA_FILENAME = "load_data.parquet"
LOAD_DATA_LOOKUP_FILENAME = "load_data_lookup.parquet"


# TODO: Remove?
@dataclass
class DimensionMap():
    # TODO: this needs QAQC checks
    from_dimension: str
    to_dimension: str
    from_key: str
    to_key: str


# TODO: Do we want to support direct mappings as well as mappings through
# association tables or only the latter?

# TODO: Remove?
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
    """Defines a dimension"""
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
            home = str(Path.home())
            path = f"{home}/.dsgrid-data/{filename}".replace("s3://", "")
            sync_command = f"aws s3 sync {filename} {path}"
            run_command(sync_command)

        # Validate that filename exists
        if not os.path.isfile(filename):
            raise ValueError(f"file {filename} does not exist")

        return filename

    @validator(
        "one_to_many_mappings",
        "many_to_one_mappings",
        "many_to_many_mappings",
        each_item=True,
    )
    def add_mapping_dimension_types(cls, val, values):
        """Find the dimension mappings types and add them."""
        module = importlib.import_module(values["module"])
        val.from_dimension_cls = getattr(module, val.from_dimension, None)
        if val.from_dimension_cls is None:
            raise ValueError(f"{module} does not define {val.from_dimension}")

        val.to_dimension_cls = getattr(module, val.to_dimension, None)
        if val.to_dimension_cls is None:
            raise ValueError(f"{module} does not define {val.to_dimension}")

        return val

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


class Dimensions(DSGBaseModel):
    """Contains dimensions defined by a dataset"""
    project_dimensions: List[Union[Dimension, TimeDimension]] = Field(
        title="project_dimensions",
        description="dimensions defined by the project",
    )
    supplemental_dimensions: Optional[List[Union[Dimension, TimeDimension]]] \
        = Field(
        title="supplemental_dimensions",
        description="supplemental dimensions",
        default=[],
    )

    @validator('project_dimensions', 'supplemental_dimensions',
               pre=True, each_item=True, always=True)
    def handle_dimension_union(cls, value):
        """
        Validate dimension type work around for pydantic Union bug
        related to: https://github.com/samuelcolvin/pydantic/issues/619
        """
        # NOTE: Errors inside Dimension or TimeDimension will be duplicated
        # by Pydantic
        if value['type'] == DimensionType.TIME.value:
            val = TimeDimension(**value)
        else:
            val = Dimension(**value)
        return val

    @validator('project_dimensions')
    def check_project_dimension(cls, val):
        """Validate project_dimensions types"""
        dimension_types = [i.dimension_type for i in val]
        required_dim_types = list(DimensionType)
        # validate required dimensions for project_dimensions
        for i in required_dim_types:
            if i not in dimension_types:
                raise ValueError(
                    f'Required project dimension {i} is not in project ',
                    'config project.project_dimensions')
        check_uniqueness(dimension_types, "project_dimension")
        return val

    @root_validator
    def check_dimension_names(cls, values: dict) -> dict:
        """Validate dimension names"""
        check_uniqueness(
            (ii.name for i in values for ii in values[i]),
            "dimension name",
        )
        return values

    @root_validator
    def check_unique_classes(cls, values: dict) -> dict:
        """Validate dimension classes are unique"""
        check_uniqueness(
            (getattr(ii, 'cls') for i in values for ii in values[i]),
            'dimension cls',
        )
        return values

    @root_validator
    def check_files(cls, values: dict) -> dict:
        """Validate dimension files across all dimensions"""
        check_uniqueness(
            (ii.filename for i in values for ii in values[i] if isinstance(
                ii, Dimension)),
            "dimension record filename",
        )
        return values

    @root_validator
    def check_dimension_mappings(cls, values: dict) -> dict:
        """ validates that a
        check that keys exist in both jsons
        check that all from_keys have a match in the to_keys json

        """
        # supplemental_mapping = {
        #   x.name: x.cls for x in values["supplemental_dimensions"]}
        # Should already have been checked.
        # assert len(supplemental_mapping) == \
        #   len(values["supplemental_dimensions"])
        # for dim in values["project_dimensions"]:
        #    mappings = getattr(dim, "mappings", [])
        #    # TODO: other mapping types
        #    for mapping in (
        #       x for x in mappings if isinstance(x, DimensionDirectMapping)):
        #        to_dim = supplemental_mapping.get(mapping.to_dimension)
        #        if to_dim is None:
        #            raise ValueError(
        #               f"dimension {mapping.to_dimension} is not stored in"
        #               f"supplemental_dimensions"
        #            )
        #        mapping.to_dimension = to_dim

        return values


class InputDataset(DSGBaseModel):
    """Defines an input dataset"""
    dataset_id: str = Field(
        title="dataset_id",
        alias="id",
        description="dataset ID",
    )
    dataset_type: str = Field(  # TODO this needs to be ENUM
       title="dataset_type",
       description="Dataset Type"
    )
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

    # @validator(datasets) # TODO: unclear if this needs to be root validator
    #   or not
    # def check_dataset_ids(cls, datasets, values):
    #     # TODO: Check for unique dataset IDs
    #     return datasets

    # TODO: check model_name, model_sector, sectors are all expected and align
    #   with the dimension records


class PROJECT_VERSION_UPDATE_TYPES(Enum):
    # TODO: we need to find general version update types that can be mapped to
    #   major, minor and patch.
    # i.e., replace input_dataset, fix project_config,
    MAJOR = 'minor'
    MINOR = 'major'
    PATCH = 'patch'


class ProjectConfigRegistrationDetails(DSGBaseModel):
    """Registration fields required by the ProjectConfig"""
    update: bool = Field(
        title="update",
        description="update boolean for project registration updates",
        default=False
    )
    update_type: Optional[PROJECT_VERSION_UPDATE_TYPES] = Field(
        title="update_type",
        description="list of project update types"
    )
    log_message: Optional[str] = Field(
        title="log_message",
        description="registration version log message"
    )

    @validator('update_type')
    def check_registration_update_type(cls, update_type, values):
        """Check registration update_type against update field"""
        if not values['update']:
            if not update_type:
                raise ValueError(
                    'If registration.update = False then '
                    'registration.update_type must be empty or None'
                    )
        if values['update']:
            if not update_type:
                raise ValueError(
                    'If registration.update = True then '
                    'registration.update_type is required.'
                    )
        return update_type

    @validator('log_message')
    def check_registration_log_message(cls, log_message, values):
        """Check registration log message against update field"""
        if not values['update']:
            log_message = 'Initial project registration.'
        if values['update']:
            if not log_message:
                raise ValueError(
                    'If registration.update = True then '
                    'registration.log_message must be declaired.')
        return log_message


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
    registration: ProjectConfigRegistrationDetails = Field(
        title="registration",
        description="project registration details"
    )
    input_datasets: InputDatasets = Field(
        title="input_datasets",
        description="input datasets for the project",
    )
    dimensions: Dimensions = Field(
        title="dimensions",
        description="dimensions",
    )

    @validator('project_id')
    def check_project_id_handle(project_id):
        """Check for valid characteris in project id"""
        # TODO: any other invalid character for the project_id?
        # TODO: may want to check for pre-existing project_id
        #       (e.g., LA100 Run 1 vs. LA100 Run 0 kind of thing)
        if '-' in project_id:
            raise ValueError(
                'invalid character "-" in project id')
        return project_id


def load_project_config(filename):
    """Load a project configuration from a file.

    Parameters
    ----------
    filename : str

    Returns
    -------
    ProjectConfig

    """
    base_dir = os.path.dirname(filename)
    orig = os.getcwd()
    os.chdir(base_dir)
    try:
        cfg = ProjectConfig(**load_data(filename))
        return cfg

    finally:
        os.chdir(orig)


def check_uniqueness(iterable, tag):
    """Raises ValueError if iterable has duplicate entries.

    Parameters
    ----------
    iterable : list | generator
    tag : str
        tag to add to the exception string

    """
    values = set()
    for item in iterable:
        if item in values:
            raise ValueError(f"duplicate {tag}: {item}")
        values.add(item)
