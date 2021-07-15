import csv
import enum
import importlib
import json
import os
from datetime import datetime
from typing import Any, Dict, List, Optional, Union

from pydantic import validator
from pydantic import Field
from semver import VersionInfo

from dsgrid.common import LOCAL_REGISTRY
from dsgrid.data_models import DSGBaseModel, serialize_model, ExtendedJSONEncoder
from dsgrid.dimension.base_models import DimensionType
from dsgrid.dimension.time import (
    LeapDayAdjustmentType,
    Period,
    TimeValueMeasurement,
    TimeFrequency,
    TimezoneType,
)
from dsgrid.registry.common import REGEX_VALID_REGISTRY_NAME
from dsgrid.utils.files import compute_file_hash, load_data
from dsgrid.utils.versioning import handle_version_or_str


class DimensionBaseModel(DSGBaseModel):
    """Common attributes for all dimensions"""

    name: str = Field(
        title="name",
        description="Dimension name",
        note="Dimension names should be descriptive, memorable, identifiable, and reusable for "
        "other datasets and projects",
    )
    dimension_type: DimensionType = Field(
        title="dimension_type",
        alias="type",
        description="Dimension Type",
        options=DimensionType.format_for_docs(),
    )
    dimension_id: Optional[str] = Field(
        title="dimension_id",
        alias="id",
        description="Unique identifier, generated by dsgrid",
        dsg_internal=True,
    )
    module: Optional[str] = Field(
        title="module",
        description="Python module that defines the dimension class",  # TODO: beter description
        default=":mod:`dsgrid.dimension.standard`",
        optional=True,
        requirements=(
            "Custom user-defined modules are not supported if running dsrid in 'online mode'."
            " To supply a custom module, dsgrid must be pointing to a custom registry or be"
            " working in `offline_mode`.",
            "If a user supplied module is provided then the module must be importable in the"
            " user's python environment",
        ),
        notes=("To register a dimension in the remote dsgrid registry, If an existimg ",),
        # TODO: NOTE: if offline mode if False, then a user-defined module is not allowed
        # TODO: provide notes (or links) about how to generate your own class
    )
    class_name: str = Field(
        title="class_name",
        alias="class",
        description="Pydantic model class name that defines the dimension records",
        notes=(
            "The dimension class defines the expected and allowable fields of the dimension"
            " records as well as their data types.",
            "``dsgrid register dimensions`` only supports dimension classes defined in"
            " the :mod:`dsgrid.dimension.standard` module.",
        ),
        # TODO: provide notes (or links) about how to generate your own class
    )
    cls: Optional[Any] = Field(
        title="cls",
        alias="dimension_class",
        description="Dimension record model class",
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
    association_table: Optional[str] = Field(  # TODO: delete this?
        title="association_table",
        description="Optional table that provides mappings of foreign keys",
        dsg_internal=True,
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
        description="Dimension records in filename that get loaded at runtime",
        default=[],
        dsg_internal=True,
    )

    @validator("filename")
    def check_file(cls, filename):
        """Validate that dimension file exists and has no errors"""
        # TODO: do we need to support S3 file paths when we already sync the registry at an
        #   earlier stage? Technically this means that the path should be local
        # validation for S3 paths (sync locally)
        if filename.startswith("s3://"):
            path = LOCAL_REGISTRY / filename.replace("s3://", "")
            breakpoint()  # TODO DT: this doesn't look right for AWS
            # sync(filename, path)

        # Validate that filename exists
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

        filename = values["filename"]
        dim_class = values["cls"]
        assert not filename.startswith("s3://")  # TODO: see above

        if records:
            raise ValueError("records should not be defined in the project config")

        # Deserialize the model, validate, add default values, ensure id
        # uniqueness, and then store as dict that can be loaded into
        # Spark later.
        # TODO: Do we want to make sure name is unique too?
        ids = set()
        # TODO: Dan - we want to add better support for csv
        if values["filename"].endswith(".csv"):
            filerecords = csv.DictReader(open(values["filename"]))
        else:
            filerecords = load_data(values["filename"])
        for record in filerecords:
            actual = dim_class(**record)
            if actual.id in ids:
                raise ValueError(
                    f"{actual.id} is stored multiple times for " f"{dim_class.__name__}"
                )
            ids.add(actual.id)
            records.append(actual.dict())

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


class TimeDimensionModel(DimensionBaseModel):
    """Defines a time dimension"""

    # TODO: what is this intended purpose?
    #       originally i thought it was to interpret start/end, but
    #       the year here is unimportant because it will be based on
    #       the weather_year
    str_format: Optional[str] = Field(  # TODO: why is this optional?
        title="str_format",
        default="%Y-%m-%d %H:%M:%s-%z",
        description="Timestamp format",
    )
    # TODO: we may want a list of start and end times;
    #       can this be string or list of strings?
    start: datetime = Field(
        title="start",
        description="First timestamp in the data",
    )
    # TODO: Is this inclusive or exclusive? --> mm:I don't know what this means
    # TODO: we may want to support a list of start and end times
    end: datetime = Field(
        title="end",
        description="Last timestamp in the data",
    )
    # TODO: it would be nice to have this be a func that splits nmbr from unit
    frequency: TimeFrequency = Field(
        title="frequency",
        description="Resolution of the time data",
        options=TimeFrequency.format_for_docs(),
    )
    includes_dst: bool = Field(
        title="includes_dst",
        description="Includes daylight savings time",
    )
    leap_day_adjustment: Optional[LeapDayAdjustmentType] = Field(
        title="leap_day_adjustment",
        default=None,
        description="TODO",
        options=LeapDayAdjustmentType.format_for_docs(),
    )
    period: Period = Field(
        title="period",
        description="TODO",
        options=Period.format_descriptions_for_docs(),
    )
    timezone: TimezoneType = Field(
        title="timezone",
        description="Timezone of data",
        options=TimezoneType.format_for_docs(),
    )
    # TODO: is this a project-level time dimension config?
    value_representation: TimeValueMeasurement = Field(
        title="value_representation",
        default="mean",
        description="TODO",
        options=TimeValueMeasurement.format_for_docs(),
        # requirements=(" ",),
        # notes=(" ",),  # TODO:
    )

    @validator("start", "end", pre=True)
    def check_times(cls, val, values):
        # TODO: technical year doesn't matter; needs to apply the weather year
        # make sure start and end time parse
        datetime.strptime(val, values["str_format"])
        # TODO: validate consistency between start, end, frequency
        return val

    def dict(self, by_alias=True, **kwargs):
        exclude = {"cls"}
        if "exclude" in kwargs and kwargs["exclude"] is not None:
            kwargs["exclude"].union(exclude)
        else:
            kwargs["exclude"] = exclude
        data = super().dict(by_alias=by_alias, **kwargs)
        data["module"] = str(data["module"])
        data["dimension_class"] = None
        data["start"] = str(data["start"])
        data["end"] = str(data["end"])
        _convert_for_serialization(data)
        return data


class DimensionReferenceModel(DSGBaseModel):
    """Reference to a dimension stored in the registry"""

    dimension_type: DimensionType = Field(
        title="dimension_type",
        alias="type",
        description="Dimension Type",
        options=DimensionType.format_for_docs(),
    )
    dimension_id: str = Field(
        title="dimension_id",
        description="Unique ID of the dimension in the registry",
        notes=(
            "The dimension ID is generated by dsgrid when a dimension is registered (see "
            ":red:`Register a Dimension`  )  and it is a concatenation of the user-provided dimension "
            "name and a auto-generated UUID.",
        ),
    )
    version: Union[str, VersionInfo] = Field(
        title="version",
        description="Version of the dimension",
        requirements=(
            "The version string must be in semver format (e.g., '1.0.0') and it must be "
            " a valid/existing version in the registry.",
        ),
        # TODO: add notes about warnings for outdated versions?
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


class _DimensionsDocsModel(DSGBaseModel):
    """Stand-alone model to point to for dimension.toml docs"""

    # TODO: For documentation of dimensions.toml, I propose that we rename the current DimensionsModel (in project.toml) to something like ProjectConfigDimensionsModel (or something) and then rename this model to DimensionsModel

    dimensions: Union[DimensionModel, TimeDimensionModel] = Field(  # TODO: provide better details!
        title="dimensions",
        alis="dimension_model",
        description="List of dimensions",
        # options=":meth:`dsgrid.configs.dimensions.DimensionModel` or TimeDimensionModel",
    )

    @validator("dimensions", pre=True, each_item=True, always=True)
    def handle_dimension_union(cls, values):
        return handle_dimension_union(values)
