"""Base functionality for all Pydantic data models used in dsgrid"""

from enum import Enum
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from pydantic import BaseModel, ValidationError
from pydantic.json import isoformat, timedelta_isoformat
from semver import VersionInfo

from dsgrid.exceptions import DSGInvalidParameter
from dsgrid.utils.files import load_data


logger = logging.getLogger(__name__)


class DSGBaseModel(BaseModel):
    """Base data model for all dsgrid data models"""

    class Config:
        title = "DSGBaseModel"
        anystr_strip_whitespace = True
        validate_assignment = True
        validate_all = True
        extra = "forbid"
        use_enum_values = False
        arbitrary_types_allowed = True
        allow_population_by_field_name = True
        json_encoders = {
            datetime: isoformat,
            timedelta: timedelta_isoformat,
            VersionInfo: lambda x: str(x),
            Enum: lambda x: x.value,
        }

    @classmethod
    def load(cls, filename):
        """Load a data model from a file.
        Temporarily changes to the file's parent directory so that Pydantic
        validators can load relative file paths within the file.

        Parameters
        ----------
        filename : str

        """
        filename = Path(filename)
        if not filename.is_file():
            raise DSGInvalidParameter(f"{filename} is not a file")

        base_dir = filename.parent.absolute()
        orig = os.getcwd()
        os.chdir(base_dir)
        try:
            return cls(**load_data(filename.name))
        except ValidationError:
            logger.exception("Failed to validate %s", filename)
            raise
        finally:
            os.chdir(orig)

    @classmethod
    def schema_json(cls, by_alias=True, indent=None) -> str:
        data = cls.schema(by_alias=by_alias)
        return json.dumps(data, indent=indent, cls=ExtendedJSONEncoder)

    @classmethod
    def get_fields_with_extra_attribute(cls, attribute):
        fields = set()
        for f, attrs in cls.__fields__.items():
            if attrs.field_info.extra.get(attribute):
                fields.add(f)
        return fields

    def dict(self, *args, by_alias=True, **kwargs):
        kw = {k: v for k, v in kwargs.items() if k not in ("by_alias",)}
        return super().dict(*args, by_alias=by_alias, **kw)

    def json(self, *args, by_alias=True, **kwargs):
        kw = {k: v for k, v in kwargs.items() if k not in ("by_alias",)}
        return super().json(*args, by_alias=by_alias, **kw)

    def serialize(self, *args, **kwargs):
        return json.loads(self.json(*args, **kwargs))

    @classmethod
    def from_file(cls, filename: Path):
        """Deserialize the model from a file. Unlike the load method,
        this does not change directories.
        """
        return cls(**load_data(filename))


class EnumValue:
    """Class to define a DSGEnum value"""

    def __init__(self, value, description, **kwargs):
        self.value = value
        self.description = description
        for kwarg, val in kwargs.items():
            self.__setattr__(kwarg, val)


class DSGEnum(Enum):
    """dsgrid Enum class"""

    def __new__(cls, *args):
        obj = object.__new__(cls)
        assert len(args) in (1, 2)
        if isinstance(args[0], EnumValue):
            obj._value_ = args[0].value
            obj.description = args[0].description
            for attr, val in args[0].__dict__.items():
                if attr not in ("value", "description"):
                    setattr(obj, attr, val)
        elif len(args) == 2:
            obj._value_ = args[0]
            obj.description = args[1]
        else:
            obj._value_ = args[0]
            obj.description = None
        return obj

    @classmethod
    def format_for_docs(cls):
        """Returns set of formatted enum values for docs."""
        return str([e.value for e in cls]).replace("'", "``")

    @classmethod
    def format_descriptions_for_docs(cls):
        """Returns formatted dict of enum values and descriptions for docs."""
        desc = {}
        for e in cls:
            desc[f"``{e.value}``"] = f"{e.description}"
        return desc


class ExtendedJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, VersionInfo):
            return str(obj)
        if isinstance(obj, Enum):
            return obj.value
        if isinstance(obj, datetime):
            return isoformat(obj)
        if isinstance(obj, timedelta):
            return timedelta_isoformat(obj)

        return json.JSONEncoder.default(self, obj)
