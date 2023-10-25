"""Base functionality for all Pydantic data models used in dsgrid"""

from enum import Enum
import json
import logging
from pathlib import Path

from pydantic import ConfigDict, BaseModel, ValidationError

from dsgrid.exceptions import DSGInvalidParameter
from dsgrid.utils.files import in_other_dir, load_data


logger = logging.getLogger(__name__)


def make_model_config(title, **kwargs) -> ConfigDict:
    """Return a Pydantic config"""
    return ConfigDict(
        title=title,
        str_strip_whitespace=True,
        validate_assignment=True,
        validate_default=True,
        extra="forbid",
        use_enum_values=False,
        arbitrary_types_allowed=True,
        populate_by_name=True,
        **kwargs,
    )


class DSGBaseModel(BaseModel):
    """Base data model for all dsgrid data models"""

    model_config = make_model_config("DSGBaseModel")

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

        with in_other_dir(filename.parent):
            try:
                return cls(**load_data(filename.name))
            except ValidationError:
                logger.exception("Failed to validate %s", filename)
                raise

    @classmethod
    def get_fields_with_extra_attribute(cls, attribute):
        fields = set()
        for f, attrs in cls.model_fields.items():
            if attrs.json_schema_extra.get(attribute):
                fields.add(f)
        return fields

    def model_dump(self, *args, by_alias=True, **kwargs):
        return super().model_dump(*args, by_alias=by_alias, **self._handle_kwargs(**kwargs))

    def model_dump_json(self, *args, by_alias=True, **kwargs):
        return super().model_dump_json(*args, by_alias=by_alias, **self._handle_kwargs(**kwargs))

    @staticmethod
    def _handle_kwargs(**kwargs):
        return {k: v for k, v in kwargs.items() if k not in ("by_alias",)}

    def serialize(self, *args, **kwargs):
        return json.loads(self.model_dump_json(*args, **kwargs))

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


# class ExtendedJSONEncoder(json.JSONEncoder):
#    def default(self, obj):
#        if isinstance(obj, VersionInfo):
#            return str(obj)
#        if isinstance(obj, Enum):
#            return obj.value
#        if isinstance(obj, datetime):
#            return isoformat(obj)
#        if isinstance(obj, timedelta):
#            return timedelta_isoformat(obj)
#
#        return json.JSONEncoder.default(self, obj)
