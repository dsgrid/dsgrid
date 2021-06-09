"""Base functionality for all Pydantic data models used in dsgrid"""

import enum
import json
import os
from pathlib import Path

from pydantic import BaseModel
from semver import VersionInfo

from dsgrid.utils.files import load_data


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
        base_dir = filename.parent.absolute()
        orig = os.getcwd()
        os.chdir(base_dir)
        try:
            cfg = cls(**load_data(filename.name))
            return cfg

        finally:
            os.chdir(orig)

    @classmethod
    def schema_json(cls, by_alias=True, indent=None) -> str:
        data = cls.schema(by_alias=by_alias)
        return json.dumps(data, indent=indent, cls=ExtendedJSONEncoder)


class ExtendedJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, VersionInfo):
            return str(obj)
        if isinstance(obj, enum.Enum):
            return obj.value

        return json.JSONEncoder.default(self, obj)


def serialize_model(model: DSGBaseModel):
    """Serialize a model to a dict, converting values as needed."""
    # TODO: we should be able to use model.json and custom JSON encoders
    # instead of doing this, at least in most cases.
    return _serialize_model_data(model.dict(by_alias=True))


def _serialize_model_data(data: dict):
    for key, val in data.items():
        data[key] = _serialize_model_item(val)
    return data


def _serialize_model_item(val):
    if isinstance(val, enum.Enum):
        return val.value
    if isinstance(val, VersionInfo):
        return str(val)
    if isinstance(val, dict):
        return _serialize_model_data(val)
    if isinstance(val, list):
        return [_serialize_model_item(x) for x in val]
    return val
