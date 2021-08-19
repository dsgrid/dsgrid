"""
Helpful utility functions for dsgrid
"""
import logging
import inspect
import json
import os

from enum import Enum

from dsgrid.exceptions import DSGJSONError

logger = logging.getLogger(__name__)


def safe_json_load(fpath):
    """Perform a json file load with better exception handling.

    Parameters
    ----------
    fpath : str
        Filepath to .json file.

    Returns
    -------
    j : dict
        Loaded json dictionary.

    Examples
    --------
    >>> json_path = "./path_to_json.json"
    >>> safe_json_load(json_path)
    {key1: value1,
     key2: value2}
    """

    if not isinstance(fpath, str):
        raise TypeError("Filepath must be str to load json: {}".format(fpath))

    if not fpath.endswith(".json"):
        raise DSGJSONError("Filepath must end in .json to load json: {}".format(fpath))

    if not os.path.isfile(fpath):
        raise DSGJSONError("Could not find json file to load: {}".format(fpath))

    try:
        with open(fpath, "r") as f:
            j = json.load(f)
    except json.decoder.JSONDecodeError as e:
        emsg = "JSON Error:\n{}\nCannot read json file: " '"{}"'.format(e, fpath)
        raise DSGJSONError(emsg)

    return j


def get_class_properties(cls):
    """Get all class properties

    Used to check against config keys

    Returns
    -------
    properties : list
        List of class properties, each of which should represent a valid
        config key/entry
    """
    properties = [
        attr for attr, attr_obj in inspect.getmembers(cls) if isinstance(attr_obj, property)
    ]

    return properties


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


def list_enum_values(enum: Enum):
    """Returns list enum values."""
    return [e.value for e in enum]
