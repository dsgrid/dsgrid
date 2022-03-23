"""
Helpful utility functions for dsgrid
"""
import logging
import inspect
import json
import os
from enum import Enum

from prettytable import PrettyTable

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


try:
    from IPython.display import display, HTML
    from IPython import get_ipython
    from ipykernel.zmqshell import ZMQInteractiveShell

    _IPYTHON_INSTALLED = True
except ImportError:
    _IPYTHON_INSTALLED = False


def in_jupyter_notebook():
    """Returns True if the current interpreter is running in a Jupyter notebook.

    Returns
    -------
    bool

    """
    if not _IPYTHON_INSTALLED:
        return False

    return isinstance(get_ipython(), ZMQInteractiveShell)


def display_table(table: PrettyTable):
    """Displays a table in an ASCII or HTML format as determined by the current interpreter.

    Parameters
    ----------
    table : PrettyTable

    """
    if in_jupyter_notebook():
        display(HTML(table.get_html_string()))
    else:
        print(table)
