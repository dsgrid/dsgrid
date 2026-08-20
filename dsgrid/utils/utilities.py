"""
Helpful utility functions for dsgrid
"""

import logging
import inspect
import importlib
import json
import os
from enum import Enum
from typing import Any, Iterable, Type, cast

from prettytable import PrettyTable
from pydantic import ValidationError

try:
    _ipython_display = cast(Any, importlib.import_module("IPython.display"))
    display = _ipython_display.display
    HTML = _ipython_display.HTML
    get_ipython = cast(Any, importlib.import_module("IPython")).get_ipython
    ZMQInteractiveShell = cast(
        Any, importlib.import_module("ipykernel.zmqshell")
    ).ZMQInteractiveShell

    _IPYTHON_INSTALLED = True
except ImportError:
    _IPYTHON_INSTALLED = False


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
        msg = "Filepath must be str to load json: {}".format(fpath)
        raise TypeError(msg)

    if not fpath.endswith(".json"):
        msg = "Filepath must end in .json to load json: {}".format(fpath)
        raise DSGJSONError(msg)

    if not os.path.isfile(fpath):
        msg = "Could not find json file to load: {}".format(fpath)
        raise DSGJSONError(msg)

    try:
        with open(fpath, "r") as f:
            j = json.load(f)
    except json.decoder.JSONDecodeError as e:
        emsg = 'JSON Error:\n{}\nCannot read json file: "{}"'.format(e, fpath)
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


def check_uniqueness(iterable: Iterable, tag: str) -> set[str]:
    """Raises ValueError if iterable has duplicate entries.

    Parameters
    ----------
    iterable : list | generator
    tag : str
        tag to add to the exception string

    Returns
    -------
    set[str]

    """
    values = set()
    for item in iterable:
        if item in values:
            msg = f"duplicate {tag}: {item}"
            raise ValueError(msg)
        values.add(item)
    return values


def sorted_with_nulls(values: Iterable) -> list:
    """Sort values that may contain ``None``, ordering ``None`` first.

    ``sorted`` raises TypeError when a collection mixes ``None`` with other types.
    Distinct-value sets read from a table can contain NULL, so use this whenever
    such a set is sorted for display in a log or error message.

    Parameters
    ----------
    values : Iterable
        Values to sort. Mutually incomparable non-null values (e.g. ``6037``
        and ``"6037"`` from sources that disagree on a column's type) fall
        back to ordering by type name and string form rather than raising.

    Returns
    -------
    list
        Sorted values, with ``None`` first if present.

    Examples
    --------
    >>> sorted_with_nulls({"b", None, "a"})
    [None, 'a', 'b']
    """
    non_null = []
    has_null = False
    for value in values:
        if value is None:
            has_null = True
        else:
            non_null.append(value)
    try:
        non_null.sort()
    except TypeError:
        # This sorts for display in an error message; a crash here would mask
        # the error being reported.
        non_null.sort(key=lambda value: (type(value).__name__, str(value)))
    return [None, *non_null] if has_null else non_null


def convert_record_dicts_to_classes(iterable, cls, check_duplicates: None | list[str] = None):
    """Convert an iterable of dicts to instances of a data class.

    Parameters
    ----------
    iterable
        Any iterable of dicts that must have an 'id' field.
    cls : class
        Instantiate a class from each dict by splatting the dict to the constructor.
    check_duplicates : None | list[str]
        If it is a list of column names, ensure that there are no duplicates among the rows.

    Returns
    -------
    list
    """
    records = []
    check_duplicates = check_duplicates or []
    values = {x: set() for x in check_duplicates}
    length = None
    for i, row in enumerate(iterable, start=1):
        if None in row:
            msg = f"row {i} has a key that is None: {row=}"
            raise ValueError(msg)
        if length is None:
            length = len(row)
        elif len(row) != length:
            msg = f"Rows have inconsistent length: first_row_length={length} row {i}: {row=}"
            raise ValueError(msg)
        try:
            record = cls(**row)
        except ValidationError as e:
            msg = f"Validation error in row {i}: {row}\n{e}"
            raise ValueError(msg) from e
        for name in check_duplicates:
            val = getattr(record, name)
            if val in values[name]:
                msg = f"{val} is listed multiple times"
                raise ValueError(msg)
            values[name].add(val)
        records.append(record)

    return records


def list_enum_values(enum: Type[Enum]):
    """Returns list enum values."""
    return [e.value for e in enum]


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


def make_unique_key(base_name: str, existing_keys: Iterable[str]) -> str:
    """Generate a unique key by appending an index if the base name already exists.

    Parameters
    ----------
    base_name : str
        The base name to use as a key.
    existing_keys : Iterable[str]
        Collection of existing keys to check against.

    Returns
    -------
    str
        A unique key, either the base name or base name with an appended index
        (e.g., 'name_1', 'name_2').

    Examples
    --------
    >>> make_unique_key("file", {"other", "another"})
    'file'
    >>> make_unique_key("file", {"file", "other"})
    'file_1'
    >>> make_unique_key("file", {"file", "file_1", "file_2"})
    'file_3'
    """
    existing = set(existing_keys)
    if base_name not in existing:
        return base_name

    index = 1
    while True:
        new_key = f"{base_name}_{index}"
        if new_key not in existing:
            return new_key
        index += 1
