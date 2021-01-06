"""File utility functions"""

import logging
import os
import json

import toml


logger = logging.getLogger(__name__)


def dump_data(data, filename, **kwargs):
    """Dump data to the filename.
    Supports JSON, TOML, or custom via kwargs.

    Parameters
    ----------
    data : dict
        data to dump
    filename : str
        file to create or overwrite

    """
    mod = _get_module_from_extension(filename, **kwargs)
    with open(filename, "w") as f_out:
        mod.dump(data, f_out, **kwargs)

    logger.debug("Dumped data to %s", filename)


def load_data(filename, **kwargs):
    """Load data from the file.
    Supports JSON, TOML, or custom via kwargs.

    Parameters
    ----------
    filename : str

    Returns
    -------
    dict

    """
    mod = _get_module_from_extension(filename, **kwargs)
    with open(filename) as f_in:
        data = mod.load(f_in)

    logger.debug("Loaded data from %s", filename)
    return data


def _get_module_from_extension(filename, **kwargs):
    ext = os.path.splitext(filename)[1].lower()
    if ext == ".json":
        mod = json
    elif ext == ".toml":
        mod = toml
    elif "mod" in kwargs:
        mod = kwargs["mod"]
    else:
        raise Exception(f"Unsupported extension {filename}")

    return mod
