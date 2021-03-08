"""File utility functions"""

import logging
import os
import json

import toml

import dsgrid.utils.aws as aws


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
        try:
            data = mod.load(f_in)
        except Exception:
            logger.exception("Failed to load data from %s", filename)
            raise

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


def exists(path):
    """Returns True if the path exists. Accounts for local or AWS S3.

    Parameters
    ----------
    path : str | Path

    Returns
    -------
    bool

    """
    if path.startswith("s3"):
        return aws.exists(path)
    return os.path.exists(path)


def list_dir(dir_name):
    """Returns the contents of a directory. Accounts for local or AWS S3.

    Parameters
    ----------
    path : str | Path

    Returns
    -------
    list

    """
    if dir_name.startswith("s3"):
        return aws.list_dir(dir_name)
    return os.listdir(dir_name)


def make_dirs(path, exist_ok=True):
    """Makes all directories for path. Accounts for local or AWS S3.

    Parameters
    ----------
    path : str

    """
    if path.name.startswith("s3"):
        aws.make_dirs(path, exist_ok=exist_ok)
    else:
        os.makedirs(path, exist_ok=exist_ok)
