"""File utility functions"""

import hashlib
import logging
import os
import json
import shutil
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import json5


logger = logging.getLogger(__name__)


def compute_file_hash(filename) -> str:
    """Compute a hash of the contents of a file.

    Parameters
    ----------
    filename : str

    Returns
    -------
    str
        hash in the form of a hex number converted to a string

    """
    return compute_hash(Path(filename).read_bytes())


def compute_hash(text: bytes) -> str:
    hash_obj = hashlib.sha256()
    hash_obj.update(text)
    return hash_obj.hexdigest()


def delete_if_exists(path: Path | str) -> None:
    """Delete a file or directory if it exists."""
    path = Path(path) if isinstance(path, str) else path
    if path.exists():
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()


def dump_data(data, filename, **kwargs) -> None:
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


def load_data(filename, **kwargs) -> dict[str, Any]:
    """Load data from the file.
    Supports JSON, JSON5, or custom via kwargs.

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


def dump_json_file(data, filename, indent=None) -> None:
    """Dump data to the JSON or JSON5 filename."""
    dump_data(data, filename, indent=indent)


def load_json_file(filename: Path | str) -> dict[str, Any]:
    """Load data from the JSON or JSON5 file."""
    return load_data(filename)


def dump_line_delimited_json(data, filename, mode="w"):
    """Dump a list of objects to the file as line-delimited JSON.

    Parameters
    ----------
    data : list
    filename : str
    mode : str
        Mode to use for opening the file, defaults to "w"

    """
    with open(filename, mode, encoding="utf-8-sig") as f_out:
        for obj in data:
            f_out.write(json.dumps(obj))
            f_out.write("\n")

    logger.debug("Dumped data to %s", filename)


def load_line_delimited_json(filename):
    """Load data from the file that is stored as line-delimited JSON.

    Parameters
    ----------
    filename : str

    Returns
    -------
    dict

    """
    objects = []
    with open(filename, encoding="utf-8-sig") as f_in:
        for i, line in enumerate(f_in):
            text = line.strip()
            if not text:
                continue
            try:
                objects.append(json.loads(text))
            except Exception:
                logger.exception("Failed to decode line number %s in %s", i, filename)
                raise

    logger.debug("Loaded data from %s", filename)
    return objects


@contextmanager
def in_other_dir(path: Path):
    """Change to another directory while user code runs.

    Parameters
    ----------
    path : Path
    """
    orig = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(orig)


def _get_module_from_extension(filename, **kwargs):
    ext = os.path.splitext(filename)[1].lower()
    if ext == ".json":
        mod = json
    elif ext == ".json5":
        mod = json5
    elif "mod" in kwargs:
        mod = kwargs["mod"]
    else:
        msg = f"Unsupported extension {filename}"
        raise NotImplementedError(msg)

    return mod
