"""
Helpful utility functions for dsgrid
"""
import logging
import subprocess
import shlex
import sys
import inspect
import json
import os

from dsgrid.exceptions import JSONError

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
        raise TypeError('Filepath must be str to load json: {}'.format(fpath))

    if not fpath.endswith('.json'):
        raise JSONError('Filepath must end in .json to load json: {}'
                        .format(fpath))

    if not os.path.isfile(fpath):
        raise JSONError('Could not find json file to load: {}'.format(fpath))

    try:
        with open(fpath, 'r') as f:
            j = json.load(f)
    except json.decoder.JSONDecodeError as e:
        emsg = ('JSON Error:\n{}\nCannot read json file: '
                '"{}"'.format(e, fpath))
        raise JSONError(emsg)

    return j


def get_class_properties(cls):
    """
    Get all class properties
    Used to check against config keys
    Returns
    -------
    properties : list
        List of class properties, each of which should represent a valid
        config key/entry
    """
    properties = [attr for attr, attr_obj
                  in inspect.getmembers(cls)
                  if isinstance(attr_obj, property)]

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

        
def run_command(cmd, output=None, cwd=None):
    """Runs a command as a subprocess.
    Parameters
    ----------
    cmd : str
        command to run
    output : None | dict
        If a dict is passed then return stdout and stderr as keys.
    cwd: str, default None
        Change the working directory to cwd before executing the process.
    Returns
    -------
    int
        return code from system; usually zero is good, non-zero is error
    Caution: Capturing stdout and stderr in memory can be hazardous with
    long-running processes that output lots of text. In those cases consider
    running subprocess.Popen with stdout and/or stderr set to a pre-configured
    file descriptor.
    """
    logger.debug(cmd)
    # Disable posix if on Windows.
    command = shlex.split(cmd, posix="win" not in sys.platform)

    if output is not None:
        pipe = subprocess.Popen(
            command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=cwd)
        out, err = pipe.communicate()
        output["stdout"] = out.decode("utf-8")
        output["stderr"] = err.decode("utf-8")
        ret = pipe.returncode
    else:
        ret = subprocess.call(command, cwd=cwd)

    if ret != 0:
        logger.debug("Command [%s] failed: %s", cmd, ret)
        if output:
            logger.debug(output["stderr"])

    return ret
