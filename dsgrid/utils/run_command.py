"""Contains helper functions to run commands as subprocesses."""

import logging
import shlex
import subprocess
import sys

from dsgrid.exceptions import DSGRuntimeError


logger = logging.getLogger(__name__)


def check_run_command(*args, **kwargs):
    """Same as run_command except that it raises an exception on failure.

    Raises
    ------
    DSGRuntimeError
        Raised if the command returns a non-zero return code.

    """
    ret = run_command(*args, **kwargs)
    if ret != 0:
        msg = f"command returned error code: {ret}"
        raise DSGRuntimeError(msg)


def run_command(cmd: str, output=None, cwd=None):
    """Runs a command as a subprocess.

    Caution: Capturing stdout and stderr in memory can be hazardous with
    long-running processes that output lots of text. In those cases consider
    running subprocess.Popen with stdout and/or stderr set to a pre-configured
    file descriptor.

    Parameters
    ----------
    cmd : str
        command to run
    output : None | dict
        If a dict is passed then return stdout and stderr as keys.
    cwd: str | default None
        Change the working directory to cwd before executing the process.

    Returns
    -------
    int
        return code from system; usually zero is good, non-zero is error

    """
    logger.debug(cmd)
    # Disable posix if on Windows.
    command = shlex.split(cmd, posix="win" not in sys.platform)

    if output is not None:
        pipe = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd)
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
