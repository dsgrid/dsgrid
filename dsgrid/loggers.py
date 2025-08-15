"""Contains logging configuration data."""

import logging
import logging.config
import os
from contextlib import contextmanager

import chronify.loggers


# ETH@20210325 - What if you want to set up logging for all loggers, or for all
# dsgrid loggers? What name should be provided? Should that be the default?
# Should filename default to None?


# ETH@20210325 - name and packages seems like two different functions? That is,
# you're either setting up logger name, or you want to set up a bunch of loggers
# for the different packages?
def setup_logging(
    name, filename, console_level=logging.INFO, file_level=logging.INFO, packages=None, mode="w"
):
    """Configures logging to file and console.

    Parameters
    ----------
    name : str
        logger name
    filename : str | None
        log filename
    console_level : int, optional
        console log level. defaults to logging.INFO
    file_level : int, optional
        file log level. defaults to logging.INFO
    packages : list, optional
        enable logging for these package names. Always adds dsgrid.
    """
    log_config = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "basic": {"format": "%(message)s"},
            "short": {
                "format": "%(asctime)s - %(levelname)s [%(name)s "
                "%(filename)s:%(lineno)d] : %(message)s",
            },
            "detailed": {
                "format": "%(asctime)s - %(levelname)s [%(name)s "
                "%(filename)s:%(lineno)d] : %(message)s",
            },
        },
        "handlers": {
            "console": {
                "level": console_level,
                "formatter": "short",
                "class": "logging.StreamHandler",
            },
            "file": {
                "class": "logging.FileHandler",
                "level": file_level,
                "filename": filename,
                "mode": mode,
                "formatter": "detailed",
            },
        },
        "loggers": {
            name: {"handlers": ["console", "file"], "level": "DEBUG", "propagate": True},
        },
    }

    packages = packages or []
    packages = set(packages)
    packages.add("dsgrid")
    for package in packages:
        log_config["loggers"][package] = {
            "handlers": ["console"],
            "level": "DEBUG",
            "propagate": True,
        }
        if filename is not None:
            log_config["loggers"][package]["handlers"].append("file")

    # ETH@20210325 - This logic should be applied to packages as well? This makes
    # me think that this should really be two functions--one for setting up a
    # logger by name and the other for setting up loggers for a list of packages.
    # DT: I think the issue is fixed, but we can still consider your point.
    if filename is None:
        log_config["handlers"].pop("file")

    logging.config.dictConfig(log_config)
    logger = logging.getLogger(name)

    # TODO: more consideration is warranted, but this is usually what we want.
    # If we migrate dsgrid to use loguru, it will be easier. We could use the TRACE level
    # in dsgrid.
    chronify.loggers.setup_logging(
        console_level="WARNING",
        file_level="DEBUG",
        filename=filename,
        mode=mode,
    )
    return logger


def check_log_file_size(filename, limit_mb=10, no_prompts=False):
    if not filename.exists():
        return

    size_mb = filename.stat().st_size / (1024 * 1024)
    if size_mb > limit_mb and not no_prompts:
        msg = f"The log file {filename} has exceeded {limit_mb} MiB. Delete it? [Y] >>> "
        val = input(msg)
        if val == "" or val.lower() == "y":
            os.remove(filename)


@contextmanager
def disable_console_logging(name="dsgrid"):
    logger = logging.getLogger(name)
    console_level = None
    try:
        for handler in logger.handlers:
            if handler.name == "console":
                console_level = handler.level
                handler.setLevel(logging.FATAL)
                break
        yield
    finally:
        for handler in logger.handlers:
            if handler.name == "console":
                assert console_level is not None
                handler.setLevel(console_level)
                break
