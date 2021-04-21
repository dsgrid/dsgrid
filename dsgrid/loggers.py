"""Contains logging configuration data."""

import logging
import logging.config


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
        enable logging for these package names
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
            name: {"handlers": ["console", "file"], "level": "DEBUG", "propagate": False},
        },
    }

    packages = packages or []
    packages = set(packages)
    packages.add("dsgrid")
    for package in packages:
        log_config["loggers"][package] = {
            "handlers": ["console", "file"],
            "level": "DEBUG",
            "propagate": False,
        }

    # ETH@20210325 - This logic should be applied to packages as well? This makes
    # me think that this should really be two functions--one for setting up a
    # logger by name and the other for setting up loggers for a list of packages.
    if filename is None:
        log_config["handlers"].pop("file")
        log_config["loggers"][name]["handlers"].remove("file")

    logging.config.dictConfig(log_config)
    logger = logging.getLogger(name)

    return logger
