"""Manages the dsgrid runtime configuration file"""

import logging
import os
from pathlib import Path
from typing import Any
from warnings import warn

from pydantic import model_validator

from dsgrid.common import BackendEngine, DEFAULT_DB_PASSWORD, DEFAULT_SCRATCH_DIR
from dsgrid.data_models import DSGBaseModel
from dsgrid.utils.files import dump_json_file, load_json_file

RC_FILENAME = ".dsgrid.json5"
DEFAULT_BACKEND = BackendEngine.DUCKDB
_LEGACY_FIELDS: tuple[str, ...] = (
    "database_name",
    "thrift_server_url",
    "use_hive_metastore",
)

logger = logging.getLogger(__name__)


class DsgridRuntimeConfig(DSGBaseModel):
    """Defines the runtime config that can be stored in users' home directories."""

    model_config = DSGBaseModel.model_config | {"extra": "ignore"}

    database_url: str | None = None
    database_user: str = "root"
    database_password: str = DEFAULT_DB_PASSWORD
    offline: bool = True
    backend_engine: BackendEngine = DEFAULT_BACKEND
    console_level: str = "info"
    file_level: str = "info"
    timings: bool = False
    reraise_exceptions: bool = False
    scratch_dir: None | Path = None

    @model_validator(mode="before")
    @classmethod
    def environment_overrides(cls, values: dict[str, Any]) -> dict[str, Any]:
        for env, field in (("DSGRID_BACKEND_ENGINE", "backend_engine"),):
            if env in os.environ:
                values[field] = os.environ[env]
        return values

    @model_validator(mode="before")
    @classmethod
    def warn_legacy_fields(cls, data: dict[str, Any]) -> dict[str, Any]:
        for field in _LEGACY_FIELDS:
            if data.pop(field, None) is not None:
                msg = (
                    f"The dsgrid runtime config field {field!r} is deprecated and is being "
                    f"ignored. Please remove it from {RC_FILENAME}. This will cause an error "
                    "in a future release."
                )
                # Log as well as warn. This validator runs at ``import dsgrid`` time, and
                # Python's default filters hide a DeprecationWarning raised from library
                # code, so the warning alone never reaches a user running the CLI -- which
                # is exactly the "silently ignored" behavior the deprecation exists to
                # avoid. The warning is kept for programmatic callers and tests.
                logger.warning(msg)
                warn(msg, DeprecationWarning, stacklevel=2)
        return data

    @classmethod
    def load(cls, filename=None) -> "DsgridRuntimeConfig":
        """Load the dsgrid runtime config if it exists or one with default values."""
        rc_file = cls.path() if filename is None else Path(filename)
        if rc_file.exists():
            data = load_json_file(rc_file)
            return cls(**data)
        return cls()

    def dump(self) -> None:
        """Dump the config to the user's home directory."""
        path = self.path()
        data = self.model_dump()
        data.pop("database_user")
        data.pop("database_password")
        dump_json_file(data, path, indent=2)
        logger.info("Wrote dsgrid config to %s", path)

    @staticmethod
    def path() -> Path:
        """Return the path to the config file."""
        return Path.home() / RC_FILENAME

    def get_scratch_dir(self) -> Path:
        """Return the scratch_dir to use."""
        return (self.scratch_dir or Path(DEFAULT_SCRATCH_DIR)).resolve()
