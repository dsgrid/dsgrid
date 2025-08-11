"""Manages the dsgrid runtime configuration file"""

import logging
import os
import sys
from pathlib import Path
from typing import Any
from warnings import warn

import json5
from pydantic import model_validator

from dsgrid.common import BackendEngine, DEFAULT_DB_PASSWORD, DEFAULT_SCRATCH_DIR
from dsgrid.data_models import DSGBaseModel

RC_FILENAME = ".dsgrid.json5"
DEFAULT_BACKEND = BackendEngine.DUCKDB
DEFAULT_THRIFT_SERVER_URL = "hive://localhost:10000/default"

logger = logging.getLogger(__name__)


class DsgridRuntimeConfig(DSGBaseModel):
    """Defines the runtime config that can be stored in users' home directories."""

    database_url: str | None = None
    database_user: str = "root"
    database_password: str = DEFAULT_DB_PASSWORD
    offline: bool = True
    backend_engine: BackendEngine = DEFAULT_BACKEND
    thrift_server_url: str = DEFAULT_THRIFT_SERVER_URL
    use_hive_metastore: bool = False
    console_level: str = "info"
    file_level: str = "info"
    timings: bool = False
    reraise_exceptions: bool = False
    scratch_dir: None | Path = None

    @model_validator(mode="before")
    @classmethod
    def environment_overrides(cls, values: dict[str, Any]) -> dict[str, Any]:
        for env, field in (
            ("DSGRID_BACKEND_ENGINE", "backend_engine"),
            ("THRIFT_SERVER_URL", "thrift_server_url"),
        ):
            if env in os.environ:
                values[field] = os.environ[env]
        return values

    @model_validator(mode="before")
    @classmethod
    def remove_legacy_fields(cls, data: dict[str, Any]) -> dict[str, Any]:
        for field in ("database_name",):
            res = data.pop(field, None)
            if res is not None:
                warn(
                    f"The dsgrid runtime config field {field} is deprecated. Please remove it. "
                    "This will cause an error in a future release.",
                )
        return data

    @classmethod
    def load(cls) -> "DsgridRuntimeConfig":
        """Load the dsgrid runtime config if it exists or one with default values."""
        rc_file = cls.path()
        if rc_file.exists():
            data = json5.loads(rc_file.read_text(encoding="utf-8-sig"))
            return cls(**data)
        return cls()

    def dump(self) -> None:
        """Dump the config to the user's home directory."""
        path = self.path()
        data = self.model_dump()
        data.pop("database_user")
        data.pop("database_password")
        with open(path, "w") as f_out:
            json5.dump(data, f_out, indent=2)
        print(f"Wrote dsgrid config to {path}", file=sys.stderr)

    @staticmethod
    def path() -> Path:
        """Return the path to the config file."""
        return Path.home() / RC_FILENAME

    def get_scratch_dir(self) -> Path:
        """Return the scratch_dir to use."""
        return self.scratch_dir or Path(DEFAULT_SCRATCH_DIR)
