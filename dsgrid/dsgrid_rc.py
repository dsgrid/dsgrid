"""Manages the dsgrid runtime configuration file"""

import logging
import os
import sys
from pathlib import Path
from typing import Any, Optional
from warnings import warn

import json5
from pydantic import model_validator

from dsgrid.common import BackendEngine, DEFAULT_DB_PASSWORD, DEFAULT_SCRATCH_DIR
from dsgrid.data_models import DSGBaseModel
from dsgrid.utils.files import dump_data

RC_FILENAME = ".dsgrid.json5"
DEFAULT_BACKEND = BackendEngine.DUCKDB
DEFAULT_CHRONFIY_BACKEND = BackendEngine.DUCKDB
DEFAULT_SPARK_CLUSTER_URL = "spark://<localhost>:7077"  # 'localhost' doesn't always work
# replace with hostname at runtime
DEFAULT_THRIFT_SERVER_EXEC = "start-thriftserver.sh"
DEFAULT_THRIFT_SERVER_URL = "hive://localhost:10000/default"

logger = logging.getLogger(__name__)


class DsgridRuntimeConfig(DSGBaseModel):
    """Defines the runtime config that can be stored in users' home directories."""

    database_url: Optional[str] = None
    database_user: str = "root"
    database_password: str = DEFAULT_DB_PASSWORD
    offline: bool = True
    backend_engine: BackendEngine = DEFAULT_BACKEND
    chronify_backend_engine: BackendEngine = DEFAULT_CHRONFIY_BACKEND
    spark_cluster_url: str = DEFAULT_SPARK_CLUSTER_URL
    thrift_server_exec: str = DEFAULT_THRIFT_SERVER_EXEC
    thrift_server_url: str = DEFAULT_THRIFT_SERVER_URL
    thrift_server_exec: str = DEFAULT_THRIFT_SERVER_URL
    console_level: str = "info"
    file_level: str = "info"
    timings: bool = False
    reraise_exceptions: bool = False
    scratch_dir: None | Path = None

    @model_validator(mode="before")
    @classmethod
    def default_engines(cls, values: dict[str, Any]) -> dict[str, Any]:
        if "DSGRID_BACKEND_ENGINE" in os.environ:
            values["backend_engine"] = os.environ["DSGRID_BACKEND_ENGINE"]
        if "CHRONIFY_BACKEND_ENGINE" in os.environ:
            values["chronify_backend_engine"] = os.environ["CHRONIFY_BACKEND_ENGINE"]
        if "THRIFT_SERVER_URL" in os.environ:
            values["thrift_server_url"] = os.environ["THRIFT_SERVER_URL"]
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
        dump_data(self.model_dump(), path, indent=2)
        print(f"Wrote dsgrid config to {path}", file=sys.stderr)

    @staticmethod
    def path() -> Path:
        """Return the path to the config file."""
        return Path.home() / RC_FILENAME

    def get_scratch_dir(self) -> Path:
        """Return the scratch_dir to use."""
        return self.scratch_dir or Path(DEFAULT_SCRATCH_DIR)

    def get_thrift_server_start_command(self) -> str:
        """Return a command that can be used to start a Thrift server."""
        return f"{self.thrift_server_exec} --master={self.spark_cluster_url}"

    def get_thrift_server_stop_command(self) -> str:
        """Return a command that can be used to stop a Thrift server."""
        return self.thrift_server_exec.replace("start-thrift", "stop-thrift")
