"""Manages the dsgrid runtime configuration file"""

import logging
import sys
from pathlib import Path

import json5

from dsgrid.common import DEFAULT_DB_PASSWORD
from dsgrid.data_models import DSGBaseModel
from dsgrid.utils.files import dump_data

RC_FILENAME = ".dsgrid.json5"

logger = logging.getLogger(__name__)


class DsgridRuntimeConfig(DSGBaseModel):
    """Defines the runtime config that can be stored in users' home directories."""

    database_name: str | None = None
    database_url: str | None = "http://localhost:8529"
    database_user: str = "root"
    database_password: str = DEFAULT_DB_PASSWORD
    offline: bool = False
    console_level: str = "info"
    file_level: str = "info"
    timings: bool = False
    reraise_exceptions: bool = False

    @classmethod
    def load(cls):
        """Load the dsgrid runtime config if it exists or one with default values."""
        rc_file = cls.path()
        if rc_file.exists():
            data = json5.loads(rc_file.read_text(encoding="utf-8"))
            return cls(**data)
        return cls()

    def dump(self):
        """Dump the config to the user's home directory."""
        path = self.path()
        dump_data(self.model_dump(), path, indent=2)
        print(f"Wrote dsgrid config to {path}", file=sys.stderr)

    @staticmethod
    def path() -> Path:
        """Return the path to the config file."""
        return Path.home() / RC_FILENAME
