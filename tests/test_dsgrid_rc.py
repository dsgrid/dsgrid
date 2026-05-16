"""Tests for the dsgrid runtime configuration loader."""

import json
import warnings
from pathlib import Path

import pytest

from dsgrid.common import BackendEngine
from dsgrid.dsgrid_rc import DsgridRuntimeConfig


def test_defaults() -> None:
    config = DsgridRuntimeConfig()
    assert config.backend_engine == BackendEngine.DUCKDB
    assert config.console_level == "info"
    assert config.offline is True


def test_load_from_file(tmp_path: Path) -> None:
    rc_file = tmp_path / "dsgrid.json5"
    rc_file.write_text(json.dumps({"backend_engine": "spark", "console_level": "debug"}))
    config = DsgridRuntimeConfig.load(filename=rc_file)
    assert config.backend_engine == BackendEngine.SPARK
    assert config.console_level == "debug"


@pytest.mark.parametrize("field", ["database_name", "thrift_server_url", "use_hive_metastore"])
def test_legacy_field_emits_deprecation_warning(field: str) -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        config = DsgridRuntimeConfig(**{field: "anything"})
    deprecation_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
    assert len(deprecation_warnings) == 1
    assert field in str(deprecation_warnings[0].message)
    assert not hasattr(config, field)


def test_unknown_field_is_ignored_without_warning() -> None:
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        DsgridRuntimeConfig(some_future_field="x")
    deprecation_warnings = [w for w in caught if issubclass(w.category, DeprecationWarning)]
    assert deprecation_warnings == []
