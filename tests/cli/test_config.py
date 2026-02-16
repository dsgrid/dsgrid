"""Tests for dsgrid config CLI commands."""

from pathlib import Path

from click.testing import CliRunner

from dsgrid.cli.dsgrid import cli
from dsgrid.registry.common import make_sqlite_url


def test_config_create_invalid_url():
    """config create with a non-SQLite URL should exit with code 1."""
    runner = CliRunner()
    result = runner.invoke(cli, ["config", "create", "postgresql://localhost/mydb"])
    assert result.exit_code == 1
    assert "Failed to parse" in result.output


def test_config_create_nonexistent_db(tmp_path):
    """config create with a SQLite URL pointing to a missing file should exit with code 1."""
    url = make_sqlite_url(tmp_path / "does_not_exist.db")
    runner = CliRunner()
    result = runner.invoke(cli, ["config", "create", url])
    assert result.exit_code == 1
    assert "does not exist" in result.output


def test_config_create_success(tmp_path, monkeypatch):
    """config create with a valid URL should succeed."""
    db_file = tmp_path / "registry.db"
    db_file.touch()
    url = make_sqlite_url(db_file)
    # Dump writes to the user's home directory; redirect it to tmp_path.
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USERPROFILE", str(tmp_path))
    runner = CliRunner()
    result = runner.invoke(cli, ["config", "create", url])
    assert result.exit_code == 0
