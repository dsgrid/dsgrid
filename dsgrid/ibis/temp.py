import atexit
import logging
import shutil
from pathlib import Path
from uuid import uuid4

import dsgrid
from dsgrid.common import BackendEngine


TEMP_TABLE_PREFIX = "tmp_dsgrid"

logger = logging.getLogger(__name__)

# Tracks parquet files written by ``create_temp_view``'s last-resort fallback so they
# can be removed by ``drop_temp_tables_and_views`` and at process exit. The fallback
# uses ``NamedTemporaryFile(delete=False)`` because the file must outlive the
# ``NamedTemporaryFile`` handle (the temp view in DuckDB references the path), so
# Python won't clean it up automatically.
_tracked_temp_files: set[Path] = set()


def track_temp_file(path: str | Path) -> None:
    """Register ``path`` for cleanup by ``drop_temp_tables_and_views`` / process exit.

    ``path`` may be a single file or a directory (e.g. a partitioned parquet
    output written as a directory of part files); both are removed on cleanup.
    """
    _tracked_temp_files.add(Path(path))


def _delete_tracked_temp_files() -> None:
    while _tracked_temp_files:
        path = _tracked_temp_files.pop()
        try:
            if path.is_dir() and not path.is_symlink():
                shutil.rmtree(path)
            else:
                path.unlink(missing_ok=True)
        except OSError as exc:
            logger.warning("Failed to delete tracked temp file %s: %s", path, exc)


atexit.register(_delete_tracked_temp_files)


def make_temp_view_name() -> str:
    """Make a random name to be used as a temporary view."""
    return f"{TEMP_TABLE_PREFIX}_{uuid4().hex}"


def drop_temp_tables_and_views() -> None:
    """Drop dsgrid temporary tables, views, and tracked parquet fallback files."""
    _delete_tracked_temp_files()

    if dsgrid.runtime_config.backend_engine != BackendEngine.SPARK:
        return

    # Lazy import: dsgrid.ibis.session imports this module transitively at
    # bootstrap (via io.py -> operations.py -> temp.py), so importing
    # session.py at module level here would create a circular import.
    from dsgrid.ibis.session import get_spark_session, is_runtime_session_active

    if not is_runtime_session_active():
        return

    spark = get_spark_session()
    for row in spark.sql(f"SHOW TABLES LIKE '*{TEMP_TABLE_PREFIX}*'").collect():
        spark.sql(f"DROP TABLE {row.tableName}")
    for row in spark.sql(f"SHOW VIEWS LIKE '*{TEMP_TABLE_PREFIX}*'").collect():
        spark.sql(f"DROP VIEW {row.viewName}")
