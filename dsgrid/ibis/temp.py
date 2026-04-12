from uuid import uuid4

import dsgrid
from dsgrid.common import BackendEngine


TEMP_TABLE_PREFIX = "tmp_dsgrid"


def make_temp_view_name() -> str:
    """Make a random name to be used as a temporary view."""
    return f"{TEMP_TABLE_PREFIX}_{uuid4().hex}"


def drop_temp_tables_and_views() -> None:
    """Drop dsgrid temporary Spark tables and views when running on Spark."""
    if dsgrid.runtime_config.backend_engine != BackendEngine.SPARK:
        return

    from dsgrid.ibis.session import get_spark_session, is_runtime_session_active

    if not is_runtime_session_active():
        return

    spark = get_spark_session()
    for row in spark.sql(f"SHOW TABLES LIKE '*{TEMP_TABLE_PREFIX}*'").collect():
        spark.sql(f"DROP TABLE {row.tableName}")
    for row in spark.sql(f"SHOW VIEWS LIKE '*{TEMP_TABLE_PREFIX}*'").collect():
        spark.sql(f"DROP VIEW {row.viewName}")
