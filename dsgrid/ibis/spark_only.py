"""Helpers that drop down to raw PySpark APIs.

The functions in this module exist only because Ibis does not abstract the
underlying behavior. They are no-ops or trivially-mapped on the DuckDB backend
and only exercise PySpark internals when the runtime backend is Spark. Keep
new APIs out of this module unless they genuinely require Spark-specific
control (partition layout, executor hints, etc.).
"""

from typing import Any, cast

import ibis

from dsgrid.ibis.backend import make_runtime_backend
from dsgrid.ibis.operations import create_temp_view
from dsgrid.ibis.temp import make_temp_view_name
from dsgrid.ibis.types import use_duckdb


def coalesce(df: ibis.Table, num_partitions: int) -> ibis.Table:
    """Reduce the number of output partitions.

    On DuckDB this is a no-op (single-file output by default). On Spark it
    coalesces the underlying PySpark DataFrame and re-registers it as an
    Ibis table so downstream writers produce ``num_partitions`` files.
    """
    if use_duckdb():
        return df
    view = create_temp_view(df)
    backend = cast(Any, make_runtime_backend())
    spark_df = backend.connection._session.sql(f"SELECT * FROM {view}")
    coalesced = spark_df.coalesce(num_partitions)
    coalesced_view = make_temp_view_name()
    coalesced.createOrReplaceTempView(coalesced_view)
    return backend.connection.table(coalesced_view)
