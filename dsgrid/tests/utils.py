from pathlib import Path

from dsgrid.ibis.session import get_runtime_session
from dsgrid.ibis.operations import drop_columns, join_multiple_columns


def read_parquet(filename: Path):
    """Read a Parquet file into an ibis table.

    Deliberately does not cache: on Spark, ``cache()`` returns a ``CachedTable``
    whose backing view is dropped once that object is garbage-collected, and
    callers immediately wrap the result in further expressions without retaining
    it -- so caching here yields a use-after-free (``TABLE_OR_VIEW_NOT_FOUND``)
    when a later operation executes the derived expression.
    """
    return get_runtime_session().read.parquet(Path(filename).as_posix())


def read_parquet_two_table_format(path: Path):
    spark = get_runtime_session()
    load_data = spark.read.parquet((path / "load_data.parquet").as_posix())
    lookup = spark.read.parquet((path / "load_data_lookup.parquet").as_posix())
    table = drop_columns(join_multiple_columns(load_data, lookup, ["id"]), "id")
    return table
