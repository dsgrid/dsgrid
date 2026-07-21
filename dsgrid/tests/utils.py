from pathlib import Path

from dsgrid.ibis.functions import cache
from dsgrid.ibis.session import get_runtime_session
from dsgrid.ibis.operations import drop_columns, join_multiple_columns
from dsgrid.ibis.table_utils import count_rows


def read_parquet(filename: Path):
    """Read a Parquet file and load it into cache. This helps debugging with pytest --pdb.
    If you don't use this, the parquet file will get deleted on a failure and you won't be able
    to inspect the dataframe.
    """
    spark = get_runtime_session()
    df = cache(spark.read.parquet(Path(filename).as_posix()))
    # Force materialization so the cached data survives deletion of the source file.
    count_rows(df)
    return df


def read_parquet_two_table_format(path: Path):
    spark = get_runtime_session()
    load_data = spark.read.parquet((path / "load_data.parquet").as_posix())
    lookup = spark.read.parquet((path / "load_data_lookup.parquet").as_posix())
    table = drop_columns(join_multiple_columns(load_data, lookup, ["id"]), "id")
    return table
