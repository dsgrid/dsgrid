import logging
import os
from datetime import datetime
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Iterable
from uuid import uuid4
from zoneinfo import ZoneInfo

import pandas as pd

from dsgrid.exceptions import DSGInvalidDimension
from dsgrid.loggers import disable_console_logging
from dsgrid.spark.types import (
    DataFrame,
    F,
    SparkConf,
    SparkSession,
    TimestampType,
    use_duckdb,
)
from dsgrid.utils.files import load_line_delimited_json, dump_data

logger = logging.getLogger(__name__)


if use_duckdb():
    g_spark = SparkSession.builder.getOrCreate()
else:
    g_spark = None


TEMP_TABLE_PREFIX = "tmp_dsgrid"


def aggregate(df: DataFrame, agg_func: str, column: str, alias: str) -> DataFrame:
    """Run an aggregate function on the dataframe."""
    if use_duckdb():
        relation = df.relation.aggregate(f"{agg_func}({column}) as {alias}")
        return DataFrame(relation.set_alias(make_temp_view_name()), df.session)
    return df.agg(getattr(F, agg_func)(column).alias(alias))


def aggregate_single_value(df: DataFrame, agg_func: str, column: str) -> Any:
    """Run an aggregate function on the dataframe."""
    alias = "__tmp__"
    if use_duckdb():
        return df.relation.aggregate(f"{agg_func}({column}) as {alias}").df().values[0][0]
    return df.agg(getattr(F, agg_func)(column).alias(alias)).collect()[0][alias]


def cache(df: DataFrame) -> DataFrame:
    """Cache the dataframe."""
    if use_duckdb():
        return df
    return df.cache()


def unpersist(df: DataFrame) -> None:
    """Unpersist the dataframe."""
    if not use_duckdb():
        df.unpersist()


def coalesce(df: DataFrame, num_partitions: int) -> DataFrame:
    """Coalesce the dataframe into num_partitions partitions."""
    if use_duckdb():
        return df
    return df.coalesce(num_partitions)


def collect_list(df: DataFrame, column: str) -> list:
    """Collect the dataframe into a list."""
    if use_duckdb():
        return [x[column] for x in df.collect()]

    return next(iter(df.select(F.collect_list(column)).first()))


def count_distinct_on_group_by(
    df: DataFrame, group_by_columns: list[str], agg_column: str, alias: str
) -> DataFrame:
    """Perform a count distinct on one column after grouping."""
    # This could be more customizable.
    if use_duckdb():
        view = create_temp_view(df)
        cols = ",".join(group_by_columns)
        query = f"""
            SELECT {cols}, COUNT(DISTINCT {agg_column}) AS {alias}
            FROM {view}
            GROUP BY {cols}
        """
        return get_spark_session().sql(query)

    return df.groupBy(*group_by_columns).agg(F.count_distinct(agg_column).alias(alias))


def create_temp_view(df: DataFrame) -> str:
    view1 = make_temp_view_name()
    view2 = make_temp_view_name()
    df.createOrReplaceTempView(view1)
    get_spark_session().sql(f"CREATE TABLE {view2} AS SELECT * from {view1}")
    return view2
    # TODO duckdb: something is broken.
    # Should be able to create a view with SQL or df.createOrReplaceTempView, but those get
    # lost or corrupted. Refer to https://github.com/duckdb/duckdb/issues/12987


def make_temp_view_name() -> str:
    return f"{TEMP_TABLE_PREFIX}_{uuid4().hex}"


def drop_temp_tables_and_views() -> None:
    """Drop all temporary views and tables."""
    drop_temp_views()
    drop_temp_tables()


def drop_temp_tables() -> None:
    """Drop all temporary tables."""
    if not use_duckdb():
        return

    spark = get_spark_session()
    query = f"SELECT * FROM pg_tables WHERE tablename LIKE '{TEMP_TABLE_PREFIX}%'"
    for row in spark.sql(query).collect():
        spark.sql(f"DROP TABLE {row.tablename}")
        logger.debug("Dropped temp table %s", row.tablename)


def drop_temp_views() -> None:
    """Drop all temporary views."""
    if not use_duckdb():
        return

    spark = get_spark_session()
    query = "SELECT view_name FROM duckdb_views() WHERE NOT internal AND view_name LIKE '{TEMP_TABLE_PREFIX}%'"
    for row in spark.sql(query).collect():
        spark.sql(f"DROP VIEW {row.view_name}")
        logger.debug("Dropped temp view %s", row.view_name)


def cross_join(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """Return a cross join of the two dataframes."""
    if use_duckdb():
        view1 = create_temp_view(df1)
        view2 = create_temp_view(df2)
        spark = get_spark_session()
        return spark.sql(f"SELECT {view1}.*, {view2}.* FROM {view1} CROSS JOIN {view2}")

    return df1.crossJoin(df2)


def except_all(df1: DataFrame, df2: DataFrame) -> DataFrame:
    """Return a dataframe with all rows in df1 that are not in df2."""
    method = _except_all_duckdb if use_duckdb() else _except_all_spark
    return method(df1, df2)


def _except_all_duckdb(df1: DataFrame, df2: DataFrame) -> DataFrame:
    view1 = create_temp_view(df1)
    view2 = create_temp_view(df2)
    query = f"""
        SELECT * FROM {view1}
        EXCEPT ALL
        SELECT * FROM {view2}
    """
    spark = get_spark_session()
    return spark.sql(query)


def _except_all_spark(df1: DataFrame, df2: DataFrame) -> DataFrame:
    return df1.exceptAll(df2)


def shift_time_zone(
    df: DataFrame, time_column: str, from_time_zone: str, to_time_zone: str, new_column: str
) -> DataFrame:
    """Shift the time zone of the time_column. new_column can be the same as time_column."""
    if use_duckdb():
        view = create_temp_view(df)
        cols = df.columns[:]
        if time_column == new_column:
            cols.remove(time_column)
        cols_str = ",".join(cols)
        query = f"""
            SELECT
                {cols_str},
                CAST(timezone('{to_time_zone}', {time_column}) AS TIMESTAMPTZ) AS {new_column}
            FROM {view}
        """
        return get_spark_session().sql(query)

    return df.withColumn(
        new_column,
        F.from_utc_timestamp(F.to_utc_timestamp(F.col(time_column), from_time_zone), to_time_zone),
    )


# TODO duckdb: these next two functions are likely incorrect.
# Usage in the codebase is questionable.
def from_utc_timestamp(
    df: DataFrame, time_column: str, time_zone: str, new_column: str
) -> DataFrame:
    """Refer to pyspark.sql.functions.from_utc_timestamp."""
    if use_duckdb():
        view = create_temp_view(df)
        cols = df.columns[:]
        if time_column == new_column:
            cols.remove(time_column)
        cols_str = ",".join(cols)
        query = f"""
            SELECT
                {cols_str},
                CAST(timezone('{time_zone}', {time_column}) AS TIMESTAMPTZ) AS {new_column}
            FROM {view}
        """
        df2 = get_spark_session().sql(query)
        return df2

    df2 = df.withColumn(new_column, F.from_utc_timestamp(time_column, time_zone))
    return df2


def to_utc_timestamp(
    df: DataFrame, time_column: str, time_zone: str, new_column: str
) -> DataFrame:
    """Refer to pyspark.sql.functions.to_utc_timestamp."""
    if use_duckdb():
        view = create_temp_view(df)
        cols = df.columns[:]
        if time_column == new_column:
            cols.remove(time_column)
        cols_str = ",".join(cols)
        query = f"""
            SELECT
                {cols_str},
                CAST(timezone('{time_zone}', {time_column}) AS TIMESTAMPTZ) AS {new_column}
            FROM {view}
        """
        df2 = get_spark_session().sql(query)
        return df2

    df2 = df.withColumn(new_column, F.to_utc_timestamp(time_column, time_zone))
    return df2


def get_duckdb_spark_session() -> SparkSession | None:
    """Return the active DuckDB Spark Session if it is set."""
    return g_spark


def get_spark_session() -> SparkSession:
    """Return the active SparkSession or create a new one is none is active."""
    spark = get_duckdb_spark_session()
    if spark is not None:
        return spark

    spark = SparkSession.getActiveSession()
    if spark is None:
        logger.warning("Could not find a SparkSession. Create a new one.")
        spark = SparkSession.builder.getOrCreate()
        log_spark_conf(spark)
    return spark


def get_current_time_zone() -> str:
    """Return the current time zone."""
    spark = get_spark_session()
    if use_duckdb():
        res = spark.sql("SELECT * FROM duckdb_settings() WHERE name = 'TimeZone'").collect()
        assert len(res) == 1
        return res[0].value

    tz = spark.conf.get("spark.sql.session.timeZone")
    assert tz is not None
    return tz


def set_current_time_zone(time_zone: str) -> None:
    """Set the current time zone."""
    spark = get_spark_session()
    if use_duckdb():
        spark.sql(f"SET TimeZone='{time_zone}'")
        return

    spark.conf.set("spark.sql.session.timeZone", time_zone)


def init_spark(name="dsgrid", check_env=True, spark_conf=None):
    """Initialize a SparkSession.

    Parameters
    ----------
    name : str
    check_env : bool
        If True, which is default, check for the SPARK_CLUSTER environment variable and attach to
        it. Otherwise, create a local-mode cluster or attach to the SparkSession that was created
        by pyspark/spark-submit prior to starting the current process.
    spark_conf : dict | None, defaults to None
        If set, Spark configuration parameters

    """
    if use_duckdb():
        return g_spark

    cluster = os.environ.get("SPARK_CLUSTER")
    conf = SparkConf().setAppName(name)
    if spark_conf is not None:
        for key, val in spark_conf.items():
            conf.set(key, val)

    out_ts_type = conf.get("spark.sql.parquet.outputTimestampType")
    if out_ts_type is None:
        conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
    elif out_ts_type != "TIMESTAMP_MICROS":
        logger.warning(
            "spark.sql.parquet.outputTimestampType is set to %s. Writing parquet files may "
            "produced undesired results.",
            out_ts_type,
        )

    if check_env and cluster is not None:
        logger.info("Create SparkSession %s on existing cluster %s", name, cluster)
        conf.setMaster(cluster)
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    with disable_console_logging():
        log_spark_conf(spark)
        logger.info("Custom configuration settings: %s", spark_conf)

    return spark


def is_dataframe_empty(df: DataFrame) -> bool:
    """Return True if the DataFrame is empty."""
    if use_duckdb():
        view = create_temp_view(df)
        spark = get_spark_session()
        col = df.columns[0]
        return spark.sql(f"SELECT {col} FROM {view} LIMIT 1").count() == 0
    return df.rdd.isEmpty()


def interval(df: DataFrame, time_column, op: str, val: Any, unit: str, alias: str) -> DataFrame:
    """Perform an interval operation ('-' or '+') on a time column."""
    if use_duckdb():
        view = create_temp_view(df)
        cols = df.columns[:]
        if alias == time_column:
            cols.remove(time_column)
        cols_str = ",".join(cols)
        query = (
            f"SELECT {time_column} {op} INTERVAL {val} {unit} AS {alias}, {cols_str} from {view}"
        )
        return get_spark_session().sql(query)

    interval_expr = F.expr(f"INTERVAL {val} SECONDS")
    match op:
        case "-":
            expr = F.col(time_column) - interval_expr
        case "+":
            expr = F.col(time_column) + interval_expr
        case _:
            msg = f"{op=} is not supported"
            raise NotImplementedError(msg)
    return df.withColumn(alias, expr)


def join_multiple_columns(
    df1: DataFrame, df2: DataFrame, columns: list[str], how="inner"
) -> DataFrame:
    """Join two dataframes on multiple columns."""
    if use_duckdb():
        view1 = create_temp_view(df1)
        view2 = create_temp_view(df2)
        view2_columns = ",".join((f"{view2}.{x}" for x in df2.columns if x not in columns))
        on_str = " AND ".join((f"{view1}.{x} = {view2}.{x}" for x in columns))
        query = f"""
            SELECT {view1}.*, {view2_columns}
            FROM {view1}
            {how} JOIN {view2}
            ON {on_str}
        """
        return get_spark_session().sql(query)

    return df1.join(df2, columns, how=how)


def log_spark_conf(spark: SparkSession):
    """Log the Spark configuration details."""
    if not use_duckdb():
        conf = spark.sparkContext.getConf().getAll()
        conf.sort(key=lambda x: x[0])
        logger.info("Spark conf: %s", "\n".join([f"{x} = {y}" for x, y in conf]))


def prepare_timestamps_for_dataframe(timestamps: Iterable[datetime]) -> Iterable[datetime]:
    """Apply necessary conversions of the timestamps for dataframe creation."""
    if use_duckdb():
        return [x.astimezone(ZoneInfo("UTC")) for x in timestamps]
    return timestamps


def read_csv(path: Path | str) -> DataFrame:
    """Return a DataFrame from a CSV file, handling special cases with duckdb."""
    spark = get_spark_session()
    if use_duckdb():
        path_ = path if isinstance(path, Path) else Path(path)
        if path_.is_dir():
            # path_str = str(path_) + "**/*.csv"
            files = list(path_.glob("*.csv"))
            assert len(files) == 1, files
            path_str = str(files[0])
        else:
            path_str = str(path_)
        # df = spark.read.csv(path_str, header=True)
        # for field in df.schema:
        #    if field.dataType is TimestampNTZType():
        #        df = df.withColumn(field.name, F.col(field.name).cast(TimestampType()))
        df = spark.createDataFrame(pd.read_csv(path_str))
        if "timestamp" in df.columns:
            # TODO duckdb: do something better
            df = df.withColumn("timestamp", F.col("timestamp").cast(TimestampType()))
        dup_cols = [x for x in df.columns if x.endswith(".1")]
        if dup_cols:
            msg = f"Detected a duplicate column in the dataset: {dup_cols}"
            raise DSGInvalidDimension(msg)
        return df
    return spark.read.csv(str(path), header=True, inferSchema=True)


def read_json(path: Path | str) -> DataFrame:
    """Return a DataFrame from a JSON file, handling special cases with duckdb.

    Warning: Use of this function with DuckDB is not efficient because it requires that we
    convert line-delimited JSON to standard JSON.
    """
    spark = get_spark_session()
    filename = str(path)
    if use_duckdb():
        with NamedTemporaryFile(suffix=".json") as f:
            f.close()
            data = load_line_delimited_json(path)
            dump_data(data, f.name)
            return spark.read.json(f.name)
    return spark.read.json(filename, mode="FAILFAST")


def read_parquet(path: Path | str) -> DataFrame:
    path = Path(path) if isinstance(path, str) else path
    spark = get_spark_session()
    if path.is_file() or not use_duckdb():
        df = spark.read.parquet(str(path))
    else:
        df = spark.read.parquet(f"{path}/**/*.parquet")
    return df


def select_expr(df: DataFrame, exprs: list[str]) -> DataFrame:
    if use_duckdb():
        view = create_temp_view(df)
        spark = get_spark_session()
        cols = ",".join(exprs)
        return spark.sql(f"SELECT {cols} FROM {view}")
    return df.selectExpr(*exprs)


def sql_from_df(df: DataFrame, query: str) -> DataFrame:
    """Run a SQL query on a dataframe with Spark."""
    logger.debug("Run SQL query [%s]", query)
    spark = get_spark_session()
    if use_duckdb():
        view = create_temp_view(df)
        query += f" FROM {view}"
        return spark.sql(query)

    query += " FROM {df}"
    return spark.sql(query, df=df)


def pivot(df: DataFrame, name_column: str, value_column: str) -> DataFrame:
    """Unpivot the dataframe."""
    method = _pivot_duckdb if use_duckdb() else _pivot_spark
    return method(df, name_column, value_column)


def _pivot_duckdb(df: DataFrame, name_column: str, value_column: str) -> DataFrame:
    view = create_temp_view(df)
    query = f"""
        PIVOT {view}
        ON {name_column}
        USING SUM({value_column})
    """
    return get_spark_session().sql(query)


def _pivot_spark(df: DataFrame, name_column: str, value_column: str) -> DataFrame:
    ids = [x for x in df.columns if x not in {name_column, value_column}]
    return df.groupBy(*ids).pivot(name_column).sum(value_column)


def unpivot(df: DataFrame, pivoted_columns, name_column: str, value_column: str) -> DataFrame:
    """Unpivot the dataframe."""
    method = _unpivot_duckdb if use_duckdb() else _unpivot_spark
    return method(df, pivoted_columns, name_column, value_column)


def _unpivot_duckdb(
    df: DataFrame, pivoted_columns, name_column: str, value_column: str
) -> DataFrame:
    view = create_temp_view(df)
    cols = ",".join(pivoted_columns)
    query = f"""
        SELECT * FROM {view}
        UNPIVOT INCLUDE NULLS (
            {value_column}
            FOR {name_column} in ({cols})
        )
    """
    spark = get_spark_session()
    df = spark.sql(query)
    return df


def _unpivot_spark(
    df: DataFrame, pivoted_columns, name_column: str, value_column: str
) -> DataFrame:
    ids = list(set(df.columns) - {value_column, *pivoted_columns})
    return df.unpivot(
        ids,
        pivoted_columns,
        name_column,
        value_column,
    )
