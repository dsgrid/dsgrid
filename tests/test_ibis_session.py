from functools import reduce

import pytest

from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidOperation, DSGInvalidParameter
from dsgrid.ibis.backend import get_runtime_backend
from dsgrid.ibis.io import write_dataframe
from dsgrid.ibis.table_utils import count_rows
from dsgrid.ibis.temp import make_temp_view_name
from dsgrid.ibis.types import use_duckdb
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.ibis.session import (
    BooleanType,
    DoubleType,
    F,
    IntegerType,
    SparkConf,
    SparkSession,
    StringType,
    StructField,
    StructType,
    create_dataframe,
    create_dataframe_from_dicts,
    create_dataframe_from_dimension_ids,
    create_dataframe_from_ids,
    create_dataframe_from_product,
    custom_runtime_conf,
    get_active_session,
    get_duckdb_runtime_session,
    get_runtime_session,
    get_spark_session,
    is_runtime_session_active,
    restart_runtime_session,
    restart_runtime_session_with_custom_conf,
    _schema_names,
    _schema_types,
)


@pytest.mark.skipif(use_duckdb(), reason="This feature is not used with DuckDB")
def test_restart_runtime_session():
    # The wrapper no longer mirrors .conf — Spark-specific conf reads go
    # through get_spark_session().
    spark = get_spark_session()
    cur_partitions = spark.conf.get("spark.sql.shuffle.partitions")
    new_partitions = str(int(cur_partitions) + 1)
    cur_compress = spark.conf.get("spark.rdd.compress")
    if cur_compress.lower() == "true":
        new_compress = "false"
    elif cur_compress.lower() == "false":
        new_compress = "true"
    else:
        assert False, cur_compress

    conf = {
        "spark.sql.shuffle.partitions": new_partitions,
        "spark.rdd.compress": new_compress,
    }
    with restart_runtime_session_with_custom_conf(conf=conf):
        spark = get_spark_session()
        assert spark.conf.get("spark.sql.shuffle.partitions") == new_partitions
        assert spark.conf.get("spark.rdd.compress") == new_compress


@pytest.mark.skipif(use_duckdb(), reason="This feature is not used with DuckDB")
def test_custom_runtime_conf():
    orig_session_tz = get_spark_session().conf.get("spark.sql.session.timeZone")
    new_session_tz = "Etc/UTC" if orig_session_tz == "UTC" else "UTC"
    conf = {"spark.sql.session.timeZone": new_session_tz}
    with custom_runtime_conf(conf):
        assert get_spark_session().conf.get("spark.sql.session.timeZone") == new_session_tz
        restart_runtime_session(force=True)
        assert get_spark_session().conf.get("spark.sql.session.timeZone") == new_session_tz
    assert get_spark_session().conf.get("spark.sql.session.timeZone") == orig_session_tz


def test_create_dataframe_from_product(tmp_path):
    with ScratchDirContext(tmp_path / "scratch") as context:
        data = {
            "geography": [f"county_{i}" for i in range(200)],
            "scenario": [f"scenario_{i}" for i in range(10)],
            "model_year": [str(x) for x in range(2020, 2030)],
            "sector": ["com", "ind", "res", "trans"],
        }
        df = create_dataframe_from_product(data, context, max_partition_size_mb=1)
        assert count_rows(df) == reduce(lambda x, y: x * y, [len(x) for x in data.values()])


@pytest.mark.skipif(not use_duckdb(), reason="DuckDB compatibility shims only apply to DuckDB")
def test_duckdb_spark_function_shim_raises():
    with pytest.raises(DSGInvalidOperation, match="Spark function F.col is not available"):
        F.col("a")


@pytest.mark.skipif(not use_duckdb(), reason="DuckDB compatibility shims only apply to DuckDB")
def test_duckdb_spark_conf_shim():
    conf = SparkConf()
    assert conf.setAppName("test") is conf
    assert conf.set("spark.sql.shuffle.partitions", "1") is conf
    assert conf.get("spark.sql.shuffle.partitions", "200") == "200"


@pytest.mark.skipif(not use_duckdb(), reason="DuckDB compatibility shims only apply to DuckDB")
def test_duckdb_runtime_session_shims():
    """The DuckDB runtime session implements the small RuntimeSession API:
    createDataFrame, sql, and read.* — the wrapper no longer mirrors
    PySpark's full session shape (.conf / .catalog / .table / etc.) now
    that the Spark-warehouse helpers that needed those have been deleted.
    """
    session = get_runtime_session()
    assert get_duckdb_runtime_session() is session
    assert get_active_session() is session
    assert SparkSession.getActiveSession() is None
    assert (
        SparkSession.builder.config("spark.sql.shuffle.partitions", "1").getOrCreate() is session
    )
    assert is_runtime_session_active()

    table_name = make_temp_view_name()
    table = session.createDataFrame([(1, "a")], ["id", "name"])
    get_runtime_backend().create_view(table_name, table)
    assert get_runtime_backend().has_table(table_name)
    assert session.sql(f"SELECT count(*) AS c FROM {table_name}").execute()["c"].iloc[0] == 1
    assert session.sql("SELECT 1 AS value").execute()["value"].iloc[0] == 1
    with pytest.raises(DSGInvalidOperation, match="keyword dataframe bindings"):
        session.sql("SELECT * FROM {table}", table=table)


@pytest.mark.skipif(not use_duckdb(), reason="DuckDB compatibility shims only apply to DuckDB")
def test_duckdb_reader_shims(tmp_path):
    session = get_runtime_session()

    schema = StructType().add("id", IntegerType(), nullable=False).add("name", StringType())
    csv_with_header = tmp_path / "with_header.csv"
    csv_with_header.write_text("id,name\n1,a\n")
    table = session.read.csv(csv_with_header.as_posix(), schema=schema)
    assert table.schema()["id"].is_integer()

    json_file = tmp_path / "table.json"
    json_file.write_text('[{"id": 1, "name": "a"}]\n')
    assert count_rows(session.read.json(json_file.as_posix())) == 1

    parquet_file = tmp_path / "table.parquet"
    write_dataframe(table, parquet_file)
    assert session.read.parquet(parquet_file.as_posix()).count().execute() == 1


@pytest.mark.skipif(not use_duckdb(), reason="DuckDB compatibility shims only apply to DuckDB")
def test_create_dataframe_helpers_duckdb():
    assert create_dataframe([("a",)], require_unique=["col0"]).count().execute() == 1
    assert list(create_dataframe_from_ids(["a", "b"], "id").columns) == ["id"]
    assert list(create_dataframe_from_dicts([{"id": "a"}]).columns) == ["id"]

    with pytest.raises(DSGInvalidParameter, match="records cannot be empty"):
        create_dataframe_from_dicts([])

    table = create_dataframe_from_dimension_ids(
        [["geo1", "2020"]],
        DimensionType.GEOGRAPHY,
        DimensionType.MODEL_YEAR,
        cache=False,
    )
    assert list(table.columns) == ["geography", "model_year"]


def test_schema_helpers():
    schema = StructType(
        [
            StructField("name", StringType()),
            StructField("active", BooleanType()),
            StructField("count", IntegerType()),
        ]
    )
    assert _schema_names(schema) == ["name", "active", "count"]
    assert _schema_names(["a", "b"]) == ["a", "b"]
    assert _schema_names(1) == []

    assert _schema_types(schema) == {
        "name": "string",
        "active": "boolean",
        "count": "int32",
    }
    assert _schema_types(schema, ibis_types=False) == {
        "name": "VARCHAR",
        "active": "BOOLEAN",
        "count": "INTEGER",
    }
    assert _schema_types(StructType().add("score", DoubleType())) == {"score": "float64"}
    assert _schema_types({"a": "string"}) == {"a": "string"}
    assert _schema_types(["a", "b"]) is None
