from functools import reduce
from typing import Optional

import ibis
import pytest

from dsgrid.exceptions import DSGInvalidField, DSGInvalidOperation
from dsgrid.time.types import DayType
from dsgrid.ibis.table_utils import table_to_pandas
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.ibis.session import (
    BooleanType,
    create_dataframe_from_product,
    custom_runtime_conf,
    F,
    get_runtime_session,
    get_type_from_union,
    IntegerType,
    restart_runtime_session,
    restart_runtime_session_with_custom_conf,
    save_table,
    SparkConf,
    StringType,
    StructField,
    StructType,
    _duckdb_type_from_spark_type,
    _read_natively,
    _schema_names,
    _schema_types,
    try_read_dataframe,
    use_duckdb,
    write_dataframe,
)


def test_try_read_dataframe_invalid(tmp_path):
    invalid = tmp_path / "table.parquet"
    invalid.mkdir()
    assert try_read_dataframe(invalid) is None
    assert not invalid.exists()


def test_try_read_dataframe_valid(tmp_path):
    spark = get_runtime_session()
    df = spark.createDataFrame([(1,)], ["a"])
    filename = tmp_path / "table.parquet"
    write_dataframe(df, filename)
    df = try_read_dataframe(filename)
    assert isinstance(df, ibis.Table)
    assert table_to_pandas(df)["a"].iloc[0] == 1


@pytest.mark.skipif(use_duckdb(), reason="This feature is not used with DuckDB")
def test_restart_runtime_session():
    spark = get_runtime_session()
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
    with restart_runtime_session_with_custom_conf(conf=conf) as new_spark:
        assert new_spark.conf.get("spark.sql.shuffle.partitions") == new_partitions
        assert new_spark.conf.get("spark.rdd.compress") == new_compress


@pytest.mark.skipif(use_duckdb(), reason="This feature is not used with DuckDB")
def test_custom_runtime_conf():
    orig_session_tz = get_runtime_session().conf.get("spark.sql.session.timeZone")
    new_session_tz = "Etc/UTC" if orig_session_tz == "UTC" else "UTC"
    conf = {"spark.sql.session.timeZone": new_session_tz}
    with custom_runtime_conf(conf):
        assert get_runtime_session().conf.get("spark.sql.session.timeZone") == new_session_tz
        restart_runtime_session(force=True)
        assert get_runtime_session().conf.get("spark.sql.session.timeZone") == new_session_tz
    assert get_runtime_session().conf.get("spark.sql.session.timeZone") == orig_session_tz


def test_create_dataframe_from_product(tmp_path):
    with ScratchDirContext(tmp_path / "scratch") as context:
        data = {
            "geography": [f"county_{i}" for i in range(200)],
            "scenario": [f"scenario_{i}" for i in range(10)],
            "model_year": [str(x) for x in range(2020, 2030)],
            "sector": ["com", "ind", "res", "trans"],
        }
        df = create_dataframe_from_product(data, context, max_partition_size_mb=1)
        assert df.count().execute() == reduce(lambda x, y: x * y, [len(x) for x in data.values()])


def test_get_type_from_union():
    assert get_type_from_union(Optional[str]) is str
    assert get_type_from_union(Optional[DayType]) is str


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
def test_save_table_duckdb_raises():
    table = get_runtime_session().createDataFrame([(1,)], ["a"])
    with pytest.raises(DSGInvalidOperation, match="save_table is not supported"):
        save_table(table, "not_supported")


def test_read_natively_unsupported_extension(tmp_path):
    filename = tmp_path / "table.txt"
    filename.write_text("a\n1\n")
    with pytest.raises(NotImplementedError, match="Unsupported file extension"):
        _read_natively(filename)


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
    assert _schema_types({"a": "string"}) == {"a": "string"}
    assert _schema_types(["a", "b"]) is None


def test_duckdb_type_from_spark_type_invalid():
    class UnsupportedType:
        pass

    with pytest.raises(NotImplementedError, match="Unsupported schema data type"):
        _duckdb_type_from_spark_type(UnsupportedType())


def test_require_unique_raises():
    table = get_runtime_session().createDataFrame([("a",), ("a",)], ["id"])
    with pytest.raises(DSGInvalidField, match="duplicate entries"):
        from dsgrid.ibis.session import _post_process_dataframe

        _post_process_dataframe(table, require_unique=["id"])
