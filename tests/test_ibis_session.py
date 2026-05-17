from enum import Enum
from functools import reduce
from typing import Optional, Union

import ibis
import pytest

from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidField, DSGInvalidOperation, DSGInvalidParameter
from dsgrid.ibis.backend import get_runtime_backend
from dsgrid.ibis.operations import cross_join_dfs, make_temp_view_name, union_all
from dsgrid.time.types import DayType
from dsgrid.ibis.table_utils import table_to_pandas
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.ibis.io import (
    CsvPartitionWriter,
    overwrite_dataframe_file,
    persist_intermediate_query,
    persist_table,
    read_dataframe,
    try_read_dataframe,
    write_dataframe,
    write_dataframe_and_auto_partition,
    _is_duckdb_io_exception,
    _is_spark_parquet_schema_exception,
    _read_natively,
    _write_table,
)
from dsgrid.ibis.models import get_type_from_union
from dsgrid.ibis.null_checks import check_for_nulls
from dsgrid.ibis.session import (
    ByteType,
    BooleanType,
    create_dataframe,
    create_dataframe_from_dicts,
    create_dataframe_from_dimension_ids,
    get_spark_session,
    create_dataframe_from_ids,
    create_dataframe_from_product,
    custom_runtime_conf,
    DoubleType,
    F,
    FloatType,
    get_active_session,
    get_duckdb_runtime_session,
    get_runtime_session,
    IntegerType,
    is_runtime_session_active,
    LongType,
    restart_runtime_session,
    restart_runtime_session_with_custom_conf,
    ShortType,
    SparkSession,
    SparkConf,
    StringType,
    StructField,
    StructType,
    TimestampNTZType,
    TimestampType,
    _duckdb_type_from_spark_type,
    _ibis_type_from_spark_type,
    _schema_names,
    _schema_types,
    use_duckdb,
    _create_ibis_table,
)
from dsgrid.ibis.tz import custom_time_zone, get_current_time_zone, set_current_time_zone


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
    # The wrapper no longer mirrors .conf — Spark-specific conf reads go
    # through get_spark_session() (Phase 14 tear-out).
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
        assert df.count().execute() == reduce(lambda x, y: x * y, [len(x) for x in data.values()])


def test_get_type_from_union():
    assert get_type_from_union(Optional[str]) is str
    assert get_type_from_union(Optional[DayType]) is str


def test_get_type_from_union_invalid():
    with pytest.raises(NotImplementedError, match="Unhandled Union type"):
        get_type_from_union(Union[str, int, None])


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
    assert session.read.json(json_file.as_posix()).count().execute() == 1

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
    assert _schema_types(StructType().add("score", DoubleType())) == {"score": "float64"}
    assert _schema_types({"a": "string"}) == {"a": "string"}
    assert _schema_types(["a", "b"]) is None


def test_schema_type_mappings():
    expected = [
        (BooleanType(), "boolean", "BOOLEAN"),
        (ByteType(), "int8", "TINYINT"),
        (ShortType(), "int16", "SMALLINT"),
        (IntegerType(), "int32", "INTEGER"),
        (LongType(), "int64", "BIGINT"),
        (FloatType(), "float32", "FLOAT"),
        (DoubleType(), "float64", "DOUBLE"),
        (StringType(), "string", "VARCHAR"),
        # TimestampType is Spark's TZ-aware instant type; map to DuckDB's
        # TIMESTAMP WITH TIME ZONE for round-trip parity. The prior code
        # mapped both Timestamp variants to "TIMESTAMP" and lost the TZ
        # distinction; TypeSpec separates them.
        (TimestampType(), "timestamp", "TIMESTAMP WITH TIME ZONE"),
        (TimestampNTZType(), "timestamp", "TIMESTAMP"),
    ]
    for data_type, ibis_type, duckdb_type in expected:
        assert _ibis_type_from_spark_type(data_type) == ibis_type
        assert _duckdb_type_from_spark_type(data_type) == duckdb_type


def test_schema_type_invalid():
    class UnsupportedType:
        pass

    with pytest.raises(NotImplementedError, match="Unsupported schema data type"):
        _ibis_type_from_spark_type(UnsupportedType())


def test_duckdb_type_from_spark_type_invalid():
    class UnsupportedType:
        pass

    with pytest.raises(NotImplementedError, match="Unsupported schema data type"):
        _duckdb_type_from_spark_type(UnsupportedType())


def test_parquet_exception_detection():
    class AnalysisException(Exception):
        pass

    class IOException(Exception):
        __module__ = "duckdb.fake"

    assert _is_spark_parquet_schema_exception(
        AnalysisException("Unable to infer schema for Parquet. It must be specified manually.")
    )
    assert _is_spark_parquet_schema_exception(AnalysisException("PATH_NOT_FOUND"))
    assert _is_spark_parquet_schema_exception(AnalysisException("Path does not exist"))
    assert not _is_spark_parquet_schema_exception(AnalysisException("other"))
    assert _is_duckdb_io_exception(IOException("bad parquet"))
    assert not _is_duckdb_io_exception(ValueError("bad parquet"))


def test_require_unique_raises():
    table = get_runtime_session().createDataFrame([("a",), ("a",)], ["id"])
    with pytest.raises(DSGInvalidField, match="duplicate entries"):
        from dsgrid.ibis.io import _post_process_dataframe

        _post_process_dataframe(table, require_unique=["id"])


def test_read_dataframe_and_write_error_paths(tmp_path):
    with pytest.raises(FileNotFoundError):
        read_dataframe(tmp_path / "missing.csv")

    unsupported = tmp_path / "table.txt"
    unsupported.write_text("a\n1\n")
    with pytest.raises(NotImplementedError, match="Unsupported file extension"):
        read_dataframe(unsupported)

    table = get_runtime_session().createDataFrame([(1,)], ["a"])
    with pytest.raises(NotImplementedError, match="Unsupported file format"):
        _write_table(table, (tmp_path / "table.invalid").as_posix(), "invalid")

    with pytest.raises(DSGInvalidParameter, match="only supports Parquet"):
        write_dataframe_and_auto_partition(table, tmp_path / "table.csv")


@pytest.mark.skipif(not use_duckdb(), reason="DuckDB file overwrite paths only apply to DuckDB")
def test_persist_and_overwrite_file_helpers(tmp_path):
    table = get_runtime_session().createDataFrame([(1, "a")], ["id", "name"])
    replacement = get_runtime_session().createDataFrame([(2, "b")], ["id", "name"])
    assert _create_ibis_table(table) is table

    csv_file = tmp_path / "table.csv"
    write_dataframe(table, csv_file)
    overwritten_csv = overwrite_dataframe_file(csv_file, replacement)
    assert overwritten_csv.count().execute() == 1

    json_file = tmp_path / "table.json"
    overwritten_json = overwrite_dataframe_file(json_file, replacement)
    assert overwritten_json.count().execute() == 1

    with pytest.raises(NotImplementedError, match="Unsupported file suffix"):
        overwrite_dataframe_file(tmp_path / "table.txt", table)

    if use_duckdb():
        duckdb_json = tmp_path / "duckdb.json"
        write_dataframe(table, duckdb_json)
        assert not duckdb_json.exists()
        assert (tmp_path / "duckdb.parquet").exists()

    with ScratchDirContext(tmp_path / "scratch") as context:
        path = persist_table(table, context, tag="test")
        assert path.exists()
        persisted = persist_intermediate_query(table, context)
        assert persisted.count().execute() == 1
        persisted_auto = persist_intermediate_query(table, context, auto_partition=True)
        assert persisted_auto.count().execute() == 1


def test_check_for_nulls_and_cross_join_union():
    table = get_runtime_session().createDataFrame([(1, "a"), (2, "b")], ["id", "name"])
    check_for_nulls(table)
    check_for_nulls(table, exclude_columns={"id", "name"})

    if use_duckdb():
        with_null_schema = StructType(
            [StructField("id", IntegerType()), StructField("name", StringType())]
        )
        with_null = get_runtime_session().createDataFrame([(1, None)], with_null_schema)
        with pytest.raises(DSGInvalidField, match="contains NULL"):
            check_for_nulls(with_null)

    assert cross_join_dfs([table]).columns == table.columns
    other = get_runtime_session().createDataFrame([("x",), ("y",)], ["letter"])
    assert cross_join_dfs([table, other]).count().execute() == 4
    # union_all preserves duplicates (matches Spark UNION ALL); Ibis's
    # default .union() would dedupe.
    assert union_all(table, table).count().execute() == 4


def test_current_time_zone_contexts():
    original = get_current_time_zone()
    try:
        set_current_time_zone("UTC")
        assert get_current_time_zone() == "UTC"
        with custom_time_zone("America/Denver"):
            assert get_current_time_zone() == "America/Denver"
        assert get_current_time_zone() == "UTC"
    finally:
        set_current_time_zone(original)


def test_csv_partition_writer_rollover(tmp_path):
    csv_dir = tmp_path / "csv_parts"
    with CsvPartitionWriter(csv_dir, max_partition_size_mb=0) as writer:
        writer.add_row(("a", "b"))
        writer.add_row(("c", "d"))

    files = sorted(csv_dir.iterdir())
    assert [x.name for x in files] == ["part1.csv", "part2.csv"]
    assert files[0].read_text() == "a,b\n"


def test_get_type_from_enum_union():
    class Example(Enum):
        ONE = "one"

    assert get_type_from_union(Optional[Example]) is str
