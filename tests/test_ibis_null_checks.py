import pytest

from dsgrid.exceptions import DSGInvalidField
from dsgrid.ibis.null_checks import check_for_nulls
from dsgrid.ibis.session import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    get_runtime_session,
)
from dsgrid.ibis.types import use_duckdb


def test_check_for_nulls():
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
