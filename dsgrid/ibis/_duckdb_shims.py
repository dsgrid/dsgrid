"""DuckDB-side stub classes that mirror the PySpark API shape.

When the runtime backend is Spark, :mod:`dsgrid.ibis.session` imports the
real symbols from pyspark and this module is unused. When the runtime
backend is DuckDB, callers see these stubs which match the PySpark API
shape (so type annotations and ``StructField``/``StructType`` builders
work the same way) without pulling in PySpark.

This module is intentionally a leaf: it has no dependencies on session
state, so importing it from session.py is safe regardless of import
order. The session-coupled DuckDB classes (``_DuckDBConf``,
``_DuckDBCatalog``, ``_DuckDBReader``, ``_SparkSessionBuilder``,
``SparkSession``) live in :mod:`dsgrid.ibis.session` itself because
they reference ``get_runtime_session`` and the runtime-backend cache.
"""

from dsgrid.exceptions import DSGInvalidOperation


class _SparkType:
    """Common base used to mark the duckdb-side type stubs."""


class BooleanType(_SparkType):
    pass


class ByteType(_SparkType):
    pass


class DoubleType(_SparkType):
    pass


class FloatType(_SparkType):
    pass


class IntegerType(_SparkType):
    pass


class LongType(_SparkType):
    pass


class ShortType(_SparkType):
    pass


class StringType(_SparkType):
    pass


class TimestampNTZType(_SparkType):
    pass


class TimestampType(_SparkType):
    pass


class StructField:
    def __init__(self, name, dataType, nullable=True):
        self.name = name
        self.dataType = dataType
        self.nullable = nullable


class StructType(list):
    def __init__(self, fields=None):
        super().__init__(fields or [])

    @property
    def names(self):
        return [field.name for field in self]

    def add(self, name, data_type, nullable=True):
        self.append(StructField(name, data_type, nullable=nullable))
        return self


class Row(tuple):
    pass


class SparkConf:
    def setAppName(self, name):
        return self

    def get(self, name, defaultValue=None):
        return defaultValue

    def set(self, name, value):
        return self


class AnalysisException(Exception):
    pass


class _UnsupportedSparkFunctions:
    """Proxy that turns any ``F.<name>(...)`` call into a typed error."""

    def __getattr__(self, name):
        def _unsupported(*args, **kwargs):
            msg = f"Spark function F.{name} is not available with the Ibis DuckDB backend"
            raise DSGInvalidOperation(msg)

        return _unsupported


F = _UnsupportedSparkFunctions()
