from types import SimpleNamespace

import dsgrid
import dsgrid.ibis.session as session
from dsgrid.common import BackendEngine
from dsgrid.ibis.temp import TEMP_TABLE_PREFIX, drop_temp_tables_and_views, make_temp_view_name


class _Rows:
    def __init__(self, rows):
        self._rows = rows

    def collect(self):
        return self._rows


class _FakeSpark:
    def __init__(self):
        self.queries = []

    def sql(self, query):
        self.queries.append(query)
        if query.startswith("SHOW TABLES"):
            return _Rows([SimpleNamespace(tableName=f"{TEMP_TABLE_PREFIX}_table")])
        if query.startswith("SHOW VIEWS"):
            return _Rows([SimpleNamespace(viewName=f"{TEMP_TABLE_PREFIX}_view")])
        return _Rows([])


def test_make_temp_view_name():
    name = make_temp_view_name()
    assert name.startswith(f"{TEMP_TABLE_PREFIX}_")
    assert name != make_temp_view_name()


def test_drop_temp_tables_noop_with_duckdb(monkeypatch):
    monkeypatch.setattr(dsgrid.runtime_config, "backend_engine", BackendEngine.DUCKDB)
    monkeypatch.setattr(
        session, "is_runtime_session_active", lambda: (_ for _ in ()).throw(AssertionError)
    )
    drop_temp_tables_and_views()


def test_drop_temp_tables_noop_when_spark_inactive(monkeypatch):
    monkeypatch.setattr(dsgrid.runtime_config, "backend_engine", BackendEngine.SPARK)
    monkeypatch.setattr(session, "is_runtime_session_active", lambda: False)
    monkeypatch.setattr(
        session, "get_spark_session", lambda: (_ for _ in ()).throw(AssertionError)
    )
    drop_temp_tables_and_views()


def test_drop_temp_tables_with_spark(monkeypatch):
    spark = _FakeSpark()
    monkeypatch.setattr(dsgrid.runtime_config, "backend_engine", BackendEngine.SPARK)
    monkeypatch.setattr(session, "is_runtime_session_active", lambda: True)
    monkeypatch.setattr(session, "get_spark_session", lambda: spark)
    drop_temp_tables_and_views()
    assert spark.queries == [
        f"SHOW TABLES LIKE '*{TEMP_TABLE_PREFIX}*'",
        f"DROP TABLE {TEMP_TABLE_PREFIX}_table",
        f"SHOW VIEWS LIKE '*{TEMP_TABLE_PREFIX}*'",
        f"DROP VIEW {TEMP_TABLE_PREFIX}_view",
    ]
