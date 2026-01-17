from pathlib import Path

import dsgrid
from dsgrid.common import BackendEngine
from dsgrid.ibis_api import get_ibis_connection


def use_duckdb():
    return dsgrid.runtime_config.backend_engine == BackendEngine.DUCKDB


def read_parquet(filename: Path):
    t = get_ibis_connection().read_parquet(str(filename))
    return t


def read_parquet_two_table_format(path: Path):
    con = get_ibis_connection()
    load_data = con.read_parquet(str(path / "load_data.parquet"))
    lookup = con.read_parquet(str(path / "load_data_lookup.parquet"))
    table = load_data.join(lookup, "id")
    return table
