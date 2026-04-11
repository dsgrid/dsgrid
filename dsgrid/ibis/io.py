import ibis
from pathlib import Path

from dsgrid.ibis.backend import make_runtime_backend, read_csv_expr
from dsgrid.ibis.types import use_duckdb


def read_csv(path: Path | str, schema: dict[str, str] | None = None) -> ibis.Table:
    """Return an Ibis table from a CSV file."""
    path = Path(path)
    path_str = path.as_posix() + "/**/*.csv" if path.is_dir() else path.as_posix()
    return read_csv_expr(path_str, schema=schema)


def read_json(path: Path | str) -> ibis.Table:
    """Return an Ibis table from a JSON file."""
    return make_runtime_backend().connection.read_json(str(path))


def read_parquet(path: Path | str) -> ibis.Table:
    path = Path(path) if isinstance(path, str) else path
    path_str = (
        path.as_posix()
        if path.is_file() or not use_duckdb()
        else f"{path.as_posix()}/**/*.parquet"
    )
    return make_runtime_backend().connection.read_parquet(path_str)
