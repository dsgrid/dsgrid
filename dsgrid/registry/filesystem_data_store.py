import logging
from pathlib import Path
from typing import Self

from dsgrid.registry.data_store_interface import DataStoreInterface
from dsgrid.spark.functions import coalesce
from dsgrid.spark.types import DataFrame
from dsgrid.utils.files import delete_if_exists
from dsgrid.utils.spark import read_dataframe, write_dataframe, write_dataframe_and_auto_partition


TABLE_FILENAME = "table.parquet"
LOOKUP_TABLE_FILENAME = "lookup_table.parquet"
MISSING_ASSOCIATIONS_TABLE_FILENAME = "missing_associations_table.parquet"
# We used to write these filenames. Keep support for old registries, for now.
ALT_TABLE_FILENAME = "load_data.parquet"
ALT_LOOKUP_TABLE_FILENAME = "load_data_lookup.parquet"

logger = logging.getLogger(__name__)


class FilesystemDataStore(DataStoreInterface):
    """Data store that stores tables in Parquet files on the local or remote filesystem."""

    @classmethod
    def create(cls, base_path: Path) -> Self:
        base_path.mkdir(exist_ok=True)
        return cls(base_path)

    @classmethod
    def load(cls, base_path: Path) -> Self:
        if not base_path.exists():
            msg = f"Base path {base_path} does not exist. Cannot load FilesystemDataStore."
            raise FileNotFoundError(msg)
        return cls(base_path)

    def read_table(self, dataset_id: str, version: str) -> DataFrame:
        filename = self._table_filename(dataset_id, version)
        if not filename.exists():
            filename = self._alt_table_filename(dataset_id, version)
        if not filename.exists():
            msg = f"Table does not exist for dataset {dataset_id}, version {version} at {filename.parent}."
            raise FileNotFoundError(msg)
        return read_dataframe(filename)

    def replace_table(self, df: DataFrame, dataset_id: str, version: str) -> None:
        filename = self._get_existing_table_filename(dataset_id, version)
        if filename is None:
            self.write_table(df, dataset_id, version)
            return
        self._replace_table(df, filename)

    def read_lookup_table(self, dataset_id: str, version: str) -> DataFrame:
        filename = self._get_existing_lookup_table_filename(dataset_id, version)
        if filename is None:
            msg = f"Table does not exist for dataset {dataset_id}, version {version}."
            raise FileNotFoundError(msg)
        return read_dataframe(filename)

    def replace_lookup_table(self, df: DataFrame, dataset_id: str, version: str) -> None:
        filename = self._get_existing_lookup_table_filename(dataset_id, version)
        if filename is None:
            self.write_lookup_table(df, dataset_id, version)
            return
        self._replace_table(df, filename)

    def read_missing_associations_tables(
        self, dataset_id: str, version: str
    ) -> dict[str, DataFrame]:
        assoc_dir = self._missing_associations_dir(dataset_id, version)
        if not assoc_dir.exists():
            return {}
        return {x.stem: read_dataframe(x) for x in assoc_dir.iterdir()}

    def write_table(
        self, df: DataFrame, dataset_id: str, version: str, overwrite: bool = False
    ) -> None:
        filename = self._table_filename(dataset_id, version)
        filename.parent.mkdir(parents=True, exist_ok=True)
        write_dataframe_and_auto_partition(df, filename)

    def write_lookup_table(
        self, df: DataFrame, dataset_id: str, version: str, overwrite: bool = False
    ) -> None:
        filename = self._lookup_table_filename(dataset_id, version)
        filename.parent.mkdir(parents=True, exist_ok=True)
        write_dataframe(coalesce(df, 1), filename, overwrite=overwrite)

    def write_missing_associations_tables(
        self, dfs: dict[str, DataFrame], dataset_id: str, version: str, overwrite: bool = False
    ) -> None:
        for name, df in dfs.items():
            filename = self._missing_associations_table_filename(name, dataset_id, version)
            filename.parent.mkdir(parents=True, exist_ok=True)
            write_dataframe_and_auto_partition(df, filename)

    def remove_tables(self, dataset_id: str, version: str) -> None:
        delete_if_exists(self._base_dir(dataset_id, version))

    @property
    def _data_dir(self) -> Path:
        return self.base_path / "data"

    def _base_dir(self, dataset_id: str, version: str) -> Path:
        return self._data_dir / dataset_id / version

    def _lookup_table_filename(self, dataset_id: str, version: str) -> Path:
        return self._data_dir / dataset_id / version / LOOKUP_TABLE_FILENAME

    def _missing_associations_dir(self, dataset_id: str, version: str) -> Path:
        return self._data_dir / dataset_id / version / "missing_associations"

    def _missing_associations_table_filename(
        self, name: str, dataset_id: str, version: str
    ) -> Path:
        return self._missing_associations_dir(dataset_id, version) / f"{name}.parquet"

    def _table_filename(self, dataset_id: str, version: str) -> Path:
        return self._data_dir / dataset_id / version / TABLE_FILENAME

    def _alt_lookup_table_filename(self, dataset_id: str, version: str) -> Path:
        return self._data_dir / dataset_id / version / ALT_LOOKUP_TABLE_FILENAME

    def _alt_table_filename(self, dataset_id: str, version: str) -> Path:
        return self._data_dir / dataset_id / version / ALT_TABLE_FILENAME

    def _get_existing_lookup_table_filename(self, dataset_id: str, version: str) -> Path | None:
        filename = self._lookup_table_filename(dataset_id, version)
        if filename.exists():
            return filename
        alt_filename = self._alt_lookup_table_filename(dataset_id, version)
        if alt_filename.exists():
            return alt_filename
        return None

    def _get_existing_table_filename(self, dataset_id: str, version: str) -> Path | None:
        filename = self._table_filename(dataset_id, version)
        if filename.exists():
            return filename
        alt_filename = self._alt_table_filename(dataset_id, version)
        if alt_filename.exists():
            return alt_filename
        return None

    @staticmethod
    def _replace_table(df: DataFrame, filename: Path) -> None:
        tmp_name = filename.parent / f"{filename.stem}_tmp.parquet"
        write_dataframe(df, tmp_name)
        delete_if_exists(filename)
        tmp_name.rename(filename)
