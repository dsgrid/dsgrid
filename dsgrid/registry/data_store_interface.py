import abc
from pathlib import Path
from typing import Self

from dsgrid.spark.types import DataFrame


class DataStoreInterface(abc.ABC):
    """Base class for data stores."""

    def __init__(self, base_path: Path):
        self._base_path = base_path

    @classmethod
    @abc.abstractmethod
    def create(cls, base_path: Path) -> Self:
        """Create the data store."""

    @classmethod
    @abc.abstractmethod
    def load(cls, base_path: Path) -> Self:
        """Load an existing data store."""

    @property
    def base_path(self) -> Path:
        """Return the base path of the data store."""
        return self._base_path

    @abc.abstractmethod
    def read_table(self, dataset_id: str, version: str) -> DataFrame:
        """Read a table from the data store."""

    @abc.abstractmethod
    def replace_table(self, df: DataFrame, dataset_id: str, version: str) -> None:
        """Replace a table in the data store."""

    @abc.abstractmethod
    def read_lookup_table(self, dataset_id: str, version: str) -> DataFrame:
        """Read a lookup table from the data store."""

    @abc.abstractmethod
    def replace_lookup_table(self, df: DataFrame, dataset_id: str, version: str) -> None:
        """Replace a lookup table in the data store."""

    @abc.abstractmethod
    def write_table(
        self, df: DataFrame, dataset_id: str, version: str, overwrite: bool = False
    ) -> None:
        """Write a table to the data store."""

    @abc.abstractmethod
    def write_lookup_table(
        self, df: DataFrame, dataset_id: str, version: str, overwrite: bool = False
    ) -> None:
        """Write a lookup table to the data store."""

    @abc.abstractmethod
    def write_missing_associations_tables(
        self, dfs: dict[str, DataFrame], dataset_id: str, version: str, overwrite: bool = False
    ) -> None:
        """Write a set of tables of missing dimension associations to the data store.
        The dictionary keys of the dfs argument should human-readable tags for the contents of
        the tables, but are not otherwise significant.
        """

    @abc.abstractmethod
    def read_missing_associations_tables(
        self, dataset_id: str, version: str
    ) -> dict[str, DataFrame]:
        """Read a missing dimensions association tables from the data store."""

    @abc.abstractmethod
    def remove_tables(self, dataset_id: str, version: str) -> None:
        """Remove the data and lookup tables from the data store."""
