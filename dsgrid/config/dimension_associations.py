import itertools
import logging
from pathlib import Path
from typing import Dict

from pyspark.sql.types import StringType

from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidDimensionAssociation
from dsgrid.utils.spark import read_dataframe


logger = logging.getLogger(__name__)


class DimensionAssociations:
    """Interface to a project's dimension associations"""

    def __init__(self, table):
        self._table = table
        if table is None:
            self._dimensions = set()
        else:
            self._dimensions = set((DimensionType.from_column(x) for x in table.columns))

    @classmethod
    def load(cls, path: Path, association_files):
        """Load dimension associations from a path.

        Parameters
        ----------
        path: Path
        association_files : list
            List of filenames with paths relative to path.
            Every dimension given must provide an association with DimensionType.DATA_SOURCE.

        Returns
        -------
        DimensionAssociations

        """
        if not association_files:
            return cls(None)

        all_dims = set()
        dims_with_data_source = set()
        associations = {}
        associations_by_data_source = {}
        for association_file in association_files:
            filename = path / association_file
            table = read_dataframe(filename, cache=True)
            for column in table.columns:
                tmp = column + "tmp_name"
                table = (
                    table.withColumn(tmp, table[column].cast(StringType()))
                    .drop(column)
                    .withColumnRenamed(tmp, column)
                )
            dims = set((DimensionType.from_column(x) for x in table.columns))
            all_dims.update(dims)
            key = _make_key(dims)
            if DimensionType.DATA_SOURCE in dims:
                dims.remove(DimensionType.DATA_SOURCE)
                dims_with_data_source.update(dims)
                associations_by_data_source[key] = table
            else:
                associations[key] = table
            logger.debug("Loaded dimension associations from %s %s", path, table.columns)

        if len(dims_with_data_source) != len(all_dims) - 1:
            raise DSGInvalidDimensionAssociation(
                "Every supplied dimension must have an association with data_source. "
                f"has_data_source = {dims_with_data_source} all = {all_dims}"
            )

        table = _join_associations_by_data_source(associations, associations_by_data_source)
        return cls(table)

    @property
    def dimension_types(self):
        """Return the stored dimension types.

        Returns
        -------
        list
            List of DimensionType

        """
        if self._table is None:
            return []
        return sorted(self._dimensions)

    def get_associations(self, *dimensions, data_source=None):
        """Return the records for the union of dimension associations associated with dimensions.

        Parameters
        ----------
        dimensions : tuple
            Any number of instances of DimensionType

        Returns
        -------
        pyspark.sql.DataFrame | None
            Returns None if there is no table matching dimensions.

        """
        if not self._dimensions.issuperset(dimensions):
            return None

        ds_column = DimensionType.DATA_SOURCE.value
        columns = [ds_column] + [x.value for x in dimensions]
        table = self._table.select(*columns)
        if data_source is not None:
            table = table.filter(f"{ds_column} = '{data_source}'")
        return table.distinct()

    def get_unique_ids(self, dimension, data_source=None):
        """Return the unique record IDs for the dimension.

        Parameters
        ----------
        dimension : DimensionType

        Returns
        -------
        set
            Set of str

        """
        if dimension not in self.dimension_types:
            return None
        col = dimension.value
        if data_source is None:
            return {getattr(x, col) for x in self._table.select(col).distinct().collect()}
        else:
            ds_column = DimensionType.DATA_SOURCE.value
            return {
                getattr(x, col)
                for x in self._table.filter(f"{ds_column} = '{data_source}'")
                .select(col)
                .distinct()
                .collect()
            }

    def has_associations(self, *dimensions, data_source=None):
        """Return True if these dimension associations are stored.

        Parameters
        ----------
        dimensions : tuple
            Any number of instances of DimensionType

        Returns
        -------
        bool

        """
        return self.get_associations(*dimensions, data_source=data_source) is not None

    def list_data_sources(self):
        """Return the stored data source values.

        Returns
        -------
        list
            List of str

        """
        col = DimensionType.DATA_SOURCE.value
        return sorted((getattr(x, col) for x in self._table.select(col).distinct().collect()))

    @property
    def table(self):
        """Return the table containing the associations.

        Returns
        -------
        pyspark.sql.DataFrame

        """
        return self._table


def _make_key(dimensions):
    return tuple(sorted(dimensions))


def _get_column_distinct_counts(df, columns):
    return {x: df.select(x).distinct().count() for x in columns}


def _join_associations_by_data_source(associations, associations_by_data_source):
    tables = []
    for dims, table in associations.items():
        table2 = table
        for i, dim in enumerate(dims):
            if i == 0:
                on_columns = [dim.value]
            else:
                on_columns = [dim.value, DimensionType.DATA_SOURCE.value]
            ds_table = associations_by_data_source[_make_key((dim, DimensionType.DATA_SOURCE))]
            table2 = table2.join(ds_table, on=on_columns)
            orig = table.distinct().count()
            final = table2.select(table.columns).distinct().count()
            if orig != final:
                raise DSGInvalidDimensionAssociation(
                    f"Dropped records when joining by data_source: {orig - final}"
                )
        tables.append(table2)

    table = tables[0]
    if len(tables) > 1:
        for other in tables[1:]:
            on_columns = list(set(other.columns).intersection(table.columns))
            if on_columns:
                table = table.join(other, on=on_columns, how="outer")
            else:
                table = table.crossJoin(other)

    return table
