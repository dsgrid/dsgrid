"""
Caches project dimension association tables in the Spark warehouse.
One table per project can be stored. That table gets deleted whenever a project update
invalidates it.

The purpose is to allow dataset submissions to reuse existing tables.
"""


import logging

import pyspark

from dsgrid.utils.spark import (
    try_load_stored_table,
    save_table,
    list_tables,
    drop_table,
)
from dsgrid.utils.timing import timer_stats_collector, track_timing


logger = logging.getLogger(__name__)

_ASSOCIATIONS_DATA_TABLE = "dimension_associations_data"


def try_load_dimension_associations(
    project_id, dataset_id, pivoted_dimension
) -> pyspark.sql.DataFrame | None:
    """Load a project's dimension associations table if it exists.

    Parameters
    ----------
    project_id : str
    dataset_id : str
    pivoted_dimension : DimensionType

    Returns
    -------
    pyspark.sql.DataFrame | None

    """
    return try_load_stored_table(
        _make_dimension_associations_table_name(project_id, dataset_id, pivoted_dimension)
    )


@track_timing(timer_stats_collector)
def save_dimension_associations(table, project_id, dataset_id, pivoted_dimension):
    """Save a project's dimension association table to the Spark warehouse.

    Parameters
    ----------
    table : pyspark.sql.DataFrame
    project_id : str
    dataset_id : str
    pivoted_dimension : DimensionType

    Returns
    -------
    str
        Name of the saved table

    """
    table_name = _make_dimension_associations_table_name(project_id, dataset_id, pivoted_dimension)
    save_table(table, table_name)
    logger.info(
        "Saved dimension associations for project_id=%s dataset_id=%s pivoted_dimension=%s",
        project_id,
        dataset_id,
        pivoted_dimension,
    )
    return table_name


def remove_project_dimension_associations(project_id):
    """Remove any stored dimension associations for project_id.

    Parameters
    ----------
    project_id : str

    """
    for table in list_tables():
        fields = table.split("__")
        if len(fields) == 4 and fields[0] == project_id and fields[-1] == _ASSOCIATIONS_DATA_TABLE:
            drop_table(table)


def _make_dimension_associations_table_name(project_id, dataset_id, pivoted_dimension):
    return "__".join((project_id, dataset_id, pivoted_dimension.value, _ASSOCIATIONS_DATA_TABLE))
