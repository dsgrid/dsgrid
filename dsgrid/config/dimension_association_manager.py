"""
Caches project dimension association tables in the Spark warehouse.
One table per project can be stored. Multiple versions of one project can share the same table.
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


logger = logging.getLogger(__name__)

_ASSOCIATIONS_DATA_TABLE = "dimension_associations_data"


def try_load_dimension_associations(project_id, data_source) -> pyspark.sql.DataFrame | None:
    """Load a project's dimension associations table if it exists.

    Parameters
    ----------
    project_id : str
    data_source : str

    Returns
    -------
    pyspark.sql.DataFrame | None

    """
    return try_load_stored_table(_make_dimension_associations_table_name(project_id, data_source))


def save_dimension_associations(table, project_id, data_source):
    """Save a project's dimension association table to the Hive Metastore.

    Parameters
    ----------
    table : pyspark.sql.DataFrame
    project_id : str
    data_source : str

    """
    table_name = _make_dimension_associations_table_name(project_id, data_source)
    save_table(table, table_name)
    logger.info(
        "Saved dimension associations for project_id=%s data_source=%s",
        project_id,
        data_source,
    )


def remove_project_dimension_associations(project_id):
    """Remove any stored dimension associations for project_id.

    Parameters
    ----------
    project_id : str

    """
    for table in list_tables():
        fields = table.split("__")
        if len(fields) == 3 and fields[0] == project_id and fields[2] == _ASSOCIATIONS_DATA_TABLE:
            drop_table(table)


def _make_dimension_associations_table_name(project_id, data_source):
    return "__".join((project_id, data_source, _ASSOCIATIONS_DATA_TABLE))
