# Data Users

Data users want to access and analyze published dsgrid datasets. You do **not** need to install dsgrid or work with the registry directly — the published data is available as Parquet files that can be read with standard tools like Python, R, or Tableau.

## What's Available

dsgrid publishes assembled, quality-checked energy demand datasets. These datasets combine contributions from many modeling teams into a unified dimensional structure so that you can query across sectors, geographies, time periods, and scenarios in a single analysis.

See [Published Datasets](../published_data/published_datasets) for the current catalog.

## Getting Started

1. **Browse the catalog** — Review [Published Datasets](../published_data/published_datasets) to find the dataset you need.
2. **Follow the tutorial** — The [Query a Dataset with Python](../published_data/working_with_published_data/tutorial) tutorial walks through loading, filtering, and exporting data using Python and DuckDB.
3. **Visualize results** — See [Visualize Data with Tableau](../published_data/working_with_published_data/visualize_with_tableau) for creating dashboards.

## Requirements

To work with published data you will need:

- **Python 3.11+** with `duckdb` and `pyarrow` (for the Python tutorial), or
- **Tableau** with a Spark JDBC driver (for Tableau visualization), or
- Any tool that can read **Parquet** files

Access to NREL HPC is required for some datasets that are not yet available on [OEDI](https://data.openei.org/). Check the dataset catalog for access details.

## Next Steps

- [Published Data](../published_data/index) — Full documentation for published datasets
- [Working with Published Data](../published_data/working_with_published_data/index) — Tutorials and tool guides
