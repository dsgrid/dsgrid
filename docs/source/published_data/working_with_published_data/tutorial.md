# Query a Dataset with Python

In this tutorial you will learn how to query data from a published dsgrid dataset using DuckDB and Python. We'll show you how to access data from NREL's HPC Kestrel cluster and from [OEDI](https://data.openei.org/).

This tutorial uses the `state_level_simplified` dataset from the [tempo project](https://github.com/dsgrid/dsgrid-project-StandardScenarios/tree/main/tempo_project) as an example.

## Query Objectives

This query will accomplish the following:

- Read data from Kestrel filesystem and OEDI S3 bucket
- Filter data for given model years, geography, scenario, and other dimensions
- Export the query results to a pandas DataFrame or CSV

## Required Knowledge

- How to set up a Python virtual environment
- How to install Python modules
- How to use a Jupyter notebook or Python interpreter

## Setup Environment

### 1. Set up Python environment

From a terminal run:

````{tab-set}

```{tab-item} Kestrel
module load python
python -m venv dsgrid-tutorial
source dsgrid-tutorial/bin/activate
```

```{tab-item} Local/Other
python -m venv dsgrid-tutorial
source dsgrid-tutorial/bin/activate  # On Windows: dsgrid-tutorial\Scripts\activate
```

````

### 2. Install required packages

```bash
pip install duckdb
pip install pandas
```

## Load Data

### 1. Enter a Python interpreter

```bash
python
```

### 2. Load .parquet files into a table

````{tab-set}

```{tab-item} Kestrel
import duckdb

tablename = "tbl"
data_dir = "/datasets/dsgrid/dsgrid-tempo-v2022"
dataset_name = "state_level_simplified"
filepath = f"{data_dir}/{dataset_name}"

duckdb.sql(f"""CREATE VIEW {tablename} AS SELECT *
           FROM read_parquet("{filepath}/table.parquet/**/*.parquet",
           hive_partitioning=true, hive_types_autocast=false)""")

duckdb.sql(f"DESCRIBE {tablename}")  # shows columns and types
duckdb.sql(f"SELECT * FROM {tablename} LIMIT 5").to_df()  # shows first 5 rows
```

```{tab-item} OEDI
import duckdb

tablename = "tbl"
data_dir = "s3://nrel-pds-dsgrid/tempo/tempo-2022/v1.0.0"
dataset_name = "state_level_simplified"
filepath = f"{data_dir}/{dataset_name}"

duckdb.sql(f"""CREATE TABLE {tablename} AS SELECT *
           FROM read_parquet('{filepath}/table.parquet/**/*.parquet',
           hive_partitioning=true, hive_types_autocast=false)""")

duckdb.sql(f"DESCRIBE {tablename}")  # shows columns and types
duckdb.sql(f"SELECT * FROM {tablename} LIMIT 5").to_df()  # shows first 5 rows
```

````

## Filter Data with DuckDB

One of the main advantages to using DuckDB is the ability to filter data while loading. If a table is created with a filter, DuckDB will not have to read all of the data to generate the requested table. This can make queries much more efficient.

Using the same `tablename` and `filepath` from the sections above:

```python
duckdb.sql(f"""CREATE TABLE {tablename} AS SELECT *
           FROM read_parquet('{filepath}/table.parquet/**/*.parquet',
           hive_partitioning=true, hive_types_autocast=false)
           WHERE state='MI' AND scenario='efs_high_ldv'
        """)
```

## Aggregation and Metadata

This example covers two distinct topics:

- Aggregation with DuckDB
- How to use dsgrid metadata in a query

dsgrid datasets contain a `metadata.json` file that specifies dimensions, their column names, query_names, and the value column of the dataset. The best way to use this metadata is to load it as `TableMetadata` using the provided `dsgrid/scripts/table_metadata.py` file. The `TableMetadata` can be loaded with pydantic installed and if using OEDI, pyarrow will also be needed to load the metadata.json.

To load the table_metadata script, either copy it from GitHub into a directory that will be used as the dsgrid_path in the Read Metadata step, or clone dsgrid and use the repository. Set the `scripts_path` variable to the directory that contains `table_metadata.py`. If using a dsgrid repo, this path will be in the `dsgrid/scripts` directory.

### Setup

````{tab-set}

```{tab-item} Kestrel
pip install pydantic
```

```{tab-item} OEDI
pip install pydantic pyarrow
```

````

### Read Metadata

````{tab-set}

```{tab-item} Kestrel
from pathlib import Path
import sys

scripts_path = Path("<insert path here>")
sys.path.append(str(scripts_path))

from scripts.table_metadata import TableMetadata

dataset_path = "/datasets/dsgrid/dsgrid-tempo-v2022/state_level_simplified"
metadata_path = f"{dataset_path}/metadata.json"
table_metadata = TableMetadata.from_file(metadata_path)
```

```{tab-item} OEDI
from pathlib import Path
import sys

scripts_path = Path("<insert path here>")
sys.path.append(str(scripts_path))

from scripts.table_metadata import TableMetadata

bucket = "nrel-pds-dsgrid"
filepath = "tempo/tempo-2022/v1.0.0/state_level_simplified/metadata.json"
table_metadata = TableMetadata.from_s3(bucket, filepath)
```

````

### Use Metadata for Aggregation

These metadata `columns_by_type` and `value_column` can be used to write queries that would apply to different datasets. The following example will query the `state_level_simplified` dataset and aggregate the results by: `model_year`, `scenario`, `geography`, and `subsector` with a column for the value summed up across groups. Each dimension could have multiple columns, so we first create the `group_by_cols` from the metadata, and use this list to create the table.

```python
group_by_dimensions = ['model_year', 'scenario', 'geography', 'subsector']
group_by_cols = []

for dimension in group_by_dimensions:
    group_by_cols.extend(table_metadata.list_columns(dimension))

group_by_str = ", ".join(group_by_cols)
value_column = table_metadata.value_column

duckdb.sql(f"""CREATE TABLE {tablename} AS
              SELECT SUM({value_column}) AS value_sum, {group_by_str}
              FROM read_parquet('{filepath}/table.parquet/**/*.parquet')
              GROUP BY {group_by_str}
                  """)
```

:::{note}
This query would also work on the `full_dataset` by using metadata for dimensions, but that query could take hours or fail because of memory limitations.
:::

## Export Data

### 1. Create a pandas DataFrame

After loading and possibly filtering data from the previous steps:

```python
dataframe = duckdb.sql(f"SELECT * FROM {tablename}").df()
```

### 2. Export DataFrame to CSV

```python
dataframe.to_csv('mydata.csv')
```

## Next Steps

- Learn more about [dataset dimensions and structure](../../user_guide/dataset_registration/index.md)
- Explore [advanced filtering techniques](../../user_guide/project_queries/filters.md)
- See [how to work with Apache Spark](../../apache_spark/index.md) for larger datasets
