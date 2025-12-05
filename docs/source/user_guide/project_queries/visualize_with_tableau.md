# Visualize Data with Tableau

[Tableau](https://www.tableau.com/) is a commercial tool for exploring and visualizing tabular data.

In addition to making visualizations, Tableau makes it easy to select, filter, group, and describe your data in tables. This can be easier than the same operations in a Python REPL with `pyspark` or `pandas`.

This page describes various ways to connect Tableau to dsgrid data after you've installed Tableau Desktop on your local computer.

## Install Tableau

**For NREL employees:** Licenses are available through theSOURCE. Go to IT Service Portal → Service Catalog → search for Tableau, and submit a ticket to get `Tableau Creator` installed (IT will install `Tableau Desktop`).

**For others:** Visit [Tableau's website](https://www.tableau.com/) to purchase or request a trial license.

## Option 1: Parquet Files on Local Computer

Connect Tableau to DuckDB to read Parquet files locally.

### Steps

#### 1. Copy Parquet Files

Copy the Parquet files from your query output to your local computer:

```bash
# From HPC to local
scp -r username@kestrel.hpc.nrel.gov:/path/to/query_output ./local_data
```

#### 2. Install DuckDB

Install [DuckDB](https://duckdb.org/docs/installation/). You want the **Command line Environment**.

**macOS:**
```bash
brew install duckdb
```

**Windows:**
Download from [DuckDB releases](https://github.com/duckdb/duckdb/releases)

**Linux:**
```bash
wget https://github.com/duckdb/duckdb/releases/latest/download/duckdb_cli-linux-amd64.zip
unzip duckdb_cli-linux-amd64.zip
```

#### 3. Install JDBC Driver

Install a JDBC driver and connect Tableau to DuckDB by following [DuckDB's instructions](https://duckdb.org/docs/guides/data_viewers/tableau).

#### 4. Create a View

Create a view of your data as noted in [DuckDB's database creation guide](https://duckdb.org/docs/guides/data_viewers/tableau#database-creation).

You can also import your data from Parquet files to a DuckDB database file:

```sql
-- Start DuckDB
duckdb my_data.db

-- Create a view from Parquet files
CREATE VIEW load_data AS
SELECT * FROM 'query_output/table.parquet';

-- Or create a table for faster access
CREATE TABLE load_data AS
SELECT * FROM 'query_output/table.parquet';
```

#### 5. Connect Tableau

1. Open Tableau Desktop
2. Connect to **More...** → **Other Databases (JDBC)**
3. Configure connection:
   - URL: `jdbc:duckdb:/path/to/my_data.db`
   - Dialect: SQL92
4. Select your view or table
5. Start visualizing!

## Option 2: Parquet Files on HPC

Connect Tableau directly to a Spark cluster running on the HPC.

### Requirements

- Active Spark cluster on Kestrel
- Network access from your computer to the HPC
- Spark JDBC driver

### Steps

Follow the [Spark-on-HPC instructions](https://github.com/daniel-thom/HPC/blob/kestrel-update/applications/spark/README.md#visualize-data-with-tableau) for detailed setup.

**Key steps:**

1. Start Spark cluster on Kestrel (see [Start Spark Cluster](../apache_spark/spark_cluster_on_kestrel))
2. Configure Spark Thrift Server
3. Install Spark JDBC driver in Tableau
4. Connect Tableau to Spark cluster
5. Query data directly from HPC storage

:::{tip}
This option is best for very large datasets that would be cumbersome to copy locally.
:::

## Option 3: CSV Files on Local Computer

The simplest approach for small to medium datasets.

### Steps

#### 1. Export to CSV

Export your dsgrid query results from Parquet to CSV:

**Using Python:**
```python
import pandas as pd

# Read Parquet file
df = pd.read_parquet('query_output/table.parquet')

# Write to CSV
df.to_csv('query_output/table.csv', index=False)
```

**Using DuckDB:**
```sql
duckdb -c "COPY (SELECT * FROM 'table.parquet') TO 'table.csv' (HEADER, DELIMITER ',')"
```

#### 2. Load in Tableau

1. Open Tableau Desktop
2. Connect to **Text file**
3. Select your CSV file
4. Configure data types if needed
5. Start visualizing!

:::{warning}
CSV files can be very large and slower to load than Parquet. Consider using Option 1 (DuckDB) for better performance.
:::

## Comparison of Options

| Option | Best For | Pros | Cons |
|--------|----------|------|------|
| **DuckDB** | Local analysis, medium-large datasets | Fast, efficient, no network needed | Requires copying data |
| **HPC Spark** | Very large datasets, team sharing | No data copy, direct HPC access | Complex setup, network required |
| **CSV** | Small datasets, simple workflows | Simple, no extra tools | Large files, slow loading |

## Tableau Tips for dsgrid Data

### Time Series Visualization

1. Convert timestamp columns to Date/DateTime types
2. Use **Marks** → **Line** for time series plots
3. Add **Geography** to columns, **Timestamp** to rows, **Value** to values

### Geographic Maps

1. Ensure geography IDs match Tableau's geographic roles
2. Assign geographic role: Right-click dimension → **Geographic Role** → **County** (or State)
3. Use **Map** view type
4. Add **Value** to color for choropleth maps

### Aggregations

1. Use **Data** → **New Calculated Field** for custom aggregations
2. Filter by dimension: Drag dimension to **Filters** shelf
3. Group records: Right-click dimension → **Create** → **Group**

### Performance Tips

- **Extract data**: Data → Extract Data (creates .hyper file for faster loading)
- **Aggregate in query**: Pre-aggregate data in dsgrid query when possible
- **Filter early**: Apply filters before loading into Tableau
- **Use DuckDB**: Faster than CSV for large datasets

## Next Steps

- Learn about [query optimization](concepts) for preparing data
- Understand [aggregations](../project_queries/concepts#aggregations)
- Follow the [query project tutorial](../tutorials/query_project)
