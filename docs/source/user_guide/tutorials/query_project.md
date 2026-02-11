# Query a Project

In this tutorial you will learn how to query a dsgrid project for aggregated data. The query will use data from the dsgrid registry stored on NREL's HPC Kestrel cluster.

## Query Objectives

This query will accomplish the following:

- Read data from the datasets `tempo_conus_2022_mapped`, `resstock_conus_2022_projected`, and `comstock_conus_2022_projected`.
- Filter data for only model years 2030, 2040, and 2050.
- Aggregate county-level load data into state-level.
- Aggregate load data by fuel type.
- Aggregate hourly data into annual data.
- Drop the subsector dimension.

## Steps

SSH to a login node to begin the tutorial.

### Step 1: Set Up Your Environment

Follow the instructions at [Run dsgrid on Kestrel](../apache_spark/run_on_kestrel) if you have not already done so.

### Step 2: Create the Query Configuration

Add this content to a file called `query.json5`:

:::{note}
There is a CLI command that can generate a query file for your project. Refer to `dsgrid query project create --help`.
:::

```javascript
{
  "name": "load-per-state",
  "version": "0.1.0",
  "project": {
    "project_id": "dsgrid_conus_2022",
    "dataset": {
      "dataset_id": "load-per-state",
      "source_datasets": [
        {
          "dataset_id": "comstock_conus_2022_projected",
          "dataset_type": "standalone"
        },
        {
          "dataset_id": "resstock_conus_2022_projected",
          "dataset_type": "standalone"
        },
        {
          "dataset_id": "tempo_conus_2022_mapped",
          "dataset_type": "standalone"
        },
      ],
      "params": {
        "dimension_filters": [
          {
            "dimension_type": "model_year",
            "dimension_name": "model_year",
            "column": "id",
            "operator": "isin",
            "value": [
              "2030",
              "2040",
              "2050",
            ],
            "filter_type": "DimensionFilterColumnOperatorModel"
          }
        ],
      }
    },
  },
  "result": {
    "replace_ids_with_names": false,
    "aggregations": [
      {
        "aggregation_function": "sum",
        "dimensions": {
          "geography": [
            {
              "dimension_name": "state",
            }
          ],
          "metric": [
            {
              "dimension_name": "end_uses_by_fuel_type",
            }
          ],
          "model_year": [
            {
              "dimension_name": "model_year",
            }
          ],
          "scenario": [
            {
              "dimension_name": "scenario",
            }
          ],
          "sector": [
            {
              "dimension_name": "sector",
            }
          ],
          "subsector": [
          ],
          "time": [
            {
              "dimension_name": "time_est",
              "function": "year",
              "alias": "year"
            }
          ],
          "weather_year": [
            {
              "dimension_name": "weather_2012",
            }
          ]
        }
      }
    ],
    "reports": [],
    "column_type": "dimension_query_names",
    "dimension_filters": [],
    "time_zone": null
  }
}
```

#### Optional: Filter by Fuel Type

If you only care about a limited number of fuel types, you could add this filter to the dataset params:

```javascript
      "params": {
        "dimension_filters": [
          {
            "dimension_type": "metric",
            "dimension_name": "end_uses_by_fuel_type",
            "column": "fuel_id",
            "value": [
              "electricity",
              "natural_gas"
            ],
            "operator": "isin",
            "negate": false,
            "filter_type": "SupplementalDimensionFilterColumnOperatorModel"
          }
        ],
      }
```

### Step 3: Start a Spark Cluster

Start a Spark cluster with two compute nodes as described in [Start Spark Cluster on Kestrel](../apache_spark/spark_cluster_on_kestrel).

### Step 4: Activate Python Environment

Activate a Python virtual environment that includes `dsgrid`:

```bash
conda activate dsgrid
```

### Step 5: Run the Query

Run the query using spark-submit:

```bash
spark-submit --master=spark://$(hostname):7077 $(which dsgrid-cli.py) query project run query.json5
```

The query may take ~55 minutes.

### Step 6: Inspect the Output

Inspect the output table using pyspark:

```bash
pyspark --master=spark://$(hostname):7077
```

```python
>>> df = spark.read.load("query_output/load-per-state-2030/table.parquet")
>>> columns = ["time_est", "state", "scenario", "sector", "weather_2012", "all_electricity"]
>>> df.sort("state", "scenario", "model_year", "time_est").show()
+-----+----------+------------+------+-------------------+------------+--------------------+-------------------+--------------------+------------------+
|state|model_year|    scenario|sector|           time_est|weather_2012|electricity_end_uses|  fuel_oil_end_uses|natural_gas_end_uses|  propane_end_uses|
+-----+----------+------------+------+-------------------+------------+--------------------+-------------------+--------------------+------------------+
|   AL|      2030|efs_high_ldv|   com|2011-12-31 22:00:00|        2012|   620.7906599513221| 0.5387437841876448|  129.36033825268063| 5.420073700645743|
|   AL|      2030|efs_high_ldv|   com|2011-12-31 23:00:00|        2012|   614.3206498969266| 0.5416918956851451|  124.89964054800879|5.5127600846910925|
|   AL|      2030|efs_high_ldv|   com|2012-01-01 00:00:00|        2012|   608.2849965404984| 0.5769061150253406|   131.3726191747269| 5.634768746851266|
|   AL|      2030|efs_high_ldv|   com|2012-01-01 01:00:00|        2012|   611.5344463308626| 0.5971660979790878|  143.24735266593729| 5.788247589054716|
|   AL|      2030|efs_high_ldv|   com|2012-01-01 02:00:00|        2012|   642.3981904857268| 0.6859885122836309|  182.33194073437588|  7.97610263995906|
|   AL|      2030|efs_high_ldv|   com|2012-01-01 03:00:00|        2012|   717.3520732997924|  4.024472764883984|  370.80760961376876|22.227861344037187|
|   AL|      2030|efs_high_ldv|   com|2012-01-01 04:00:00|        2012|   613.8294652248369| 2.0143366691532707|   343.2025577876601| 16.94645372648664|
|   AL|      2030|efs_high_ldv|   com|2012-01-01 05:00:00|        2012|   658.7328024466709|  1.353741822119555|  350.85640893192993| 14.51586872394028|
|   AL|      2030|efs_high_ldv|   com|2012-01-01 06:00:00|        2012|   699.8174234582644| 0.8924590078874647|   389.4158152004862| 10.21988642248965|
|   AL|      2030|efs_high_ldv|   com|2012-01-01 07:00:00|        2012|   743.2481362935839| 0.6068532986319386|   450.8920847000712| 7.672432329899141|
|   AL|      2030|efs_high_ldv|   com|2012-01-01 08:00:00|        2012|     793.64723585044| 0.5041067604373506|   475.3169294837448| 6.838798780678826|
|   AL|      2030|efs_high_ldv|   com|2012-01-01 09:00:00|        2012|   841.1101704879942|0.45131205367098215|  467.61967258296016| 6.426079631558903|
|   AL|      2030|efs_high_ldv|   com|2012-01-01 10:00:00|        2012|   869.4957512282607|0.35165281820491173|   442.2650153173674| 6.157433321806227|
|   AL|      2030|efs_high_ldv|   com|2012-01-01 11:00:00|        2012|   882.6659925381028|0.33634962431492477|   407.6767924458409| 6.193132473615856|
|   AL|      2030|efs_high_ldv|   com|2012-01-01 12:00:00|        2012|   871.6219175694675| 0.4538962808891562|   406.1393196887077|6.4655789088596896|
|   AL|      2030|efs_high_ldv|   com|2012-01-01 13:00:00|        2012|   866.2804144237266| 0.7825813246602221|  425.44571896883167|  7.59699881137887|
|   AL|      2030|efs_high_ldv|   com|2012-01-01 14:00:00|        2012|   898.4427579031129| 1.1631267804567154|  413.79402978076445| 9.040045654010711|
|   AL|      2030|efs_high_ldv|   com|2012-01-01 15:00:00|        2012|   884.7541250936392| 1.6279990839937193|   409.6531761201896|10.819410305573035|
|   AL|      2030|efs_high_ldv|   com|2012-01-01 16:00:00|        2012|   861.7277658774752| 2.1224667695070067|  398.15739373650064|12.415366975813644|
|   AL|      2030|efs_high_ldv|   com|2012-01-01 17:00:00|        2012|    832.487984827325| 2.6171310594480213|   375.1621027331863|13.728169793981515|
+-----+----------+------------+------+-------------------+------------+--------------------+-------------------+--------------------+------------------+
```

## Next Steps

- Learn how to [filter queries](../project_queries/how_to_filter) for more specific results
- Understand [query concepts](../project_queries/concepts) in more detail
- Explore [creating derived datasets](create_derived_dataset) from query results
