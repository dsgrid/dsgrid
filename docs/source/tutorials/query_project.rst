***************
Query a project
***************
In this tutorial you will learn how to query a dsgrid project for aggregated data. The query will
use data from the dsgrid registry stored on NREL's HPC Eagle cluster.

Query objectives
================
This query will accomplish the following:

- Read data from the datasets ``tempo_conus_2022_mapped``, ``resstock_conus_2022_projected``, and
  ``comstock_conus_2022_projected``.
- Filter data for only ``model_year == "2030"``.
- Aggregate county-level electricity load into state-level.
- Drop the subsector dimension.

Steps
=====
ssh to a login node to begin the tutorial.

1. Follow the instructions at :ref:`how-to-run-dsgrid-eagle` if you have not already done so.

2. Add this content to a file called ``query.json5``:

.. code-block:: JavaScript

    {
      "name": "load-per-state-2030",
      "version": "0.1.0",
      "project": {
        "project_id": "dsgrid_conus_2022",
        "dataset": {
          "dataset_id": "load-per-state-2030",
          "source_datasets": [
            {
              "dataset_id": "tempo_conus_2022_mapped",
              "dataset_type": "standalone"
            },
            // TODO: one parquet file is corrupt
            //{
            //  "dataset_id": "resstock_conus_2022_projected",
            //  "dataset_type": "standalone"
            //},
            {
              "dataset_id": "comstock_conus_2022_projected",
              "dataset_type": "standalone"
            },
          ],
          "params": {
            "dimension_filters": [
              {
                "dimension_type": "model_year",
                "dimension_query_name": "model_year",
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
                  "dimension_query_name": "state",
                }
              ],
              "metric": [
                {
                  "dimension_query_name": "electricity_collapsed",
                }
              ],
              "model_year": [
                {
                  "dimension_query_name": "model_year",
                }
              ],
              "scenario": [
                {
                  "dimension_query_name": "scenario",
                }
              ],
              "sector": [
                {
                  "dimension_query_name": "sector",
                }
              ],
              "subsector": [
              ],
              "time": [
                {
                  "dimension_query_name": "time_est",
                }
              ],
              "weather_year": [
                {
                  "dimension_query_name": "weather_2012",
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

3. Start a Spark cluster with two compute nodes as described in
   :ref:`how-to-start-spark-cluster-eagle`.

4. Activate a Python virtual environment that includes ``dsgrid``.

.. code-block:: console

    $ conda activate dsgrid

5. Run the query.

.. code-block:: console

    $ spark-submit --master=spark://$(hostname):7077 $(which dsgrid-cli.py) query project run query.json5

The query may take ~55 minutes.

7. Inspect the output table.

.. code-block:: console

    $ pyspark --master=spark://$(hostname):7077
    >>> df = spark.read.load("query_output/load-per-state-2030/table.parquet")
    >>> columns = ["time_est", "state", "scenario", "sector", "weather_2012", "all_electricity"]
    >>> df.select(*columns).sort("state", "scenario", "sector", "time_est").show()
    +-------------------+-----+------------+------+------------+-----------------+
    |           time_est|state|    scenario|sector|weather_2012|  all_electricity|
    +-------------------+-----+------------+------+------------+-----------------+
    |2011-12-31 22:00:00|   AL|efs_high_ldv|   com|        2012|591530.9312699143|
    |2011-12-31 22:00:00|   AL|efs_high_ldv|   com|        2012|570805.7339524698|
    |2011-12-31 22:00:00|   AL|efs_high_ldv|   com|        2012|620790.6599513218|
    |2011-12-31 23:00:00|   AL|efs_high_ldv|   com|        2012|585767.0034227732|
    |2011-12-31 23:00:00|   AL|efs_high_ldv|   com|        2012|565593.3072942605|
    |2011-12-31 23:00:00|   AL|efs_high_ldv|   com|        2012|614320.6498969268|
    |2012-01-01 00:00:00|   AL|efs_high_ldv|   com|        2012|608284.9965404986|
    |2012-01-01 00:00:00|   AL|efs_high_ldv|   com|        2012|579238.2480479811|
    |2012-01-01 00:00:00|   AL|efs_high_ldv|   com|        2012|558513.9857333278|
    |2012-01-01 01:00:00|   AL|efs_high_ldv|   com|        2012|580551.2310713478|
    |2012-01-01 01:00:00|   AL|efs_high_ldv|   com|        2012|558087.4697867837|
    |2012-01-01 01:00:00|   AL|efs_high_ldv|   com|        2012|611534.4463308627|
    |2012-01-01 02:00:00|   AL|efs_high_ldv|   com|        2012|578300.1197815704|
    |2012-01-01 02:00:00|   AL|efs_high_ldv|   com|        2012|605670.3679296541|
    |2012-01-01 02:00:00|   AL|efs_high_ldv|   com|        2012|642398.1904857266|
    |2012-01-01 03:00:00|   AL|efs_high_ldv|   com|        2012|717352.0732997915|
    |2012-01-01 03:00:00|   AL|efs_high_ldv|   com|        2012| 627243.080874411|
    |2012-01-01 03:00:00|   AL|efs_high_ldv|   com|        2012| 666558.324922294|
    |2012-01-01 04:00:00|   AL|efs_high_ldv|   com|        2012|613829.4652248364|
    |2012-01-01 04:00:00|   AL|efs_high_ldv|   com|        2012|553451.3528789664|
    +-------------------+-----+------------+------+------------+-----------------+
