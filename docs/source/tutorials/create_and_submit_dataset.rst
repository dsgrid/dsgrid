***************************
Create and Submit a Dataset
***************************
In this tutorial you will learn how to create a dsgrid dataset and register it with a project by
following the example of the TEMPO dataset from `dsgrid-project-StandardScenarios
<https://github.com/dsgrid/dsgrid-project-StandardScenarios>`_.

.. note:: It is possible to register a dataset without connecting it to a project. The main
   difference is that the information about dimension mappings below is not relevant.

Dataset attributes and dimensions must be defined in a
dataset config as defined by :ref:`dataset-config`. This tutorial will give step-by-step
instructions on how to assign values in this file.

1. Create a ``dataset.json5`` with these fields (but choose your own values):

.. code-block:: JavaScript

    {
      dataset_id: "tempo_conus_2022",
      dataset_type: "modeled",
      data_source: "tempo",
      sector_description: "transportation",
      description: "CONUS 2022 TEMPO county-level results for PEV charging.",
      origin_creator: "Arthur Yip",
      origin_organization: "NREL",
      origin_contributors: [
        "Arthur Yip",
        "Brian Bush",
        "Christopher Hoehne",
        "Paige Jadun",
        "Catherine Ledna",
        "Matteo Muratori",
      ],
      origin_project: "dsgrid CONUS 2022",
      origin_date: "Dec 21 2021",
      origin_version: "dsgrid",
      source: "https://github.nrel.gov/mmurator/TMM/commit/54f715e0fe015a578dd0594c7031c046cfe27907",
      data_classification: "low",
      tags: [
        "transportation",
        "conus-2022",
        "reference",
        "tempo",
      ],
    }

2. Choose a data format as described in :ref:`dataset-formats`. This dataset uses the two-table
   (standard) format with dimension information stored like this:

   - Time: Representative period format with hourly data for one week per month.
   - Metric: record IDs are pivoted in the data table.
   - Geography: Stored in lookup table.
   - Subsector: Stored in lookup table.
   - Model Year: Stored in lookup table.
   - Scenario: Stored in lookup table.
   - Sector: Trivial
   - Weather Year: Trivial

   Add the relevant settings to your dataset.json5.

.. code-block:: JavaScript

      data_schema_type: "standard",
      trivial_dimensions: [
        "sector",
        "weather_year",
      ],
      // The time in this dataset has no time zone. It is based on the local time perceived by the
      // people being modeled. dsgrid will map times to the project's geography time zone.
      use_project_geography_time_zone: true,
      data_schema: {
        load_data_column_dimension: "metric",
      },


3. Identify the dimension records required by the project. Here is what is listed in the
   ``project.json5``:

.. code-block:: JavaScript

    required_dimensions: {
      single_dimensional: {
        sector: {
          base: ["trans"],
        },
        subsector: {
          supplemental: [
            {
              name: "Subsectors by Sector Collapsed",
              record_ids: ["transportation_subsectors"],
            },
          ],
        },
        metric: {
          supplemental: [
            {
              name: "transportation-end-uses-collapsed",
              record_ids: ["transportation_end_uses"],
            },
          ],
        },
      }
    }

Follow the instructions at :ref:`project-viewer` to start the viewer. Once you have the data table
loaded in your browser, type ``transportation`` in the filter row of the ``dimension_query_name``
column and then select ``transportation_subsectors`` or ``transportation_end_uses``. The dimension
record table will get populated with record IDs.

If you prefer working in an interactive Python session, here is example code to do the same thing:

.. code-block:: python

    In [1]: from dsgrid.dimension.base_models import DimensionType
       ...: from dsgrid.registry.registry_manager import RegistryManager
       ...: from dsgrid.registry.registry_database import DatabaseConnection
       ...:
       ...: manager = RegistryManager.load(
       ...:     DatabaseConnection(
       ...:         hostname="dsgrid-registry.hpc.nrel.gov",
       ...:         database="standard-scenarios",
       ...:     )
       ...: )
       ...: project = manager.project_manager.load_project("dsgrid_conus_2022")
       ...: project.config.get_dimension_records("transportation_end_uses").show()
       ...: project.config.get_dimension_records("transportation_subsectors").show()
    +-------------------+--------------------+-----------+----+
    |                 id|                name|    fuel_id|unit|
    +-------------------+--------------------+-----------+----+
    |electricity_ev_l1l2|Electricity EV L1/L2|electricity| kWh|
    |electricity_ev_dcfc|  Electricty EV DCFC|electricity| kWh|
    +-------------------+--------------------+-----------+----+

    +------------+------------+
    |          id|        name|
    +------------+------------+
    | bev_compact| Bev Compact|
    | bev_midsize| Bev Midsize|
    |  bev_pickup|  Bev Pickup|
    |     bev_suv|     Bev Suv|
    |phev_compact|Phev Compact|
    |phev_midsize|Phev Midsize|
    | phev_pickup| Phev Pickup|
    |    phev_suv|    Phev Suv|
    +------------+------------+

Alteratively, you can browse the source files, such as `this records file
<https://github.com/dsgrid/dsgrid-project-StandardScenarios/blob/main/dsgrid_project/dimensions/supplemental/transportation_subsectors.csv>`_.

3. Add dimension configs to the ``dimensions`` section of your ``dataset.json5`` for each dimension
   that is unique for your dataset. If you use a dimension from the project or another dataset, add
   its dimension ID to the ``dimension_references`` section of the file. For example,

.. code-block:: JavaScript

    dimensions: [
      {
        "class": "County",
        type: "geography",
        name: "ACS County 2018",
        display_name: "County",
        file: "dimensions/counties.csv",
        description: "American Community Survey US counties, 2018.",
      },
    ]

4. Create dimension mappings for all dimensions that are different than the project. Add mappings
   to ``dimension_mappings.json5`` and records to ``dimension_mappings/<your-mapping>.csv``. Here
   are two examples.

   - The TEMPO dataset uses a superset of county records compared to the project (it includes
     Alaska and Hawaii). The counties in common have the same IDs. Here is the resulting dimension
     mapping metadata and records. All IDs that exist in TEMPO but not the project have a ``null``
     entry for ``to_id``.

.. code-block:: JavaScript

    {
      description: "ACS County 2018 to ACS County 2020 L48",
      file: "dimension_mappings/county_to_county.csv",
      dimension_type: "geography",
      mapping_type: "many_to_one_aggregation",
    },

   Records file snippet::

    from_id,to_id
    01001,01001
    01003,01003
    01005,01005
    01007,01007
    02013,
    02016,


   - The TEMPO dataset projects electricity load from 2018 to 2050 with only even years. The
     project expects model years from 2010 to 2050. The TEMPO dataset uses this mapping to meet the
     project requirements.

.. code-block:: JavaScript

    {
      description: "2010-2050 from interpolating for every other year and 0 for 2010-2017",
      dimension_type: "model_year",
      file: "dimension_mappings/model_year_to_model_year.csv",
      mapping_type: "many_to_many_explicit_multipliers",
    },

Records file snippet::

    from_id,to_id,from_fraction
    2018,2010,0
    2018,2011,0
    2018,2012,0
    2018,2013,0
    2018,2014,0
    2018,2015,0
    2018,2016,0
    2018,2017,0
    2018,2018,1
    2018,2019,0.5
    2020,2019,0.5
    2020,2020,1
    2020,2021,0.5
    2022,2021,0.5


5. Create ``load_data.parquet``. This data table includes time columns (``day_of_week``, ``hour``,
   ``month``) and metric columns (``L1andL2`` and ``DCFC``). Other dimensions will go into the
   ``load_data_lookup.parquet``. Each unique time array needs to have a unique ``id``. The TEMPO
   team decided to encode internal information into specific bytes of each value, but that is
   optional. Other datasets use 1 to N.

   Refer to :ref:`dataset-formats` for guidance about partitions.

::

    >>> spark.read.parquet("tempo_conus_2022/1.0.0/load_data.parquet").show()
    +-----------+----+-----+---------+---------+---------+
    |day_of_week|hour|month|  L1andL2|     DCFC|       id|
    +-----------+----+-----+---------+---------+---------+
    |          0|   0|   12|484.81393|405.39902|109450511|
    |          0|   1|   12|150.94759|      0.0|109450511|
    |          0|   2|   12|      0.0|      0.0|109450511|
    |          0|   3|   12|      0.0|      0.0|109450511|
    |          0|   4|   12|      0.0|      0.0|109450511|
    |          0|   5|   12|      0.0|      0.0|109450511|
    |          0|   6|   12|      0.0|      0.0|109450511|
    |          0|   7|   12|      0.0|      0.0|109450511|
    |          0|   8|   12|      0.0|      0.0|109450511|
    |          0|   9|   12|      0.0|      0.0|109450511|
    |          0|  10|   12|      0.0|      0.0|109450511|
    |          0|  11|   12|      0.0|      0.0|109450511|
    |          0|  12|   12|312.24542|      0.0|109450511|
    |          0|  13|   12|  270.221|      0.0|109450511|
    |          0|  14|   12|180.36609|      0.0|109450511|
    |          0|  15|   12|1078.6263|      0.0|109450511|
    |          0|  16|   12| 656.5123|      0.0|109450511|
    |          0|  17|   12|1092.3519|      0.0|109450511|
    |          0|  18|   12| 959.8675|      0.0|109450511|
    |          0|  19|   12| 841.9459|      0.0|109450511|
    +-----------+----+-----+---------+---------+---------+

5. Create ``load_data_lookup.parquet``. The ``id`` column should match the values in
   ``load_data.parquet`` so that a single table can be produced by joining the two tables on that
   column. If the dataset is missing data for specific dimension combinations, include a row for
   each combination and set ``id`` to ``null``.

::

    >>> spark.read.parquet("tempo_conus_2022/1.0.0/load_data_lookup.parquet").show()
    +---------+--------------------+----------+--------+------------------+
    |geography|           subsector|model_year|      id|          scenario|
    +---------+--------------------+----------+--------+------------------+
    |    06085|Single_Driver+Low...|      2022| 1060853|ldv_sales_evs_2035|
    |    06085|Single_Driver+Low...|      2022| 2060853|ldv_sales_evs_2035|
    |    06085|Single_Driver+Low...|      2022| 3060853|ldv_sales_evs_2035|
    |    06085|Single_Driver+Low...|      2022| 4060853|ldv_sales_evs_2035|
    |    06085|Single_Driver+Low...|      2022| 5060853|ldv_sales_evs_2035|
    |    06085|Single_Driver+Low...|      2022| 6060853|ldv_sales_evs_2035|
    |    06085|Single_Driver+Low...|      2022| 7060853|ldv_sales_evs_2035|
    |    06085|Single_Driver+Low...|      2022| 8060853|ldv_sales_evs_2035|
    |    06085|Single_Driver+Low...|      2022| 9060853|ldv_sales_evs_2035|
    |    06085|Single_Driver+Low...|      2022|10060853|ldv_sales_evs_2035|
    |    06085|Single_Driver+Low...|      2022|11060853|ldv_sales_evs_2035|
    |    06085|Single_Driver+Low...|      2022|12060853|ldv_sales_evs_2035|
    |    06085|Single_Driver+Low...|      2022|13060853|ldv_sales_evs_2035|
    |    06085|Single_Driver+Low...|      2022|14060853|ldv_sales_evs_2035|
    |    06085|Single_Driver+Low...|      2022|15060853|ldv_sales_evs_2035|
    |    06085|Single_Driver+Low...|      2022|16060853|ldv_sales_evs_2035|
    |    06085|Single_Driver+Low...|      2022|17060853|ldv_sales_evs_2035|
    |    06085|Single_Driver+Low...|      2022|18060853|ldv_sales_evs_2035|
    |    06085|Single_Driver+Low...|      2022|19060853|ldv_sales_evs_2035|
    |    06085|Single_Driver+Low...|      2022|20060853|ldv_sales_evs_2035|
    +---------+--------------------+----------+--------+------------------+

6. Register and submit the dataset. This requires a properly-configured Spark cluster because of
   the data size. Smaller datasets may succeed with Spark in local mode. Refer to
   :ref:`spark-overview` to setup a Spark cluster.

   This command assumes that ``dataset.json5``, ``dimension_mappings.json5``,
   and the directory containing ``load_data.parquet`` and ``load_data_lookup.parquet`` are in a
   directory called ``base_dir``.

   When running this command dsgrid will perform numerous validations in order to verify dataset
   consistency and that the project requirements are met. It may take up to an hour on an HPC
   compute node.

   TODO: offline mode for verification, online mode for the final registration.

.. code-block:: console

    $ spark-submit --master=spark://<master_hostname>::7077 $(which dsgrid-cli.py) registry \
        projects \
        register-and-submit-dataset \
        --project-id dsgrid_conus_2022 \
        --dimension-mapping-file base_dir/dimension_mappings.json5 \
        --log-message "Register and submit TEMPO dataset" \
        base_dir/dataset.json5 \
        base_dir/tempo_load_data
