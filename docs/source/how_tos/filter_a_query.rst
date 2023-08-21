.. _filter-a-query:

*********************
How to filter a query
*********************
dsgrid offers several ways to filter the result of a query. It is important to understand some
dsgrid behaviors to get an optimal result. Please refer to :ref:`project-queries-explanation` for
details.

The examples below show how to define the filters in ``JSON5`` or ``Python`` as well as the
equivalent implementation if you were to filter the dataframe with ``Spark`` in ``Python``
(``pyspark``).

All examples except ``DimensionFilterBetweenColumnOperatorModel`` assume that the dataframe being
filtered is the dimension record table. ``DimensionFilterBetweenColumnOperatorModel`` assumes that
the table is the load data dataframe with time-series information.

.. note:: Whenever multiple filters are provided in an array dsgrid performs an ``and`` across all
   filters.

1. Filter the table where a dimension column matches an expression. This example filters the
   geography dimension by selecting only data where the county matches the ID ``06037`` (Los
   Angeles, CA). This is equivalent to ``column == "06037"``. You can use any mathematical
   operator.

.. note:: All values for dimensions in the filters must be strings.

.. tabs::

   .. code-tab:: js JSON5

      dimension_filters: [
        {
          dimension_type: "geography",
          dimension_query_name: "county",
          operator: "==",
          value: "06037",
          filter_type: "DimensionFilterExpressionModel",
          negate: false,
        },
      ]

   .. code-tab:: py

      dimension_filters=[
          DimensionFilterExpressionModel(
              dimension_type=DimensionType.GEOGRAPHY",
              dimension_query_name="county",
              operator="==",
              value="06037",
              negate=False,
          ),
      ]

   .. code-tab:: py pyspark

      df.filter("geography == '06037'")


2. Similar to the first but use a raw expression. ``DimensionFilterExpressionModel`` creates a
   string by inserting the input parameters and adding required quotes.
   ``DimensionFilterExpressionRawModel`` uses your exact value. This allows you to make a more
   complex, custom expression. The following example reaches the same result as above. You can use
   any expression supported by `pyspark.sql.DataFrame.filter
   <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html>`_.

.. tabs::

   .. code-tab:: js JSON5

      dimension_filters: [
        {
          dimension_type: "geography",
          dimension_query_name: "county",
          value: "== '06037'",
          filter_type: "DimensionFilterExpressionRawModel",
          negate: false,
        },
      ]

   .. code-tab:: py

      dimension_filters=[
          DimensionFilterExpressionRawModel(
              dimension_type=DimensionType.GEOGRAPHY",
              dimension_query_name="county",
              value="== '06037'",
              negate=False,
          ),
      ]

   .. code-tab:: py pyspark

      df.filter("geography == '06037'")

3. Filter a table where a dimension column matches a Spark SQL operator. This is useful for cases
   where you want to match non-exact strings or use a list of possible values.

.. tabs::

   .. code-tab:: js JSON5

      dimension_filters: [
        {
          dimension_type: "model_year",
          dimension_query_name: "model_year",
          column: "id",
          operator: "isin",
          value: [
            "2030",
            "2040",
            "2050",
          ],
          filter_type: "DimensionFilterColumnOperatorModel"
          negate: false,
        },
        {
          dimension_type: "sector",
          dimension_query_name: "sector",
          column: "id",
          operator: "startswith",
          value: "com",
          filter_type: "DimensionFilterColumnOperatorModel"
          negate: false,
        },
      ],

   .. code-tab:: py

      dimension_filters=[
          DimensionFilterColumnOperatorModel(
              dimension_type=DimensionType.MODEL_YEAR,
              dimension_query_name="model_year",
              column=id,
              operator="isin",
              value=[
                  "2030",
                  "2040",
                  "2050",
              ],
              negate=False,
          ),
          DimensionFilterColumnOperatorModel(
              dimension_type="sector",
              dimension_query_name="sector",
              column="id",
              operator="startswith",
              value="com",
              filter_type="DimensionFilterColumnOperatorModel"
              negate=False,
          ),
      ]

   .. code-tab:: py pyspark

      df.filter(df["model_year"].isin(["2030", "2040", "2050"])) \
        .filter(df["sector"].startswith("com"))

4. Filter a table with values from a subset dimension. This example filters the table to
   include only electricity end uses.

.. tabs::

   .. code-tab:: js JSON5

      dimension_filters: [
        {
          dimension_type: "metric",
          dimension_query_names: ["electricity_end_uses"],
          filter_type: "subset"
        },
      ],

   .. code-tab:: py

      dimension_filters=[
          SubsetDimensionFilterModel(
              dimension_type=DimensionType.METRIC,
              dimension_query_names=["electricity_end_uses"],
          ),
      ]

   .. code-tab:: py pyspark

      df.filter(df["end_use"].isin(["electricity_cooling", "electricity_heating"]))

5. Filter a table with values from a supplemental dimension. This example filters the table to
   include only end uses with a fuel type of electricity.  Note that in many cases this filter type
   is redundant with subset filters.

.. tabs::

   .. code-tab:: js JSON5

      dimension_filters: [
        {
          dimension_type: "subsector",
          dimension_query_name: "subsectors_by_sector",
          column: "id",
          operator: "isin",
          value: ["commercial_subsectors", "residential_subsectors"],
          filter_type: "supplemental_column_operator"
        },
      ],

   .. code-tab:: py

      dimension_filters=[
          SupplementalDimensionFilterColumnOperatorModel(
              dimension_type=DimensionType.SUBSECTOR,
              dimension_query_name="subsectors_by_sector",
              column="id",
              operator="isin",
              value=["commercial_subsectors", "residential_subsectors"],
          ),
      ]

   .. code-tab:: py pyspark

      df.filter(df["id"].isin(["commercial_subsectors", "residential_subsectors"]))

6. Filter a table with times between two timestamps (inclusive on both sides).

.. tabs::

   .. code-tab:: js JSON5

      dimension_filters: [
        {
          dimension_type: "time",
          dimension_query_name: "time_est",
          column: "time_est",
          lower_bound: "2012-07-01 00:00:00",
          upper_bound: "2012-08-01 00:00:00",
          filter_type: "DimensionFilterBetweenColumnOperatorModel"
          negate: false,
        },
      ],

   .. code-tab:: py

      dimension_filters=[
          DimensionFilterBetweenColumnOperatorModel(
              dimension_type=DimensionType.TIME,
              dimension_query_name="time_est",
              column="time_est",
              lower_bound="2012-07-01 00:00:00",
              upper_bound="2012-08-01 00:00:00",
              negate=False,
          ),
      ]

   .. code-tab:: py pyspark

      df.filter(df["timestamp"].between("2012-07-01 00:00:00", "2012-08-01 00:00:00"))
