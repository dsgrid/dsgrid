.. _filter-a-query:

*********************
How to Filter a Query
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
   Angeles, CA). This is equivalent to ``column == "06037"``. You can use any SQL expression.
   Refer to the `Spark documentation
   <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.filter.html>`_
   for more information.

.. note:: All values for dimensions in the filters must be strings.

.. tabs::

   .. code-tab:: js JSON5

      dimension_filters: [
        {
          dimension_type: "geography",
          dimension_name: "county",
          operator: "==",
          value: "06037",
          filter_type: "expression",
          negate: false,
        },
      ]

   .. code-tab:: py

      dimension_filters=[
          DimensionFilterExpressionModel(
              dimension_type=DimensionType.GEOGRAPHY",
              dimension_name="county",
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
          dimension_name: "county",
          value: "== '06037'",
          filter_type: "expression_raw",
          negate: false,
        },
      ]

   .. code-tab:: py

      dimension_filters=[
          DimensionFilterExpressionRawModel(
              dimension_type=DimensionType.GEOGRAPHY",
              dimension_name="county",
              value="== '06037'",
              negate=False,
          ),
      ]

   .. code-tab:: py pyspark

      df.filter("geography == '06037'")

3. Filter a table where the specified column matches the specified value(s) according to the Spark
   SQL operator. This is useful for cases where you want to match partial strings or use a list of
   possible values.

.. tabs::

   .. code-tab:: js JSON5

      dimension_filters: [
        {
          dimension_type: "model_year",
          dimension_name: "model_year",
          column: "id",
          operator: "isin",
          value: [
            "2030",
            "2040",
            "2050",
          ],
          filter_type: "column_operator"
          negate: false,
        },
        {
          dimension_type: "sector",
          dimension_name: "sector",
          column: "id",
          operator: "startswith",
          value: "com",
          filter_type: "column_operator"
          negate: false,
        },
      ],

   .. code-tab:: py

      dimension_filters=[
          DimensionFilterColumnOperatorModel(
              dimension_type=DimensionType.MODEL_YEAR,
              dimension_name="model_year",
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
              dimension_name="sector",
              column="id",
              operator="startswith",
              value="com",
              filter_type="column_operator"
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

      # Note that these are example dimension record IDs for demonstration purposes.
      df.filter(df["end_use"].isin(["electricity_cooling", "electricity_heating"]))

5. Filter a table with records from a supplemental dimension. This example filters the table to
   include only counties in Colorado or New Mexico. Note that it does not change the dimensionality
   of the data or perform aggregations. It only tells dsgrid to filter out counties that don't have
   a mapping in the supplemental dimension records.

.. tabs::

   .. code-tab:: js JSON5

      dimension_filters: [
        {
          dimension_type: "geography",
          dimension_name: "state",
          column: "id",
          operator: "isin",
          value: ["CO", "NM"],
          filter_type: "supplemental_column_operator"
        },
      ],

   .. code-tab:: py

      dimension_filters=[
          SupplementalDimensionFilterColumnOperatorModel(
              dimension_type=DimensionType.GEOGRAPHY,
              dimension_name="state",
              column="id",
              operator="isin",
              value=["CO", "NM"],
          ),
      ]

   .. code-tab:: py pyspark

      df.filter(df["id"].isin(["CO", "NM"]))

6. Filter a table with times between two timestamps (inclusive on both sides).

.. tabs::

   .. code-tab:: js JSON5

      dimension_filters: [
        {
          dimension_type: "time",
          dimension_name: "time_est",
          column: "time_est",
          lower_bound: "2012-07-01 00:00:00",
          upper_bound: "2012-08-01 00:00:00",
          filter_type: "between_column_operator"
          negate: false,
        },
      ],

   .. code-tab:: py

      dimension_filters=[
          DimensionFilterBetweenColumnOperatorModel(
              dimension_type=DimensionType.TIME,
              dimension_name="time_est",
              column="time_est",
              lower_bound="2012-07-01 00:00:00",
              upper_bound="2012-08-01 00:00:00",
              negate=False,
          ),
      ]

   .. code-tab:: py pyspark

      df.filter(df["timestamp"].between("2012-07-01 00:00:00", "2012-08-01 00:00:00"))
