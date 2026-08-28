# Query Concepts

## Project Queries

This section describes how dsgrid processes queries when you run the command `dsgrid query project run -o query_output`.

dsgrid performs the following steps:

1. **Check for cached data** - Check to see if a cached version of the dataset portion of the query is stored in `query_output/cached_tables`. If so, skip to step 5.

2. **Create project-mapped datasets** - The following occurs for each dataset:
   - Check to see if a cached project-mapped dataset already exists. If so, skip to the next step.
   - Pre-filter dataset according to the `dimension_filters` in the dataset data model of the query.
   - Map dataset dimensions to the project.
   - Convert units.
   - Evaluate the query by writing the table to the filesystem in the directory `query_output/cached_project_mapped_datasets`.
     - dsgrid restarts the runtime session for this query. If custom runtime configuration parameters are defined in the `runtime_conf_per_dataset` data model of the query for this dataset, dsgrid will apply them in the new session.
     - If the backend is Spark, the value `spark.sql.shuffle.partitions` may need to be increased for very large datasets.

:::{note}
Currently, there is no way to skip caching of the dataset. If it is not performed, the backend query can grow too large and take too long to complete in normal compute node allocations.
:::

3. **Combine datasets** - Combine the datasets as specified by the `expression` in the dataset data model of the query. The default is to take a union of all datasets.

4. **Persist intermediate table** - If the option `--persist-intermediate-table` is `true` (which is the default) then dsgrid will evaluate the query from the previous step by writing the table to the filesystem in the directory `query_output/cached_tables`. This can be disabled by setting `--no-persist-intermediate-table`.

5. **Apply result filters** - Apply any dimension_filters defined in the `result` data model of the query.

6. **Apply aggregations/disaggregations** - Apply any aggregations or disaggregations defined in the `result` data model of the query.

7. **Replace IDs with names** - If the field `replace_ids_with_names` in the `result` data model is `true`, replace all dimension record IDs with names.

8. **Sort columns** - If the field `sort_columns` in the `result` data model is `true`, sort the table by those columns.

9. **Write output** - Evaluate the query for the previous steps by writing the table to the filesystem in the directory `query_output/<query-name>`.

10. **Run reports** - Run any `reports` defined in the `result` data model of the query.

## Performance Considerations

The project-mapping step is by far the most time consuming. There are some trade-offs to consider.

### Persisting Intermediate Tables

- **If you will only use one query result**, you may want to disable this behavior because it will use less filesystem space and may take less time.
- **However, persisting is always safer**:
  - Persisting makes it easier for the active backend to complete its jobs.
  - If your compute nodes are revoked because of a timeout and you persisted the tables, you can resume from that point.

### Pre-filtering vs Post-filtering

You can choose to filter data before or after the datasets are mapped to the project and combined.

- **If you will only use the query result once**, you are likely better off pre-filtering as much as possible.
- **If you will run many queries on the same datasets** that will use different filters, consider using post-filters because you will only run the project-mapping one time.

## Next Steps

- Follow the [Query a Project Tutorial](../tutorials/query_project) for hands-on practice
- Learn [how to filter queries](../how_tos/how_to_filter)
- Explore [aggregation options](aggregations.md)
- Understand [output formats](output_formats.md)
