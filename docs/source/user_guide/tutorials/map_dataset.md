# Map a Dataset to a Project's Dimensions

It is often beneficial to map a dataset to a project's dimensions before running queries with other datasets that perform aggregations or filters. Mapping a dataset with Spark can be an expensive operation that takes several iterations to figure out with Spark. It is easier to debug in isolation. Once complete, the cached result can be used for subsequent queries.

This page assumes that you have already registered a dataset and submitted it to a project. It also assumes that you have populated your `~/.dsgrid.json5` file with the location of your dsgrid registry.

Spark runtime details are not covered here. Refer to [Apache Spark Overview](../apache_spark/overview).

## Basic Operation

dsgrid offers a CLI command to perform the mapping operation. This is its simplest form:

```bash
dsgrid query project map-dataset my-project-id my-dataset-id
```

By default, this will attempt to map all dimensions by performing three Spark queries:

1. Map all dimensions other than time that do not already match the project. Apply scaling factors if assigned and automatically convert units if applicable. Persist the result to the filesystem.
2. Map the time dimension. Persist the result to the filesystem.
3. Finalize the table: convert user-defined options, such as column names. Add null rows as necessary.

If the dataset is less than 10 GB, this process should run smoothly with Spark. If the dataset grows to hundreds of GBs or more, you may experience problems. Our recommendation is to use the dsgrid mapping plan features described below to work in an iterative manner.

## Mapping Plan

Create a mapping plan as shown in the data model at [Dataset Mapping Plan Reference](../../software_reference/data_models/dimension_mapping_model). This plan allows you to specify the order of mapping operations as well as whether to persist intermediate tables.

If you set `persist=true` for an operation, dsgrid will persist the query to the filesystem and record a metadata file. It can resume from that checkpoint on subsequent iterations.

**Points to consider when creating a mapping plan:**

- If a dimension mapping operation will reduce the size of data, perhaps because it is aggregating data, list that operation first and persist it.
- If a dimension mapping operation will increase the size of data, such as a disaggregation or duplication, list that operation last and persist the query just before it. We have experienced the most problems with Spark with this type of operation.
- Some disaggregation operations can cause data skew. dsgrid will automatically enable techniques to handle this condition with certain mapping types. If you experience this problem, you may need to set `handle_data_skew: true` in the mapping plan for that operation. Refer to [Executors Spilling to Disk](../apache_spark/overview.md#executors-spilling-to-disk) for information on how to identify this condition.

### Example Mapping Plan

Below is an example mapping plan in JSON formation. The dataset in this example has a one-to-one mapping for the scenario dimension, a many-to-many mapping for the model_year dimension, and a disaggregation from state to county for the geography dimension. The Spark query for the geography disaggregation is failing. Here is our rationale for the plan:

1. Persist the result after mapping the scenario and model_year dimensions. This part is working, but takes some time. We may have to run the geography disaggregation several times, and so we want to avoid repeating this work.
2. Persist the result after mapping the geography dimension so that we don't have to repeat the work once we figure out the solution.

```javascript
{
  dataset_id: "my-dataset-id",
  mappings: [
    {
      name: "scenario",
    },
    {
      name: "model_year",
      persist: true,
    },
    {
      name: "county",
      persist: true,
    },
  ],
}
```

## Execution with a Mapping Plan

```bash
dsgrid query project map-dataset my-project-id my-dataset-id \
    --mapping-plan plan.json5
```

Observe progress in the console. Whenever dsgrid persists an intermediate query, it will log a message like this:

```
2025-07-08 14:29:21,762 - INFO [dsgrid.dataset.dataset_mapping_manager dataset_mapping_manager.py:99] : Saved checkpoint in /kfs3/scratch/dthom/dsgrid-project/__dsgrid_scratch__/tmpgn_6xbst.json
```

If the job fails, you can resume by specifying that checkpoint file as follows:

```bash
dsgrid query project map-dataset my-project-id my-dataset-id \
    --mapping-plan plan.json5 \
    --checkpoint-file /kfs3/scratch/dthom/dsgrid-project/__dsgrid_scratch__/tmpgn_6xbst.json
```

Note that the checkpoint file defines what mapping operations completed and contains a reference to the persisted table. You can use that table to perform your own debugging. You could look at the size and number of partitions of the table, for example, to see if it is what you expect.

```bash
$ cat /kfs3/scratch/dthom/ief-registry-y2-3/__dsgrid_scratch__/tmpgn_6xbst.json
{
  "dataset_id": "my-dataset-id",
  "completed_operation_names": [
      "scenario",
      "model_year",
  ],
  "persisted_table_filename": "/kfs3/scratch/dthom/dsgrid-project/__dsgrid_scratch__/tmpcrpladhx.parquet",
  "mapping_plan_hash": "558083c65760db8fc7bcbbaf48cc94fd1364198b941b6ad845213877d794200c",
  "timestamp": "2025-07-08T14:29:21.746195"
}
```

## Next Steps

- Learn about [dimension mapping concepts](../dataset_mapping/concepts)
- Explore [querying project data](query_project)
- Understand [Apache Spark performance tuning](../apache_spark/overview.md#troubleshooting-configuration-problems)
