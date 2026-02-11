# Create a Derived Dataset

In this tutorial you will learn how to query a dsgrid project to produce and register a derived dataset. The tutorial uses the comstock_conus_2022_projected derived dataset from [dsgrid-project-StandardScenarios](https://github.com/dsgrid/dsgrid-project-StandardScenarios) as an example.

You can run all commands in this tutorial except the last one on NREL's HPC Kestrel cluster (the dataset is already registered).

## Steps

SSH to a login node to begin the tutorial.

### Step 1: Set Up Your Spark Cluster

You will need at least four compute nodes to create this dataset in about an hour. Follow the instructions at [Run dsgrid on Kestrel](../apache_spark/run_on_kestrel) if you have not already done so. The instructions now assume that you are logged in to the compute node that is running the Spark master process.

### Step 2: Copy the Query File

Copy the query file for this derived dataset from [github](https://github.com/dsgrid/dsgrid-project-StandardScenarios/blob/main/dsgrid_project/derived_datasets/comstock_conus_2022_projected.json5).

### Step 3: Set Environment Variables

Set these environment variables to avoid repeated typing:

:::{note}
The value of 2400 for NUM_PARTITIONS is based on observations from processing this ~1 TB dataset.
:::

```bash
export SPARK_CLUSTER=spark://$(hostname):7077
export QUERY_OUTPUT_DIR=query-output
export DSGRID_CLI=$(which dsgrid-cli.py)
export NUM_PARTITIONS=2400
```

### Step 4: Create the Derived Dataset

Create the `comstock_conus_2022_projected` dataset. The `comstock_conus_2022_reference` dataset has load data for a single year. This query applies the `aeo2021_reference_commercial_energy_use_growth_factors` dataset to project the load values through the model year 2050.

```bash
spark-submit \
    --master ${SPARK_CLUSTER} \
    --conf spark.sql.shuffle.partitions=${NUM_PARTITIONS} \
    ${DSGRID_CLI} \
    query \
    project \
    run \
    comstock_conus_2022_projected.json5 \
    -o ${QUERY_OUTPUT}
```

### Step 5: Create Derived Dataset Config Files

Generate the configuration files for the derived dataset:

```bash
spark-submit \
    --master ${SPARK_CLUSTER} \
    --conf spark.sql.shuffle.partitions=${NUM_PARTITIONS} \
    ${DSGRID_CLI} \
    query \
    project \
    create-derived-dataset-config \
    ${QUERY_OUTPUT}/comstock_conus_2022_projected \
    comstock-dd
```

### Step 6: Edit Configuration Files

Edit the output files in `comstock-dd` as desired.

### Step 7: Register the Derived Dataset

Register the derived dataset with the dsgrid registry:

```bash
spark-submit \
    --master ${SPARK_CLUSTER} \
    --conf spark.sql.shuffle.partitions=${NUM_PARTITIONS} \
    ${DSGRID_CLI} \
    registry \
    datasets \
    register \
    comstock-dd/dataset.json5 \
    ${QUERY_OUTPUT}/comstock_conus_2022_projected \
    -l Register_comstock_conus_2022_projected
```

### Step 8: Submit to the Project

Submit the derived dataset to the project:

```bash
spark-submit \
    --master ${SPARK_CLUSTER} \
    --conf spark.sql.shuffle.partitions=${NUM_PARTITIONS} \
    ${DSGRID_CLI} \
    registry \
    projects \
    submit-dataset \
    -p dsgrid_conus_2022 \
    -d comstock_conus_2022_projected \
    -r comstock-dd/dimension_mapping_references.json5 \
    -l Submit_comstock_conus_2022_projected
```

## Next Steps

- Learn about [derived dataset concepts](../project_derived_datasets/concepts)
- Understand [query processing](../project_queries/concepts) in more detail
- Explore [querying project data](query_project) for analysis
