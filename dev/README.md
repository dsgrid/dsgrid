# dsgrid Developer README

[Dependencies](#developer-dependencies) | [Tests](#tests) | [EFS Project Repository](#testingexploring-with-the-efs-project-repository) | [Interactive Exploration](#interactive-exploration) | [Existing Spark Cluster](#use-existing-spark-cluster) | [Spark Standalone Cluster](#spark-standalone-cluster) | [Publish Documentation](#publish-documentation)

## Developer Dependencies

**pip extras**

```
pip install -e .[tests]

# or

pip install -e .[dev] # includes what is needed for tests and code development

# or

pip install -e .[admin] # dev plus what is needed for creating documentation and releasing packages
```

**Setting up pre-commit hooks**

```
pre-commit install
```

**Additional software required for publishing documentation:**

- [Pandoc](https://pandoc.org/installing.html)

## Tests

### Setup
The tests use the [test data repository](https://github.com/dsgrid/dsgrid-test-data.git)
as a git submodule in `./dsgrid-test-data`. It is a minimal version of the EFS project and
datasets. You must initialize this submodule and keep it updated.

Initialize the submodule:
```
git submodule init
git submodule update
```

Update the submodule when there are new changes in the test data repository:
```
git submodule update --remote --merge
```

### Run tests

To run all tests, including AWS tests:
```
pytest
```

If you want to exclude AWS tests:
```
pytest tests
```

If you only want to run AWS tests:
```
pytest tests_aws
```

### Workflow for developing a feature that changes code and data

If you are developing a feature that requires changing code and data then you will need to keep
the submodule synchronized. Here is an example workflow:

1. Create a dsgrid branch.
```
git checkout -b feature-x
```

2. Create a data branch inside the submodule.
```
cd dsgrid-test-data
git checkout -b feature-x
cd ..
```

3. Implement and test your feature.

4. Commit your code and data changes in each repository.

5. Update the dsgrid repo to point to the correct data commit.
```
git submodule set-branch -b feature-x dsgrid-test-data
git add .gitmodules dsgrid-test-data
git commit -m "Point dsgrid-test-data to feature-x branch"
```

6. Push both branches to GitHub. **Note**: Using a forked data repository is not supported.

7. Open two pull requests.

8. Address comments, if needed. If you make new commits to the data branch then you must update
the dsgrid branch before pushing back to GitHub.
```
git add dsgrid-test-data
git commit -m "Update dsgrid-test-data"
```

9. Merge the data pull request.

10. Update the dsgrid branch to point back to the `main` data branch.
```
cd dsgrid-test-data
git checkout main
git pull origin main
# The feature branch is now in main. Delete it.
git branch -d feature-x
cd ..
git submodule set-branch -b main dsgrid-test-data
git add .gitmodules dsgrid-test-data
git commit -m "Point dsgrid-test-data to main branch"
git push origin feature-x
```

11. After CI passes, merge the dsgrid pull request.

### Test registry
The setup code creates a local registry for testing in `./tests/data/registry`.
In order to save time on repeated runs it will not re-create the registry on
repeated runs as long as there are no new commits.

### Pytest options

option flag           | effect
--------------------- | ------
--log-cli-level=debug | emits log messages to the console. level can be set to debug, info, warn, error


## Testing/exploring with the EFS project repository

You can create a local registry with the [EFS project repository](https://github.com/dsgrid/dsgrid-project-EFS)
and use it for testing and exploration.

Clone the repository to your system.
```
git clone https://github.com/dsgrid/dsgrid-project-EFS $HOME/dsgrid-project-EFS
```

Download the EFS datasets from AWS or Eagle (`/projects/dsgrid/efs_datasets/converted_output/commercial`)
to a local path and set an environment variable for it.

This is what that directory should contain:

```
tree ~/.dsgrid-data
.dsgrid-data
└── efs_comstock
    ├── convert_dsg.log
    ├── dimensions.json
    ├── enduse.csv
    ├── geography.csv
    ├── load_data.parquet
    │   ├── _SUCCESS
    │   ├── part-00000-080db910-6446-4ff4-ae98-293d79a8f61a-c000.snappy.parquet
    │   ├── part-00001-080db910-6446-4ff4-ae98-293d79a8f61a-c000.snappy.parquet
    │   ├── part-00002-080db910-6446-4ff4-ae98-293d79a8f61a-c000.snappy.parquet
    │   ├── part-00003-080db910-6446-4ff4-ae98-293d79a8f61a-c000.snappy.parquet
    │   ├── part-00004-080db910-6446-4ff4-ae98-293d79a8f61a-c000.snappy.parquet
    │   ├── part-00005-080db910-6446-4ff4-ae98-293d79a8f61a-c000.snappy.parquet
    │   ├── part-00006-080db910-6446-4ff4-ae98-293d79a8f61a-c000.snappy.parquet
    │   ├── part-00007-080db910-6446-4ff4-ae98-293d79a8f61a-c000.snappy.parquet
    │   ├── part-00008-080db910-6446-4ff4-ae98-293d79a8f61a-c000.snappy.parquet
    │   └── part-00009-080db910-6446-4ff4-ae98-293d79a8f61a-c000.snappy.parquet
    ├── load_data_lookup.parquet
    │   ├── _SUCCESS
    │   └── part-00000-69955796-0a87-4d5f-ba55-b8cbd4b1372d-c000.snappy.parquet
    ├── scale_factors.json
    ├── sector.csv
    ├── test.parquet
    └── time.csv
```


Set environment variables to point to the registry and datasets.
```
export DSGRID_REGISTRY_PATH=./local-registry
export DSGRID_LOCAL_DATA_DIRECTORY=~/.dsgrid-data
```

Create and populate the registry.
```
python tests/make_us_data_registry.py $DSGRID_REGISTRY_PATH -p $HOME/dsgrid-project-EFS -d $DSGRID_LOCAL_DATA_DIRECTORY
```

Now you can run any `dsgrid registry` command.

## Interactive Exploration

In addition to the CLI tools you can use `scripts/registry.py` to explore a registry interactively.

One signficant advantage of this method is that you don't have to create a new Spark application on
every command. That saves several seconds for each iteration.

Be sure to use the `debug` function from the `devtools` package when exploring Pydantic models.
```
ipython -i scripts/registry.py -- --path=$DSGRID_REGISTRY_PATH --offline
In [1]: manager.show()

In [2]: dataset = dataset_manager.get_by_id("efs_comstock")

In [3]: debug(dataset.model)
```

## Interactive Exploration in a Jupyter notebook

It is advantageous to use a Jupyter notebook when you need to find dimension and dimension mapping IDs.
The registry show commands will detect if the code is running in a notebook and display HTML tables.
Wrapped text can be copied with the mouse. This is cumbersome in ASCII tables.

Here is an example notebook (assuming you have already downloaded a registry to ./local-registry):
```
from IPython.core.display import display, HTML
display(HTML("<style>.container { width:100% !important; }</style>"))

from dsgrid.registry.registry_manager import RegistryManager

mgr = RegistryManager.load("./local-registry", offline_mode=True)
mgr.dimension_manager.show()
```

## Interactive Exploration in a Jupyter notebook UI

The dsgrid team has developed a simple UI to interact with the registry. Here's how to use it.

```
$ dsgrid install-notebooks
```

That copies the dsgrid notebooks to `~/dsgrid-notebooks`.

If you are running a Spark cluster then set the environment variable `SPARK_CLUSTER`.

```
$ export SPARK_CLUSTER=spark://<hostname>:7077
```

Start Jupyter and open `registration.ipynb`.

Handling of stdout and stderr needs improvement. If the notebook gets cluttered, the best solution is to
re-execute the cell that creates the UI. You can pass default values for all text box fields in order to
avoid having to re-enter them every time.

```
app = RegistrationGui(
    defaults={
       "local_registry": "/my-local-registry",
        "project_file": "/repos/dsgrid-project-StandardScenarios/dsgrid_project/project.json5",
        "dataset_file": "/repos/dsgrid-project-StandardScenarios/dsgrid_project/datasets/modeled/comstock/dataset.json5",
        "dimension_mapping_file": "/repos/dsgrid-project-StandardScenarios/dsgrid_project/datasets/modeled/comstock/dimension_mappings.json5",
        "dataset_path": "/dsgrid-data/data-StandardScenarios/conus_2022_reference_comstock",
        "log_message": "log message",
    }
)
```

Note that you can access the dsgrid registry manager instances from the `app` instance.

```
from dsgrid.dimension.base_models import DimensionType
project_config = app.project_manager.get_by_id("dsgrid_conus_2022")
geography_dim = project_config.get_base_dimension(DimensionType.GEOGRAPHY)
spark_df = geography_dim.get_records_dataframe()
pandas_df = spark_df.toPandas()
```

You can debug Pydantic data models with the devtools package.

```
from devtools import debug
debug(project_config.model)
debug(geography_dim.model)
```

## Spark Standalone Cluster

It can be advantageous to create a standalone cluster instead of starting Spark from within a
Python process for these reasons:
- Easier to tune Spark parameters for performance and monitoring.
- Use the Spark web UI to inspect job details.

Note that while most unit tests work with a standalone cluster the tests in
`tests/cli/test_registry.py` do not. It's likely because that test will attempt to create multiple
clusters on the same system.

The full instructions to create a cluster are at http://spark.apache.org/docs/latest/spark-standalone.html.
The rest of this section documents a limited set that should work on your system.

Install Spark locally rather than rely on the pyspark installation from pip.
Refer to https://spark.apache.org/docs/latest/ for installation instructions.

Here is one way to configure and start a cluster.

1. Ensure that the environment variable `SPARK_HOME` is set to your installation directory.
2. Customize values in `$SPARK_HOME/conf/spark-defaults.conf` and/or `$SPARK_HOME/conf/spark-env.sh`.

3. Start the master with this command:
```
$SPARK_HOME/sbin/start-master.sh
```

4. Open http://localhost:8080/ in your browser and copy the cluster URL and port. It will be
something like `spark://hostname:7077`.

5. Start a worker with this command. Give the worker as much memory as you can afford. You can also
configure this in step #2.
```
$SPARK_HOME/sbin/start-worker.sh -m 16g spark://<hostname>:<port>
```

Monitor cluster tasks in your browser.

## Use existing Spark cluster

Start `spark-submit` or `pyspark` with the master assigned to the cluster URL.
```
$ pyspark --master spark://hostname:7077
```

## Running a Spark cluster on Eagle

This section describes how you can run scripts on any number of Eagle compute nodes. You can use
JADE to
- Allocate compute nodes.
- Create a Spark cluster on those nodes.
- Run one or more scripts on the cluster.
- Collect resource utilization metrics from each node.

1. Install JADE. Requires at least v0.6.3. JADE documentation is here: https://nrel.github.io/jade/index.html.
Documentation for the Spark configuration is here: https://nrel.github.io/jade/spark_jobs.html.

```
$ pip install NREL-jade
```

2. Put your scripts in a text file like this:

```
$ cat commands.txt
python my_script.py
```

3. Create the JADE configuration

```
$ jade config create commands.txt
Created configuration with 1 jobs.
Dumped configuration to config.json.
```

4. Create an HPC configuration file. The default behavior is to allocate a single node. You can edit
the resulting file to use more nodes.

```
jade config hpc -c hpc_config.toml -t slurm -a <your-allocation> --partition=debug --walltime=01:00:00
Created HPC config file hpc_config.toml
```

5. Add a Spark configuration. The `-c` option specifies the path to the dsgrid Singularity
container on Eagle.

```
jade spark config -c /projects/dsgrid/containers/dsgrid  --update-config-file=config.json
```

6. Optionally, customize Spark configuration parameters in the `spark/conf` folder created in the
previous step.

7. Submit the jobs to SLURM. You will likely want to include resource monitoring as shown here.

```
jade submit-jobs config.json -R periodic -r1
```

8. Monitor log files as needed:

- `<output-dir>/*.o` contains stdout.
- `<output-dir>/*.e` contains stderr.
- Refer to https://nrel.github.io/jade/tutorial.html#job-status for help with JADE status checking.
- After all jobs finish `<output-dir>/spark_logs` will contain Spark log files.
- After all jobs finish `<output-dir>/stats` will interactive resource utilization plots.
- In the future we will have Spark metrics recorded in JSON files.

## Connecting a Jupyter notebook to a Spark cluster on Eagle

Make sure you have already installed the dsgrid notebooks in your scratch directory. That installs
notebooks as well as a script that will start a Jupyter notebook on Eagle. It is called `start_notebook.sh`.

1. Create a JADE configuration per the above steps. However, the command passed to JADE must be
`bash dsgrid-notebooks/start_notebook.sh`.
2. Consider whether you want to run the notebook server from the container or from your local conda environment.
Set the JADE config parameter `run_user_script_outside_container` appropriately in `config.json`.
3. Submit the JADE job. Once the job starts run `tail -f <output-dir>/*.e`. It will eventually show
the Jupyter notebook URL as well as the the command you need to run to open an SSH tunnel.
4. Open the SSH tunnel.
5. Connect to the notebook.

When you are done with your work, save and close the notebook. You can release the Eagle compute node
allocation in one of these ways:

1. Stop the Jupyter server and allow JADE to shutdown cleanly. Do this if you care about collecting
Spark logs or compute node resource utilization stats. ssh to the Spark master node (this is the compute
node in the ssh tunnel commmand) and run `jupyter notebook stop 8889`. If you used a different port to
start the notebook server, adjust accordingly.
2. Use JADE to cancel the job. Do this if you have other other jobs running and don't want to accidentally
cancel them. Run `jade cancel-jobs <output-dir>`.
3. Use SLURM to cancel the job. Find the job ID with `squeue` and then run `scancel <job-id>`.
4. Are you confident that you have no other jobs running? `scancel -u $USER`

### Executing scripts
There are two basic ways to submit scripts to a Spark cluster.

1. [RECOMMENDED] Submit your script with `spark-submit` or start an interactive session with `pyspark` and run code.
Those will create the SparkSession automatically based on the CLI inputs. Refer to its help. You can
customize any part of the configuration.

2. Connect to a SparkSession from within Python. Here is an example. Refer to the Spark
documentation for other options. Note that you cannot affect some settings from within Python.
`spark.driver.memory` and `spark.executor.instances` are two examples. Those have to be set
before the Spark JVM is started. Those can only be modified through `pyspark` or `spark-submit`.

```
    from pyspark.sql import SparkSession
    from pyspark import SparkConf, SparkContext
    conf = SparkConf().setAppName("my_app") \
        .setMaster("spark://<node_name>:7077")
    sc = SparkContext(conf=conf)
    spark = (
            SparkSession.builder.config(conf=conf)
            .getOrCreate()
        )
```

## dsgrid container

The dsgrid team maintains a Docker container built with the Dockerfile in the root of this
repository. It includes the dsgrid software, Spark, as well as secondary tools like Jupyter. This
can be used to run dsgrid software on any computer (local, Eagle, or the cloud). The team converts
the container to Singularity so that it can be used on Eagle. The container images are located at
`/projects/dsgrid/containers/`. Here's how to start a shell with important directories
mounted:

```
$ module load singularity-container
$ singularity shell \
    -B /scratch:/scratch \
    -B /projects:/projects \
    /projects/dsgrid/containers/nvidia_spark_v0.0.3.sif
```

Here's how to run a script in the container:

```
$ module load singularity-container
$ singularity exec \
    -B /scratch:/scratch \
    -B /projects:/projects \
    /projects/dsgrid/containers/nvidia_spark_v0.0.3.sif \
    my-script.sh
```

## Attach to a Spark cluster from a conda environment

There are cases where you may want to run scripts against a Spark cluster from your own conda
environment. For example, you may have new code in a dsgrid branch that is not in the container.

Ensure that you have Java installed in your conda environment. Most people already have Java
installed on their personal computers, so this is typically only a problem on Eagle or the cloud.

```
$ conda install openjdk
```

Verify the installation by checking that this environment variable is set:
```
$ echo $JAVA_HOME
```

## Queries

When developing code to support queries you will need to use a miniature registry. Running against
full datasets in a project will take forever. Here is an example of how to create a simplified version
of the StandardScenarios project. Run this on an Eagle compute node after configuring a cluster.

This uses a configuration file from the dsgrid-test-data repository that selects a few dimension records
from each dataset.

It assumes that you have synced the dsgrid registry to `/scratch/$USER/standard-registry`.

```
$ dsgrid-admin make-filtered-registry /scratch/$USER/standard-registry small-registry ~/repos/dsgrid-test-data/filtered_registries/simple_standard_scenarios.json
```

### Create a query
Create a default query file (JSON) with the dsgrid CLI and then custom it.

Here is an example CLI command that will create a query file with default filter and aggregations.

```
$ dsgrid query project create \
    --offline \
    --registry-path=./dsgrid-test-data/filtered_registries/simple_standard_scenarios \
    --filters expression \
    --default-per-dataset-aggregation \
    --default-result-aggregation \
    my_query_name \
    dsgrid_conus_2022
```

Customize the filter and aggregation values.

### Run a query

Submit the query with this command:

```
$ dsgrid query project run \
    --offline \
    --registry-path=./dsgrid-test-data/filtered_registries/simple_standard_scenarios \
    query.json
```

If you need to customize the Spark configuration then you will want to run the command through `spark-submit`.
This is a bit more complicated because that tool needs to be able to locate the Python script (`dsgrid` in this case)
and detect that it is a Python script.

1. Find the location of your `dsgrid` command.
```
$ which dsgrid
/Users/dthom/miniconda3/envs/dsgrid/bin/dsgrid
```

2. Substitute `dsgrid-cli.py` for the usual `dsgrid`. This allows `spark-submit` to detect that it is Python.
(If you know of something more clever and less irritating, please share it with us.)

```
$ spark-submit --master spark://hostname:7077 \
    --conf spark.sql.shuffle.partitions=500 \
    /Users/dthom/miniconda3/envs/dsgrid/bin/dsgrid-cli.py \
    query project run \
    --offline \
    --registry-path=./dsgrid-test-data/filtered_registries/simple_standard_scenarios \
    query.json
```

### Programmatic queries
It may be easier to develop and run queries in Python. Follow examples in `~/repos/dsgrid/tests/test_queries.py`.


## API server
Set these environment variables with your desired values.
```
$ export DSGRID_LOCAL_REGISTRY=~/.dsgrid-registry
$ export DSGRID_QUERY_OUTPUT_DIR=api_query_output
$ export DSGRID_API_SERVER_STORE_DIR=.
```

Start the API server with this command:
```
$ uvicorn dsgrid.api.app:app
```
When developing the API, add this option in order to automatically reload the server when you save a file.
```
$ uvicorn dsgrid.api.app:app --reload
```

Check the output for the address and port.
The examples below assume that the server is running at http://127.0.0.1:8000.

Send commands in the terminal with `curl`, in a browser (be sure to install an extension to pretty-print the JSON output),
or in an API-specific tool. `Insomnia` has a free version here: https://insomnia.rest/download. There are many other tools.

Here are some `curl` examples that you can run in a terminal. Install `jq` from https://stedolan.github.io/jq/download/
in order to be able to pretty-print and filter the output.

View the projects.
```
$ curl -s http://127.0.0.1:8000/projects | jq .
```

View all dimension IDs.
```
$ curl -s http://127.0.0.1:8000/dimensions | jq '.dimensions | .[].dimension_id'
```

Show all dimension query names for a project.
```
$ curl -s http://127.0.0.1:8000/projects/dsgrid_conus_2022/dimensions/dimension_query_names | jq .
```

### API documentation
FastAPI generates API documentation at these links:
- http://127.0.0.1:8000/docs (with Swagger)
- http://127.0.0.1:8000/redoc (with Redocly)


## Publish Documentation

The documentation is built with [Sphinx](http://sphinx-doc.org/index.html). There are several steps to creating and publishing the documentation:

1. Convert .md input files to .rst
2. Refresh API documentation
3. Build the HTML docs
4. Push to GitHub

### Sphinx Style Guide

1. Follow the heading hierarchy convention defined by
[Sphinx](https://www.sphinx-doc.org/en/master/usage/restructuredtext/basics.html#sections).

2. Line length limit: 99 characters

3. Indentation: 4 spaces unless the text follows a bullet or continues a Sphinx directive.

```
# 4 spaces
::

    some raw text

# 4 spaces
.. code-block:: python

    import dsgrid

# 3 spaces
.. note:: some note
   continued

# 2 spaces
- a bullet description
  continued

```

### Markdown to reStructuredText

Markdown files are registered in `doc/md_files.txt`. Paths in that file should be relative to the docs folder and should exclude the file extension. For every file listed there, the `dev/md_to_rst.py` utility will expect to find a markdown (`.md`) file, and will look for an optional `.postfix` file, which is expected to contain `.rst` code to be appended to the `.rst` file created by converting the input `.md` file. Thus, running `dev/md_to_rst.py` on the `doc/md_files.txt` file will create revised `.rst` files, one for each entry listed in the registry. In summary:

```
cd doc
python ../dev/md_to_rst.py md_files.txt
```

### Refresh API Documentation

- Make sure dsgrid is installed or is in your PYTHONPATH
- Delete the contents of `source/api`.
- Run `sphinx-apidoc -o source/api ../dsgrid` from the `docs` folder.
- 'git push' changes to the documentation source code as needed.
- Make the documentation per below

### Building HTML Docs

From the `docs/` folder, run `make html` for Mac and Linux; `make.bat html` for Windows.

### Pushing to GitHub Pages

**TODO:** Structure our GitHub Pages to preserve documentation for different
versions. Dheepak probably has suggestions.

#### Mac/Linux

```
make github
```

#### Windows

```
make.bat html
```

## Release on pypi

*Not yet available*
