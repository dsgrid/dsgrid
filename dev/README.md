# dsgrid Developer README

[Dependencies](#developer-dependencies) | [Tests](#tests) | [Spark](#spark) | [EFS Project Repository](#testingexploring-with-the-efs-project-repository) | [Interactive Exploration](#interactive-exploration)

## Developer Dependencies

The dsgrid codebase can be executed with either Apache Spark or DuckDB as the backend.
The backend can be selected in the dsgrid config file (`dsgrid config create --help`).

If you have a Windows computer, you may want to run only with DuckDB. Getting Spark correctly
configured can be challenging.

**pip extras**

```
pip install -e .[tests]

# or

pip install -e .[dev,spark] --group=pyhive # includes what is needed for tests and code development
(Leave off `spark` and `pyhive` if you don't need/want to use spark.)

# or

pip install -e .[dev,doc] # dev plus what is needed for creating documentation and releasing packages
```

**Java**
Spark requires Java. Most people already have Java installed on their personal computers, so this
is typically only a problem on an HPC or the cloud. Check if you have it. Both of these commands
must work.
```
$ java --version
openjdk 11.0.12 2021-07-20
$ echo $JAVA_HOME
/Users/dthom/brew/Cellar/openjdk@11/11.0.12
```
If you don't have java installed:
```
$ conda install openjdk
```

Alternatively, if you use a Mac, you can install it with Homebrew.

```
$ brew install openjdk
```

**Setting up pre-commit hooks**

```
pre-commit install
```

**Additional software required for publishing documentation:**

- [Pandoc](https://pandoc.org/installing.html)

## Spark
You need to have some familiarity with Spark in order to run non-trivial tasks in dsgrid.
This [page](https://dsgrid.github.io/dsgrid/spark_overview.html) provides an overview and explains
various ways to use Spark in dsgrid.

If you test dsgrid with Spark, you'll need to download Apache Thrift Server, which is included
with the Spark installation. Chronify uses Spark through a Thrift Server to run SQL queries.

You can extract this package anywhere on your filesystem. Start the server before running tests
and shut it down afterwards.
```
$ wget https://dlcdn.apache.org/spark/spark-3.5.4/spark-3.5.4-bin-hadoop3.tgz
$ tar -xzf spark-3.5.4-bin-hadoop3.tgz
```

```
$ <your-base-path>/spark-3.5.4-bin-hadoop3/sbin/start-thrift-server.sh
$ <your-base-path>/spark-3.5.4-bin-hadoop3/sbin/stop-thrift-server.sh
```

Refer to .github/workflows/pull_request_tests.yml to see exactly how CI is run with both DuckDB
and Spark.

## Tests

### Setup
The tests will create their own registries and clean up after themselves.

The tests use the [test data repository](https://github.com/dsgrid/dsgrid-test-data.git)
as a git submodule in `./dsgrid-test-data`. It is a minimal version of the EFS project and
datasets. You must initialize this submodule and keep it updated.

Initialize the submodule:
```
$ git submodule init
$ git submodule update
```

Update the submodule when there are new changes in the test data repository:
```
$ git submodule update --remote --merge
```

### Updating the simple-standard-scenarios registry
If you update the configs or data for the StandardScenarios registry then you'll need to update
the test data repository per these instructions.

1. Register and submit all datasets to a clean registry. The initial registry can be created with
this command after modifiying the paths specified in the JSON5 file. Three compute nodes
on Kestrel are recommended in order to complete the job in one hour. Two may be sufficient if you
run multiple iterations.

Run this from your scratch directory.

```
$ dsgrid-admin create-registry --data-path standard-scenarios-registry-data sqlite:///./standard-scenarios.db
```
```
$ spark-submit \
    --master=spark://$(hostname):7077 \
    --conf spark.sql.shuffle.partitions=2400 \
    $(which dsgrid-cli.py) --url sqlite:///./standard-scenarios.db registry bulk-register \
        --data-path=standard-scenarios-registry-data \
        tests/data/standard_scenarios_registration.json5
```

2. Acquire a new compute node. It can be any type of node and you only need it for an hour.
Create a local version of the dsgrid repository script `scripts/create_simple_standard_scenarios.sh`.
Edit the environment variables at the top of the script as necessary and then run it. It will
make a new registry database, filter the StandardScenarios data, unpivot the ComStock datasets
(for test coverage), and create a backup of the database.

3. Copy (e.g., cp, scp, rsync) the output files (registry .db and Parquet) from the
previous step to a dsgrid-test-data repository in the directory
`dsgrid-test-data/filtered_registries/simple_standard_scenarios`. Edit the SQLite database table
`key_value` such that the value associated with the key `data_path` is set to
`dsgrid-test-data/filtered_registries/simple_standard_scenarios`.

```
$ sqlite3 /path/to/registry.db
sqlite> UPDATE key_value
SET value='dsgrid-test-data/filtered_registries/simple_standard_scenarios'
WHERE key = 'data_path';
```

4. Run this dsgrid script from the base directory of the test data repository. It shortens the
names of the Parquet directories and files created by Spark so that they will not exceed the
260 character limit on Windows filesystems. Adjust your path as needed.

```
$ python <your-repo-path>/dsgrid/scripts/shorten_simple_std_scns_filenames.py
```

5. Make a branch, commit, push it to GitHub, and open a pull request.

6. From a dsgrid repository where the `dsgrid-test-data` submodule contains the branch created
in the previous step, run `python tests/simple_standard_scenarios_datasets.py` and add
the modified file
`dsgrid-test-data/filtered_registries/simple_standard_scenarios/expected_datasets/raw_stats.json`
to the same pull request.

### Run tests

To run all tests, including AWS tests:
```
pytest
```

If you want to exclude AWS tests:
```
pytest tests
```

**TODO** AWS tests have not been updated to support the registry database.
If you only want to run AWS tests:
```
pytest tests_aws
```

If you're running tests on an HPC set up a custom configuration you will want to:

- `export SPARK_CONF_DIR=$(pwd)/conf`
- `cd ~/dsgrid`
- `module load conda`
- activate your conda environment
- `pytest tests`

If you did not set up a Spark cluster and are instead running Spark in local mode you can skip the above command to `export SPARK_CONF_DIR=$(pwd)/conf` and will want to instead `export SPARK_LOCAL_DIRS=/tmp/scratch/<slurm-job-id>`.

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

Download the EFS datasets from AWS or HPC (`/projects/dsgrid/efs_datasets/converted_output/commercial`)
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
$ dsgrid-admin create-registry --data-path=tests/data/registry cached-test-dsgrid
```
```
$ dsgrid registry bulk-register tests/data/test_efs_registration.json
```

Now you can run any `dsgrid registry` command.

## Register projects and datasets under development
When developing projects and datasets you will likely have to run the registration commands many
times. dsgrid provides a helper script for this purpose. Refer to
`dsgrid registry bulk-register --help` and
the data models in `dsgrid/config/registration_models.py`. They automate the registration process
so that you don't have to write your own scripts. Example config files are
`tests/data/test_efs_registration.json` and `tests/data/standard_scenarios_registration.json`.

## Interactive Exploration

In addition to the CLI tools you can use `scripts/registry.py` to explore a registry interactively.

One signficant advantage of this method is that you don't have to create a new Spark application on
every command. That saves several seconds for each iteration.

Be sure to use the `debug` function from the `devtools` package when exploring Pydantic models.
```
ipython -i scripts/registry.py -- --path=$DSGRID_REGISTRY_PATH
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

from dsgrid.registry.registry_database import DatabaseConnection
from dsgrid.registry.registry_manager import RegistryManager

conn = DatabaseConnection(url="sqlite:///<path-to-registry.db>")
mgr = RegistryManager.load(conn, offline_mode=True)
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
$ export SPARK_CLUSTER=spark://$(hostname):7077
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
        "dataset_path": "/dsgrid-data/data-StandardScenarios/comstock_conus_2022_reference",
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

## Registry Database
The dsgrid registry is stored in a SQLite database. Please refer to this
[page](registry_database.md) for more information.

## Queries

When developing code to support queries you will need to use a miniature registry. Running against
full datasets in a project will take forever. Here is an example of how to create a simplified version
of the StandardScenarios project. Run this on an HPC compute node after configuring a cluster.

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

```
$ spark-submit --master spark://hostname:7077 \
    --conf spark.sql.shuffle.partitions=500 \
    /Users/dthom/miniconda3/envs/dsgrid/bin/dsgrid-cli.py \
    query project run \
    --registry-path=./dsgrid-test-data/filtered_registries/simple_standard_scenarios \
    query.json
```

### Programmatic queries
It may be easier to develop and run queries in Python. Follow examples in `~/repos/dsgrid/tests/test_queries.py`.


## API server
Set these environment variables with your desired values.
```
$ export DSGRID_REGISTRY_DATABASE_URL=sqlite:///dsgrid-test-data/filtered_registries/simple_standard_scenarios/registry.db
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

## Project Viewer
dsgrid provides a Dash application that allows you to browse the registry. Once you have started
the API server as described above, run
```
$ python dsgrid/apps/project_viewer/app.py
```

## Release on pypi

*Not yet available*
