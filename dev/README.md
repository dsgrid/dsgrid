# dsgrid Developer README

[Dependencies](#developer-dependencies) | [Tests](#tests) | [Spark](#spark) | [EFS Project Repository](#testingexploring-with-the-efs-project-repository) | [Interactive Exploration](#interactive-exploration) | [Publish Documentation](#publish-documentation)

## Developer Dependencies

**pip extras**

```
pip install -e .[tests]

# or

pip install -e .[dev] # includes what is needed for tests and code development

# or

pip install -e .[admin] # dev plus what is needed for creating documentation and releasing packages
```

**Java**
Spark requires Java. Most people already have Java installed on their personal computers, so this
is typically only a problem on Eagle or the cloud. Check if you have it. Both of these commands
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

**Setting up pre-commit hooks**

```
pre-commit install
```

**Additional software required for publishing documentation:**

- [Pandoc](https://pandoc.org/installing.html)

## Spark
You need to have some familiarity with Spark in order to run non-trivial tasks in dsgrid.
This [page](spark_overview.md) provides an overview and explains various ways to use Spark in
dsgrid.

## ArangoDB
dsgrid stores registry information in an ArangoDB database. You must install it locally in
order to work with a local instance of dsgrid. There are two ways to install ArangoDB
on your computer.

Once installed, the easiest way to mange the database manually is through Arango's web UI,
available at http://localhost:8529

### Native installation
1. Install `ArangoDB Community Edition` locally by following instructions at
https://www.arangodb.com/download-major/. If asked and you plan to run dsgrid tests, set the root password to openSesame to match the defaults.

Add the `bin` directory to your system path. On a Mac it will be in a location like this:

    $HOME/Applications/ArangoDB3-CLI.app/Contents/Resources/opt/arangodb/bin

though you may have chosen to install to `/Applications`. On Windows, it will be in a location like this:

    C:\Users\$USER\AppData\Local\ArangoDB3 3.10.5\usr\bin

and the executable installer will have already added it to your path (User variables, Path).

Note the configuration files in this directory on Mac:

    $HOME/Applications/ArangoDB3-CLI.app/Contents/Resources/opt/arangodb/etc/arangodb3

and this directory on Windows:

    C:\Users\$USER\AppData\Local\ArangoDB3 3.10.5\etc\arangodb3

Customize as desired, particularly regarding authentication.

2. Start the database by running `arangodb` on Mac and `arangod` on Windows. Also on Windows, it is preferable to run `arangod` from the path like `C:\Users\$USER\AppData\Local\ArangoDB3 3.10.5`. Alternatively, you can directly use the ArangoDB Server shortcut on your Windows desktop.

On Mac if it gives the error
`cannot find configuration file` then make this directory and copy the configuration files.

```
$ mkdir ~/.arangodb
$ cp ~/Applications/ArangoDB3-CLI.app/Contents/Resources/opt/arangodb/etc/arangodb3/*conf ~/.arangodb
```

If you don't copy the files, you can specify the config file with `arangodb --conf <your-path>/arangod.conf`

### Docker container

#### Local
Run the ArangoDB Docker container by following instructions at
https://www.arangodb.com/download-major/docker/. For example:

```
docker run --name=arango-container -p 8529:8529 -e ARANGO_ROOT_PASSWORD=openSesame arangodb/arangodb:3.10.4
```

Once the docker container is running, Arango commands will need to be preceded with `docker exec`. For example, using the container name from above:

```
docker exec arango-container arangorestore ...
```

#### Eagle
The dsgrid repository includes `scripts/start_arangodb_on_eagle.sh`. It will start an ArangoDB
on a compute node using the debug partition. It stores Arango files in `/scratch/${USER}/arangodb3`
and `/scratch/${USER}/arangodb3-apps`. If you would like to use a completely new database,
delete those directories before running the script.

Note that Slurm will log stdout/stderr from `arangod` into `./arango_<job-id>.o/e`.

The repository also includes `scripts/start_spark_and_arango_on_eagle.sh`. It starts Spark as well
as ArangoDB, but you must have cloned `https://github.com/NREL/HPC.git`. It looks for the repo at
`~/repos/HPC`, but you can set a custom value on the command line, such as the example below.

You may want to adjust the number and type of nodes in the script based on your Spark requirements.
```
$ sbatch scripts/start_spark_and_arango_on_eagle.sh ~/HPC
```

Note that Slurm will log stdout/stderr from into `./dsgrid_infra<job-id>.o/e`. Look at the .o
file to see the URL for the Spark cluster and the Spark configuration directory.

It is advised to gracefully shut down the database if you want to ensure that all updates have
been persisted to files. To do that:

1. ssh to the compute node running the database.
2. Identify the process ID of `arangod`. In this example the PID is `195412`.
```
$ ps -ef | grep arango
dthom    195412 195392  0 09:31 ?        00:00:06 arangod --server.authentication=true --config /tmp/arangod.conf
```
3. Send `SIGTERM` to the process.
```
$ kill -s TERM 195412
```
4. `arangod` will detect the signal and gracefully shutdown.

Modify the HPC parameters as needed. Or run the commands manually. *Note that you should never run
ArangoDB on a login node.*

If you need to start a Spark cluster, you can do that on the same compute node running the database.


## Tests

### Setup
You must be running a local instance of ArangoDB in order to run the tests. It can be a native
installation or use Docker. The only requirement is that it be available at http://localhost:8529.

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

### Import/restore simple-standard-scenarios registry
Some tests require a filtered StandardScenarios registry. The test data repository
contains a JSON-exported database that you must import into your local ArangoDB instance.

You can use a native ArangoDB installation or the docker container. If using Docker you must have bind-mounted the `dsgrid-test-data` directory, as in `docker run -v $(pwd)/dsgrid-test-data:/dsgrid-test-data`.

```
$ arangorestore \
    --create-database \
    --input-directory \
    dsgrid-test-data/filtered_registries/simple_standard_scenarios/dump \
    --server.database simple-standard-scenarios \
    --include-system-collections true
```

```
$ docker exec arango-container arangorestore \
    --create-database \
    --input-directory \
    /dsgrid-test-data/filtered_registries/simple_standard_scenarios/dump \
    --server.database simple-standard-scenarios \
    --include-system-collections true
```

If you are running on Eagle, run this script from the dsgrid repository. Note that you can run this
command on a login node. `DB_HOSTNAME` is the node name of the compute node running Arango.
```
$ bash scripts/restore_simple_standard_scenarios.sh <path-to-your-local-dsgrid-test-data> <DB_HOSTNAME>
```

You will have to repeat this process anytime the test data is updated.
**TODO**: Automate this process.

### Updating the simple-standard-scenarios registry
If you update the configs or data for the StandardScenarios registry then you'll need to update
the test data repository per these instructions.

1. Register and submit all datasets to a clean registry. The initial registry can be created with
this command after modifiying the paths specified in the JSON5 file. Four `bigmem` compute nodes
on Eagle are recommended in order to complete the job in one hour. Two may be sufficient if you
run multiple iterations.

Run this from your scratch directory.

```
$ spark-submit \
    --master=spark://$(hostname):7077 \
    --conf spark.sql.shuffle.partitions=2400 \
    dsgrid/tests/register.py tests/data/standard_scenarios_registration.json
```

2. Acquire a new compute node. It can be any time of node and you only need it for an hour.
Create a local version of the dsgrid repository script `scripts/create_simple_standard_scenarios.sh`.
Edit the environment variables at the top of the script as necessary and then run it. It will
filter the StandardScenarios data and then create, register, and submit-to-project these derived
datasets:

- `comstock_conus_2022_projected`
- `resstock_conus_2022_projected`
- `tempo_conus_2022_mapped`

3. Copy (e.g., cp, scp, rsync) or arangodump the output files (registry JSON and Parquet) from the
previous step to a dsgrid-test-data repository in the directory
`dsgrid-test-data/filtered_registries/simple_standard_scenarios`. Edit the file
`dump/data_path_*.data.json` to ensure that the `data_path` value is set to
`dsgrid-test-data/filtered_registries/simple_standard_scenarios`. Make a branch, commit, push
it to GitHub, and open a pull request.

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

If you're running tests on Eagle and used the `scripts/start_spark_and_arango_on_eagle.sh` or set up a custom configuration you will want to:

- ssh to the node where Arango is running
- change to the directory from which you ran the `start_spark_and_arango_on_eagle.sh` script or otherwise identify the full path to the Spark config directory
- `export SPARK_CONF_DIR=$(pwd)/conf` (You can also get the correct command from `grep SPARK dsgrid_infra*.o`.)
- `cd ~/dsgrid`
- `module load conda`
- activate your conda environment
- `pytest tests`

If you did not set up a Spark cluster and are instead running Spark in local mode you can skip the above command to `export SPARK_CONF_DIR=$(pwd)/conf` and will want to instead `export SPARK_LOCAL_DIRS=/tmp/scratch`.

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
python dsgrid/tests/register.py tests/data/test_efs_registration.json
```

Now you can run any `dsgrid registry` command.

## Register projects and datasets under development
When developing projects and datasets you will likely have to run the registration commands many
times. dsgrid provides a helper script for this purpose. Refer to `dsgrid/tests/register.py` and
the data models in `dsgrid/tests/registration_models.py`. They automate the registration process
so that you don't have to write your own scripts. Example config files are
`tests/data/test_efs_registration.json` and `tests/data/standard_scenarios_registration.json`.

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

from dsgrid.registry.registry_database import DatabaseConnection
from dsgrid.registry.registry_manager import RegistryManager

conn = DatabaseConnection()
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
