# Local Development Tools

This page describes tools and workflows that are useful when developing dsgrid, its projects, and
its datasets.

## Register Projects and Datasets Repeatedly

When developing projects and datasets, you will likely have to run the registration commands many
times. Instead of writing your own scripts, use `dsgrid registry bulk-register` with a
registration file that lists the projects and datasets to register and submit. Refer to
`dsgrid registry bulk-register --help` and the data models in
`dsgrid/config/registration_models.py`. Example files are `tests/data/test_efs_registration.json5`
and `tests/data/standard_scenarios_registration.json5`.

If a bulk registration fails partway through, it writes a journal file. Pass that file with
`--journal-file` to skip the projects and datasets that were already registered.

## Explore a Registry in a Jupyter Notebook

A Jupyter notebook is convenient when you need to find dimension and dimension mapping IDs. The
registry `show` methods detect when they are running in a notebook and display HTML tables, from
which wrapped text can be copied with the mouse.

```python
from dsgrid.registry.common import DatabaseConnection
from dsgrid.registry.registry_manager import RegistryManager

conn = DatabaseConnection(url="sqlite:///<path-to-registry.db>")
mgr = RegistryManager.load(conn, offline_mode=True)
mgr.dimension_manager.show()
```

Use the `debug` function from the `devtools` package (included in the `dev` extra) to
pretty-print Pydantic models:

```python
from devtools import debug

dataset = mgr.dataset_manager.get_by_id("comstock_conus_2022_reference")
debug(dataset.model)
```

## Run dsgrid Through spark-submit

If you need to customize the Spark configuration, run dsgrid commands through `spark-submit`.
`spark-submit` needs to be able to locate the Python script and detect that it is Python, so use
the `dsgrid-cli.py` entry point (installed alongside `dsgrid`) instead of `dsgrid`:

```bash
spark-submit --master spark://<hostname>:7077 \
    --conf spark.sql.shuffle.partitions=500 \
    $(which dsgrid-cli.py) \
    query project run \
    query.json5
```

Refer to [Apache Spark](../../user_guide/apache_spark/overview.md) for more information.

## API Server

dsgrid includes a [FastAPI](https://fastapi.tiangolo.com) server. Set these environment variables
with your desired values:

```bash
export DSGRID_REGISTRY_DATABASE_URL=sqlite:///dsgrid-test-data/filtered_registries/simple_standard_scenarios/registry.db
export DSGRID_QUERY_OUTPUT_DIR=api_query_output
export DSGRID_API_SERVER_STORE_DIR=.
```

Start the API server:

```bash
uvicorn dsgrid.api.app:app
```

When developing the API, add the `--reload` option to automatically reload the server when you
save a file:

```bash
uvicorn dsgrid.api.app:app --reload
```

Check the output for the address and port. The examples below assume that the server is running
at `http://127.0.0.1:8000`.

You can send requests with `curl` in a terminal, in a browser (with an extension to pretty-print
JSON), or in an API tool such as [Insomnia](https://insomnia.rest/download). Install
[jq](https://jqlang.github.io/jq/download/) to pretty-print and filter the output of `curl`.

View the projects:

```bash
curl -s http://127.0.0.1:8000/projects | jq .
```

View all dimension IDs:

```bash
curl -s http://127.0.0.1:8000/dimensions | jq '.dimensions | .[].dimension_id'
```

Show all dimension names for a project:

```bash
curl -s http://127.0.0.1:8000/projects/dsgrid_conus_2022/dimensions/dimension_names | jq .
```

### API Documentation

FastAPI generates API documentation at these links:

- `http://127.0.0.1:8000/docs` (Swagger UI)
- `http://127.0.0.1:8000/redoc` (ReDoc)

## Project Viewer

dsgrid provides a [Dash](https://dash.plotly.com) application for browsing the registry through
the API server. Start the API server as described above first, then run:

```bash
python dsgrid/apps/project_viewer/app.py
```

Open the address printed in the output (by default `http://127.0.0.1:8050/`) in a browser. The
viewer connects to the API server at `http://127.0.0.1:8000` by default. To use a different API
server, edit the **URL** field just below the "dsgrid Project Viewer" banner in the viewer.
