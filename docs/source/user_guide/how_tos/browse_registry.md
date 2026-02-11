# How to Browse the Registry

## CLI

### List Registry Components

Assuming you have already [configured dsgrid](../../getting_started/installation.md#save-your-configuration) to point to the right database (URL and name), you can list components by type: projects, datasets, dimensions, dimension mappings:

```bash
dsgrid registry projects list
dsgrid registry datasets list
dsgrid registry dimensions list
dsgrid registry dimension-mappings list
```

You can filter the output of each table like this:

```bash
dsgrid registry dimensions list -f Type==geography
```

You can also list all components at once:

```bash
dsgrid registry list
```

You can also browse different registries by specifying the database URL and name directly on the command line:

```bash
dsgrid -u http://dsgrid-registry.hpc.nrel.gov:8529 -N standard-scenarios registry list
```

## Project Viewer

dsgrid provides a Dash application that allows you to browse the registry in a web UI.

### Step 1: Set Environment Variables

Set these environment variables in preparation for starting the dsgrid API server:

```bash
export DSGRID_REGISTRY_DATABASE_URL=sqlite:///<your-db-path>
export DSGRID_QUERY_OUTPUT_DIR=api_query_output
export DSGRID_API_SERVER_STORE_DIR=.
```

### Step 2: Start the Server

Start the dsgrid API server:

```bash
uvicorn dsgrid.api.app:app
```

Check the output for the address and port. The examples below assume that the server is running at http://127.0.0.1:8000.

### Step 3: Start the Project Viewer App

Launch the project viewer application:

```bash
python dsgrid/apps/project_viewer/app.py
```

## Next Steps

- Learn about [dataset registration](../dataset_registration/index)
- Explore [querying projects](../project_queries/index)
- Understand the [CLI reference](../../reference/cli)
