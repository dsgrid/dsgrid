**************************
How to Browse the Registry
**************************

CLI
===

List Registry Components
-------------------------
You can list components by type: projects, datasets, dimensions, dimension mappings.

.. code-block:: console

    $ dsgrid registry projects list
    $ dsgrid registry datasets list
    $ dsgrid registry dimensions list
    $ dsgrid registry dimension-mappings list

You can filter the output of each table like this:

.. code-block:: console

    $ dsgrid registry dimensions list -f Type==geography

You can also list all components at once:

.. code-block:: console

    $ dsgrid registry list


.. _project-viewer:

Project Viewer
==============
dsgrid provides a Dash application that allows you to browse the registry in a web UI.

1. Set these environment variables in preparation for starting the dsgrid API server.

.. code-block:: console

    $ export DSGRID_REGISTRY_DATABASE_URL=http://dsgrid-registry.hpc.nrel.gov:8529
    $ export DSGRID_REGISTRY_DATABASE_NAME=standard-scenarios
    $ export DSGRID_QUERY_OUTPUT_DIR=api_query_output
    $ export DSGRID_API_SERVER_STORE_DIR=.

2. Start the server

.. code-block:: console

    $ uvicorn dsgrid.api.app:app

Check the output for the address and port.
The examples below assume that the server is running at http://127.0.0.1:8000.

3. Start the project viewer app.

.. code-block:: console

    $ python dsgrid/apps/project_viewer/app.py
