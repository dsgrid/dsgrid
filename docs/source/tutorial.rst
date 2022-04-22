########
Tutorial
########

****************
Submit a dataset
****************

Step 1: Create dimensions
-------------------------
Before creating dimension csv files, the dataset contributor should review the
project dimensions to determine which match their dataset directly and which ones
are different and need to be registered.

If new dimensions need to be registered that are different from the project, go
ahead and create the dimension csv files (in a project repository) and fill out
the dimensions in the dataset.toml file.

For any dimension that uses the same defintion as project dimension, you can find the dimension ID in the project.toml.
Here is how to find the project's dimensions.

.. TODO: we should add a CLI command to list a project's dimensions.

.. code-block:: bash

    mkdir project_output_dir
    dsgrid registry projects dump <project_id> -d project_output_dir
    cat project_output_dir/project.toml

Alternatively, you can browse all dimensions in the registry with this command:

.. code-block:: bash

    dsgrid registry dimensions list

Once you've identified the dimension IDs and versions, copy them to the ``dataset.toml``.


Step 2: Generate dimension mappings (to map dataset to project)
---------------------------------------------------------------
If the dataset has different dimensions compared to the project, you will need to provide a dimension mapping back to the project before you can submit the dataset to the project.

Create dimension mapping csvs + create the ``dimension_mappings.toml`` file.

.. TODO: step 2 needs improvement

Step 3: Register and submit dataset to the project
--------------------------------------------------
Test it in offline mode first:

.. code-block:: bash

    dsgrid registry --offline projects register-and-submit-dataset \
        --project-id {project_id} \
        --dimension-mapping-file {path-to-dimension_mappings.toml}
        --log-message {log_message} \
        {dataset.toml} \
        {directory-containing-load-data}

If it worked, go ahead and register to AWS by repeating the previous command wihtout the ``--offline`` option.
