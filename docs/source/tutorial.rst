########
Tutorial
########

****************
Submit a dataset
****************

Step 1: Register dimensions
---------------------------
Before creating dimension csv files, the dataset contributor should review the project dimensions to determine which match their dataset directly and which ones are different and need to be registered.

If new dimensions need to be registered that are different from the project, go ahead and create the dimension csv files (in the project repository) and fill out the dimension.toml file. Once these are ready to go, we can register them.

Test it in dry run mode first:

.. code-block:: bash

    dsgrid registry --dry-run dimensions register {path-to-dimension.toml} -l "{log message}"

If it worked, go ahead and register to AWS:

.. code-block:: bash
    
    dsgrid registry --dry-run dimensions register {path-to-dimension.toml} -l "{log message}"


Step 2: Update dataset.toml with the dimension uuids and versions
-----------------------------------------------------------------
The terminal will list out the registered dimensions. We will need to use these to pull out the version and dimension ID references. You can also see what dimensions are currently in the dsgrid registry by using the  cli command:

.. code-block:: bash

    dsgrid registry dimensions list

Get the dimension ID and version information and update the ``dataset.toml`` for each dimension.

For any dimension that uses the same defintion as project dimension, you can find the dimension ID in the ``project.toml``. Copy this information to the ``dataset.toml``.

Step 3: Register dataset
------------------------
.. code-block:: bash
    
    dsgrid registry datasets register {path-to-dataset.toml} -l "{log message}”


Step 4: Generate dimension mappings (to map dataset to project)
---------------------------------------------------------------
If the dataset has different dimensions compared to the project, you will need to provide a dimension mapping back to the project before you can submit the dataset to the project.

Create dimension mapping csvs + create the ``dimension_mappings.toml`` file

Register via cli

.. code-block:: bash

    dsgrid registry dimension-mappings register {path-to-dimension_mappings.toml} -l "{log message}”


Step 5: Submit dataset to the project
-------------------------------------
Currently this is not ready yet, but it will be step 5 in this sequence.

Create a ``dimension_mapping_references.toml`` file

.. code-block:: bash

    dsgrid registry projects submit-dataset -d {dataset_id} -p {project_id} -m {path-to-dimension_mapping_references.toml} -l "{log message}"