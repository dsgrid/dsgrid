************
Registry CLI
************

The dsgrid registry CLI provides an interface into the dsgrid registry. Use the
dsgrid registry CLI to register new or access existing datasets, dimensions and
dimension mapping records, and configs (including dimension, dimension mapping,
dataset, and project configs).

.. command-output:: dsgrid registry --help

To submit a dataset to dsgrid, you may need to register new dimensions first.
You may also need to list the dimensions registry to pull dimension_ids
required by your dataset config file.

.. command-output:: dsgrid registry dimensions --help

Dimension mappings allow dataset contributors to map their preferred dimension
definitions to those expected by the project. Datasets planned for submission
to specific dsgrid projects will likely need to provide one or more such mappings.

.. command-output:: dsgrid registry dimension-mappings --help

Once dimensions and dimension-mappings are registered, a dataset contributor is
ready to try registering their dataset once a dataset config file has been
prepared, and the dataset is on disk in a supported dsgrid .parquet format.

.. command-output:: dsgrid registry datasets --help

Finally, registered datasets may be submitted to projects. Projects generally
need to be expecting a dataset, and all of the expected data and dimension-mappings
must be in place, for the submission process to be successful.

.. command-output:: dsgrid registry projects --help
