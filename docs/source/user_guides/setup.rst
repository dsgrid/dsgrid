Setup
======

Install dsgrid
Github
AWS configs
Repo organization


Project Repo Organization
-----------------

.. todo:: 
    Provide a means to generate a new empty repo dir

We recommend that dsgrid project repositories use the following directory organization structure:

    .. code-block::

        .
        ├── dsgrid_project
            ├── datasets
            │   ├── benchmark
            │   ├── historical
            │   └── sector_models
            │       ├── comstock
            │       │   ├── dimension_mappings
            │       │   ├── dimensions
            │       │   ├── dataset.toml
            │       │   ├── dimension_mapping_references.toml
            │       │   ├── dimension_mappings.toml
            │       │   ├── dimensions.toml
            │       └── ...   
            ├── dimension_mappings        
            ├── dimensions
            ├── dimensions.toml
            ├── dimension_mappings.toml
            └── project.toml



In the directory structure above, all project files are stored in the ``dsgrid_project`` root directory. And within this root directory we have:

    * ``datasets``: This is where we define input datasets, oranized first by type (i.e., ``datasets/historical``, ``datasets/benchmark``, ``datasets/sector_models``) and then by source (e.g., ``datasets/sector_models/comstock``) for the dsgrid project
    * ``dimension_mappings``: This where we store :under:`project-level` dimension mapping records (csv or json)
    * ``dimensons``: This is where we store :under:`project-level` dimension records (csv or json)
    * ``dimensons.toml``: :under:`Project-level` dimension configs for new dimensions that need to be registered
    * ``dimension_mappings.toml``: :under:`project-level` dimension mappings configs for new dimension mappings that need to be registered
    * ``project.toml``: This is the main project configuration file. See ::ref

    Within each `datasets/{type}/{source}` sub-folder are the following files:

        * ``dimension_mappings``:  This where we store :under:`dataset-level` dimension mapping records (csv or json)
        * ``dimensions``: This is where we store :under:`dataset-level` dimension records (csv or json)
        * ``dataset.toml``: This is the dataset configuration file
        * ``dimension_mapping_references.toml``: References to registered dataset-to-project dimension mappings that map the dataset to the project; to be used when submitting a dataset to a project
        * ``dimension_mappings.toml``: :under:`Dataset-level`  dimension mappings configs for new dimension mappings that need to be registered
        * ``dimensions.toml``: :under:`dataset-level` dimension configs for new dimensions that need to be registered