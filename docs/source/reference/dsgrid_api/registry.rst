********
Registry
********

Registry Managers
=================

.. autoclass:: dsgrid.registry.registry_manager.RegistryManager
   :members: load, dataset_manager, dimension_mapping_manager, dimension_manager, project_manager

.. autoclass:: dsgrid.registry.project_registry_manager.ProjectRegistryManager
   :members: load_project, get_by_id, show

.. autoclass:: dsgrid.registry.dataset_registry_manager.DatasetRegistryManager
   :members: get_by_id, show

.. autoclass:: dsgrid.registry.dimension_registry_manager.DimensionRegistryManager
   :members: get_by_id, show

.. autoclass:: dsgrid.registry.dimension_mapping_registry_manager.DimensionMappingRegistryManager
   :members: get_by_id, show

.. autopydantic_model:: dsgrid.registry.registry_database.DatabaseConnection

Project
=======

.. autoclass:: dsgrid.project.Project
   :members: get_dataset, version

.. autoclass:: dsgrid.config.project_config.ProjectConfig
   :members: get_base_dimension, get_base_dimension_query_names, get_base_dimension_to_query_name_mapping, get_base_to_supplemental_config, get_base_to_supplemental_dimension_mappings_by_types, get_base_to_supplemental_mapping_records, get_dimension, get_dimension_records, get_required_dimension_record_ids, get_supplemental_dimension_to_query_name_mapping, list_dimension_query_names, list_registered_dataset_ids, list_supplemental_dimensions, list_unregistered_dataset_ids

.. autopydantic_model:: dsgrid.config.project_config.ProjectDimensionQueryNamesModel

.. autopydantic_model:: dsgrid.config.project_config.DimensionsByCategoryModel

Dimension
=========

.. autoclass:: dsgrid.config.dimension_config.DimensionConfig
   :members: get_records_dataframe, get_unique_ids


Examples
========
.. code-block:: python

    from dsgrid.dimension.base_models import DimensionType
    from dsgrid.registry.registry_manager import RegistryManager
    from dsgrid.registry.registry_database import DatabaseConnection

    manager = RegistryManager.load(
        DatabaseConnection(
            hostname="dsgrid-registry.hpc.nrel.gov",
            database="standard-scenarios",
        )
    )
    project = manager.project_manager.load_project("dsgrid_conus_2022")
    geo_dim = project.config.get_base_dimension(DimensionType.GEOGRAPHY)
    geo_dim.get_records_dataframe().show()
    print(geo_dim.get_unique_ids())
    # Show the records for a supplemental dimension.
    project.config.get_dimension_records("commercial_end_uses").show()
