.. _dimension-mapping-config:

************************
Dimension Mapping Config
************************

Registered Dimension Mappings
=============================
.. autopydantic_model:: dsgrid.config.mapping_tables.MappingTableModel
.. autopydantic_model:: dsgrid.config.mapping_tables.MappingTableRecordModel
.. autopydantic_model:: dsgrid.config.dimension_mapping_base.DimensionMappingBaseModel

.. _dimension-mapping-reference:

Dimension Mapping Reference
---------------------------
.. autopydantic_model:: dsgrid.config.dimension_mapping_base.DimensionMappingReferenceModel

.. autopydantic_model:: dsgrid.config.dimension_mapping_base.DimensionMappingReferenceListModel
.. autopydantic_model:: dsgrid.config.dimension_mappings_config.DimensionMappingsConfigModel

.. TODO: separate by base-to-supplemental and dataset-to-project
.. autopydantic_model:: dsgrid.config.mapping_tables.DatasetBaseToProjectMappingTableModel
.. autopydantic_model:: dsgrid.config.mapping_tables.DatasetBaseToProjectMappingTableListModel
.. autopydantic_model:: dsgrid.config.dimension_mapping_base.DimensionMappingDatasetToProjectBaseModel

.. _unregistered-dimension-mappings:

Unregistered Dimension Mappings
===============================

.. autopydantic_model:: dsgrid.config.mapping_tables.MappingTableByNameModel
.. autopydantic_model:: dsgrid.config.dimension_mapping_base.DimensionMappingPreRegisteredBaseModel
.. autopydantic_model:: dsgrid.config.dimensions.DimensionReferenceModel
