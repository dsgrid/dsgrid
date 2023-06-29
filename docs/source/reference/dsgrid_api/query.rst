*****
Query
*****

Data Models
===========

.. autopydantic_model:: dsgrid.query.models.ProjectQueryModel
.. autopydantic_model:: dsgrid.query.models.ProjectQueryParamsModel
.. autopydantic_model:: dsgrid.query.models.QueryResultParamsModel
.. autopydantic_model:: dsgrid.query.models.DatasetModel
.. autopydantic_model:: dsgrid.query.models.StandaloneDatasetModel
.. autopydantic_model:: dsgrid.query.models.ExponentialGrowthDatasetModel
.. autopydantic_model:: dsgrid.query.models.AggregationModel
.. autopydantic_model:: dsgrid.query.models.ColumnModel
.. autopydantic_model:: dsgrid.query.models.DimensionQueryNamesModel
.. autopydantic_model:: dsgrid.query.models.FilteredDatasetModel
.. autopydantic_model:: dsgrid.query.models.ReportInputModel
.. autopydantic_model:: dsgrid.query.models.SparkConfByDataset

.. autoclass:: dsgrid.query.models.ColumnType
.. autoclass:: dsgrid.query.models.DatasetType
.. autoclass:: dsgrid.query.models.ReportType
.. autoclass:: dsgrid.query.models.TableFormatType

Submission
==========

.. autoclass:: dsgrid.query.query_submitter.ProjectQuerySubmitter
   :members:


Examples
========

.. code-block:: python

    from dsgrid.dimension.base_models import DimensionType
    from dsgrid.registry.registry_manager import RegistryManager
    from dsgrid.registry.registry_database import DatabaseConnection
    from dsgrid.query.models import (
        AggregationModel,
        DatasetModel,
        DimensionQueryNamesModel,
        ProjectQueryParamsModel,
        ProjectQueryModel,
        QueryResultParamsModel,
        StandaloneDatasetModel,
    )
    from dsgrid.query.query_submitter import ProjectQuerySubmitter

    manager = RegistryManager.load(
        DatabaseConnection(
            hostname="dsgrid-registry.hpc.nrel.gov",
            database="standard-scenarios",
        )
    )
    project = manager.project_manager.load_project("dsgrid_conus_2022")
    query = ProjectQueryModel(
        name="Total Electricity Use By State and Sector",
        project=ProjectQueryParamsModel(
            project_id="dsgrid_conus_2022",
            dataset=DatasetModel(
                dataset_id="electricity_use",
                source_datasets=[
                    StandaloneDatasetModel(dataset_id="comstock_conus_2022_projected"),
                    StandaloneDatasetModel(dataset_id="resstock_conus_2022_projected"),
                    StandaloneDatasetModel(dataset_id="tempo_conus_2022_mapped"),
                ],
            ),
        ),
        result=QueryResultParamsModel(
            aggregations=[
                AggregationModel(
                    dimensions=DimensionQueryNamesModel(
                        geography=["state"],
                        metric=["electricity_collapsed"],
                        model_year=[],
                        scenario=[],
                        sector=["sector"],
                        subsector=[],
                        time=[],
                        weather_year=[],
                    ),
                    aggregation_function="sum",
                ),
            ],
        ),
    )
