import abc
import logging

from dsgrid.config.project_config import ProjectConfig
from dsgrid.dimension.base_models import DimensionType
from dsgrid.query.models import AggregationModel
from dsgrid.query.query_context import QueryContext


logger = logging.getLogger(__name__)


class TableFormatHandlerBase(abc.ABC):
    """Base class for table format handers"""

    def __init__(self, project_config: ProjectConfig, dataset_id=None):
        self._project_config = project_config
        self._dataset_id = dataset_id

    @abc.abstractmethod
    def process_aggregations(self, df, aggregations: AggregationModel, context: QueryContext):
        """Aggregate the dimensional data as specified by aggregations.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
        aggregations : AggregationModel
        context : QueryContext

        Returns
        -------
        pyspark.sql.DataFrame

        """

    @property
    def project_config(self) -> ProjectConfig:
        return self._project_config

    @property
    def dataset_id(self) -> str:
        return self._dataset_id

    @abc.abstractmethod
    def add_columns(self, df, dimension_query_names, context: QueryContext, aggregation_allowed):
        """Add columns to the dataframe.

        Parameters
        ----------
        df : pyspark.sql.DataFrame
        dimension_query_names : list
        context : QueryContext
        aggregation_allowed : bool
            Set to False if adding a column is not allowed to change the load values in df.

        """

    def convert_columns_to_query_names(self, df):
        # All columns start off as base dimension names but need to be query names.
        base_to_query_name_mapping = self.project_config.get_base_dimension_to_query_name_mapping()
        columns = set(df.columns)
        for dim_type in DimensionType:
            if dim_type == DimensionType.TIME:
                time_dim = self._project_config.get_base_dimension(dim_type)
                time_cols = time_dim.get_timestamp_load_data_columns()
                # TODO DT: Should we enforce that projects can only have one time column?
                assert len(time_cols) == 1, time_cols
                existing_col = time_cols[0]
            elif dim_type.value in columns:
                existing_col = dim_type.value
            else:
                continue

            new_col = base_to_query_name_mapping[dim_type]
            if existing_col != new_col:
                df = df.withColumnRenamed(existing_col, new_col)
                logger.info("Converted column from %s to %s", existing_col, new_col)
        return df

    def replace_ids_with_names(self, df):
        orig = df
        all_query_names = set(self._project_config.list_dimension_query_names())
        for dimension_query_name in set(df.columns).intersection(all_query_names):
            assert not {"id", "name"}.intersection(df.columns), df.columns
            dim_config = self._project_config.get_dimension(dimension_query_name)
            if dim_config.model.dimension_type == DimensionType.TIME:
                # Time doesn't have records.
                continue
            records = dim_config.get_records_dataframe().select("id", "name")
            df = (
                df.join(records, on=df[dimension_query_name] == records.id)
                .drop("id", dimension_query_name)
                .withColumnRenamed("name", dimension_query_name)
            )
        assert df.count() == orig.count(), f"counts changed {df.count()} {orig.count()}"
        return df
