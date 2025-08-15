"""Interface to a dsgrid project."""

import json
import logging

from pathlib import Path
from semver import VersionInfo
from sqlalchemy import Connection

from dsgrid.common import VALUE_COLUMN
from dsgrid.config.project_config import ProjectConfig
from dsgrid.dataset.dataset import Dataset
from dsgrid.dataset.growth_rates import apply_exponential_growth_rate, apply_annual_multiplier
from dsgrid.dimension.base_models import DimensionType, DimensionCategory
from dsgrid.dimension.dimension_filters import (
    DimensionFilterSingleQueryNameBaseModel,
    SubsetDimensionFilterModel,
)
from dsgrid.exceptions import DSGInvalidQuery, DSGValueNotRegistered
from dsgrid.query.query_context import QueryContext
from dsgrid.query.models import (
    StandaloneDatasetModel,
    ProjectionDatasetModel,
    DatasetConstructionMethod,
    ColumnType,
)
from dsgrid.registry.dataset_registry_manager import DatasetRegistryManager
from dsgrid.registry.dimension_mapping_registry_manager import DimensionMappingRegistryManager
from dsgrid.registry.dimension_registry_manager import DimensionRegistryManager
from dsgrid.utils.files import compute_hash
from dsgrid.spark.functions import (
    is_dataframe_empty,
)
from dsgrid.spark.types import DataFrame
from dsgrid.utils.spark import (
    read_dataframe,
    try_read_dataframe,
    restart_spark_with_custom_conf,
    write_dataframe_and_auto_partition,
    get_active_session,
)
from dsgrid.utils.timing import timer_stats_collector, track_timing, Timer


logger = logging.getLogger(__name__)


class Project:
    """Interface to a dsgrid project."""

    def __init__(
        self,
        config: ProjectConfig,
        version: str,
        dataset_configs,
        dimension_mgr: DimensionRegistryManager,
        dimension_mapping_mgr: DimensionMappingRegistryManager,
        dataset_mgr: DatasetRegistryManager,
    ):
        self._spark = get_active_session()
        self._config = config
        self._version = version
        self._dataset_configs = dataset_configs
        self._datasets = {}
        self._dataset_mgr = dataset_mgr
        self._dimension_mgr = dimension_mgr
        self._dimension_mapping_mgr = dimension_mapping_mgr

    @property
    def config(self) -> ProjectConfig:
        """Returns the ProjectConfig."""
        return self._config

    @property
    def dimension_manager(self):
        return self._dimension_mgr

    @property
    def dimension_mapping_manager(self):
        return self._dimension_mapping_mgr

    @property
    def version(self):
        """Return the version of the project.

        Returns
        -------
        str

        """
        return self._version

    def is_registered(self, dataset_id):
        """Provides the status of dataset_id within this project.

        Parameters
        ----------
        dataset_id : str

        Returns
        -------
        bool
            True if dataset_id is in this project's config and the dataset has been
            registered with (successfully submitted to) this project; False if dataset_id
            is in this project's config but the dataset is not yet available.

        Throws
        ------
        DSGValueNotRegistered
            If dataset_id is not in this project's config.
        """
        if dataset_id not in self.list_datasets():
            msg = f"{dataset_id} is not expected by {self.config.model.project_id}"
            raise DSGValueNotRegistered(msg)

        return dataset_id in self._dataset_configs

    def get_dataset(self, dataset_id, conn: Connection | None = None) -> Dataset:
        """Returns a Dataset. Calls load_dataset if it hasn't already been loaded.

        Parameters
        ----------
        dataset_id : str

        Returns
        -------
        Dataset

        """
        if dataset_id in self._datasets:
            dataset = self._datasets[dataset_id]
        else:
            dataset = self.load_dataset(dataset_id, conn=conn)
        return dataset

    def load_dataset(self, dataset_id, conn: Connection | None = None) -> Dataset:
        """Loads a dataset.

        Parameters
        ----------
        dataset_id : str

        Returns
        -------
        Dataset

        """
        if dataset_id not in self._dataset_configs:
            msg = f"dataset_id={dataset_id} is not registered in the project"
            raise DSGValueNotRegistered(msg)
        config = self._dataset_configs[dataset_id]
        input_dataset = self._config.get_dataset(dataset_id)
        dataset = Dataset.load(
            config,
            self._dimension_mgr,
            self._dimension_mapping_mgr,
            self._dataset_mgr.store,
            mapping_references=input_dataset.mapping_references,
            conn=conn,
        )
        self._datasets[dataset_id] = dataset
        return dataset

    def unload_dataset(self, dataset_id):
        """Unloads a dataset.

        Parameters
        ----------
        dataset_id : str

        """
        self._datasets.pop(dataset_id, None)

    def _iter_datasets(self):
        for dataset in self.config.model.datasets:
            yield dataset

    def list_datasets(self):
        return [x.dataset_id for x in self._iter_datasets()]

    @track_timing(timer_stats_collector)
    def process_query(self, context: QueryContext, cached_datasets_dir: Path) -> dict[str, Path]:
        """Return a dictionary of dataset_id to dataframe path for all datasets in the query."""
        self._build_filtered_record_ids_by_dimension_type(context)

        # Note: Store DataFrame filenames instead of objects because the SparkSession will get
        # restarted for each dataset. The Spark DataFrame keeps a reference to the session that
        # created it, and so that reference will be invalid.
        df_filenames = {}
        for dataset in context.model.project.dataset.source_datasets:
            if isinstance(dataset, StandaloneDatasetModel):
                path = self._process_dataset(context, cached_datasets_dir, dataset.dataset_id)
            elif isinstance(dataset, ProjectionDatasetModel):
                path = self._process_projection_dataset(context, cached_datasets_dir, dataset)
            else:
                msg = f"Unsupported type: {type(dataset)}"
                raise NotImplementedError(msg)
            df_filenames[dataset.dataset_id] = path

        if not df_filenames:
            logger.warning("No data matched %s", context.model.name)

        return df_filenames

    def _build_filtered_record_ids_by_dimension_type(self, context: QueryContext):
        record_ids: dict[DimensionType, DataFrame] = {}
        for dim_filter in context.model.project.dataset.params.dimension_filters:
            dim_type = dim_filter.dimension_type
            if dim_type == DimensionType.TIME:
                # TODO #196
                # This needs to handled by the dataset handler function _prefilter_time_dimension
                msg = "Pre-filtering time is not supported yet"
                raise NotImplementedError(msg)
            if isinstance(dim_filter, SubsetDimensionFilterModel):
                df = dim_filter.get_filtered_records_dataframe(self._config.get_dimension).select(
                    "id"
                )
            else:
                query_name = dim_filter.dimension_name
                records = self._config.get_dimension_records(query_name)
                df = dim_filter.apply_filter(records).select("id")
                supp_query_names = set(
                    self._config.list_dimension_names(category=DimensionCategory.SUPPLEMENTAL)
                )
                if query_name in supp_query_names:
                    assert isinstance(dim_filter, DimensionFilterSingleQueryNameBaseModel)
                    base_query_name = getattr(
                        context.base_dimension_names, dim_filter.dimension_type.value
                    )
                    base_dim = self._config.get_dimension(base_query_name)
                    supp_dim = self._config.get_dimension(query_name)
                    mapping_records = self._config.get_base_to_supplemental_mapping_records(
                        base_dim, supp_dim
                    )
                    df = (
                        mapping_records.join(df, on=mapping_records.to_id == df.id)
                        .select("from_id")
                        .withColumnRenamed("from_id", "id")
                        .distinct()
                    )

            if dim_type in record_ids:
                df = record_ids[dim_type].join(df, "id")
            if is_dataframe_empty(df):
                msg = f"Query filter produced empty records: {dim_filter}"
                raise DSGInvalidQuery(msg)
            record_ids[dim_type] = df

        for dimension_type, ids in record_ids.items():
            context.set_record_ids_by_dimension_type(dimension_type, ids)

    def _process_dataset(
        self,
        context: QueryContext,
        cached_datasets_dir: Path,
        dataset_id: str,
    ) -> Path:
        """Return a Path to the created DataFrame. Does not return a DataFrame object because
        the SparkSession will be restarted.

        """
        logger.info("Start processing query for dataset_id=%s", dataset_id)
        hash_dir = self._compute_dataset_hash_and_serialize(
            context, cached_datasets_dir, dataset_id
        )
        cached_dataset_path = hash_dir / (dataset_id + ".parquet")
        metadata_file = cached_dataset_path.with_suffix(".json5")
        if try_read_dataframe(cached_dataset_path) is None:
            # An alternative solution is to call custom_spark_conf instead.
            # That changes some settings without restarting the SparkSession.
            # Results were not as good with that solution.
            # Observations on queries with comstock and resstock showed that Spark
            # used many fewer executors on the second query. That was with a standalone
            # cluster on Kestrel with dynamic allocation enabled.
            # We don't understand why that is the case. It may not be an issue with YARN as
            # the cluster manager on AWS.
            # Queries on standalone clusters will be easier to debug if we restart the session
            # for each big job.
            with restart_spark_with_custom_conf(
                conf=context.model.project.get_spark_conf(dataset_id),
                force=True,
            ):
                logger.info("Build project-mapped dataset %s", dataset_id)
                # Call load_dataset instead of get_dataset because the latter won't be valid here
                # after the SparkSession restart.
                with self._dimension_mgr.db.engine.connect() as conn:
                    dataset = self.load_dataset(dataset_id, conn=conn)
                    with Timer(timer_stats_collector, "build_project_mapped_dataset"):
                        df = dataset.make_project_dataframe(context, self._config)
                        context.serialize_dataset_metadata_to_file(
                            dataset.dataset_id, metadata_file
                        )
                        write_dataframe_and_auto_partition(df, cached_dataset_path)
        else:
            assert metadata_file.exists(), metadata_file
            context.set_dataset_metadata_from_file(dataset_id, metadata_file)
            logger.info("Use cached project-mapped dataset %s", dataset_id)

        logger.info("Finished processing query for dataset_id=%s", dataset_id)
        return cached_dataset_path

    def _process_projection_dataset(
        self,
        context: QueryContext,
        cached_datasets_dir: Path,
        dataset: ProjectionDatasetModel,
    ) -> Path:
        logger.info(
            "Apply %s for dataset_id=%s",
            dataset.construction_method.value,
            dataset.initial_value_dataset_id,
        )
        hash_dir = self._compute_dataset_hash_and_serialize(
            context, cached_datasets_dir, dataset.dataset_id
        )
        cached_dataset_path = hash_dir / (dataset.dataset_id + ".parquet")
        metadata_file = cached_dataset_path.with_suffix(".json5")
        if try_read_dataframe(cached_dataset_path) is None:
            self._build_projection_dataset(
                context,
                cached_datasets_dir,
                dataset,
                cached_dataset_path,
                metadata_file,
            )
        else:
            assert metadata_file.exists(), metadata_file
            context.set_dataset_metadata_from_file(dataset.dataset_id, metadata_file)
            logger.info("Use cached project-mapped dataset %s", dataset.dataset_id)

        return cached_dataset_path

    @track_timing(timer_stats_collector)
    def _build_projection_dataset(
        self,
        context: QueryContext,
        cached_datasets_dir: Path,
        dataset: ProjectionDatasetModel,
        dataset_path: Path,
        metadata_file: Path,
    ):
        def get_myear_column(dataset_id):
            match context.model.result.column_type:
                case ColumnType.DIMENSION_TYPES:
                    return DimensionType.MODEL_YEAR.value
                case ColumnType.DIMENSION_NAMES:
                    pass
                case _:
                    msg = f"BUG: unhandled {context.model.result.column_type=}"
                    raise NotImplementedError(msg)
            names = list(
                context.get_dimension_column_names(DimensionType.MODEL_YEAR, dataset_id=dataset_id)
            )
            assert len(names) == 1, f"{dataset_id=} {names=}"
            return names[0]

        iv_path = self._process_dataset(
            context,
            cached_datasets_dir,
            dataset.initial_value_dataset_id,
        )
        gr_path = self._process_dataset(
            context,
            cached_datasets_dir,
            dataset.growth_rate_dataset_id,
        )
        model_year_column = get_myear_column(dataset.initial_value_dataset_id)
        model_year_column_gr = get_myear_column(dataset.growth_rate_dataset_id)
        if model_year_column != model_year_column_gr:
            msg = (
                "BUG: initial_value and growth rate datasets have different model_year columns: "
                f"{model_year_column=} {model_year_column_gr=}"
            )
            raise Exception(msg)
        match context.model.result.column_type:
            case ColumnType.DIMENSION_NAMES:
                time_columns = context.get_dimension_column_names(
                    DimensionType.TIME, dataset_id=dataset.initial_value_dataset_id
                )
            case ColumnType.DIMENSION_TYPES:
                dset = self.get_dataset(dataset.initial_value_dataset_id)
                time_dim = dset.config.get_time_dimension()
                assert time_dim is not None
                time_columns = set(time_dim.get_load_data_time_columns())
            case _:
                msg = f"BUG: unhandled {context.model.result.column_type=}"
                raise NotImplementedError(msg)
        with restart_spark_with_custom_conf(
            conf=context.model.project.get_spark_conf(dataset.dataset_id),
            force=True,
        ):
            logger.info("Build projection dataset %s", dataset.dataset_id)
            iv_df = read_dataframe(iv_path)
            gr_df = read_dataframe(gr_path)
            value_columns = {VALUE_COLUMN}
            match dataset.construction_method:
                case DatasetConstructionMethod.EXPONENTIAL_GROWTH:
                    df = apply_exponential_growth_rate(
                        dataset, iv_df, gr_df, time_columns, model_year_column, value_columns
                    )
                case DatasetConstructionMethod.ANNUAL_MULTIPLIER:
                    df = apply_annual_multiplier(iv_df, gr_df, time_columns, value_columns)
                case _:
                    msg = f"BUG: Unsupported {dataset.construction_method=}"
                    raise NotImplementedError(msg)
            df = write_dataframe_and_auto_partition(df, dataset_path)

            time_dim = self._config.get_base_time_dimension()
            assert time_dim is not None
            time_columns = time_dim.get_load_data_time_columns()
            context.set_dataset_metadata(
                dataset.dataset_id,
                context.model.result.column_type,
                time_columns,
            )
            context.serialize_dataset_metadata_to_file(dataset.dataset_id, metadata_file)

    def _compute_dataset_hash_and_serialize(
        self, context: QueryContext, cached_datasets_dir: Path, dataset_id: str
    ) -> Path:
        """Create a hash that can be used to identify whether the mapping of the dataset to
        project dimensions can be skipped based on a previous query.

        If a directory with the hash does not already exist, create it and serialize the content
        used to create the hash.

        Examples of changes that will invalidate the query:
          - Bump to project major version number
          - Change to a dataset version
          - Change to a project's dimension requirements for a dataset
          - Change to a dataset dimension mapping

        Returns
        -------
        str
            Directory based on the hash
        """
        dataset_query_info = {
            "project_id": self._config.model.project_id,
            "project_major_version": VersionInfo.parse(self._config.model.version).major,
            "dataset": self._config.get_dataset(dataset_id).model_dump(mode="json"),
            "dataset_query_params": context.model.project.dataset.params.model_dump(mode="json"),
        }
        text = json.dumps(dataset_query_info, indent=2)
        hash_dir_name = compute_hash(text.encode())
        hash_dir = cached_datasets_dir / hash_dir_name
        if not hash_dir.exists():
            hash_dir.mkdir()
            model_file = hash_dir / "model.json"
            model_file.write_text(text)
        return hash_dir
