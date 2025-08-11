import logging

from sqlalchemy import Connection

from dsgrid.config.simple_models import RegistrySimpleModel
from dsgrid.config.dataset_schema_handler_factory import make_dataset_schema_handler
from dsgrid.spark.functions import is_dataframe_empty
from dsgrid.utils.timing import track_timing, timer_stats_collector
from .registry_manager import RegistryManager


logger = logging.getLogger(__name__)


class FilterRegistryManager(RegistryManager):
    """Specialized RegistryManager that performs filtering operations."""

    @track_timing(timer_stats_collector)
    def filter(self, simple_model: RegistrySimpleModel, conn: Connection | None = None):
        """Filter the registry as described by simple_model.

        Parameters
        ----------
        simple_model : RegistrySimpleModel
            Filter all configs and data according to this model.
        """
        if conn is None:
            with self.project_manager.db.engine.begin() as conn:
                self._filter(conn, simple_model)
        else:
            self._filter(conn, simple_model)

    def _filter(self, conn: Connection, simple_model: RegistrySimpleModel):
        project_ids_to_keep = {x.project_id for x in simple_model.projects}
        to_remove = [
            x for x in self._project_mgr.list_ids(conn=conn) if x not in project_ids_to_keep
        ]
        for project_id in to_remove:
            self._project_mgr.remove(project_id, conn=conn)

        dataset_ids_to_keep = {x.dataset_id for x in simple_model.datasets}
        dataset_ids_to_remove = set(self._dataset_mgr.list_ids(conn=conn)) - dataset_ids_to_keep
        for dataset_id in dataset_ids_to_remove:
            self._dataset_mgr.remove(dataset_id, conn=conn)

        modified_dims = set()
        modified_dim_records = {}

        def handle_dimension(simple_dim, dim):
            records = dim.get_records_dataframe()
            df = records.filter(records.id.isin(simple_dim.record_ids))
            filtered_records = [x.asDict() for x in df.collect()]
            modified_dims.add(dim.model.dimension_id)
            modified_dim_records[dim.model.dimension_id] = {
                x.id for x in df.select("id").distinct().collect()
            }
            return filtered_records

        logger.info("Filter project dimensions")
        for project in simple_model.projects:
            changed_project = False
            project_config = self._project_mgr.get_by_id(project.project_id, conn=conn)
            indices_to_remove = []
            for i, dataset in enumerate(project_config.model.datasets):
                if dataset.dataset_id in dataset_ids_to_remove:
                    indices_to_remove.append(i)
            for index in reversed(indices_to_remove):
                project_config.model.datasets.pop(index)
                changed_project = True
            for simple_dim in project.dimensions.base_dimensions:
                for dim in project_config.list_base_dimensions(
                    dimension_type=simple_dim.dimension_type
                ):
                    dim.model.records = handle_dimension(simple_dim, dim)
                    self.dimension_manager.db.replace(conn, dim.model)

            for simple_dim in project.dimensions.supplemental_dimensions:
                for dim in project_config.list_supplemental_dimensions(simple_dim.dimension_type):
                    if dim.model.name == simple_dim.dimension_name:
                        dim.model.records = handle_dimension(simple_dim, dim)
                        self.dimension_manager.db.replace(conn, dim.model)
            if changed_project:
                self.project_manager.db.replace(conn, project_config.model)

        logger.info("Filter dataset dimensions")
        for dataset in simple_model.datasets:
            logger.info("Filter dataset %s", dataset.dataset_id)
            dataset_config = self._dataset_mgr.get_by_id(dataset.dataset_id, conn=conn)
            for simple_dim in dataset.dimensions:
                dim = dataset_config.get_dimension(simple_dim.dimension_type)
                dim.model.records = handle_dimension(simple_dim, dim)
                self.dimension_manager.db.replace(conn, dim.model)
            handler = make_dataset_schema_handler(
                conn,
                dataset_config,
                self._dimension_mgr,
                self._dimension_mapping_mgr,
                store=self._data_store,
            )
            handler.filter_data(dataset.dimensions, self._data_store)

        logger.info("Filter dimension mapping records")
        for mapping in self._dimension_mapping_mgr.iter_configs():
            records = None
            changed = False
            from_id = mapping.model.from_dimension.dimension_id
            to_id = mapping.model.to_dimension.dimension_id
            if from_id in modified_dims or to_id in modified_dims:
                records = mapping.get_records_dataframe()
                if from_id in modified_dims:
                    records = records.filter(records.from_id.isin(modified_dim_records[from_id]))
                    changed = True
                if to_id in modified_dims:
                    records = records.filter(records.to_id.isin(modified_dim_records[to_id]))
                    changed = True

            # TODO: probably need to remove a dimension mapping if it is empty
            if records is not None and changed and not is_dataframe_empty(records):
                mapping.model.records = [x.asDict() for x in records.collect()]
                self.dimension_mapping_manager.db.replace(conn, mapping.model)
                logger.info(
                    "Filtered dimension mapping records from ID %s", mapping.model.mapping_id
                )
