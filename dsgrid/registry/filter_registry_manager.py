import logging

from dsgrid.config.simple_models import RegistrySimpleModel
from dsgrid.config.dataset_schema_handler_factory import make_dataset_schema_handler
from dsgrid.utils.timing import track_timing, timer_stats_collector
from .registry_manager import RegistryManager


logger = logging.getLogger(__name__)


class FilterRegistryManager(RegistryManager):
    """Specialized RegistryManager that performs filtering operations."""

    @track_timing(timer_stats_collector)
    def filter(self, simple_model: RegistrySimpleModel):
        """Filter the registry as described by simple_model.

        Parameters
        ----------
        simple_model : RegistrySimpleModel
            Filter all configs and data according to this model.

        """
        project_ids_to_keep = {x.project_id for x in simple_model.projects}
        to_remove = [x for x in self._project_mgr.list_ids() if x not in project_ids_to_keep]
        for project_id in to_remove:
            self._project_mgr.remove(project_id)

        dataset_ids_to_keep = {x.dataset_id for x in simple_model.datasets}
        to_remove = [x for x in self._dataset_mgr.list_ids() if x not in dataset_ids_to_keep]
        for dataset_id in to_remove:
            self._dataset_mgr.remove(dataset_id)

        modified_dims = set()
        modified_dim_records = {}

        # Note: Use pandas to write CSVs because Spark produces directories.

        def handle_dimension(simple_dim, dim):
            records = dim.get_records_dataframe()
            filename = dim.src_dir / dim.model.filename
            df = records.filter(records.id.isin(simple_dim.record_ids))
            df.toPandas().to_csv(filename, index=False)
            modified_dims.add(dim.model.dimension_id)
            modified_dim_records[dim.model.dimension_id] = {
                x.id for x in df.select("id").distinct().collect()
            }

        logger.info("Filter project dimensions")
        for project in simple_model.projects:
            project_config = self._project_mgr.get_by_id(project.project_id)
            for simple_dim in project.dimensions.base_dimensions:
                dim = project_config.get_base_dimension(simple_dim.dimension_type)
                handle_dimension(simple_dim, dim)

            for simple_dim in project.dimensions.supplemental_dimensions:
                for dim in project_config.get_supplemental_dimensions(simple_dim.dimension_type):
                    if dim.model.query_name == simple_dim.query_name:
                        handle_dimension(simple_dim, dim)

        logger.info("Filter dataset dimensions")
        for dataset in simple_model.datasets:
            logger.info("Filter dataset %s", dataset.dataset_id)
            dataset_config = self._dataset_mgr.get_by_id(dataset.dataset_id)
            for simple_dim in dataset.dimensions:
                dim = dataset_config.get_dimension(simple_dim.dimension_type)
                handle_dimension(simple_dim, dim)
            handler = make_dataset_schema_handler(
                dataset_config, self._dimension_mgr, self._dimension_mapping_mgr
            )
            handler.filter_data(dataset.dimensions)

        logger.info("Filter dimension mapping records")
        for mapping in self._dimension_mapping_mgr.iter_configs():
            records = None
            from_id = mapping.model.from_dimension.dimension_id
            to_id = mapping.model.to_dimension.dimension_id
            if from_id in modified_dims or to_id in modified_dims:
                records = mapping.get_records_dataframe()
                if from_id in modified_dims:
                    records = records.filter(records.from_id.isin(modified_dim_records[from_id]))
                if to_id in modified_dims:
                    records = records.filter(records.to_id.isin(modified_dim_records[to_id]))
            if records is not None:
                filename = mapping.src_dir / mapping.model.filename
                records.toPandas().to_csv(filename, index=False)
                logger.info("Filtered dimension mapping records from %s", filename)

        for project in simple_model.projects:
            project_config = self._project_mgr.get_by_id(project.project_id)
            project_config.serialize(project_config.src_dir, force=True)

        for dataset in simple_model.datasets:
            dataset_config = self._dataset_mgr.get_by_id(dataset.dataset_id)
            dataset_config.serialize(dataset_config.src_dir, force=True)
