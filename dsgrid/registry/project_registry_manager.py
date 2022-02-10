"""Manages the registry for dimension projects"""

from dsgrid.config.dimension_associations import DimensionAssociations
import getpass
import io
import itertools
import logging
from contextlib import redirect_stdout
from typing import List

from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import (
    DSGInvalidDataset,
    DSGInvalidDimensionAssociation,
    DSGValueNotRegistered,
    DSGDuplicateValueRegistered,
)
from dsgrid import timer_stats_collector
from dsgrid.common import REGISTRY_FILENAME
from dsgrid.config.dataset_schema_handler_factory import make_dataset_schema_handler
from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.config.dimension_mapping_base import (
    DimensionMappingReferenceModel,
    DimensionMappingReferenceListModel,
)
from dsgrid.config.project_config import ProjectConfig
from dsgrid.registry.common import (
    make_initial_config_registration,
    ConfigKey,
    DatasetRegistryStatus,
    ProjectRegistryStatus,
)
from dsgrid.utils.spark import create_dataframe_from_dimension_ids
from dsgrid.utils.timing import track_timing, timer_stats_collector, Timer
from .common import VersionUpdateType
from .project_update_checker import ProjectUpdateChecker
from .dataset_registry_manager import DatasetRegistryManager
from .dimension_registry_manager import DimensionRegistryManager
from .project_registry import ProjectRegistry, ProjectRegistryModel
from .registry_manager_base import RegistryManagerBase


logger = logging.getLogger(__name__)


class ProjectRegistryManager(RegistryManagerBase):
    """Manages registered dimension projects."""

    def __init__(self, path, params):
        super().__init__(path, params)
        self._projects = {}  # ConfigKey to ProjectModel
        self._dataset_mgr = None
        self._dimension_mgr = None
        self._dimension_mapping_mgr = None

    @classmethod
    def load(
        cls, path, fs_interface, dataset_manager, dimension_manager, dimension_mapping_manager
    ):
        mgr = cls._load(path, fs_interface)
        mgr.dataset_manager = dataset_manager
        mgr.dimension_manager = dimension_manager
        mgr.dimension_mapping_manager = dimension_mapping_manager
        return mgr

    @staticmethod
    def name():
        return "Projects"

    @property
    def dataset_manager(self):
        return self._dataset_mgr

    @dataset_manager.setter
    def dataset_manager(self, val: DatasetRegistryManager):
        self._dataset_mgr = val

    @property
    def dimension_manager(self):
        return self._dimension_mgr

    @dimension_manager.setter
    def dimension_manager(self, val: DimensionRegistryManager):
        self._dimension_mgr = val

    @property
    def dimension_mapping_manager(self):
        return self._dimension_mapping_mgr

    @dimension_mapping_manager.setter
    def dimension_mapping_manager(self, val: DimensionRegistryManager):
        self._dimension_mapping_mgr = val

    @staticmethod
    def registry_class():
        return ProjectRegistry

    def get_by_id(self, config_id, version=None):
        self._check_if_not_registered(config_id)
        if version is None:
            version = self._registry_configs[config_id].model.version
        key = ConfigKey(config_id, version)
        return self.get_by_key(key)

    def get_by_key(self, key):
        if not self.has_id(key.id, version=key.version):
            raise DSGValueNotRegistered(f"project={key}")

        project = self._projects.get(key)
        if project is not None:
            return project

        project = ProjectConfig.load(
            self.get_config_file(key.id, key.version),
            self._dimension_mgr,
            self._dimension_mapping_mgr,
        )
        self._projects[key] = project
        return project

    def get_registry_lock_file(self, config_id):
        return f"configs/.locks/{config_id}.lock"

    def register(self, config_file, submitter, log_message, force=False):
        config = ProjectConfig.load(config_file, self._dimension_mgr, self._dimension_mapping_mgr)
        lock_file_path = self.get_registry_lock_file(config.config_id)
        with self.cloud_interface.make_lock_file(lock_file_path):
            self._register(config, submitter, log_message, force=force)

    def _register(self, config, submitter, log_message, force=False):
        self._check_if_already_registered(config.model.project_id)
        self._run_checks(config)

        registration = make_initial_config_registration(submitter, log_message)

        if self.dry_run_mode:
            logger.info(
                "%s Project validated for registration project_id=%s",
                self._log_dry_run_mode_prefix(),
                config.model.project_id,
            )
            return

        registry_model = ProjectRegistryModel(
            project_id=config.model.project_id,
            version=registration.version,
            description=config.model.description,
            registration_history=[registration],
        )
        registry_config = ProjectRegistry(registry_model)
        registry_dir = self.get_registry_directory(config.model.project_id)
        data_dir = registry_dir / str(registration.version)

        # Serialize the registry file as well as the updated ProjectConfig to the registry.
        self.fs_interface.mkdir(data_dir)
        registry_filename = registry_dir / REGISTRY_FILENAME
        registry_config.serialize(registry_filename, force=True)
        config.serialize(self.get_config_directory(config.config_id, registry_config.version))

        self._update_registry_cache(config.model.project_id, registry_config)

        if not self.offline_mode:
            registry_dir = self.get_registry_directory(config.config_id)
            self.sync_push(registry_dir)

        logger.info(
            "%s Registered project %s with version=%s",
            self._log_offline_mode_prefix(),
            config.model.project_id,
            registration.version,
        )

    def _run_checks(self, config: ProjectConfig):
        self._check_dimension_associations(config)

    def _check_dimension_associations(self, config: ProjectConfig):
        for dimension_type in config.dimension_associations.dimension_types:
            assoc_ids = config.dimension_associations.get_unique_ids(dimension_type)
            dim = config.get_base_dimension(dimension_type)
            dim_record_ids = dim.get_unique_ids()
            diff = assoc_ids.difference(dim_record_ids)
            if diff:
                raise DSGInvalidDimensionAssociation(
                    f"Dimension association for {dimension_type} has invalid records: {diff}"
                )

    def submit_dataset(
        self, project_id, dataset_id, dimension_mapping_files, submitter, log_message
    ):
        """Registers a dataset with a project. This can only be performed on the
        latest version of the project.

        Parameters
        ----------
        project_id : str
        dataset_id : str
        dimension_mapping_files : tuple
            dimension mapping filenames
        submitter : str
            Submitter name
        log_message : str

        Raises
        ------
        DSGValueNotRegistered
            Raised if the project_id or dataset_id is not registered.
        DSGDuplicateValueRegistered
            Raised if the dataset is already registered with the project.
        ValueError
            Raised if the project does not contain this dataset.

        """
        config = self.get_by_id(project_id)
        lock_file_path = self.get_registry_lock_file(config.config_id)
        with self.cloud_interface.make_lock_file(lock_file_path):
            self._submit_dataset(
                config, dataset_id, dimension_mapping_files, submitter, log_message
            )

    def _submit_dataset(
        self,
        project_config: ProjectConfig,
        dataset_id,
        dimension_mapping_files,
        submitter,
        log_message,
    ):
        self._check_if_not_registered(project_config.config_id)
        dataset_config = self._dataset_mgr.get_by_id(dataset_id)
        dataset_model = project_config.get_dataset(dataset_id)
        if dataset_model.status == DatasetRegistryStatus.REGISTERED:
            raise DSGDuplicateValueRegistered(
                f"dataset={dataset_id} has already been submitted to project={project_config.config_id}"
            )

        mapping_references = []
        for filename in dimension_mapping_files:
            for ref in DimensionMappingReferenceListModel.load(filename).references:
                if not self.dimension_mapping_manager.has_id(ref.mapping_id, version=ref.version):
                    raise DSGValueNotRegistered(f"mapping_id={ref.mapping_id}")
                mapping_references.append(ref)

        project_config.add_dataset_dimension_mappings(dataset_config, mapping_references)
        self._check_dataset_base_to_project_base_mappings(
            project_config,
            dataset_config,
            mapping_references,
        )

        if self.dry_run_mode:
            logger.info(
                "%s Dataset submission to project validated dataset_id=%s project_id=%s",
                self._log_dry_run_mode_prefix(),
                dataset_id,
                project_config.config_id,
            )
            return

        dataset_model.mapping_references = mapping_references
        dataset_model.status = DatasetRegistryStatus.REGISTERED
        if project_config.are_all_datasets_submitted():
            new_status = ProjectRegistryStatus.COMPLETE
        else:
            new_status = ProjectRegistryStatus.IN_PROGRESS
        project_config.set_status(new_status)
        version = self._update(project_config, submitter, VersionUpdateType.MINOR, log_message)

        if not self.offline_mode:
            self.sync_push(self.get_registry_directory(project_config.config_id))

        logger.info(
            "%s Registered dataset %s with version=%s in project %s",
            self._log_offline_mode_prefix(),
            dataset_id,
            version,
            project_config.config_id,
        )

    @track_timing(timer_stats_collector)
    def _check_dataset_base_to_project_base_mappings(
        self,
        project_config: ProjectConfig,
        dataset_config: DatasetConfig,
        mapping_references: List[DimensionMappingReferenceModel],
    ):
        """Check that a dataset has all project-required dimension records."""
        handler = make_dataset_schema_handler(
            dataset_config, self._dimension_mgr, self._dimension_mapping_mgr, mapping_references
        )
        pivot_dimension = handler.get_pivot_dimension_type()
        exclude_dims = set([DimensionType.TIME, DimensionType.DATA_SOURCE, pivot_dimension])
        types = [x for x in DimensionType if x not in exclude_dims]
        dimension_pairs = [tuple(sorted((x, y))) for x, y in itertools.combinations(types, 2)]

        data_source = dataset_config.model.data_source
        dim_table = handler.get_unique_dimension_rows().drop("id")
        associations = project_config.dimension_associations
        # TODO: check a unified project table against dim_table
        # project_table = self._make_single_table(project_config, data_source, pivot_dimension)
        for type1, type2 in dimension_pairs:
            records = associations.get_associations(type1, type2, data_source=data_source)
            if records is None:
                records = self._get_project_dimensions_table(project_config, type1, type2)
            columns = (type1.value, type2.value)
            with Timer(timer_stats_collector, "evaluate dimension record counts"):
                record_count = records.count()
                count = records.select(*columns).intersect(dim_table.select(*columns)).count()
            if count != record_count:
                table = records.select(*columns).exceptAll(dim_table.select(*columns))
                dataset_id = dataset_config.model.dataset_id
                with io.StringIO() as buf, redirect_stdout(buf):
                    table.show(n=table.count())
                    logger.error(
                        "Dataset %s is missing records for %s:\n%s",
                        dataset_id,
                        (type1, type2),
                        buf.getvalue(),
                    )
                raise DSGInvalidDataset(
                    f"Dataset {dataset_id} is missing records for {(type1, type2)}"
                )
        self._check_pivot_dimension_columns(project_config, handler, pivot_dimension)

    def _make_single_table(self, config: ProjectConfig, data_source, pivot_dimension):
        # TODO: prototype code from Meghan - needs testing
        ds = DimensionType.DATA_SOURCE.value
        table = config.dimension_associations.table.filter(f"{ds} = '{data_source}'").drop(
            pivot_dimension.value
        )
        exclude = set((DimensionType.TIME, DimensionType.DATA_SOURCE, pivot_dimension))
        all_dimensions = set(d.value for d in DimensionType if d not in exclude)
        missing_dimensions = all_dimensions.difference(table.columns)
        for dim in missing_dimensions:
            table_count = table.count()
            other = (
                config.get_base_dimension(DimensionType(dim))
                .get_records_dataframe()
                .select("id")
                .withColumnRenamed("id", dim)
            )
            table = table.crossJoin(other)
            assert table.count() == other.count() * table_count

        return table

    @staticmethod
    def _get_project_dimensions_table(project_config, type1, type2):
        pdim1 = project_config.get_base_dimension(type1)
        pdim1_ids = pdim1.get_unique_ids()
        pdim2 = project_config.get_base_dimension(type2)
        pdim2_ids = pdim2.get_unique_ids()
        records = itertools.product(pdim1_ids, pdim2_ids)
        return create_dataframe_from_dimension_ids(records, type1, type2)

    @staticmethod
    def _check_pivot_dimension_columns(project_config, handler, dim_type):
        d_dim_ids = set(handler.get_pivot_dimension_columns_mapped_to_project().values())
        associations = project_config.dimension_associations
        table = associations.get_associations(DimensionType.DATA_SOURCE, dim_type)
        if table is None:
            p_dim_ids = project_config.get_base_dimension(dim_type).get_unique_ids()
        else:
            p_dim_ids = {
                getattr(x, dim_type.value) for x in table.select(dim_type.value).collect()
            }
        diff = d_dim_ids.symmetric_difference(p_dim_ids)
        if diff:
            raise DSGInvalidDataset(f"load data pivoted columns have invalid values: {diff}")

    def update_from_file(
        self, config_file, config_id, submitter, update_type, log_message, version
    ):
        config = ProjectConfig.load(
            config_file, self.dimension_manager, self.dimension_mapping_manager
        )
        self._check_update(config, config_id, version)
        self.update(config, update_type, log_message, submitter=submitter)

    def update(self, config, update_type, log_message, submitter=None):
        if submitter is None:
            submitter = getpass.getuser()
        lock_file_path = self.get_registry_lock_file(config.config_id)
        with self.cloud_interface.make_lock_file(lock_file_path):
            return self._update(config, submitter, update_type, log_message)

    def _update(self, config, submitter, update_type, log_message):
        old_config = self.get_by_id(config.config_id)
        checker = ProjectUpdateChecker(old_config.model, config.model)
        result = checker.run()
        self._run_checks(config)
        registry = self.get_registry_config(config.config_id)
        old_key = ConfigKey(config.config_id, registry.version)
        version = self._update_config(config, submitter, update_type, log_message)
        new_key = ConfigKey(config.config_id, version)
        self._projects.pop(old_key, None)
        self._projects[new_key] = config

        if not self.offline_mode:
            self.sync_push(self.get_registry_directory(config.config_id))

        return version

    def remove(self, config_id):
        self._remove(config_id)
        for key in [x for x in self._projects if x.id == config_id]:
            self._projects.pop(key)
