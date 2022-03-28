"""Manages the registry for dimension projects"""

import getpass
import itertools
import logging
from pathlib import Path
from typing import Union, List, Dict

from prettytable import PrettyTable
import pyspark.sql.functions as F
from semver import VersionInfo

from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import (
    DSGInvalidDataset,
    DSGInvalidDimension,
    DSGInvalidDimensionAssociation,
    DSGInvalidParameter,
    DSGValueNotRegistered,
    DSGDuplicateValueRegistered,
)
from dsgrid import timer_stats_collector
from dsgrid.common import REGISTRY_FILENAME
from dsgrid.config.dataset_schema_handler_factory import make_dataset_schema_handler
from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.config.dimension_mapping_base import (
    DimensionMappingReferenceModel,
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
from dsgrid.utils.files import dump_data, load_data
from dsgrid.utils.filters import transform_and_validate_filters, matches_filters
from dsgrid.utils.utilities import display_table
from .common import (
    DimensionKey,
    VersionUpdateType,
    auto_register_dimensions,
    RegistryType,
)
from .registration_context import RegistrationContext
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

    def finalize_registration(self, config_ids, error_occurred):
        assert len(config_ids) == 1
        project_id = config_ids[0]

        if error_occurred:
            logger.info("Remove intermediate project after error")
            self.remove(project_id)
            return

        # TODO DT: add mechanism to verify that nothing has changed.
        lock_file_path = self.get_registry_lock_file(project_id)
        with self.cloud_interface.make_lock_file(lock_file_path):
            if not self.offline_mode:
                self.sync_push(self.get_registry_directory(project_id))

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

    @track_timing(timer_stats_collector)
    def _auto_register_dimension_mappings(
        self,
        mappings: List,
        mapping_file: Path,
        dimension_name_mapping: Dict,
        context: RegistrationContext,
        submitter: str,
        log_message: str,
        force: bool,
    ):
        """Registers dimension mappings in mapping_file and then updates
        mappings with the registered dimension mapping IDs and versions.

        Registration of dimension mappingss is all or none. mappings may be partially
        updated if an exception occurs.

        Raises
        ------
        DSGInvalidDimension
            Raised if a dimension is specified incorrectly.
        DSGInvalidDimensionMapping
            Raised if a dimension mapping is specified incorrectly.

        """
        mapping_config_data = load_data(mapping_file)
        for mapping in mapping_config_data["mappings"]:
            for dim in ("from_dimension", "to_dimension"):
                if "name" in mapping[dim]:
                    if "dimension_id" in mapping[dim]:
                        raise DSGInvalidDimension(f"dimension_id cannot be present with name")
                    name = mapping[dim].pop("name")
                    label = (name, mapping[dim]["type"])
                    if label not in dimension_name_mapping:
                        raise DSGInvalidDimension(
                            f"{label} was not provided as a registered dimension"
                        )
                    mapping[dim]["dimension_id"] = dimension_name_mapping[label].id
                    mapping[dim]["version"] = str(dimension_name_mapping[label].version)

        directory = mapping_file.parent
        tmp_mapping_file = directory / ("tmp_" + mapping_file.name)
        dump_data(mapping_config_data, tmp_mapping_file)

        try:
            self._dimension_mapping_mgr.register(
                tmp_mapping_file,
                submitter,
                log_message,
                force=force,
                context=context,
            )
        finally:
            tmp_mapping_file.unlink()

        # Note: Ensure that we don't raise exceptions in the rest of this method.
        # Otherwise, we would need to remove the registered dimension mappings.

        for mapping_id in context.get_ids(RegistryType.DIMENSION_MAPPING):
            mapping = self._dimension_mapping_mgr.get_by_id(mapping_id)
            mappings.append(
                DimensionMappingReferenceModel(
                    from_dimension_type=mapping.model.from_dimension.dimension_type,
                    to_dimension_type=mapping.model.to_dimension.dimension_type,
                    mapping_id=mapping_id,
                    version=self._dimension_mapping_mgr.get_current_version(mapping_id),
                ).dict()
            )

    @track_timing(timer_stats_collector)
    def register(
        self,
        config_file,
        submitter,
        log_message,
        dimension_file=None,
        base_to_supplemental_dimension_mapping_file=None,
        force=False,
        context=None,
    ):
        error_occurred = False
        need_to_finalize = context is None
        if context is None:
            context = RegistrationContext()

        config_data = load_data(config_file)
        try:
            self._register_project_and_dimensions(
                config_file,
                config_data,
                submitter,
                log_message,
                context,
                dimension_file=dimension_file,
                base_to_supplemental_dimension_mapping_file=base_to_supplemental_dimension_mapping_file,
                force=force,
            )
        except Exception:
            error_occurred = True
            raise
        finally:
            if need_to_finalize:
                context.finalize(error_occurred)

    @track_timing(timer_stats_collector)
    def _register_project_and_dimensions(
        self,
        config_file: Path,
        config_data: Dict,
        submitter: str,
        log_message: str,
        context: RegistrationContext,
        dimension_file=None,
        base_to_supplemental_dimension_mapping_file=None,
        force=False,
    ):
        self._check_if_already_registered(config_data["project_id"])
        if dimension_file is None and base_to_supplemental_dimension_mapping_file is not None:
            raise DSGInvalidParameter(
                "dimension_file cannot be None if base_to_supplemental_dimension_mapping_file is set"
            )
        if dimension_file is not None:
            config_data_lists = [
                config_data["dimensions"]["base_dimensions"],
                config_data["dimensions"]["supplemental_dimensions"],
            ]
            dimension_name_mapping = auto_register_dimensions(
                config_data_lists,
                self._dimension_mgr,
                dimension_file,
                submitter,
                log_message,
                force,
                context,
            )
        if base_to_supplemental_dimension_mapping_file is not None:
            if "dimension_mappings" not in config_data:
                config_data["dimension_mappings"] = {}
            if "base_to_supplemental" not in config_data["dimension_mappings"]:
                config_data["dimension_mappings"]["base_to_supplemental"] = []
            self._auto_register_dimension_mappings(
                config_data["dimension_mappings"]["base_to_supplemental"],
                base_to_supplemental_dimension_mapping_file,
                dimension_name_mapping,
                context,
                submitter,
                log_message,
                force,
            )

        config = ProjectConfig.load(
            config_file, self._dimension_mgr, self._dimension_mapping_mgr, data=config_data
        )
        self._register(config, submitter, log_message, force)
        context.add_id(RegistryType.PROJECT, config.model.project_id, self)

    @track_timing(timer_stats_collector)
    def _register(self, config, submitter, log_message, force):
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
        registry_config.serialize(registry_filename, force=force)
        config.serialize(self.get_config_directory(config.config_id, registry_config.version))

        self._update_registry_cache(config.model.project_id, registry_config)

        logger.info(
            "%s Registered project %s with version=%s",
            self._log_offline_mode_prefix(),
            config.model.project_id,
            registration.version,
        )

    @track_timing(timer_stats_collector)
    def _run_checks(self, config: ProjectConfig):
        self._check_dimension_associations(config)

    @track_timing(timer_stats_collector)
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

    @track_timing(timer_stats_collector)
    def register_and_submit_dataset(
        self,
        dataset_config_file,
        dataset_path,
        project_id,
        submitter,
        log_message,
        dimension_file=None,
        dimension_mapping_file=None,
    ):
        context = RegistrationContext()
        error_occurred = False
        try:
            self._dataset_mgr.register(
                dataset_config_file,
                dataset_path,
                submitter,
                log_message,
                dimension_file=dimension_file,
                context=context,
            )
            self.submit_dataset(
                project_id,
                context.get_ids(RegistryType.DATASET)[0],
                submitter,
                log_message,
                dimension_mapping_file=dimension_mapping_file,
                context=context,
            )
        except Exception:
            error_occurred = True
            raise
        finally:
            context.finalize(error_occurred)

    @track_timing(timer_stats_collector)
    def submit_dataset(
        self,
        project_id,
        dataset_id,
        submitter,
        log_message,
        dimension_mapping_file=None,
        dimension_mapping_reference_file=None,
        context=None,
    ):
        """Registers a dataset with a project. This can only be performed on the
        latest version of the project.

        Parameters
        ----------
        project_id : str
        dataset_id : str
        dimension_mapping_file : Path or None
            Base-to-base dimension mapping file
        dimension_mapping_reference_file : Path or None
            Optionally contains references to already-registered dimension mappings.
        submitter : str
            Submitter name
        log_message : str
        context : None or RegistrationContext

        Raises
        ------
        DSGValueNotRegistered
            Raised if the project_id or dataset_id is not registered.
        DSGDuplicateValueRegistered
            Raised if the dataset is already registered with the project.
        ValueError
            Raised if the project does not contain this dataset.

        """
        need_to_finalize = context is None
        error_occurred = False
        if context is None:
            context = RegistrationContext()

        config = self.get_by_id(project_id)
        try:
            self._submit_dataset_and_register_mappings(
                config,
                dataset_id,
                submitter,
                log_message,
                dimension_mapping_file,
                dimension_mapping_reference_file,
                context,
            )
        except Exception:
            error_occurred = True
            raise
        finally:
            if need_to_finalize:
                context.finalize(error_occurred)

    def _submit_dataset_and_register_mappings(
        self,
        project_config: ProjectConfig,
        dataset_id,
        submitter,
        log_message,
        dimension_mapping_file,
        dimension_mapping_reference_file,  # TODO DT: probably don't need this
        context,
    ):
        logger.info("Submit dataset=%s to project=%s.", dataset_id, project_config.config_id)
        self._check_if_not_registered(project_config.config_id)
        dataset_config = self._dataset_mgr.get_by_id(dataset_id)
        dataset_model = project_config.get_dataset(dataset_id)
        if dataset_model.status == DatasetRegistryStatus.REGISTERED:
            raise DSGDuplicateValueRegistered(
                f"dataset={dataset_id} has already been submitted to project={project_config.config_id}"
            )

        if dimension_mapping_file is not None:
            dataset_config = self._dataset_mgr.get_by_id(dataset_id)
            dimension_name_mapping = {
                (y.model.name, x.type.value): x for x, y in project_config.base_dimensions.items()
            }
            for key, val in dataset_config.dimensions.items():
                dimension_name_mapping[(val.model.name, key.type.value)] = key

            mapping_references = []
            self._auto_register_dimension_mappings(
                mapping_references,
                dimension_mapping_file,
                dimension_name_mapping,
                context,
                submitter,
                log_message,
                False,
            )
        references = [DimensionMappingReferenceModel(**x) for x in mapping_references]
        self._submit_dataset(project_config, dataset_config, submitter, log_message, references)

    def _submit_dataset(
        self,
        project_config: ProjectConfig,
        dataset_config: DatasetConfig,
        submitter: str,
        log_message: str,
        mapping_references: List[DimensionMappingReferenceModel],
    ):
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
                dataset_config.model.dataset_id,
                project_config.model.project_id,
            )

        dataset_model = project_config.get_dataset(dataset_config.model.dataset_id)
        dataset_model.mapping_references = mapping_references
        dataset_model.status = DatasetRegistryStatus.REGISTERED
        if project_config.are_all_datasets_submitted():
            new_status = ProjectRegistryStatus.COMPLETE
        else:
            new_status = ProjectRegistryStatus.IN_PROGRESS
        project_config.set_status(new_status)
        version = self._update(project_config, submitter, VersionUpdateType.MINOR, log_message)

        logger.info(
            "%s Registered dataset %s with version=%s in project %s",
            self._log_offline_mode_prefix(),
            dataset_config.model.dataset_id,
            version,
            project_config.model.project_id,
        )

    @track_timing(timer_stats_collector)
    def _check_dataset_base_to_project_base_mappings(
        self,
        project_config: ProjectConfig,
        dataset_config: DatasetConfig,
        mapping_references: List[DimensionMappingReferenceModel],
    ):
        """Check that a dataset has all project-required dimension records."""
        logger.info("Check dataset-base-to-project-base dimension mappings.")
        handler = make_dataset_schema_handler(
            dataset_config, self._dimension_mgr, self._dimension_mapping_mgr, mapping_references
        )
        pivot_dimension = handler.get_pivot_dimension_type()
        exclude_dims = set([DimensionType.TIME, DimensionType.DATA_SOURCE, pivot_dimension])

        data_source_dim_id = [
            x.id for x in dataset_config.dimensions if x.type == DimensionType.DATA_SOURCE
        ][0]
        data_source = [
            x.id for x in self._dimension_mgr.get_by_id(data_source_dim_id).model.records
        ][
            0
        ]  # this assumes only data_source per dataset
        dim_table = (
            handler.get_unique_dimension_rows().drop("id").drop(DimensionType.DATA_SOURCE.value)
        )

        cols = [x.value for x in DimensionType if x not in exclude_dims]
        project_table = (
            self._make_single_table(project_config, data_source, pivot_dimension)
            .select(*cols)
            .distinct()
        )
        diff = project_table.exceptAll(dim_table.select(*cols).distinct())
        if not diff.rdd.isEmpty():
            dataset_id = dataset_config.config_id
            project_id = project_config.config_id
            out_file = f"{dataset_id}__{project_id}__missing_dimension_record_combinations.csv"
            diff.write.options(header=True).mode("overwrite").csv(out_file)
            logger.error(
                "Dataset %s is missing required dimension records from project %s. "
                "Recorded missing records in %s.",
                dataset_id,
                project_id,
                out_file,
            )
            raise DSGInvalidDataset(
                f"Dataset {dataset_config.config_id} is missing required dimension records"
            )
        self._check_pivot_dimension_columns(
            project_config, handler, project_config.dimension_associations, data_source
        )

    @staticmethod
    @track_timing(timer_stats_collector)
    def _make_single_table(config: ProjectConfig, data_source, pivot_dimension):
        ds = DimensionType.DATA_SOURCE.value
        table = config.dimension_associations.table
        table_columns = set()
        # table is None when the project doesn't define any dimension associations.
        if table is not None:
            table = (
                config.dimension_associations.table.filter(f"{ds} = '{data_source}'")
                .drop(pivot_dimension.value)
                .drop(ds)
            )
            table_columns.update(table.columns)

        exclude = set((DimensionType.TIME, DimensionType.DATA_SOURCE, pivot_dimension))
        all_dimensions = set(d.value for d in DimensionType if d not in exclude)
        missing_dimensions = all_dimensions.difference(table_columns)
        for dim in missing_dimensions:
            table_count = 0 if table is None else table.count()
            other = (
                config.get_base_dimension(DimensionType(dim))
                .get_records_dataframe()
                .select("id")
                .withColumnRenamed("id", dim)
            )
            if table is None:
                table = other
            else:
                table = table.crossJoin(other)
                assert table.count() == other.count() * table_count

        return table

    @staticmethod
    @track_timing(timer_stats_collector)
    def _get_project_dimensions_table(project_config, type1, type2, associations, data_source):
        """for each dimension type x, record is the same as project's unless a relevant association is provided."""
        pdim1_ids = associations.get_unique_ids(type1, data_source)
        if pdim1_ids is None:
            pdim1_ids = project_config.get_base_dimension(type1).get_unique_ids()

        pdim2_ids = associations.get_unique_ids(type2, data_source)
        if pdim2_ids is None:
            pdim2_ids = project_config.get_base_dimension(type2).get_unique_ids()

        records = itertools.product(pdim1_ids, pdim2_ids)
        return create_dataframe_from_dimension_ids(records, type1, type2)

    @staticmethod
    @track_timing(timer_stats_collector)
    def _check_pivot_dimension_columns(project_config, handler, associations, data_source):
        """pivoted dimension record is the same as project's unless a relevant association is provided."""
        logger.info("Check pivoted dimension columns.")
        d_dim_ids = handler.get_pivot_dimension_columns_mapped_to_project()
        pivot_dim = handler.get_pivot_dimension_type()
        p_dim_ids = associations.get_unique_ids(pivot_dim, data_source)
        if p_dim_ids is None:
            p_dim_ids = project_config.get_base_dimension(pivot_dim).get_unique_ids()

        if d_dim_ids.symmetric_difference(p_dim_ids):
            raise DSGInvalidDataset(
                f"Mismatch between project and {data_source} dataset pivoted {pivot_dim.value} dimension, "
                "please double-check data, and any relevant association_table and dimension_mapping. "
                f"\n - Invalid column(s) in {data_source} load data according to project: {d_dim_ids.difference(p_dim_ids)}"
                f"\n - Missing column(s) in {data_source} load data according to project: {p_dim_ids.difference(d_dim_ids)}"
            )

    def update_from_file(
        self, config_file, config_id, submitter, update_type, log_message, version
    ):
        config = ProjectConfig.load(
            config_file, self.dimension_manager, self.dimension_mapping_manager
        )
        self._check_update(config, config_id, version)
        self.update(config, update_type, log_message, submitter=submitter)

    @track_timing(timer_stats_collector)
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

    def show(
        self,
        filters: List[str] = None,
        max_width: Union[int, Dict] = None,
        drop_fields: List[str] = None,
        **kwargs,
    ):
        """Show registry in PrettyTable

        Parameters
        ----------
        filters : list or tuple
            List of filter expressions for reigstry content (e.g., filters=["Submitter==USER", "Description contains comstock"])
        max_width
            Max column width in PrettyTable, specify as a single value or as a dict of values by field name
        drop_fields
            List of field names not to show

        """

        if filters:
            logger.info("List registry for: %s", filters)

        table = PrettyTable(title=self.name())
        all_field_names = (
            "ID",
            "Version",
            "Status",
            "Datasets",
            "Date",
            "Submitter",
            "Description",
        )
        # TODO: may want dataset and dataset status to be separate columns
        # TODO: this block can be refactored into base, registry should be in HTML table for notebook.
        if drop_fields is None:
            table.field_names = all_field_names
        else:
            table.field_names = tuple(x for x in all_field_names if x not in drop_fields)

        if max_width is None:
            table._max_width = {
                "ID": 20,
                "Status": 12,
                "Datasets": 30,
                "Date": 10,
                "Description": 30,
            }
        if isinstance(max_width, int):
            table.max_width = max_width
        elif isinstance(max_width, dict):
            table._max_width = max_width

        if filters:
            transformed_filters = transform_and_validate_filters(filters)
        field_to_index = {x: i for i, x in enumerate(table.field_names)}
        rows = []
        for config_id, registry_config in self._registry_configs.items():
            last_reg = registry_config.model.registration_history[0]
            config = self.get_by_id(config_id)

            all_fields = (
                config_id,
                last_reg.version,
                config.model.status.value,
                ",\n".join([f"{x.dataset_id}: {x.status.value}" for x in config.model.datasets]),
                last_reg.date.strftime("%Y-%m-%d %H:%M:%S"),
                last_reg.submitter,
                registry_config.model.description,
            )
            if drop_fields is None:
                row = all_fields
            else:
                row = tuple(
                    y for (x, y) in zip(all_field_names, all_fields) if x not in drop_fields
                )

            if not filters or matches_filters(row, field_to_index, transformed_filters):
                rows.append(row)

        rows.sort(key=lambda x: x[0])
        table.add_rows(rows)
        table.align = "l"
        display_table(table)
