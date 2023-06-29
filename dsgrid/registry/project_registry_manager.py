"""Manages the registry for dimension projects"""

import getpass
import itertools
import logging
import os
import shutil
import tempfile
from pathlib import Path
from typing import Union

import json5
from prettytable import PrettyTable

from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import (
    DSGInvalidDataset,
    DSGInvalidDimensionMapping,
    DSGValueNotRegistered,
    DSGDuplicateValueRegistered,
)
from dsgrid.config.dataset_schema_handler_factory import make_dataset_schema_handler
from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.config.dimension_association_manager import remove_project_dimension_associations
from dsgrid.config.dimensions import DimensionModel, DimensionReferenceByNameModel
from dsgrid.config.dimensions_config import DimensionsConfig, DimensionsConfigModel
from dsgrid.config.dimension_mapping_base import (
    DimensionMappingReferenceModel,
    DimensionMappingReferenceListModel,
    DimensionMappingType,
)
from dsgrid.config.dimension_mappings_config import (
    DimensionMappingsConfig,
    DimensionMappingsConfigModel,
)
from dsgrid.config.mapping_tables import (
    MappingTableModel,
    MappingTableByNameModel,
    DatasetBaseToProjectMappingTableListModel,
)
from dsgrid.config.project_config import ProjectConfig, RequiredDimensionRecordsModel
from dsgrid.project import Project
from dsgrid.registry.common import (
    make_initial_config_registration,
    ConfigKey,
    DatasetRegistryStatus,
    ProjectRegistryStatus,
)
from dsgrid.utils.timing import track_timing, timer_stats_collector
from dsgrid.utils.files import load_data, in_other_dir
from dsgrid.utils.filters import transform_and_validate_filters, matches_filters
from dsgrid.utils.spark import (
    models_to_dataframe,
    get_unique_values,
    restart_spark_with_custom_conf,
)
from dsgrid.utils.utilities import check_uniqueness, display_table
from .common import (
    VersionUpdateType,
    RegistryType,
)
from .registration_context import RegistrationContext
from .project_update_checker import ProjectUpdateChecker
from .dataset_registry_manager import DatasetRegistryManager
from .dimension_mapping_registry_manager import DimensionMappingRegistryManager
from .dimension_registry_manager import DimensionRegistryManager
from .registry_interface import ProjectRegistryInterface
from .registry_manager_base import RegistryManagerBase


_SUPPLEMENTAL_TMP_DIR = "__tmp_supplemental__"

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
        cls, path, fs_interface, dataset_manager, dimension_manager, dimension_mapping_manager, db
    ):
        mgr = cls._load(path, fs_interface)
        mgr.dataset_manager = dataset_manager
        mgr.dimension_manager = dimension_manager
        mgr.dimension_mapping_manager = dimension_mapping_manager
        mgr.db = db
        return mgr

    @staticmethod
    def config_class():
        return ProjectConfig

    @property
    def db(self) -> ProjectRegistryInterface:
        return self._db

    @db.setter
    def db(self, db: ProjectRegistryInterface):
        self._db = db

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
    def dimension_mapping_manager(self, val: DimensionMappingRegistryManager):
        self._dimension_mapping_mgr = val

    def finalize_registration(self, config_ids, error_occurred):
        assert len(config_ids) == 1
        project_id = config_ids[0]

        if error_occurred:
            logger.info("Remove intermediate project after error")
            self.remove(project_id)

    def get_by_id(self, project_id, version=None):
        if version is None:
            version = self._db.get_latest_version(project_id)

        key = ConfigKey(project_id, version)
        project = self._projects.get(key)
        if project is not None:
            return project

        if version is None:
            model = self.db.get_latest(project_id)
        else:
            model = self.db.get_by_version(project_id, version)

        config = ProjectConfig(model)
        self._update_dimensions_and_mappings(config)
        self._projects[key] = config
        return config

    def _update_dimensions_and_mappings(self, config: ProjectConfig):
        base_dimensions = self._dimension_mgr.load_dimensions(
            config.model.dimensions.base_dimension_references
        )
        supplemental_dimensions = self._dimension_mgr.load_dimensions(
            config.model.dimensions.supplemental_dimension_references
        )
        base_to_supp_mappings = self._dimension_mapping_mgr.load_dimension_mappings(
            config.model.dimension_mappings.base_to_supplemental_references
        )
        config.update_dimensions(base_dimensions, supplemental_dimensions)
        config.update_dimension_mappings(base_to_supp_mappings)

    def load_project(self, project_id: str, version=None) -> Project:
        """Load a project from the registry.

        Parameters
        ----------
        project_id : str
        version : str

        Returns
        -------
        Project
        """
        dataset_manager = self._dataset_mgr
        config = self.get_by_id(project_id, version=version)

        dataset_configs = {}
        for dataset_id in config.list_registered_dataset_ids():
            dataset_config = dataset_manager.get_by_id(dataset_id)
            dataset_configs[dataset_id] = dataset_config

        return Project(
            config,
            config.model.version,
            dataset_configs,
            self._dimension_mgr,
            self._dimension_mapping_mgr,
        )

    def register(
        self,
        config_file,
        submitter,
        log_message,
    ):
        config = ProjectConfig.load(config_file)
        src_dir = config_file.parent
        return self.register_from_config(config, src_dir, submitter, log_message)

    def register_from_config(
        self,
        config,
        src_dir,
        submitter,
        log_message,
        context=None,
    ):
        error_occurred = False
        context = RegistrationContext()

        try:
            self._register_project_and_dimensions(
                config,
                src_dir,
                submitter,
                log_message,
                context,
            )
        except Exception:
            error_occurred = True
            raise
        finally:
            context.finalize(error_occurred)

    def _register_project_and_dimensions(
        self,
        config: ProjectConfig,
        src_dir: Path,
        submitter: str,
        log_message: str,
        context: RegistrationContext,
    ):
        model = config.model
        logger.info("Start registration of project %s", model.project_id)
        self._check_if_already_registered(model.project_id)
        tmp_dirs = []
        try:
            if model.dimensions.base_dimensions:
                logger.info("Register base dimensions")
                self._register_dimensions_from_models(
                    model.dimensions.base_dimensions,
                    model.dimensions.base_dimension_references,
                    context,
                    submitter,
                    log_message,
                )
            if model.dimensions.supplemental_dimensions:
                logger.info("Register supplemental dimensions")
                self._register_dimensions_from_models(
                    model.dimensions.supplemental_dimensions,
                    model.dimensions.supplemental_dimension_references,
                    context,
                    submitter,
                    log_message,
                )
            logger.info("Register all-in-one supplemental dimensions")
            supp_dir = src_dir / _SUPPLEMENTAL_TMP_DIR
            assert not supp_dir.exists(), f"{supp_dir} exists"
            supp_dir.mkdir()
            tmp_dirs.append(supp_dir)
            model.dimension_mappings.base_to_supplemental += self._register_all_in_one_dimensions(
                src_dir,
                supp_dir,
                model,
                context,
                submitter,
                log_message,
            )
            if model.dimension_mappings.base_to_supplemental:
                self._register_base_to_supplemental_mappings(
                    src_dir,
                    model,
                    context,
                    submitter,
                    log_message,
                )

            self._update_dimensions_and_mappings(config)
            self._register(config, submitter, log_message)
            context.add_id(RegistryType.PROJECT, config.model.project_id, self)
        finally:
            for directory in tmp_dirs:
                shutil.rmtree(directory)

    def _register_base_to_supplemental_mappings(
        self,
        src_dir,
        model,
        context,
        submitter,
        log_message,
    ):
        mappings = []
        name_mapping = {}
        for ref in itertools.chain(
            model.dimensions.base_dimension_references,
            model.dimensions.supplemental_dimension_references,
        ):
            dim = self._dimension_mgr.get_by_id(ref.dimension_id)
            name_mapping[(dim.model.name, ref.dimension_type)] = ref

        for mapping in model.dimension_mappings.base_to_supplemental:
            from_dim = name_mapping[
                (mapping.from_dimension.name, mapping.from_dimension.dimension_type)
            ]
            to_dim = name_mapping[(mapping.to_dimension.name, mapping.to_dimension.dimension_type)]

            with in_other_dir(src_dir):
                mapping_model = MappingTableModel.from_pre_registered_model(
                    mapping, from_dim, to_dim
                )
                mappings.append(mapping_model)

        mapping_config = DimensionMappingsConfig.load_from_model(
            DimensionMappingsConfigModel(mappings=mappings),
        )
        mapping_ids = self._dimension_mapping_mgr.register_from_config(
            mapping_config, submitter, log_message, context=context
        )
        model.dimension_mappings.base_to_supplemental_references += (
            self._dimension_mapping_mgr.make_dimension_mapping_references(mapping_ids)
        )
        model.dimension_mappings.base_to_supplemental.clear()

    def _register_dimensions_from_models(
        self,
        dimensions: list,
        dimension_references,
        context,
        submitter,
        log_message,
    ):
        dim_model = DimensionsConfigModel(dimensions=dimensions)
        dims_config = DimensionsConfig.load_from_model(dim_model)
        dimension_ids = self._dimension_mgr.register_from_config(
            dims_config, submitter, log_message, context=context
        )
        # Order of the next two is required for Pydantic validation.
        dimension_references += self._dimension_mgr.make_dimension_references(dimension_ids)
        dimensions.clear()
        return dimension_references

    def _register_all_in_one_dimensions(
        self,
        src_dir,
        supp_dir,
        model,
        context,
        submitter,
        log_message,
    ):
        new_dimensions = []
        new_mappings = {}
        dim_type_to_ref = {x.dimension_type: x for x in model.dimensions.base_dimension_references}
        for dimension_type in (x for x in DimensionType if x != DimensionType.TIME):
            dim_ref = dim_type_to_ref[dimension_type]
            dim_config = self._dimension_mgr.get_by_id(dim_ref.dimension_id)
            dt_str = dimension_type.value
            if dt_str.endswith("y"):
                dt_plural = dt_str[:-1] + "ies"
            else:
                dt_plural = dt_str + "s"
            dt_all_plural = f"all_{dt_plural}"
            dim_name = f"all_{model.project_id}_{dt_plural}"
            dim_name_formal = f"All {dt_plural.title()}"
            dim_record_file = supp_dir / f"{dt_all_plural}.csv"
            dim_text = f"id,name\n{dt_all_plural},{dim_name_formal}\n"
            dim_record_file.write_text(dim_text)
            map_record_file = supp_dir / f"lookup_{dt_str}_to_{dt_all_plural}.csv"
            with open(map_record_file, "w") as f_out:
                f_out.write("from_id,to_id\n")
                for record in dim_config.get_unique_ids():
                    f_out.write(record)
                    f_out.write(",")
                    f_out.write(dt_all_plural)
                    f_out.write("\n")

            with in_other_dir(src_dir):
                new_dim = DimensionModel(
                    filename=str(Path(_SUPPLEMENTAL_TMP_DIR) / dim_record_file.name),
                    name=dim_name,
                    display_name=dim_name_formal,
                    dimension_type=dimension_type,
                    module="dsgrid.dimension.base_models",
                    class_name="DimensionRecordBaseModel",
                    description=dim_name_formal,
                )
            new_dimensions.append(new_dim)
            with in_other_dir(src_dir):
                new_mappings[dimension_type] = MappingTableByNameModel(
                    filename=str(Path(_SUPPLEMENTAL_TMP_DIR) / map_record_file.name),
                    mapping_type=DimensionMappingType.MANY_TO_ONE_AGGREGATION,
                    from_dimension=DimensionReferenceByNameModel(
                        dimension_type=dimension_type,
                        name=dim_config.model.name,
                    ),
                    to_dimension=DimensionReferenceByNameModel(
                        dimension_type=dimension_type,
                        name=dim_name,
                    ),
                    description=f"Aggregation map for all {dt_str}s",
                )

        self._register_dimensions_from_models(
            new_dimensions,
            model.dimensions.supplemental_dimension_references,
            context,
            submitter,
            log_message,
        )

        return list(new_mappings.values())

    @track_timing(timer_stats_collector)
    def _register(self, config: ProjectConfig, submitter, log_message):
        registration = make_initial_config_registration(submitter, log_message)
        self._run_checks(config)
        remove_project_dimension_associations(config.model.project_id)

        model = self.db.insert(config.model, registration)
        logger.info(
            "%s Registered project %s with version=%s",
            self._log_offline_mode_prefix(),
            model.project_id,
            registration.version,
        )

    @track_timing(timer_stats_collector)
    def _run_checks(self, config: ProjectConfig):
        dims = [x for x in config.iter_dimensions()]
        check_uniqueness((x.model.name for x in dims), "dimension name")
        check_uniqueness((x.model.display_name for x in dims), "dimension display name")
        check_uniqueness(
            (getattr(x.model, "cls") for x in config.model.dimensions.base_dimensions),
            "dimension cls",
        )
        for dataset_id in config.list_unregistered_dataset_ids():
            for field in RequiredDimensionRecordsModel.__fields__:
                # This will check that all dimension record IDs listed in the requirements
                # exist in the project.
                config.get_required_dimension_record_ids(dataset_id, DimensionType(field))

    @track_timing(timer_stats_collector)
    def register_and_submit_dataset(
        self,
        dataset_config_file,
        dataset_path,
        project_id,
        submitter,
        log_message,
        dimension_mapping_file=None,
        dimension_mapping_references_file=None,
        autogen_reverse_supplemental_mappings=None,
    ):
        context = RegistrationContext()
        error_occurred = False
        try:
            self._dataset_mgr.register(
                dataset_config_file,
                dataset_path,
                submitter,
                log_message,
                context=context,
            )
            self.submit_dataset(
                project_id,
                context.get_ids(RegistryType.DATASET)[0],
                submitter,
                log_message,
                dimension_mapping_file=dimension_mapping_file,
                dimension_mapping_references_file=dimension_mapping_references_file,
                autogen_reverse_supplemental_mappings=autogen_reverse_supplemental_mappings,
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
        dimension_mapping_references_file=None,
        autogen_reverse_supplemental_mappings=None,
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
        dimension_mapping_references_file : Path or None
        autogen_reverse_supplemental_mappings : set[DimensionType] or None
            Dimensions on which to attempt create reverse mappings from supplemental dimensions.
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
                dimension_mapping_references_file,
                autogen_reverse_supplemental_mappings,
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
        dimension_mapping_references_file,
        autogen_reverse_supplemental_mappings,
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

        # Issue #241
        # self._check_dataset_time_interval_type(project_config, dataset_config)

        references = []
        if dimension_mapping_file is not None:
            references += self._register_mappings_from_file(
                project_config,
                dataset_config,
                dimension_mapping_file,
                submitter,
                log_message,
                context,
            )
        if dimension_mapping_references_file is not None:
            for ref in DimensionMappingReferenceListModel.load(
                dimension_mapping_references_file
            ).references:
                if not self.dimension_mapping_manager.has_id(ref.mapping_id, version=ref.version):
                    raise DSGValueNotRegistered(f"mapping_id={ref.mapping_id}")
                references.append(ref)

        if autogen_reverse_supplemental_mappings:
            references += self._auto_register_reverse_supplemental_mappings(
                project_config,
                dataset_config,
                references,
                autogen_reverse_supplemental_mappings,
                submitter,
                log_message,
                context,
            )

        self._submit_dataset(project_config, dataset_config, submitter, log_message, references)

    def _register_mappings_from_file(
        self,
        project_config,
        dataset_config,
        dimension_mapping_file,
        submitter,
        log_message,
        context,
    ):
        references = []
        src_dir = dimension_mapping_file.parent
        mappings = DatasetBaseToProjectMappingTableListModel(
            **load_data(dimension_mapping_file)
        ).mappings
        dataset_mapping = {x.dimension_type: x for x in dataset_config.model.dimension_references}
        project_mapping = {
            x.dimension_type: x for x in project_config.model.dimensions.base_dimension_references
        }
        mapping_tables = []
        for mapping in mappings:
            with in_other_dir(src_dir):
                mapping_table = MappingTableModel.from_pre_registered_model(
                    mapping,
                    dataset_mapping[mapping.dimension_type],
                    project_mapping[mapping.dimension_type],
                )
            mapping_tables.append(mapping_table)

        mappings_config = DimensionMappingsConfig.load_from_model(
            DimensionMappingsConfigModel(mappings=mapping_tables)
        )
        mapping_ids = self._dimension_mapping_mgr.register_from_config(
            mappings_config, submitter, log_message, context=context
        )
        for mapping_id in mapping_ids:
            mapping_config = self._dimension_mapping_mgr.get_by_id(mapping_id)
            references.append(
                DimensionMappingReferenceModel(
                    from_dimension_type=mapping_config.model.from_dimension.dimension_type,
                    to_dimension_type=mapping_config.model.to_dimension.dimension_type,
                    mapping_id=mapping_id,
                    version=str(self._dimension_mapping_mgr.get_latest_version(mapping_id)),
                )
            )

        return references

    def _auto_register_reverse_supplemental_mappings(
        self,
        project_config: ProjectConfig,
        dataset_config: DatasetConfig,
        mapping_references: list[DimensionMappingReferenceModel],
        autogen_reverse_supplemental_mappings: set[str],
        submitter,
        log_message,
        context,
    ):
        references = []
        p_model = project_config.model
        p_supp_dim_ids = {
            x.dimension_id for x in p_model.dimensions.supplemental_dimension_references
        }
        d_dim_from_ids = set()
        for ref in mapping_references:
            mapping_config = self._dimension_mapping_mgr.get_by_id(ref.mapping_id)
            d_dim_from_ids.add(mapping_config.model.from_dimension.dimension_id)

        needs_mapping = []
        for dim in dataset_config.model.dimension_references:
            if (
                dim.dimension_type in autogen_reverse_supplemental_mappings
                and dim.dimension_id in p_supp_dim_ids
                and dim.dimension_id not in d_dim_from_ids
            ):
                needs_mapping.append((dim.dimension_id, dim.version))
            # else:
            #     This dimension is the same as a project base dimension.
            #     or
            #     The dataset may only need to provide a subset of records, and those are
            #     checked in the dimension association table.

        if len(needs_mapping) != len(autogen_reverse_supplemental_mappings):
            raise DSGInvalidDimensionMapping(
                f"Mappings to autgen [{needs_mapping}] does not match user-specified "
                f"autogen_reverse_supplemental_mappings={autogen_reverse_supplemental_mappings}"
            )

        new_mappings = []
        for from_id, from_version in needs_mapping:
            from_dim = self._dimension_mgr.get_by_id(from_id, version=from_version)
            to_dim, to_version = project_config.get_base_dimension_and_version(
                from_dim.model.dimension_type
            )
            mapping, version = self._try_get_mapping(
                project_config, from_dim, from_version, to_dim, to_version
            )
            if mapping is None:
                p_mapping, _ = self._try_get_mapping(
                    project_config, to_dim, to_version, from_dim, from_version
                )
                assert (
                    p_mapping is not None
                ), f"from={to_dim.model.dimension_id} to={from_dim.model.dimension_id}"
                records = models_to_dataframe(p_mapping.model.records)
                fraction_vals = get_unique_values(records, "from_fraction")
                if len(fraction_vals) != 1 and next(iter(fraction_vals)) != 1.0:
                    raise DSGInvalidDimensionMapping(
                        f"Cannot auto-generate a dataset-to-project mapping from from a project "
                        "supplemental dimension unless the from_fraction column is empty or only "
                        f"has values of 1.0: {p_mapping.model.mapping_id} - {fraction_vals}"
                    )
                reverse_records = (
                    records.drop("from_fraction")
                    .selectExpr("to_id AS from_id", "from_id AS to_id")
                    .toPandas()
                )
                dst = Path(tempfile.gettempdir()) / f"reverse_{p_mapping.config_id}.csv"
                # Use pandas because spark creates a CSV directory.
                reverse_records.to_csv(dst, index=False)
                dimension_type = from_dim.model.dimension_type.value
                new_mappings.append(
                    {
                        "description": f"Maps {dataset_config.config_id} {dimension_type} to project",
                        "dimension_type": dimension_type,
                        "file": str(dst),
                        "mapping_type": DimensionMappingType.MANY_TO_MANY_EXPLICIT_MULTIPLIERS.value,
                    }
                )
            else:
                reference = DimensionMappingReferenceModel(
                    from_dimension_type=from_dim.model.dimension_type,
                    to_dimension_type=from_dim.model.dimension_type,
                    mapping_id=mapping.model.mapping_id,
                    version=version,
                )
                references.append(reference)

        if new_mappings:
            # We don't currently have a way to register a single dimension mapping. It would be
            # better to register these mappings directly. But, this code was already here.
            mapping_file = Path(tempfile.gettempdir()) / "dimension_mappings.json5"
            mapping_file.write_text(json5.dumps({"mappings": new_mappings}, indent=2))
            to_delete = [mapping_file] + [x["file"] for x in new_mappings]
            try:
                references += self._register_mappings_from_file(
                    project_config,
                    dataset_config,
                    mapping_file,
                    submitter,
                    log_message,
                    context,
                )
            finally:
                for filename in to_delete:
                    Path(filename).unlink()

        return references

    def _try_get_mapping(self, project_config, from_dim, from_version, to_dim, to_version):
        dimension_type = from_dim.model.dimension_type
        for ref in project_config.model.dimension_mappings.base_to_supplemental_references:
            if (
                ref.from_dimension_type == dimension_type
                and ref.to_dimension_type == dimension_type
            ):
                mapping_config = self._dimension_mapping_mgr.get_by_id(ref.mapping_id)
                if (
                    mapping_config.model.from_dimension.dimension_id == from_dim.model.dimension_id
                    and mapping_config.model.from_dimension.version == from_version
                    and mapping_config.model.to_dimension.dimension_id == to_dim.model.dimension_id
                    and mapping_config.model.to_dimension.version == to_version
                ):
                    return mapping_config, ref.version

        return None, None

    def _submit_dataset(
        self,
        project_config: ProjectConfig,
        dataset_config: DatasetConfig,
        submitter: str,
        log_message: str,
        mapping_references: list[DimensionMappingReferenceModel],
    ):
        project_config.add_dataset_dimension_mappings(dataset_config, mapping_references)
        if os.environ.get("__DSGRID_SKIP_CHECK_DATASET_TO_PROJECT_MAPPING__") is not None:
            logger.warning("Skip dataset-to-project mapping checks")
        else:
            # This operation can be very problematic if there are many executors and runs much
            # faster with a single executor on a single core.
            # The dynamic allocation settings will only take effect if the worker was started
            # with spark.shuffle.service.enabled=true
            conf = {
                "spark.dynamicAllocation.enabled": True,
                "spark.dynamicAllocation.shuffleTracking.enabled": True,
                "spark.dynamicAllocation.maxExecutors": 1,
                "spark.executor.cores": 1,
            }
            with restart_spark_with_custom_conf(conf):
                self._check_dataset_base_to_project_base_mappings(
                    project_config,
                    dataset_config,
                    mapping_references,
                )

        dataset_model = project_config.get_dataset(dataset_config.model.dataset_id)

        dataset_model.mapping_references = mapping_references
        dataset_model.status = DatasetRegistryStatus.REGISTERED
        if project_config.are_all_datasets_submitted():
            new_status = ProjectRegistryStatus.COMPLETE
        else:
            new_status = ProjectRegistryStatus.IN_PROGRESS
        project_config.set_status(new_status)
        model = self._update(
            project_config,
            submitter,
            VersionUpdateType.MINOR,
            log_message,
            clear_cached_association_tables=False,
        )

        logger.info(
            "%s Registered dataset %s with version=%s in project %s",
            self._log_offline_mode_prefix(),
            dataset_config.model.dataset_id,
            model.version,
            model.project_id,
        )

    def _check_dataset_time_interval_type(
        self, project_config: ProjectConfig, dataset_config: DatasetConfig
    ):
        dtime = dataset_config.get_dimension(DimensionType.TIME)
        ptime = project_config.get_base_dimension(DimensionType.TIME)

        df = dtime.build_time_dataframe()
        dtime._convert_time_to_project_time_interval(df=df, project_time_dim=ptime, wrap_time=True)

    @track_timing(timer_stats_collector)
    def _check_dataset_base_to_project_base_mappings(
        self,
        project_config: ProjectConfig,
        dataset_config: DatasetConfig,
        mapping_references: list[DimensionMappingReferenceModel],
    ):
        """Check that a dataset has all project-required dimension records."""
        logger.info("Check dataset-base-to-project-base dimension mappings.")
        handler = make_dataset_schema_handler(
            dataset_config,
            self._dimension_mgr,
            self._dimension_mapping_mgr,
            mapping_references,
            project_time_dim=project_config.get_base_dimension(DimensionType.TIME),
        )
        pivoted_dimension = handler.get_pivoted_dimension_type()
        exclude_dims = {DimensionType.TIME, pivoted_dimension}

        dim_table = handler.get_unique_dimension_rows().drop("id")
        dataset_id = dataset_config.config_id
        assoc_table = project_config.load_dimension_associations(
            dataset_id, pivoted_dimension=pivoted_dimension, try_load_cache=False
        )

        dimension_types = set(DimensionType).difference(exclude_dims)
        cols = [x.value for x in dimension_types]
        project_table = assoc_table.select(*cols).distinct()
        diff = project_table.exceptAll(dim_table.select(*cols).distinct())
        if not diff.rdd.isEmpty():
            project_id = project_config.config_id
            out_file = f"{dataset_id}__{project_id}__missing_dimension_record_combinations.csv"
            diff.cache()
            diff.write.options(header=True).mode("overwrite").csv(out_file)
            logger.error(
                "Dataset %s is missing required dimension records from project %s. "
                "Recorded missing records in %s",
                dataset_id,
                project_id,
                out_file,
            )
            diff_counts = {}
            for col in diff.columns:
                diff_counts[col] = diff.select(col).distinct().count()
            diff.unpersist()
            dim_table.cache()
            dataset_counts = {}
            for col in diff.columns:
                dataset_counts[col] = dim_table.select(col).distinct().count()
                if dataset_counts[col] != diff_counts[col]:
                    logger.error(
                        "Error contributor: column=%s dataset_distinct_count=%s missing_distinct_count=%s",
                        col,
                        dataset_counts[col],
                        diff_counts[col],
                    )
            dim_table.unpersist()

            raise DSGInvalidDataset(
                f"Dataset {dataset_config.config_id} is missing required dimension records"
            )
        project_pivoted_ids = project_config.get_required_dimension_record_ids(
            dataset_id, pivoted_dimension
        )
        self._check_pivoted_dimension_columns(handler, project_pivoted_ids, dataset_id)

    @staticmethod
    def _check_pivoted_dimension_columns(handler, project_pivoted_ids, dataset_id):
        """pivoted dimension record is the same as project's unless a relevant association is provided."""
        logger.info("Check pivoted dimension columns.")
        d_dim_ids = handler.get_pivoted_dimension_columns_mapped_to_project()
        pivoted_dim = handler.get_pivoted_dimension_type()

        if d_dim_ids.symmetric_difference(project_pivoted_ids):
            raise DSGInvalidDataset(
                f"Mismatch between project and {dataset_id} dataset pivoted {pivoted_dim.value} dimension, "
                "please double-check data, and any relevant association_table and dimension_mapping. "
                f"\n - Invalid column(s) in {dataset_id} load data according to project: {d_dim_ids.difference(project_pivoted_ids)}"
                f"\n - Missing column(s) in {dataset_id} load data according to project: {project_pivoted_ids.difference(d_dim_ids)}"
            )

    def update_from_file(
        self, config_file, config_id, submitter, update_type, log_message, version
    ):
        config = ProjectConfig.load(config_file)
        self._update_dimensions_and_mappings(config)
        self._check_update(config, config_id, version)
        self.update(config, update_type, log_message, submitter=submitter)

    @track_timing(timer_stats_collector)
    def update(self, config, update_type, log_message, submitter=None):
        if submitter is None:
            submitter = getpass.getuser()
        # Until we have robust checking of changes to dimension requirements, always clear
        # the cached association tables in a user update.
        return self._update(
            config, submitter, update_type, log_message, clear_cached_association_tables=True
        )

    def _update(
        self, config, submitter, update_type, log_message, clear_cached_association_tables
    ):
        if clear_cached_association_tables:
            remove_project_dimension_associations(config.model.project_id)
        old_config = self.get_by_id(config.model.project_id)
        old_version = old_config.model.version
        checker = ProjectUpdateChecker(old_config.model, config.model)
        checker.run()
        self._run_checks(config)

        old_key = ConfigKey(config.config_id, old_version)
        model = self._update_config(config, submitter, update_type, log_message)
        new_config = ProjectConfig(model)
        self._update_dimensions_and_mappings(new_config)
        new_key = ConfigKey(new_config.model.project_id, new_config.model.version)
        self._projects.pop(old_key, None)
        self._projects[new_key] = new_config

        return model

    def remove(self, project_id):
        self.db.delete_all(project_id)
        for key in [x for x in self._projects if x.id == project_id]:
            self._projects.pop(key)

        logger.info("Removed %s from the registry.", project_id)

    def show(
        self,
        filters: list[str] = None,
        max_width: Union[int, dict] = None,
        drop_fields: list[str] = None,
        return_table: bool = False,
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
        for model in self.db.iter_models():
            registration = self.db.get_registration(model)
            all_fields = (
                model.project_id,
                model.version,
                model.status.value,
                ",\n".join([f"{x.dataset_id}: {x.status.value}" for x in model.datasets]),
                registration.date.strftime("%Y-%m-%d %H:%M:%S"),
                registration.submitter,
                registration.log_message,
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
        if return_table:
            return table
        display_table(table)
