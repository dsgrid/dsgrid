"""Manages the registry for dimension projects"""

import getpass
import logging
import os
import tempfile
from collections import defaultdict
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Type, Union

import json5
import pandas as pd
from prettytable import PrettyTable
from pyspark.sql import DataFrame

from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import (
    DSGInvalidDataset,
    DSGInvalidDimensionMapping,
    DSGValueNotRegistered,
    DSGDuplicateValueRegistered,
    DSGInvalidParameter,
)
from dsgrid.config.dataset_schema_handler_factory import make_dataset_schema_handler
from dsgrid.config.dataset_config import DatasetConfig
from dsgrid.config.dimensions import DimensionModel
from dsgrid.config.dimensions_config import DimensionsConfig, DimensionsConfigModel
from dsgrid.config.dimension_mapping_base import (
    DimensionReferenceModel,
    DimensionMappingReferenceModel,
    DimensionMappingReferenceListModel,
    DimensionMappingType,
)
from dsgrid.config.dimension_mappings_config import (
    DimensionMappingsConfig,
    DimensionMappingsConfigModel,
)
from dsgrid.config.supplemental_dimension import (
    SupplementalDimensionModel,
    SupplementalDimensionsListModel,
)
from dsgrid.config.input_dataset_requirements import (
    InputDatasetDimensionRequirementsListModel,
    InputDatasetListModel,
)
from dsgrid.config.mapping_tables import (
    MappingTableModel,
    MappingTableByNameModel,
    DatasetBaseToProjectMappingTableListModel,
)
from dsgrid.config.project_config import (
    ProjectConfig,
    ProjectConfigModel,
    RequiredDimensionRecordsModel,
    SubsetDimensionGroupModel,
    SubsetDimensionGroupListModel,
)
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
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.utils.spark import (
    models_to_dataframe,
    get_unique_values,
    persist_intermediate_table,
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


logger = logging.getLogger(__name__)


class ProjectRegistryManager(RegistryManagerBase):
    """Manages registered dimension projects."""

    def __init__(
        self,
        path: Path,
        params,
        dataset_manager: DatasetRegistryManager,
        dimension_manager: DimensionRegistryManager,
        dimension_mapping_manager: DimensionMappingRegistryManager,
        db: ProjectRegistryInterface,
    ):
        super().__init__(path, params)
        self._projects: dict[ConfigKey, ProjectConfig] = {}
        self._dataset_mgr = dataset_manager
        self._dimension_mgr = dimension_manager
        self._dimension_mapping_mgr = dimension_mapping_manager
        self._db = db

    @classmethod
    def load(
        cls,
        path: Path,
        fs_interface,
        dataset_manager: DatasetRegistryManager,
        dimension_manager: DimensionRegistryManager,
        dimension_mapping_manager: DimensionMappingRegistryManager,
        db: ProjectRegistryInterface,
    ):
        return cls._load(
            path, fs_interface, dataset_manager, dimension_manager, dimension_mapping_manager, db
        )

    @staticmethod
    def config_class() -> Type:
        return ProjectConfig

    @property
    def db(self) -> ProjectRegistryInterface:
        return self._db

    @db.setter
    def db(self, db: ProjectRegistryInterface):
        self._db = db

    @staticmethod
    def name() -> str:
        return "Projects"

    @property
    def dataset_manager(self) -> DatasetRegistryManager:
        return self._dataset_mgr

    @property
    def dimension_manager(self) -> DimensionRegistryManager:
        return self._dimension_mgr

    @property
    def dimension_mapping_manager(self) -> DimensionMappingRegistryManager:
        return self._dimension_mapping_mgr

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
        subset_dimensions = self._get_subset_dimensions(config)
        config.update_dimensions(base_dimensions, subset_dimensions, supplemental_dimensions)
        config.update_dimension_mappings(base_to_supp_mappings)

    def _get_subset_dimensions(self, config: ProjectConfig):
        subset_dimensions = defaultdict(dict)
        for subset_dim in config.model.dimensions.subset_dimensions:
            selectors = {
                ConfigKey(x.dimension_id, x.version): self._dimension_mgr.get_by_id(
                    x.dimension_id, version=x.version
                )
                for x in subset_dim.selector_references
            }
            subset_dimensions[subset_dim.dimension_type][subset_dim.name] = selectors
        return subset_dimensions

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
        config: ProjectConfig,
        src_dir: Path,
        submitter: str,
        log_message: str,
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
        if model.dimensions.base_dimensions:
            logger.info("Register base dimensions")
            for ref in self._register_dimensions_from_models(
                model.dimensions.base_dimensions,
                context,
                submitter,
                log_message,
            ):
                model.dimensions.base_dimension_references.append(ref)
            model.dimensions.base_dimensions.clear()
        if model.dimensions.subset_dimensions:
            self._register_subset_dimensions(
                model,
                model.dimensions.subset_dimensions,
                context,
                submitter,
                log_message,
            )
        if model.dimensions.supplemental_dimensions:
            logger.info("Register supplemental dimensions")
            self._register_supplemental_dimensions_from_models(
                src_dir,
                model,
                model.dimensions.supplemental_dimensions,
                context,
                submitter,
                log_message,
            )
            model.dimensions.supplemental_dimensions.clear()
        logger.info("Register all-in-one supplemental dimensions")
        self._register_all_in_one_dimensions(
            src_dir,
            model,
            context,
            submitter,
            log_message,
        )

        self._update_dimensions_and_mappings(config)
        for subset_dimension in model.dimensions.subset_dimensions:
            subset_dimension.selectors.clear()
        self._register(config, submitter, log_message)
        context.add_id(RegistryType.PROJECT, config.model.project_id, self)

    def _register_dimensions_from_models(
        self,
        dimensions: list,
        context,
        submitter,
        log_message,
    ):
        dim_model = DimensionsConfigModel(dimensions=dimensions)
        dims_config = DimensionsConfig.load_from_model(dim_model)
        dimension_ids = self._dimension_mgr.register_from_config(
            dims_config, submitter, log_message, context=context
        )
        return self._dimension_mgr.make_dimension_references(dimension_ids)

    def _register_supplemental_dimensions_from_models(
        self,
        src_dir: Path,
        model: ProjectConfigModel,
        dimensions: list,
        context,
        submitter,
        log_message,
    ):
        """Registers supplemental dimensions and creates base-to-supplemental mappings for those
        new dimensions.
        """
        dims = []
        for x in dimensions:
            data = x.serialize()
            data.pop("mapping", None)
            dims.append(DimensionModel(**data))

        refs = self._register_dimensions_from_models(
            dims,
            context,
            submitter,
            log_message,
        )

        model.dimensions.supplemental_dimension_references += refs
        self._register_base_to_supplemental_mappings(
            src_dir,
            model,
            dimensions,
            refs,
            context,
            submitter,
            log_message,
        )

    def _register_base_to_supplemental_mappings(
        self,
        src_dir: Path,
        model: ProjectConfigModel,
        dimensions: list[SupplementalDimensionModel],
        dimension_references: list[DimensionReferenceModel],
        context,
        submitter,
        log_message,
    ):
        base_mapping = {x.dimension_type: x for x in model.dimensions.base_dimension_references}
        mappings = []
        if len(dimensions) != len(dimension_references):
            raise Exception(f"Bug: mismatch in sizes: {dimensions=} {dimension_references=}")

        for dim, ref in zip(dimensions, dimension_references):
            base_dim = base_mapping[dim.dimension_type]
            with in_other_dir(src_dir):
                mapping_model = MappingTableModel.from_pre_registered_model(
                    dim.mapping,
                    base_dim,
                    ref,
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

    def _register_subset_dimensions(
        self,
        model: ProjectConfigModel,
        subset_dimensions: list[SubsetDimensionGroupModel],
        context: RegistrationContext,
        submitter: str,
        log_message: str,
    ):
        logger.info("Register subset dimensions")
        self._register_dimensions_from_subset_dimension_groups(
            subset_dimensions,
            model.dimensions.base_dimension_references,
            context,
            submitter,
            log_message,
        )
        self._register_supplemental_dimensions_from_subset_dimensions(
            model,
            subset_dimensions,
            context,
            submitter,
            log_message,
        )

    def _register_dimensions_from_subset_dimension_groups(
        self,
        subset_dimensions: list[SubsetDimensionGroupModel],
        base_dimension_references: list[DimensionReferenceModel],
        context: RegistrationContext,
        submitter: str,
        log_message: str,
    ):
        """Registers a dimension for each subset specified in the project config's subset
        dimension groups. Appends references to those dimensions to subset_dimensions, which is
        part of the project config.
        """
        with TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            dimensions = []
            subset_refs = {}
            for subset_dimension in subset_dimensions:
                base_dim = None
                for ref in base_dimension_references:
                    if ref.dimension_type == subset_dimension.dimension_type:
                        base_dim = self._dimension_mgr.get_by_id(ref.dimension_id)
                        break
                assert base_dim is not None, subset_dimension
                base_records = base_dim.get_records_dataframe()
                self._check_subset_dimension_consistency(subset_dimension, base_records)
                for selector in subset_dimension.selectors:
                    new_records = base_records.filter(base_records["id"].isin(selector.records))
                    filename = tmp_path / f"{subset_dimension.name}_{selector.name}.csv"
                    new_records.toPandas().to_csv(filename, index=False)
                    dim = DimensionModel(
                        filename=str(filename),
                        name=selector.name,
                        display_name=selector.name,
                        dimension_type=subset_dimension.dimension_type,
                        module=base_dim.model.module,
                        class_name=base_dim.model.class_name,
                        description=selector.description,
                    )
                    dimensions.append(dim)
                    key = (subset_dimension.dimension_type, selector.name)
                    if key in subset_refs:
                        raise Exception(f"Bug: unhandled case of duplicate dimension name: {key=}")
                    subset_refs[key] = subset_dimension

            dim_model = DimensionsConfigModel(dimensions=dimensions)
            dims_config = DimensionsConfig.load_from_model(dim_model)
            dimension_ids = self._dimension_mgr.register_from_config(
                dims_config, submitter, log_message, context=context
            )
            for dimension_id in dimension_ids:
                dim = self._dimension_mgr.get_by_id(dimension_id)
                key = (dim.model.dimension_type, dim.model.name)
                subset_dim = subset_refs[key]
                subset_dim.selector_references.append(
                    DimensionReferenceModel(
                        dimension_id=dimension_id,
                        dimension_type=subset_dim.dimension_type,
                        version="1.0.0",
                    )
                )

    def _check_subset_dimension_consistency(
        self,
        subset_dimension: SubsetDimensionGroupModel,
        base_records: DataFrame,
    ) -> None:
        base_record_ids = get_unique_values(base_records, "id")
        diff = subset_dimension.record_ids.difference(base_record_ids)
        if diff:
            msg = (
                f"subset dimension {subset_dimension.name} "
                f"uses dimension records not present in the base dimension: {diff}"
            )
            raise DSGInvalidParameter(msg)

        diff = base_record_ids.difference(subset_dimension.record_ids)
        if diff:
            msg = (
                f"subset dimension {subset_dimension.name} "
                f"does not list these base dimension records: {diff}"
            )
            raise DSGInvalidParameter(msg)

    def _register_supplemental_dimensions_from_subset_dimensions(
        self,
        model: ProjectConfigModel,
        subset_dimensions: list[SubsetDimensionGroupModel],
        context: RegistrationContext,
        submitter: str,
        log_message: str,
    ):
        """Registers a supplemental dimension for each subset specified in the project config's
        subset dimension groups. Also registers a mapping from the base dimension to each new
        supplemental dimension. Appends references to those dimensions to the project config's
        supplemental_dimension_references list.
        """
        with TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            dimensions = []
            for subset_dimension_group in subset_dimensions:
                if not subset_dimension_group.create_supplemental_dimension:
                    continue
                dimension_type = subset_dimension_group.dimension_type
                base_dim = None
                for ref in model.dimensions.base_dimension_references:
                    if ref.dimension_type == dimension_type:
                        base_dim = self._dimension_mgr.get_by_id(ref.dimension_id)
                        break
                assert base_dim is not None, subset_dimension_group
                records = {"id": [], "name": []}
                mapping_records = []
                dim_record_ids = set()
                # The pydantic validator has already checked consistency of these columns.
                for column in subset_dimension_group.selectors[0].column_values:
                    records[column] = []
                for selector in subset_dimension_group.selectors:
                    records["id"].append(selector.name)
                    records["name"].append(selector.name)
                    if selector.column_values:
                        for column, value in selector.column_values.items():
                            records[column].append(value)
                    for record_id in selector.records:
                        mapping_records.append({"from_id": record_id, "to_id": selector.name})
                        dim_record_ids.add(record_id)

                filename = tmp_path / f"{subset_dimension_group.dimension_query_name}.csv"
                pd.DataFrame(records).to_csv(filename, index=False)

                for record_id in base_dim.get_unique_ids().difference(dim_record_ids):
                    mapping_records.append({"from_id": record_id, "to_id": ""})
                map_record_file = (
                    tmp_path / f"{subset_dimension_group.dimension_query_name}_mapping.csv"
                )
                pd.DataFrame.from_records(mapping_records).to_csv(map_record_file, index=False)

                dim = SupplementalDimensionModel(
                    filename=str(filename),
                    name=subset_dimension_group.name,
                    display_name=subset_dimension_group.display_name,
                    dimension_type=dimension_type,
                    module=base_dim.model.module,
                    class_name=base_dim.model.class_name,
                    description=subset_dimension_group.description,
                    mapping=MappingTableByNameModel(
                        filename=str(map_record_file),
                        mapping_type=DimensionMappingType.MANY_TO_MANY_EXPLICIT_MULTIPLIERS,
                        description=f"Aggregation map for {subset_dimension_group.name}",
                    ),
                )
                dimensions.append(dim)

            self._register_supplemental_dimensions_from_models(
                tmpdir,
                model,
                dimensions,
                context,
                submitter,
                log_message,
            )

    def _register_all_in_one_dimensions(
        self,
        src_dir,
        model,
        context,
        submitter,
        log_message,
    ):
        with TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            new_dimensions = []
            dim_type_to_ref = {
                x.dimension_type: x for x in model.dimensions.base_dimension_references
            }
            # Metric is excluded because fuel_id and unit may not be the same for all records.
            # Time doesn't have records.
            exclude = {DimensionType.METRIC, DimensionType.TIME}
            for dimension_type in (x for x in DimensionType if x not in exclude):
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
                dim_record_file = tmp_path / f"{dt_all_plural}.csv"
                dim_text = f"id,name\n{dt_all_plural},{dim_name_formal}\n"
                dim_record_file.write_text(dim_text)
                map_record_file = tmp_path / f"lookup_{dt_str}_to_{dt_all_plural}.csv"
                with open(map_record_file, "w") as f_out:
                    f_out.write("from_id,to_id\n")
                    for record in dim_config.get_unique_ids():
                        f_out.write(record)
                        f_out.write(",")
                        f_out.write(dt_all_plural)
                        f_out.write("\n")

                with in_other_dir(src_dir):
                    new_dim = SupplementalDimensionModel(
                        filename=str(dim_record_file),
                        name=dim_name,
                        display_name=dim_name_formal,
                        dimension_type=dimension_type,
                        module="dsgrid.dimension.base_models",
                        class_name="DimensionRecordBaseModel",
                        description=dim_name_formal,
                        mapping=MappingTableByNameModel(
                            filename=str(map_record_file),
                            mapping_type=DimensionMappingType.MANY_TO_ONE_AGGREGATION,
                            description=f"Aggregation map for all {dt_str}s",
                        ),
                    )
                new_dimensions.append(new_dim)

            self._register_supplemental_dimensions_from_models(
                src_dir,
                model,
                new_dimensions,
                context,
                submitter,
                log_message,
            )

    @track_timing(timer_stats_collector)
    def _register(self, config: ProjectConfig, submitter: str, log_message: str):
        registration = make_initial_config_registration(submitter, log_message)
        self._run_checks(config)

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
            for field in RequiredDimensionRecordsModel.model_fields:
                # This will check that all dimension record IDs listed in the requirements
                # exist in the project.
                config.get_required_dimension_record_ids(dataset_id, DimensionType(field))

    @track_timing(timer_stats_collector)
    def register_and_submit_dataset(
        self,
        dataset_config_file: Path,
        dataset_path: Path,
        project_id: str,
        submitter: str,
        log_message: str,
        dimension_mapping_file=None,
        dimension_mapping_references_file=None,
        autogen_reverse_supplemental_mappings=None,
    ):
        if not self.has_id(project_id):
            msg = f"{project_id=}"
            raise DSGValueNotRegistered(msg)

        dataset_config = DatasetConfig.load_from_user_path(dataset_config_file, dataset_path)
        dataset_id = dataset_config.model.dataset_id
        config = self.get_by_id(project_id)
        # This will raise an exception if the dataset_id is not part of the project or already
        # registered.
        self._raise_if_not_unregistered(config, dataset_id)

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
        project_id: str,
        dataset_id: str,
        submitter: str,
        log_message: str,
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

    def register_subset_dimensions(
        self,
        project_id: str,
        filename: Path,
        submitter: str,
        log_message: str,
    ):
        """Register new subset dimensions."""
        config = self.get_by_id(project_id)
        subset_model = SubsetDimensionGroupListModel.from_file(filename)
        context = RegistrationContext()
        error_occurred = False
        try:
            self._register_subset_dimensions(
                config.model,
                subset_model.subset_dimensions,
                context,
                submitter,
                log_message,
            )
            self._update_config(config, submitter, VersionUpdateType.PATCH, log_message)
        except Exception:
            error_occurred = True
            raise
        finally:
            context.finalize(error_occurred)

    def register_supplemental_dimensions(
        self,
        project_id: str,
        filename: Path,
        submitter: str,
        log_message: str,
    ):
        """Register new supplemental dimensions."""
        config = self.get_by_id(project_id)
        model = SupplementalDimensionsListModel.from_file(filename)
        context = RegistrationContext()
        error_occurred = False
        try:
            self._register_supplemental_dimensions_from_models(
                filename.parent,
                config.model,
                model.supplemental_dimensions,
                context,
                submitter,
                log_message,
            )
            self._update_config(config, submitter, VersionUpdateType.PATCH, log_message)
        except Exception:
            error_occurred = True
            raise
        finally:
            context.finalize(error_occurred)

    def add_dataset_requirements(
        self,
        project_id: str,
        filename: Path,
        submitter: str,
        log_message: str,
    ):
        """Add requirements for one or more datasets to the project."""
        config = self.get_by_id(project_id)
        model = InputDatasetListModel.from_file(filename)
        existing_ids = {x.dataset_id for x in config.model.datasets}
        for dataset in model.datasets:
            if dataset.dataset_id in existing_ids:
                msg = f"{dataset.dataset_id} is already stored in the project"
                raise DSGInvalidParameter(msg)
            if dataset.status != DatasetRegistryStatus.UNREGISTERED:
                msg = f"New dataset {dataset.dataset_id} status must be unregistered: {dataset.status}"
                raise DSGInvalidParameter(msg)

        config.model.datasets += model.datasets
        self._update_config(config, submitter, VersionUpdateType.PATCH, log_message)

    def replace_dataset_dimension_requirements(
        self,
        project_id: str,
        filename: Path,
        submitter: str,
        log_message: str,
    ):
        """Replace dataset requirements in a project."""
        config = self.get_by_id(project_id)
        model = InputDatasetDimensionRequirementsListModel.from_file(filename)
        for dataset in model.dataset_dimension_requirements:
            found = False
            for i in range(len(config.model.datasets)):
                if config.model.datasets[i].dataset_id == dataset.dataset_id:
                    config.model.datasets[i].required_dimensions = dataset.required_dimensions
                    if config.model.datasets[i].status == DatasetRegistryStatus.REGISTERED:
                        config.model.datasets[i].status = DatasetRegistryStatus.UNREGISTERED
                        logger.info(
                            "Changed dataset %s status to %s in project %s",
                            dataset.dataset_id,
                            config.model.datasets[i].status.value,
                            project_id,
                        )
                        # TODO: When issue #309 is addressed, we need to set all dependent
                        # derived datasets to unregistered also.
                        found = True
                        break
            if not found:
                msg = f"{dataset.dataset_type} is not present in the project config"
                raise DSGInvalidParameter(msg)

        self._update_config(config, submitter, VersionUpdateType.MAJOR, log_message)

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
        self._raise_if_not_unregistered(project_config, dataset_id)
        dataset_config = self._dataset_mgr.get_by_id(dataset_id)

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

    def _raise_if_not_unregistered(self, project_config: ProjectConfig, dataset_id: str) -> None:
        # This will raise if the dataset is not specified in the project.
        dataset_model = project_config.get_dataset(dataset_id)
        status = dataset_model.status
        if status != DatasetRegistryStatus.UNREGISTERED:
            raise DSGDuplicateValueRegistered(
                f"{dataset_id=} cannot be submitted to project={project_config.config_id} with "
                f"{status=}"
            )

    def _register_mappings_from_file(
        self,
        project_config: ProjectConfig,
        dataset_config: DatasetConfig,
        dimension_mapping_file: Path,
        submitter: str,
        log_message: str,
        context: RegistrationContext,
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

    def _try_get_mapping(
        self, project_config: ProjectConfig, from_dim, from_version, to_dim, to_version
    ):
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

        dataset_id = dataset_config.model.dataset_id
        wrap_time = project_config.get_dataset(dataset_id).wrap_time_allowed

        df = dtime.build_time_dataframe()
        dtime._convert_time_to_project_time(df, ptime, wrap_time=wrap_time)

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
        dataset_id = dataset_config.config_id

        with ScratchDirContext(self._params.scratch_dir) as context:
            mapped_dataset_table = handler.make_dimension_association_table()
            project_table = self._make_dimension_associations(project_config, dataset_id, context)
            cols = sorted(project_table.columns)

            diff = project_table.select(*cols).exceptAll(mapped_dataset_table.select(*cols))
            if not diff.rdd.isEmpty():
                self._handle_dimension_association_errors(
                    diff, mapped_dataset_table, dataset_config, project_config.config_id
                )

    def _handle_dimension_association_errors(
        self, diff, mapped_dataset_table, dataset_config, project_id
    ):
        dataset_id = dataset_config.config_id
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
        diff_counts = {x: diff.select(x).distinct().count() for x in diff.columns}
        diff.unpersist()
        mapped_dataset_table.cache()
        dataset_counts = {}
        for col in diff.columns:
            dataset_counts[col] = mapped_dataset_table.select(col).distinct().count()
            if dataset_counts[col] != diff_counts[col]:
                logger.error(
                    "Error contributor: column=%s dataset_distinct_count=%s missing_distinct_count=%s",
                    col,
                    dataset_counts[col],
                    diff_counts[col],
                )
        mapped_dataset_table.unpersist()
        raise DSGInvalidDataset(
            f"Dataset {dataset_config.config_id} is missing required dimension records. "
            "Please look in the log file for more information."
        )

    @track_timing(timer_stats_collector)
    def _make_dimension_associations(
        self,
        config: ProjectConfig,
        dataset_id: str,
        context: ScratchDirContext,
    ):
        logger.info("Make dimension association table for %s", dataset_id)
        df = config.make_dimension_association_table(dataset_id, context)
        df = persist_intermediate_table(df, context)
        logger.info("Wrote dimension associations for dataset %s", dataset_id)
        return df

    def update_from_file(
        self, config_file, config_id, submitter, update_type, log_message, version
    ):
        config = ProjectConfig.load(config_file)
        self._update_dimensions_and_mappings(config)
        self._check_update(config, config_id, version)
        self.update(config, update_type, log_message, submitter=submitter)

    @track_timing(timer_stats_collector)
    def update(
        self, config: ProjectConfig, update_type: VersionUpdateType, log_message, submitter=None
    ):
        submitter = submitter or getpass.getuser()
        return self._update(config, submitter, update_type, log_message)

    def _update(
        self,
        config: ProjectConfig,
        submitter: str,
        update_type: VersionUpdateType,
        log_message: str,
    ):
        old_config = self.get_by_id(config.model.project_id)
        checker = ProjectUpdateChecker(old_config.model, config.model)
        checker.run()
        self._run_checks(config)

        new_config = self._update_config(config, submitter, update_type, log_message)
        return new_config.model

    def _update_config(self, config, submitter, update_type, log_message):
        old_version = config.model.version
        old_key = ConfigKey(config.config_id, old_version)
        model = super()._update_config(config, submitter, update_type, log_message)
        new_config = ProjectConfig(model)
        self._update_dimensions_and_mappings(new_config)
        new_key = ConfigKey(new_config.model.project_id, new_config.model.version)
        self._projects.pop(old_key, None)
        self._projects[new_key] = new_config
        return new_config

    def remove(self, project_id: str):
        self.db.delete_all(project_id)
        for key in [x for x in self._projects if x.id == project_id]:
            self._projects.pop(key)

        logger.info("Removed %s from the registry.", project_id)

    def show(
        self,
        filters: list[str] | None = None,
        max_width: Union[int, dict] | None = None,
        drop_fields: list[str] | None = None,
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
