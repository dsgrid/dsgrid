"""Manages the registry for dimension projects"""

import logging
import os
import tempfile
from collections import defaultdict
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Type, Union

from dsgrid.utils.dataset import handle_dimension_association_errors
import json5
import pandas as pd
from prettytable import PrettyTable
from sqlalchemy import Connection

from dsgrid.config.dimension_config import (
    DimensionBaseConfig,
    DimensionBaseConfigWithFiles,
)
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import (
    DSGInvalidDataset,
    DSGInvalidDimension,
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
    DatasetBaseDimensionNamesModel,
    ProjectConfig,
    ProjectConfigModel,
    RequiredBaseDimensionModel,
    RequiredDimensionRecordsByTypeModel,
    RequiredDimensionRecordsModel,
    SubsetDimensionGroupModel,
    SubsetDimensionGroupListModel,
)
from dsgrid.project import Project
from dsgrid.registry.common import (
    ConfigKey,
    DatasetRegistryStatus,
    ProjectRegistryStatus,
    RegistryManagerParams,
)
from dsgrid.spark.functions import (
    cache,
    except_all,
    is_dataframe_empty,
    unpersist,
)
from dsgrid.spark.types import (
    DataFrame,
    F,
    use_duckdb,
)
from dsgrid.utils.timing import track_timing, timer_stats_collector
from dsgrid.utils.files import load_data, in_other_dir
from dsgrid.utils.filters import transform_and_validate_filters, matches_filters
from dsgrid.utils.scratch_dir_context import ScratchDirContext
from dsgrid.utils.spark import (
    models_to_dataframe,
    get_unique_values,
    persist_table,
    read_dataframe,
)
from dsgrid.utils.utilities import check_uniqueness, display_table
from dsgrid.registry.registry_interface import ProjectRegistryInterface
from .common import (
    VersionUpdateType,
    RegistryType,
)
from .registration_context import RegistrationContext
from .project_update_checker import ProjectUpdateChecker
from .dataset_registry_manager import DatasetRegistryManager
from .dimension_mapping_registry_manager import DimensionMappingRegistryManager
from .dimension_registry_manager import DimensionRegistryManager
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
        params: RegistryManagerParams,
        dataset_manager: DatasetRegistryManager,
        dimension_manager: DimensionRegistryManager,
        dimension_mapping_manager: DimensionMappingRegistryManager,
        db: ProjectRegistryInterface,
    ):
        return cls._load(
            path,
            params,
            dataset_manager,
            dimension_manager,
            dimension_mapping_manager,
            db,
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

    def get_by_id(
        self,
        project_id: str,
        version: str | None = None,
        conn: Connection | None = None,
    ) -> ProjectConfig:
        if version is None:
            assert self._db is not None
            version = self._db.get_latest_version(conn, project_id)

        key = ConfigKey(project_id, version)
        project = self._projects.get(key)
        if project is not None:
            return project

        if version is None:
            model = self.db.get_latest(conn, project_id)
        else:
            model = self.db.get_by_version(conn, project_id, version)

        assert isinstance(model, ProjectConfigModel)
        config = ProjectConfig(model)
        self._update_dimensions_and_mappings(conn, config)
        self._projects[key] = config
        return config

    def _update_dimensions_and_mappings(self, conn: Connection | None, config: ProjectConfig):
        base_dimensions = self._dimension_mgr.load_dimensions(
            config.model.dimensions.base_dimension_references, conn=conn
        )
        supplemental_dimensions = self._dimension_mgr.load_dimensions(
            config.model.dimensions.supplemental_dimension_references, conn=conn
        )
        base_to_supp_mappings = self._dimension_mapping_mgr.load_dimension_mappings(
            config.model.dimension_mappings.base_to_supplemental_references, conn=conn
        )
        subset_dimensions = self._get_subset_dimensions(conn, config)
        config.set_dimensions(base_dimensions, subset_dimensions, supplemental_dimensions)
        config.set_dimension_mappings(base_to_supp_mappings)

    def _get_subset_dimensions(self, conn: Connection | None, config: ProjectConfig):
        subset_dimensions: dict[
            DimensionType, dict[str, dict[ConfigKey, DimensionBaseConfig]]
        ] = defaultdict(dict)
        for subset_dim in config.model.dimensions.subset_dimensions:
            selectors = {
                ConfigKey(x.dimension_id, x.version): self._dimension_mgr.get_by_id(
                    x.dimension_id, version=x.version, conn=conn
                )
                for x in subset_dim.selector_references
            }
            subset_dimensions[subset_dim.dimension_type][subset_dim.name] = selectors
        return subset_dimensions

    def load_project(
        self,
        project_id: str,
        version: str | None = None,
        conn: Connection | None = None,
    ) -> Project:
        """Load a project from the registry.

        Parameters
        ----------
        project_id : str
        version : str

        Returns
        -------
        Project
        """
        if conn is None:
            with self.db.engine.connect() as conn:
                return self._load_project(conn, project_id, version=version)
        else:
            return self._load_project(conn, project_id, version=version)

    def _load_project(self, conn: Connection, project_id: str, version=None) -> Project:
        dataset_manager = self._dataset_mgr
        config = self.get_by_id(project_id, version=version, conn=conn)

        dataset_configs = {}
        for dataset_id in config.list_registered_dataset_ids():
            dataset_config = dataset_manager.get_by_id(dataset_id, conn=conn)
            dataset_configs[dataset_id] = dataset_config

        return Project(
            config,
            config.model.version,
            dataset_configs,
            self._dimension_mgr,
            self._dimension_mapping_mgr,
            self._dataset_mgr,
        )

    def register(
        self,
        config_file: Path,
        submitter: str,
        log_message: str,
    ) -> None:
        """Register a project from a config file."""
        with RegistrationContext(
            self.db, log_message, VersionUpdateType.MAJOR, submitter
        ) as context:
            config = ProjectConfig.load(config_file)
            src_dir = config_file.parent
            self.register_from_config(config, src_dir, context)

    def register_from_config(
        self,
        config: ProjectConfig,
        src_dir: Path,
        context: RegistrationContext,
    ):
        """Register a project from an existing config."""
        self._register_project_and_dimensions(
            config,
            src_dir,
            context,
        )

    def _register_project_and_dimensions(
        self,
        config: ProjectConfig,
        src_dir: Path,
        context: RegistrationContext,
    ):
        model = config.model
        logger.info("Start registration of project %s", model.project_id)
        self._check_if_already_registered(context.connection, model.project_id)
        if model.dimensions.base_dimensions:
            logger.info("Register base dimensions")
            for ref in self._register_dimensions_from_models(
                model.dimensions.base_dimensions,
                context,
            ):
                model.dimensions.base_dimension_references.append(ref)
            model.dimensions.base_dimensions.clear()
        if model.dimensions.subset_dimensions:
            self._register_subset_dimensions(
                model,
                model.dimensions.subset_dimensions,
                context,
            )
        if model.dimensions.supplemental_dimensions:
            logger.info("Register supplemental dimensions")
            self._register_supplemental_dimensions_from_models(
                src_dir,
                model,
                model.dimensions.supplemental_dimensions,
                context,
            )
            model.dimensions.supplemental_dimensions.clear()
        logger.info("Register all-in-one supplemental dimensions")
        self._register_all_in_one_dimensions(
            src_dir,
            model,
            context,
        )

        self._update_dimensions_and_mappings(context.connection, config)
        for subset_dimension in model.dimensions.subset_dimensions:
            subset_dimension.selectors.clear()
        self._register(config, context)
        context.add_id(RegistryType.PROJECT, config.model.project_id, self)

    def _register_dimensions_from_models(
        self,
        dimensions: list,
        context: RegistrationContext,
    ):
        dim_model = DimensionsConfigModel(dimensions=dimensions)
        dims_config = DimensionsConfig.load_from_model(dim_model)
        dimension_ids = self._dimension_mgr.register_from_config(dims_config, context)
        return self._dimension_mgr.make_dimension_references(context.connection, dimension_ids)

    def _register_supplemental_dimensions_from_models(
        self,
        src_dir: Path,
        model: ProjectConfigModel,
        dimensions: list,
        context: RegistrationContext,
    ):
        """Registers supplemental dimensions and creates base-to-supplemental mappings for those
        new dimensions.
        """
        dims = []
        for x in dimensions:
            data = x.serialize()
            data.pop("mapping", None)
            dims.append(DimensionModel(**data))

        refs = self._register_dimensions_from_models(dims, context)

        model.dimensions.supplemental_dimension_references += refs
        self._register_base_to_supplemental_mappings(
            src_dir,
            model,
            dimensions,
            refs,
            context,
        )

    def _register_base_to_supplemental_mappings(
        self,
        src_dir: Path,
        model: ProjectConfigModel,
        dimensions: list[SupplementalDimensionModel],
        dimension_references: list[DimensionReferenceModel],
        context: RegistrationContext,
    ):
        conn = context.connection
        base_dim_mapping = defaultdict(list)
        base_dim_refs: dict[str, DimensionReferenceModel] = {}
        for ref in model.dimensions.base_dimension_references:
            dim = self._dimension_mgr.get_by_id(
                ref.dimension_id, version=ref.version, conn=context.connection
            )
            base_dim_mapping[ref.dimension_type].append(dim)
            base_dim_refs[dim.model.dimension_id] = ref

        mappings = []
        if len(dimensions) != len(dimension_references):
            msg = f"Bug: mismatch in sizes: {dimensions=} {dimension_references=}"
            raise Exception(msg)

        for dim, ref in zip(dimensions, dimension_references):
            base_dim: DimensionBaseConfig | None = None
            if dim.mapping.project_base_dimension_name is None:
                base_dims = base_dim_mapping[ref.dimension_type]
                if len(base_dims) > 1:
                    msg = (
                        "If there are multiple base dimenions for a dimension type, each "
                        "supplemental dimension mapping must supply a project_base_dimension_name. "
                        f"{dim.label}"
                    )
                    raise DSGInvalidDimensionMapping(msg)
                base_dim = base_dims[0]
            else:
                for base_dim_ in base_dim_mapping[dim.dimension_type]:
                    if base_dim_.model.name == dim.mapping.project_base_dimension_name:
                        if base_dim is not None:
                            msg = (
                                "A supplemental dimension can only be mapped to one base dimension:"
                                f" supplemental dimension = {dim.label} "
                                f"base dimensions = {base_dim.model.label} and "
                                f"{base_dim_.model.label}"
                            )
                            raise DSGInvalidDimensionMapping(msg)
                        base_dim = base_dim_
                if base_dim is None:
                    msg = f"Bug: unable to find base dimension for {dim.mapping.project_base_dimension_name}"
                    raise Exception(msg)
            with in_other_dir(src_dir):
                assert base_dim is not None
                mapping_model = MappingTableModel.from_pre_registered_model(
                    dim.mapping,
                    base_dim_refs[base_dim.model.dimension_id],
                    ref,
                )
                mappings.append(mapping_model)

        mapping_config = DimensionMappingsConfig.load_from_model(
            DimensionMappingsConfigModel(mappings=mappings),
        )
        mapping_ids = self._dimension_mapping_mgr.register_from_config(mapping_config, context)
        model.dimension_mappings.base_to_supplemental_references += (
            self._dimension_mapping_mgr.make_dimension_mapping_references(mapping_ids, conn=conn)
        )

    def _register_subset_dimensions(
        self,
        model: ProjectConfigModel,
        subset_dimensions: list[SubsetDimensionGroupModel],
        context: RegistrationContext,
    ):
        logger.info("Register subset dimensions")
        self._register_dimensions_from_subset_dimension_groups(
            subset_dimensions,
            model.dimensions.base_dimension_references,
            context,
        )
        self._register_supplemental_dimensions_from_subset_dimensions(
            model,
            subset_dimensions,
            context,
        )

    def _register_dimensions_from_subset_dimension_groups(
        self,
        subset_dimensions: list[SubsetDimensionGroupModel],
        base_dimension_references: list[DimensionReferenceModel],
        context: RegistrationContext,
    ):
        """Registers a dimension for each subset specified in the project config's subset
        dimension groups. Appends references to those dimensions to subset_dimensions, which is
        part of the project config.
        """
        conn = context.connection
        with TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            dimensions = []
            subset_refs = {}
            for subset_dimension in subset_dimensions:
                base_dim = None
                for ref in base_dimension_references:
                    if ref.dimension_type == subset_dimension.dimension_type:
                        base_dim = self._dimension_mgr.get_by_id(ref.dimension_id, conn=conn)
                        break
                assert isinstance(base_dim, DimensionBaseConfigWithFiles), subset_dimension
                base_records = base_dim.get_records_dataframe()
                self._check_subset_dimension_consistency(subset_dimension, base_records)
                for selector in subset_dimension.selectors:
                    new_records = base_records.filter(base_records["id"].isin(selector.records))
                    filename = tmp_path / f"{subset_dimension.name}_{selector.name}.csv"
                    new_records.toPandas().to_csv(filename, index=False)
                    dim = DimensionModel(
                        file=str(filename),
                        name=selector.name,
                        type=subset_dimension.dimension_type,
                        module=base_dim.model.module,
                        class_name=base_dim.model.class_name,
                        description=selector.description,
                    )
                    dimensions.append(dim)
                    key = (subset_dimension.dimension_type, selector.name)
                    if key in subset_refs:
                        msg = f"Bug: unhandled case of duplicate dimension name: {key=}"
                        raise Exception(msg)
                    subset_refs[key] = subset_dimension

            dim_model = DimensionsConfigModel(dimensions=dimensions)
            dims_config = DimensionsConfig.load_from_model(dim_model)
            dimension_ids = self._dimension_mgr.register_from_config(dims_config, context)
            for dimension_id in dimension_ids:
                dim = self._dimension_mgr.get_by_id(dimension_id, conn=conn)
                key = (dim.model.dimension_type, dim.model.name)
                subset_dim = subset_refs[key]
                subset_dim.selector_references.append(
                    DimensionReferenceModel(
                        dimension_id=dimension_id,
                        type=subset_dim.dimension_type,
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
    ):
        """Registers a supplemental dimension for each subset specified in the project config's
        subset dimension groups. Also registers a mapping from the base dimension to each new
        supplemental dimension. Appends references to those dimensions to the project config's
        supplemental_dimension_references list.
        """
        conn = context.connection
        with TemporaryDirectory() as tmpdir:
            tmp_path = Path(tmpdir)
            dimensions = []
            for subset_dimension_group in subset_dimensions:
                if not subset_dimension_group.create_supplemental_dimension:
                    continue
                dimension_type = subset_dimension_group.dimension_type
                base_dims: list[DimensionBaseConfigWithFiles] = []
                for ref in model.dimensions.base_dimension_references:
                    if ref.dimension_type == dimension_type:
                        base_dim = self._dimension_mgr.get_by_id(ref.dimension_id, conn=conn)
                        if (
                            subset_dimension_group.base_dimension_name is None
                            or base_dim.model.name == subset_dimension_group.base_dimension_name
                        ):
                            base_dims.append(base_dim)
                            break
                if len(base_dims) == 0:
                    msg = f"Did not find a base dimension for {subset_dimension_group=}"
                    raise Exception(msg)
                elif len(base_dims) > 1:
                    msg = (
                        f"Found multiple base dimensions for {dimension_type=}. Please specify "
                        f"'base_dimension_name' in {subset_dimension_group=}"
                    )
                    raise DSGInvalidParameter(msg)
                base_dim = base_dims[0]
                records: dict[str, list[Any]] = {"id": [], "name": []}
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

                filename = tmp_path / f"{subset_dimension_group.name}.csv"
                pd.DataFrame(records).to_csv(filename, index=False)

                for record_id in base_dim.get_unique_ids().difference(dim_record_ids):
                    mapping_records.append({"from_id": record_id, "to_id": ""})
                map_record_file = tmp_path / f"{subset_dimension_group.name}_mapping.csv"
                pd.DataFrame.from_records(mapping_records).to_csv(map_record_file, index=False)

                dim = SupplementalDimensionModel(
                    file=str(filename),
                    name=subset_dimension_group.name,
                    type=dimension_type,
                    module=base_dim.model.module,
                    class_name=base_dim.model.class_name,
                    description=subset_dimension_group.description,
                    mapping=MappingTableByNameModel(
                        file=str(map_record_file),
                        mapping_type=DimensionMappingType.MANY_TO_MANY_EXPLICIT_MULTIPLIERS,
                        description=f"Aggregation map for {subset_dimension_group.name}",
                        project_base_dimension_name=base_dim.model.name,
                    ),
                )
                dimensions.append(dim)

            self._register_supplemental_dimensions_from_models(
                tmp_path,
                model,
                dimensions,
                context,
            )

    def _register_all_in_one_dimensions(
        self,
        src_dir,
        model,
        context: RegistrationContext,
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
                dim_config = self._dimension_mgr.get_by_id(
                    dim_ref.dimension_id, conn=context.connection
                )
                assert isinstance(dim_config, DimensionBaseConfigWithFiles)
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
                        file=str(dim_record_file),
                        name=dim_name,
                        type=dimension_type,
                        module="dsgrid.dimension.base_models",
                        class_name="DimensionRecordBaseModel",
                        description=dim_name_formal,
                        mapping=MappingTableByNameModel(
                            file=str(map_record_file),
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
            )

    def _register(self, config: ProjectConfig, context: RegistrationContext):
        self._run_checks(config)

        config.model.version = "1.0.0"
        model = self.db.insert(context.connection, config.model, context.registration)
        assert isinstance(model, ProjectConfigModel)
        logger.info(
            "%s Registered project %s with version=%s",
            self._log_offline_mode_prefix(),
            model.project_id,
            config.model.version,
        )

    def _run_checks(self, config: ProjectConfig):
        dims = [x for x in config.iter_dimensions()]
        check_uniqueness((x.model.name for x in dims), "dimension name")
        self._check_base_dimensions(config)

        for dataset_id in config.list_unregistered_dataset_ids():
            for field in RequiredDimensionRecordsModel.model_fields:
                # This will check that all dimension record IDs listed in the requirements
                # exist in the project.
                config.get_required_dimension_record_ids(dataset_id, DimensionType(field))

    def _check_base_dimensions(self, config: ProjectConfig) -> None:
        found_time = False
        for dim in config.list_base_dimensions():
            if dim.model.dimension_type == DimensionType.TIME:
                if found_time:
                    msg = "Only one time dimension is allowed in a project."
                    raise DSGInvalidDimension(msg)
                found_time = True

        assert found_time
        self._set_dataset_record_requirement_definitions_names(config)
        self._check_dataset_record_requirement_definitions(config)

    def _set_dataset_record_requirement_definitions_names(
        self,
        config: ProjectConfig,
    ) -> None:
        def set_dimension_name(req: RequiredBaseDimensionModel) -> None:
            if req.dimension_name is None and req.dimension_name is not None:
                dim = config.get_dimension_by_name(req.dimension_name)
                req.dimension_name = None
                req.dimension_name = dim.model.name

        for dataset in config.model.datasets:
            dim_type_as_fields = RequiredDimensionRecordsModel.model_fields.keys()
            for field in dim_type_as_fields:
                req = getattr(dataset.required_dimensions.single_dimensional, field)
                for base_field in ("base", "base_missing"):
                    set_dimension_name(getattr(req, base_field))
                for multi_dim in dataset.required_dimensions.multi_dimensional:
                    req = getattr(multi_dim, field)
                    for base_field in ("base", "base_missing"):
                        set_dimension_name(getattr(req, base_field))

    def _check_dataset_record_requirement_definitions(
        self,
        config: ProjectConfig,
    ) -> None:
        for dataset in config.model.datasets:
            dim_type_as_fields = RequiredDimensionRecordsModel.model_fields.keys()
            for dim_type_as_field in dim_type_as_fields:
                dim_type = DimensionType(dim_type_as_field)
                required_dimension_records = getattr(
                    dataset.required_dimensions.single_dimensional, dim_type_as_field
                )
                self._check_base_dimension_record_requirements(
                    required_dimension_records, dim_type, config, dataset.dataset_id
                )
                for multi_dim in dataset.required_dimensions.multi_dimensional:
                    required_dimension_records = getattr(multi_dim, dim_type_as_field)
                    self._check_base_dimension_record_requirements(
                        required_dimension_records, dim_type, config, dataset.dataset_id
                    )

    def _check_base_dimension_record_requirements(
        self,
        req_dim_records: RequiredDimensionRecordsByTypeModel,
        dim_type: DimensionType,
        config: ProjectConfig,
        dataset_id: str,
    ) -> None:
        base_dims = config.list_base_dimensions(dimension_type=dim_type)
        for base_field in ("base", "base_missing"):
            reqs = getattr(req_dim_records, base_field)
            if reqs.record_ids and reqs.dimension_name is None:
                if len(base_dims) == 1:
                    reqs.dimension_name = base_dims[0].model.name
                    logger.debug(
                        "Assigned dimension_name=%s for %s dataset_id=%s",
                        reqs.dimension_name,
                        dim_type,
                        dataset_id,
                    )
                else:
                    msg = (
                        f"{dataset_id=} requires a base dimension name for "
                        f"{dim_type} because the project has {len(base_dims)} base dimensions."
                    )
                    raise DSGInvalidDimensionMapping(msg)
            # Only one of base and base_missing can be set, and that was already checked.
            break

    @track_timing(timer_stats_collector)
    def register_and_submit_dataset(
        self,
        dataset_config_file: Path,
        project_id: str,
        submitter: str,
        log_message: str,
        dimension_mapping_file=None,
        dimension_mapping_references_file=None,
        autogen_reverse_supplemental_mappings=None,
        data_base_dir: Path | None = None,
        missing_associations_base_dir: Path | None = None,
    ):
        with RegistrationContext(
            self.db, log_message, VersionUpdateType.MINOR, submitter
        ) as context:
            conn = context.connection
            if not self.has_id(project_id, conn=conn):
                msg = f"{project_id=}"
                raise DSGValueNotRegistered(msg)

            dataset_config = DatasetConfig.load_from_user_path(
                dataset_config_file,
                data_base_dir=data_base_dir,
                missing_associations_base_dir=missing_associations_base_dir,
            )
            dataset_id = dataset_config.model.dataset_id
            config = self.get_by_id(project_id, conn=conn)
            # This will raise an exception if the dataset_id is not part of the project or already
            # registered.
            self._raise_if_not_unregistered(config, dataset_id)

            self._dataset_mgr.register(
                dataset_config_file,
                context=context,
                data_base_dir=data_base_dir,
                missing_associations_base_dir=missing_associations_base_dir,
            )
            self.submit_dataset(
                project_id,
                context.get_ids(RegistryType.DATASET)[0],
                dimension_mapping_file=dimension_mapping_file,
                dimension_mapping_references_file=dimension_mapping_references_file,
                autogen_reverse_supplemental_mappings=autogen_reverse_supplemental_mappings,
                context=context,
            )

    @track_timing(timer_stats_collector)
    def submit_dataset(
        self,
        project_id: str,
        dataset_id: str,
        submitter: str | None = None,
        log_message: str | None = None,
        dimension_mapping_file: Path | None = None,
        dimension_mapping_references_file: Path | None = None,
        autogen_reverse_supplemental_mappings: list[DimensionType] | None = None,
        context: RegistrationContext | None = None,
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
        autogen_reverse_supplemental_mappings : list[DimensionType] or None
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
        if context is None:
            assert submitter is not None
            assert log_message is not None
            with RegistrationContext(
                self.db, log_message, VersionUpdateType.MINOR, submitter
            ) as context:
                config = self.get_by_id(project_id, conn=context.connection)
                self._submit_dataset_and_register_mappings(
                    config,
                    dataset_id,
                    dimension_mapping_file,
                    dimension_mapping_references_file,
                    autogen_reverse_supplemental_mappings,
                    context,
                )
        else:
            config = self.get_by_id(project_id, conn=context.connection)
            self._submit_dataset_and_register_mappings(
                config,
                dataset_id,
                dimension_mapping_file,
                dimension_mapping_references_file,
                autogen_reverse_supplemental_mappings,
                context,
            )

    def register_subset_dimensions(
        self,
        project_id: str,
        filename: Path,
        submitter: str,
        log_message: str,
        update_type: VersionUpdateType,
    ):
        """Register new subset dimensions."""
        with RegistrationContext(self.db, log_message, update_type, submitter) as context:
            config = self.get_by_id(project_id, conn=context.connection)
            subset_model = SubsetDimensionGroupListModel.from_file(filename)
            self._register_subset_dimensions(
                config.model,
                subset_model.subset_dimensions,
                context,
            )
            self._make_new_config(config, context)

    def register_supplemental_dimensions(
        self,
        project_id: str,
        filename: Path,
        submitter: str,
        log_message: str,
        update_type: VersionUpdateType,
    ):
        """Register new supplemental dimensions."""
        with RegistrationContext(self.db, log_message, update_type, submitter) as context:
            config = self.get_by_id(project_id, conn=context.connection)
            model = SupplementalDimensionsListModel.from_file(filename)
            self._register_supplemental_dimensions_from_models(
                filename.parent,
                config.model,
                model.supplemental_dimensions,
                context,
            )
            self._make_new_config(config, context)

    def add_dataset_requirements(
        self,
        project_id: str,
        filename: Path,
        submitter: str,
        log_message: str,
        update_type: VersionUpdateType,
    ):
        """Add requirements for one or more datasets to the project."""
        with RegistrationContext(self.db, log_message, update_type, submitter) as context:
            config = self.get_by_id(project_id, conn=context.connection)
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
            self._make_new_config(config, context)

    def replace_dataset_dimension_requirements(
        self,
        project_id: str,
        filename: Path,
        submitter: str,
        log_message: str,
        update_type: VersionUpdateType,
    ):
        """Replace dataset requirements in a project."""
        with RegistrationContext(self.db, log_message, update_type, submitter) as context:
            config = self.get_by_id(project_id, conn=context.connection)
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

            self._make_new_config(config, context)

    def _submit_dataset_and_register_mappings(
        self,
        project_config: ProjectConfig,
        dataset_id: str,
        dimension_mapping_file: Path | None,
        dimension_mapping_references_file: Path | None,
        autogen_reverse_supplemental_mappings: list[DimensionType] | None,
        context: RegistrationContext,
    ) -> None:
        logger.info("Submit dataset=%s to project=%s.", dataset_id, project_config.config_id)
        self._check_if_not_registered(context.connection, project_config.config_id)
        self._raise_if_not_unregistered(project_config, dataset_id)
        dataset_config = self._dataset_mgr.get_by_id(dataset_id, conn=context.connection)

        references = []
        if dimension_mapping_file is not None:
            references += self._register_mappings_from_file(
                project_config,
                dataset_config,
                dimension_mapping_file,
                context,
            )
        if dimension_mapping_references_file is not None:
            for ref in DimensionMappingReferenceListModel.load(
                dimension_mapping_references_file
            ).references:
                if not self.dimension_mapping_manager.has_id(
                    ref.mapping_id, version=ref.version, conn=context.connection
                ):
                    msg = f"mapping_id={ref.mapping_id}"
                    raise DSGValueNotRegistered(msg)
                references.append(ref)

        if autogen_reverse_supplemental_mappings:
            references += self._auto_register_reverse_supplemental_mappings(
                project_config,
                dataset_config,
                references,
                set((x.value for x in autogen_reverse_supplemental_mappings)),
                context,
            )

        self._submit_dataset(project_config, dataset_config, references, context)

    def _raise_if_not_unregistered(self, project_config: ProjectConfig, dataset_id: str) -> None:
        # This will raise if the dataset is not specified in the project.
        dataset_model = project_config.get_dataset(dataset_id)
        status = dataset_model.status
        if status != DatasetRegistryStatus.UNREGISTERED:
            msg = (
                f"{dataset_id=} cannot be submitted to project={project_config.config_id} with "
                f"{status=}"
            )
            raise DSGDuplicateValueRegistered(msg)

    def _register_mappings_from_file(
        self,
        project_config: ProjectConfig,
        dataset_config: DatasetConfig,
        dimension_mapping_file: Path,
        context: RegistrationContext,
    ):
        references = []
        src_dir = dimension_mapping_file.parent
        mappings = DatasetBaseToProjectMappingTableListModel(
            **load_data(dimension_mapping_file)
        ).mappings
        dataset_mapping = {x.dimension_type: x for x in dataset_config.model.dimension_references}
        project_mapping: dict[DimensionType, list[DimensionBaseConfig]] = defaultdict(list)
        project_mapping_refs: dict[str, DimensionReferenceModel] = {}
        for ref in project_config.model.dimensions.base_dimension_references:
            dim = self._dimension_mgr.get_by_id(
                ref.dimension_id, version=ref.version, conn=context.connection
            )
            project_mapping[ref.dimension_type].append(dim)
            project_mapping_refs[dim.model.dimension_id] = ref
        mapping_tables = []
        for mapping in mappings:
            base_dim: DimensionBaseConfig | None = None
            if mapping.project_base_dimension_name is None:
                base_dims = project_mapping[mapping.dimension_type]
                if len(base_dims) > 1:
                    msg = (
                        "If there are multiple project base dimensions for a dimension type, the "
                        "dataset dimension mapping must supply a project_base_dimension_name. "
                        f"{mapping}"
                    )
                    raise DSGInvalidDimensionMapping(msg)
                base_dim = base_dims[0]
            else:
                for base_dim_ in project_mapping[mapping.dimension_type]:
                    if base_dim_.model.name == mapping.project_base_dimension_name:
                        base_dim = base_dim_
                if base_dim is None:
                    msg = f"Bug: unable to find base dimension for {mapping.project_base_dimension_name}"
                    raise Exception(msg)
            with in_other_dir(src_dir):
                assert base_dim is not None
                mapping_table = MappingTableModel.from_pre_registered_model(
                    mapping,
                    dataset_mapping[mapping.dimension_type],
                    project_mapping_refs[base_dim.model.dimension_id],
                )
            mapping_tables.append(mapping_table)

        mappings_config = DimensionMappingsConfig.load_from_model(
            DimensionMappingsConfigModel(mappings=mapping_tables)
        )
        mapping_ids = self._dimension_mapping_mgr.register_from_config(mappings_config, context)
        for mapping_id in mapping_ids:
            mapping_config = self._dimension_mapping_mgr.get_by_id(
                mapping_id, conn=context.connection
            )
            references.append(
                DimensionMappingReferenceModel(
                    from_dimension_type=mapping_config.model.from_dimension.dimension_type,
                    to_dimension_type=mapping_config.model.to_dimension.dimension_type,
                    mapping_id=mapping_id,
                    version=str(
                        self._dimension_mapping_mgr.get_latest_version(
                            mapping_id, conn=context.connection
                        )
                    ),
                )
            )

        return references

    def _auto_register_reverse_supplemental_mappings(
        self,
        project_config: ProjectConfig,
        dataset_config: DatasetConfig,
        mapping_references: list[DimensionMappingReferenceModel],
        autogen_reverse_supplemental_mappings: set[str],
        context: RegistrationContext,
    ):
        conn = context.connection
        references = []
        p_model = project_config.model
        p_supp_dim_ids = {
            x.dimension_id for x in p_model.dimensions.supplemental_dimension_references
        }
        d_dim_from_ids = set()
        for ref in mapping_references:
            mapping_config = self._dimension_mapping_mgr.get_by_id(ref.mapping_id, conn=conn)
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
            msg = (
                f"Mappings to autgen [{needs_mapping}] does not match user-specified "
                f"autogen_reverse_supplemental_mappings={autogen_reverse_supplemental_mappings}"
            )
            raise DSGInvalidDimensionMapping(msg)

        new_mappings = []
        for from_id, from_version in needs_mapping:
            to_dim = self._dimension_mgr.get_by_id(from_id, version=from_version, conn=conn)
            from_dim, to_version = project_config.get_base_dimension_and_version(
                to_dim.model.dimension_type
            )
            mapping, version = self._try_get_mapping(
                project_config, to_dim, from_version, from_dim, to_version, context
            )
            if mapping is None:
                p_mapping, _ = self._try_get_mapping(
                    project_config, from_dim, to_version, to_dim, from_version, context
                )
                assert (
                    p_mapping is not None
                ), f"from={from_dim.model.dimension_id} to={to_dim.model.dimension_id}"
                records = models_to_dataframe(p_mapping.model.records)
                fraction_vals = get_unique_values(records, "from_fraction")
                if len(fraction_vals) != 1 and next(iter(fraction_vals)) != 1.0:
                    msg = (
                        f"Cannot auto-generate a dataset-to-project mapping from from a project "
                        "supplemental dimension unless the from_fraction column is empty or only "
                        f"has values of 1.0: {p_mapping.model.mapping_id} - {fraction_vals}"
                    )
                    raise DSGInvalidDimensionMapping(msg)
                reverse_records = (
                    records.drop("from_fraction")
                    .select(F.col("to_id").alias("from_id"), F.col("from_id").alias("to_id"))
                    .toPandas()
                )
                dst = Path(tempfile.gettempdir()) / f"reverse_{p_mapping.config_id}.csv"
                # Use pandas because spark creates a CSV directory.
                reverse_records.to_csv(dst, index=False)
                dimension_type = to_dim.model.dimension_type.value
                new_mappings.append(
                    {
                        "description": f"Maps {dataset_config.config_id} {dimension_type} to project",
                        "dimension_type": dimension_type,
                        "file": str(dst),
                        "mapping_type": DimensionMappingType.MANY_TO_MANY_EXPLICIT_MULTIPLIERS.value,
                    }
                )
            else:
                assert version is not None
                reference = DimensionMappingReferenceModel(
                    from_dimension_type=to_dim.model.dimension_type,
                    to_dimension_type=to_dim.model.dimension_type,
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
                    context,
                )
            finally:
                for filename in to_delete:
                    Path(filename).unlink()

        return references

    def _try_get_mapping(
        self,
        project_config: ProjectConfig,
        from_dim,
        from_version,
        to_dim,
        to_version,
        context: RegistrationContext,
    ):
        conn = context.connection
        dimension_type = from_dim.model.dimension_type
        for ref in project_config.model.dimension_mappings.base_to_supplemental_references:
            if (
                ref.from_dimension_type == dimension_type
                and ref.to_dimension_type == dimension_type
            ):
                mapping_config = self._dimension_mapping_mgr.get_by_id(ref.mapping_id, conn=conn)
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
        mapping_references: list[DimensionMappingReferenceModel],
        context: RegistrationContext,
    ):
        project_config.add_dataset_dimension_mappings(dataset_config, mapping_references)
        project_config.add_dataset_base_dimension_names(
            dataset_config.model.dataset_id,
            self._id_base_dimension_names_in_dataset(
                project_config, dataset_config, mapping_references
            ),
        )
        if os.environ.get("__DSGRID_SKIP_CHECK_DATASET_TO_PROJECT_MAPPING__") is not None:
            logger.warning("Skip dataset-to-project mapping checks")
        else:
            self._check_dataset_base_to_project_base_mappings(
                project_config,
                dataset_config,
                mapping_references,
                context,
            )

        dataset_model = project_config.get_dataset(dataset_config.model.dataset_id)

        dataset_model.mapping_references = mapping_references
        dataset_model.status = DatasetRegistryStatus.REGISTERED
        if project_config.are_all_datasets_submitted():
            new_status = ProjectRegistryStatus.COMPLETE
        else:
            new_status = ProjectRegistryStatus.IN_PROGRESS
        project_config.set_status(new_status)
        config = self.update_with_context(project_config, context)
        self._db.add_contains_dataset(context.connection, config.model, dataset_config.model)

        logger.info(
            "%s Registered dataset %s with version=%s in project %s",
            self._log_offline_mode_prefix(),
            dataset_config.model.dataset_id,
            config.model.version,
            config.model.project_id,
        )

    @track_timing(timer_stats_collector)
    def _check_dataset_base_to_project_base_mappings(
        self,
        project_config: ProjectConfig,
        dataset_config: DatasetConfig,
        mapping_references: list[DimensionMappingReferenceModel],
        context: RegistrationContext,
    ):
        """Check that a dataset has all project-required dimension records."""
        logger.info("Check dataset-base-to-project-base dimension mappings.")
        data_store = self._dataset_mgr.store
        handler = make_dataset_schema_handler(
            context.connection,
            dataset_config,
            self._dimension_mgr,
            self._dimension_mapping_mgr,
            store=data_store,
            mapping_references=mapping_references,
        )
        dataset_id = dataset_config.config_id

        with ScratchDirContext(self._params.scratch_dir) as scontext:
            project_table = self._make_dimension_associations(project_config, dataset_id, scontext)
            mapped_dataset_table = handler.make_mapped_dimension_association_table(scontext)
            project_table = handler.remove_expected_missing_mapped_associations(
                data_store, project_table, scontext
            )
            cols = sorted(project_table.columns)
            cache(mapped_dataset_table)
            diff: DataFrame | None = None

            try:
                # This check is relatively short and will show the user clear errors.
                _check_distinct_column_values(project_table, mapped_dataset_table)
                # This check is long and will produce a full table of differences.
                # It may require some effort from the user.
                diff = except_all(project_table.select(*cols), mapped_dataset_table.select(*cols))
                cache(diff)
                if not is_dataframe_empty(diff):
                    dataset_id = dataset_config.model.dataset_id
                    handle_dimension_association_errors(diff, mapped_dataset_table, dataset_id)
            finally:
                unpersist(mapped_dataset_table)
                if diff is not None:
                    unpersist(diff)

    def _id_base_dimension_names_in_dataset(
        self,
        project_config: ProjectConfig,
        dataset_config: DatasetConfig,
        mapping_references: list[DimensionMappingReferenceModel],
    ) -> DatasetBaseDimensionNamesModel:
        base_dimension_names: dict[DimensionType, str] = {}
        for ref in mapping_references:
            mapping = self._dimension_mapping_mgr.get_by_id(ref.mapping_id, version=ref.version)
            base_dim = self._dimension_mgr.get_by_id(
                mapping.model.to_dimension.dimension_id,
                version=mapping.model.to_dimension.version,
            ).model
            base_dimension_names[base_dim.dimension_type] = base_dim.name

        project_base_dims_by_type: dict[DimensionType, list[DimensionBaseConfig]] = defaultdict(
            list
        )
        for dim in project_config.list_base_dimensions():
            project_base_dims_by_type[dim.model.dimension_type].append(dim)

        dataset_id = dataset_config.model.dataset_id
        for dim_type in DimensionType:
            if dim_type == DimensionType.TIME:
                assert len(project_base_dims_by_type[dim_type]) == 1
                base_dimension_names[dim_type] = project_base_dims_by_type[dim_type][0].model.name
                continue
            if dim_type not in base_dimension_names:
                project_base_dims = project_base_dims_by_type[dim_type]
                if len(project_base_dims) > 1:
                    for project_dim in project_base_dims:
                        assert isinstance(project_dim, DimensionBaseConfigWithFiles)
                        project_records = project_dim.get_records_dataframe()
                        project_record_ids = get_unique_values(project_records, "id")
                        dataset_dim = dataset_config.get_dimension_with_records(dim_type)
                        assert dataset_dim is not None
                        dataset_records = dataset_dim.get_records_dataframe()
                        dataset_record_ids = get_unique_values(dataset_records, "id")
                        if dataset_record_ids.issubset(project_record_ids):
                            project_dim_name = project_dim.model.name
                            if dim_type in base_dimension_names:
                                msg = (
                                    f"Found multiple project base dimensions for {dataset_id=} "
                                    f"and {dim_type=}: {base_dimension_names[dim_type]} and "
                                    f"{project_dim_name}. Please specify a mapping."
                                )
                                raise DSGInvalidDataset(msg)

                            base_dimension_names[dim_type] = project_dim_name
                    if dim_type not in base_dimension_names:
                        msg = (
                            f"Bug: {dim_type} has multiple base dimensions in the project, dataset "
                            f"{dataset_id} does not specify a mapping, and dsgrid could not "
                            "discern which base dimension to use."
                        )
                        raise DSGInvalidDataset(msg)
                else:
                    base_dimension_names[dim_type] = project_base_dims[0].model.name

        data = {k.value: v for k, v in base_dimension_names.items()}
        return DatasetBaseDimensionNamesModel(**data)

    @track_timing(timer_stats_collector)
    def _make_dimension_associations(
        self,
        config: ProjectConfig,
        dataset_id: str,
        context: ScratchDirContext,
    ) -> DataFrame:
        logger.info("Make dimension association table for %s", dataset_id)
        df = config.make_dimension_association_table(dataset_id, context)
        if use_duckdb():
            df2 = df
        else:
            # This operation is slow with Spark. Ensure that we only evaluate the query once.
            df2 = read_dataframe(persist_table(df, context, "dimension_associations"))
        logger.info("Wrote dimension associations for dataset %s", dataset_id)
        return df2

    def update_from_file(
        self,
        config_file,
        project_id: str,
        submitter: str,
        update_type: VersionUpdateType,
        log_message: str,
        version: str,
    ) -> ProjectConfig:
        with RegistrationContext(self.db, log_message, update_type, submitter) as context:
            config = ProjectConfig.load(config_file)
            self._update_dimensions_and_mappings(context.connection, config)
            self._check_update(context.connection, config, project_id, version)
            return self.update_with_context(config, context)

    @track_timing(timer_stats_collector)
    def update(
        self,
        config: ProjectConfig,
        update_type: VersionUpdateType,
        log_message: str,
        submitter: str | None = None,
    ) -> ProjectConfig:
        with RegistrationContext(self.db, log_message, update_type, submitter) as context:
            self._update_dimensions_and_mappings(context.connection, config)
            return self.update_with_context(config, context)

    def update_with_context(
        self, config: ProjectConfig, context: RegistrationContext
    ) -> ProjectConfig:
        old_config = self.get_by_id(config.model.project_id, conn=context.connection)
        checker = ProjectUpdateChecker(old_config.model, config.model)
        checker.run()
        self._run_checks(config)
        return self._make_new_config(config, context)

    def _make_new_config(
        self, config: ProjectConfig, context: RegistrationContext
    ) -> ProjectConfig:
        old_version = config.model.version
        old_key = ConfigKey(config.config_id, old_version)
        model = self._update_config(config, context)
        assert isinstance(model, ProjectConfigModel)
        new_config = ProjectConfig(model)
        self._update_dimensions_and_mappings(context.connection, new_config)
        new_key = ConfigKey(new_config.model.project_id, new_config.model.version)
        self._projects.pop(old_key, None)
        self._projects[new_key] = new_config
        return new_config

    def finalize_registration(self, conn: Connection, config_ids: set[str], error_occurred: bool):
        if error_occurred:
            logger.info("Remove intermediate project after error")
            for key in [x for x in self._projects if x.id in config_ids]:
                self._projects.pop(key)

    def remove(self, config_id: str, conn: Connection | None = None) -> None:
        self.db.delete_all(conn, config_id)
        for key in [x for x in self._projects if x.id == config_id]:
            self._projects.pop(key)

        logger.info("Removed %s from the registry.", config_id)

    def show(
        self,
        conn: Connection | None = None,
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

        transformed_filters = transform_and_validate_filters(filters) if filters else None
        field_to_index = {x: i for i, x in enumerate(table.field_names)}
        rows = []
        for model in self.db.iter_models(conn):
            assert isinstance(model, ProjectConfigModel)
            registration = self.db.get_registration(conn, model)
            all_fields = (
                model.project_id,
                model.version,
                model.status.value,
                ",\n".join([f"{x.dataset_id}: {x.status.value}" for x in model.datasets]),
                registration.timestamp.strftime("%Y-%m-%d %H:%M:%S"),
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


def _check_distinct_column_values(project_table: DataFrame, mapped_dataset_table: DataFrame):
    """Ensure that the mapped dataset has the same distinct values as the project for all
    columns. This should be called before running a full comparison of the two tables.
    """
    has_mismatch = False
    for column in project_table.columns:
        project_distinct = {x[column] for x in project_table.select(column).distinct().collect()}
        dataset_distinct = {
            x[column] for x in mapped_dataset_table.select(column).distinct().collect()
        }
        if diff_values := project_distinct.difference(dataset_distinct):
            has_mismatch = True
            logger.error(
                "The mapped dataset has different distinct values than the project "
                "for column=%s: diff=%s",
                column,
                diff_values,
            )

    if has_mismatch:
        msg = (
            "The mapped dataset has different distinct values than the project for one or "
            "more columns. Please look in the log file for the exact records."
        )
        raise DSGInvalidDataset(msg)
