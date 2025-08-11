import logging
from typing import Any

from dsgrid.config.mapping_tables import MappingTableConfig, MappingTableModel
from dsgrid.config.dataset_config import DatasetConfig, DatasetConfigModel
from dsgrid.config.dimension_config import DimensionConfig
from dsgrid.config.project_config import ProjectConfig
from dsgrid.exceptions import DSGInvalidParameter
from dsgrid.registry.registration_context import RegistrationContext
from dsgrid.registry.common import (
    ConfigKey,
    RegistryType,
    VersionUpdateType,
)
from dsgrid.registry.registry_manager import RegistryManager


logger = logging.getLogger(__name__)


class RegistryAutoUpdater:
    """Performs auto-updates on the registry."""

    def __init__(self, manager: RegistryManager) -> None:
        self._project_mgr = manager.project_manager
        self._dataset_mgr = manager.dataset_manager
        self._dimension_mgr = manager.dimension_manager
        self._dimension_mapping_mgr = manager.dimension_mapping_manager
        self._db = self._dimension_mapping_mgr.db

    def update_dependent_configs(
        self,
        config,
        original_version: str,
        update_type: VersionUpdateType,
        log_message: str,
        submitter: str | None = None,
    ):
        """Update all configs that consume this config. Recursive.
        This is an in incomplete, experimental feature, and is subject to change.
        Should only be called by an admin that understands the consequences.
        Passing a dimension may trigger an update to a project and a dimension mapping.
        The change to that dimension mapping may trigger another update to the project.
        This guarantees that each config version will only be bumped once.

        It is up to the caller to ensure changes are synced to the remote registry if not in
        offline mode.

        Datasets likely need to be resubmitted to their projects.

        Parameters
        ----------
        config : ConfigBase
        original_version : str
            Original version of the config
        update_type : VersionUpdateType
        log_message : str
        """
        with RegistrationContext(
            self._project_mgr.db, log_message, update_type, submitter
        ) as context:
            return self.update_dependent_configs_with_context(config, original_version, context)

    def update_dependent_configs_with_context(
        self, config, original_version: str, context: RegistrationContext
    ):
        if isinstance(config, DimensionConfig):
            self._update_dimension_users(config, original_version, context)
        elif isinstance(config, MappingTableConfig):
            self._update_dimension_mapping_users(config, original_version, context)
        elif isinstance(config, DatasetConfig):
            self._update_dataset_users(config, original_version, context)
        else:
            msg = f"Updates of configs dependent on {type(config)}"
            raise NotImplementedError(msg)

    def _update_dimension_users(
        self,
        config: DimensionConfig,
        original_version: str,
        context: RegistrationContext,
    ):
        # Order is important because
        # - dimension mappings may have this dimension.
        # - datasets may have this dimension.
        # - projects may have this dimension as well as updated mappings and datasets.
        new_mappings = {}
        new_datasets = {}

        if config.model.version == original_version:
            msg = f"current version cannot be the same as the original: {original_version}"
            raise DSGInvalidParameter(msg)

        affected = self._db.get_containing_models(
            context.connection, config.model, version=original_version
        )
        for mapping in self._update_dimension_mappings_with_dimensions(
            affected, config, original_version
        ):
            key = ConfigKey(mapping.model.mapping_id, mapping.model.version)
            new_mapping = self._dimension_mapping_mgr.update_with_context(mapping, context)
            assert key not in new_mappings
            new_mappings[key] = new_mapping
            logger.info(
                "Updated dimension mapping %s to %s as a result of dimension update",
                new_mapping.model.mapping_id,
                new_mapping.model.version,
            )
        for dataset in self._update_datasets_with_dimensions(affected, config, original_version):
            key = ConfigKey(dataset.model.dataset_id, dataset.model.version)
            new_dataset = self._dataset_mgr.update_with_context(dataset, context)
            assert key not in new_datasets
            new_datasets[key] = new_dataset
            logger.info(
                "Updated dataset %s to %s as a result of dimension update",
                new_dataset.model.dataset_id,
                new_dataset.model.version,
            )

        self._update_projects(
            context,
            dimensions={ConfigKey(config.model.dimension_id, original_version): config},
            dimension_mappings=new_mappings,
            datasets=new_datasets,
        )

    def _update_dimension_mapping_users(
        self,
        config: MappingTableConfig,
        original_version: str,
        context: RegistrationContext,
    ) -> None:
        self._update_projects(
            context,
            dimension_mappings={ConfigKey(config.model.mapping_id, original_version): config},
        )

    def _update_dataset_users(
        self,
        config: DatasetConfig,
        original_version: str,
        context: RegistrationContext,
    ) -> None:
        self._update_projects(
            context,
            datasets={ConfigKey(config.model.dataset_id, original_version): config},
        )

    def _update_dimension_mappings_with_dimensions(
        self,
        affected: dict[RegistryType, list[Any]],
        dim: DimensionConfig,
        original_version: str,
    ) -> list[MappingTableConfig]:
        mapping_updates: list[MappingTableConfig] = []
        for model in affected[RegistryType.DIMENSION_MAPPING]:
            assert isinstance(model, MappingTableModel)
            updated = False
            config = self._dimension_mapping_mgr.get_by_id(model.mapping_id, model.version)
            if (
                config.model.from_dimension.dimension_id == dim.model.dimension_id
                and config.model.from_dimension.version == original_version
            ):
                config.model.from_dimension.version = dim.model.version
                updated = True
            elif (
                config.model.to_dimension.dimension_id == dim.model.dimension_id
                and config.model.from_dimension.version == original_version
            ):
                config.model.to_dimension.version = dim.model.version
                updated = True
            if updated:
                mapping_updates.append(config)

        return mapping_updates

    def _update_datasets_with_dimensions(
        self,
        affected: dict[RegistryType, list[Any]],
        dim: DimensionConfig,
        original_version: str,
    ) -> list[DatasetConfig]:
        new_datasets = []
        for model in affected[RegistryType.DATASET]:
            assert isinstance(model, DatasetConfigModel)
            updated = False
            config = self._dataset_mgr.get_by_id(model.dataset_id, model.version)
            for ref in config.model.dimension_references:
                if ref.dimension_id == dim.model.dimension_id and ref.version == original_version:
                    ref.version = dim.model.version
                    updated = True
            if updated:
                new_datasets.append(config)

        return new_datasets

    def _update_projects(
        self,
        context: RegistrationContext,
        dimensions: dict[ConfigKey, DimensionConfig] | None = None,
        dimension_mappings: dict[ConfigKey, MappingTableConfig] | None = None,
        datasets: dict[ConfigKey, DatasetConfig] | None = None,
    ) -> None:
        updated_projects = {}
        if dimensions is not None:
            self._update_projects_with_new_dimensions(updated_projects, dimensions, context)
        if dimension_mappings is not None:
            self._update_projects_with_new_dimension_mappings(
                updated_projects, dimension_mappings, context
            )
        if datasets is not None:
            self._update_projects_with_new_datasets(updated_projects, datasets, context)

        for project_config in updated_projects.values():
            new_project = self._project_mgr.update_with_context(project_config, context)
            logger.info(
                "Updated project %s to %s as a result of dependent config updates.",
                new_project.model.project_id,
                new_project.model.version,
            )
        # TODO: Re-submit changed datasets to projects. dataset-to-project mappings might
        # take some work.

    def _update_projects_with_new_dimensions(
        self,
        updated_projects: dict[str, ProjectConfig],
        dimensions: dict[ConfigKey, DimensionConfig],
        context: RegistrationContext,
    ) -> None:
        """Updates the latest project configurations in place if they consume the dimensions.
        Edits updated_projects as necessary.
        """
        for key, dim in dimensions.items():
            for model in self._db.get_containing_models(
                context.connection,
                dim.model,
                version=key.version,
                parent_model_type=RegistryType.PROJECT,
            )[RegistryType.PROJECT]:
                config = updated_projects.get(
                    model.project_id, self._project_mgr.get_by_id(model.project_id)
                )
                updated = False
                for ref in config.model.dimensions.base_dimension_references:
                    if ref.dimension_id == dim.model.dimension_id and ref.version == key.version:
                        ref.version = dim.model.version
                        updated = True
                        break
                for ref in config.model.dimensions.supplemental_dimension_references:
                    if ref.dimension_id == dim.model.dimension_id and ref.version == key.version:
                        ref.version = dim.model.version
                        updated = True
                        break
                if updated and config.model.project_id not in updated_projects:
                    updated_projects[config.model.project_id] = config

    def _update_projects_with_new_dimension_mappings(
        self,
        updated_projects: dict[str, ProjectConfig],
        mappings: dict[ConfigKey, MappingTableConfig],
        context: RegistrationContext,
    ) -> None:
        """Updates the latest project configurations in place if they consume the mappings.
        Edits updated_projects as necessary.
        """
        for key, mapping in mappings.items():
            for model in self._db.get_containing_models(
                context.connection,
                mapping.model,
                version=key.version,
                parent_model_type=RegistryType.PROJECT,
            )[RegistryType.PROJECT]:
                config = updated_projects.get(
                    model.project_id, self._project_mgr.get_by_id(model.project_id)
                )
                updated = False
                for ref in config.model.dimension_mappings.base_to_supplemental_references:
                    if ref.mapping_id == mapping.model.mapping_id and ref.version == key.version:
                        ref.version = mapping.model.version
                        updated = True
                        break
                for ref in config.model.dimensions.supplemental_dimension_references:
                    if ref.dimension_id == mapping.model.mapping_id and ref.version == key.version:
                        ref.version = mapping.model.version
                        updated = True
                        break
                if updated and config.model.project_id not in updated_projects:
                    updated_projects[config.model.project_id] = config

    def _update_projects_with_new_datasets(
        self,
        updated_projects: dict[str, ProjectConfig],
        datasets: dict[ConfigKey, DatasetConfig],
        context: RegistrationContext,
    ) -> None:
        for key, dataset in datasets.items():
            for model in self._db.get_containing_models(
                context.connection,
                dataset.model,
                version=key.version,
                parent_model_type=RegistryType.PROJECT,
            )[RegistryType.PROJECT]:
                config = updated_projects.get(
                    model.project_id, self._project_mgr.get_by_id(model.project_id)
                )
                updated = False
                for dataset_ in config.model.datasets:
                    if (
                        dataset_.dataset_id == dataset.model.dataset_id
                        and dataset_.version == key.version
                    ):
                        dataset_.version = dataset.model.version
                        updated = True
                        break
                if updated and config.model.project_id not in updated_projects:
                    updated_projects[config.model.project_id] = config
