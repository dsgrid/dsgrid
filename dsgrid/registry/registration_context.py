import logging
from typing import List

from dsgrid.exceptions import DSGInvalidParameter
from .common import RegistryType


logger = logging.getLogger(__name__)


class RegistrationContext:
    """Maintains state information across a multi-config registration process."""

    def __init__(self):
        self._ids = {
            RegistryType.DATASET: [],
            RegistryType.DIMENSION: [],
            RegistryType.DIMENSION_MAPPING: [],
            RegistryType.PROJECT: [],
        }
        self._managers = {
            RegistryType.DATASET: None,
            RegistryType.DIMENSION: None,
            RegistryType.DIMENSION_MAPPING: None,
            RegistryType.PROJECT: None,
        }

    def add_id(self, config_type: RegistryType, config_id: str, manager):
        """Add a config ID that has been registered.

        Parameters
        ----------
        config_type : RegistryType
        config_id : str

        Raises
        ------
        DSGInvalidParameter
            Raised if the config ID is already stored.

        """
        self.add_ids(config_type, [config_id], manager)

    def add_ids(self, config_type: RegistryType, config_ids: List[str], manager):
        """Add multiple config IDs that have been registered.

        Parameters
        ----------
        config_type : RegistryType
        config_ids : List[str]

        Raises
        ------
        DSGInvalidParameter
            Raised if a config ID is already stored.

        """
        # TODO DT: we should record some version from the registry so that we know we can sync safely.
        config_list = self._ids[config_type]
        if self._managers[config_type] is None:
            self._managers[config_type] = manager
        diff = set(config_ids).intersection(config_list)
        if diff:
            raise DSGInvalidParameter(
                f"One or more config IDs are already tracked: {config_type} {diff}"
            )

        logger.debug("Added registered IDs: %s %s", config_type, config_ids)
        config_list += config_ids

    def get_ids(self, config_type: RegistryType):
        """Return the config IDs for config_type that have been registered with this context.

        Parameters
        ----------
        config_type : RegistryType

        Returns
        -------
        List[str]

        """
        return self._ids[config_type]

    def finalize(self, error_occurred):
        """Perform final registration actions. If successful, sync all newly-registered configs
        and data with the remote registry. If there was an error, remove all intermediate
        registrations.

        Parameters
        ----------
        error_occurred : bool
            Set to True if there was a registration error. Remove all intermediate registrations.

        """
        try:
            mapping_ids = self._ids[RegistryType.DIMENSION_MAPPING]
            if mapping_ids:
                manager = self._managers[RegistryType.DIMENSION_MAPPING]
                manager.finalize_registration(mapping_ids, error_occurred)
                self._ids[RegistryType.DIMENSION_MAPPING].clear()
                self._managers[RegistryType.DIMENSION_MAPPING] = None

            dimension_ids = self._ids[RegistryType.DIMENSION]
            if dimension_ids:
                manager = self._managers[RegistryType.DIMENSION]
                manager.finalize_registration(dimension_ids, error_occurred)
                self._ids[RegistryType.DIMENSION].clear()
                self._managers[RegistryType.DIMENSION] = None

            dataset_ids = self._ids[RegistryType.DATASET]
            if dataset_ids:
                manager = self._managers[RegistryType.DATASET]
                manager.finalize_registration(dataset_ids, error_occurred)
                self._ids[RegistryType.DATASET].clear()
                self._managers[RegistryType.DATASET] = None

            project_ids = self._ids[RegistryType.PROJECT]
            if project_ids:
                manager = self._managers[RegistryType.PROJECT]
                manager.finalize_registration(project_ids, error_occurred)
                self._ids[RegistryType.PROJECT].clear()
                self._managers[RegistryType.PROJECT] = None
        except Exception:
            logger.exception(
                "An unexpected error occurred in finalize_registration. "
                "Please notify the dsgrid team because registry recovery may be required."
            )
            raise
