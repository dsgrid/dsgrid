import logging

from dsgrid.exceptions import DSGInvalidParameter
from dsgrid.utils.timing import timer_stats_collector, track_timing
from .common import RegistryType
from .registry_manager_base import RegistryManagerBase


logger = logging.getLogger(__name__)


class RegistrationContext:
    """Maintains state information across a multi-config registration process."""

    def __init__(self):
        self._managers = {
            # This order is required for cleanup in self.finalize().
            RegistryType.DIMENSION_MAPPING: None,
            RegistryType.DIMENSION: None,
            RegistryType.DATASET: None,
            RegistryType.PROJECT: None,
        }

    def __del__(self):
        for registry_type in list(RegistryType):
            manager = self._managers.get(registry_type)
            if manager is not None:
                logger.warning(
                    "RegistrationContext destructed with a reference to %s manager",
                    registry_type.value,
                )
                if not manager.offline_mode and manager.has_lock():
                    logger.error(
                        "RegistrationContext destructed with a lock on the remote registry. "
                        "Please contact the dsgrid team. Type=%s IDs=%s",
                        registry_type.value,
                        manager.ids,
                    )

    def add_id(self, registry_type: RegistryType, config_id: str, manager: RegistryManagerBase):
        """Add a config ID that has been registered.

        Parameters
        ----------
        registry_type : RegistryType
        config_id : str
        manager : RegistryManagerBase

        Raises
        ------
        DSGInvalidParameter
            Raised if the config ID is already stored.

        """
        self.add_ids(registry_type, [config_id], manager)

    def add_ids(
        self, registry_type: RegistryType, config_ids: list[str], manager: RegistryManagerBase
    ):
        """Add multiple config IDs that have been registered.

        Parameters
        ----------
        registry_type : RegistryType
        config_ids : list[str]
        manager : RegistryManagerBase

        Raises
        ------
        DSGInvalidParameter
            Raised if a config ID is already stored.

        """
        manager_context = self._managers[registry_type]
        if manager_context is None:
            manager_context = RegistryManagerContext(manager)
            self._managers[registry_type] = manager_context
            # manager.acquire_registry_locks(config_ids)
            # manager_context.set_locked()

        diff = set(config_ids).intersection(manager_context.ids)
        if diff:
            raise DSGInvalidParameter(
                f"One or more config IDs are already tracked: {registry_type} {diff}"
            )

        logger.debug("Added registered IDs: %s %s", registry_type, config_ids)
        manager_context.ids += config_ids

    def get_ids(self, registry_type: RegistryType):
        """Return the config IDs for registry_type that have been registered with this context.

        Parameters
        ----------
        registry_type : RegistryType

        Returns
        -------
        list[str]

        """
        manager_context = self._managers[registry_type]
        assert manager_context is not None, registry_type
        return manager_context.ids

    @track_timing(timer_stats_collector)
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
            for registry_type, manager_context in self._managers.items():
                if manager_context is not None:
                    if manager_context.ids:
                        manager_context.manager.finalize_registration(
                            manager_context.ids, error_occurred
                        )
                        manager_context.ids.clear()
                        manager_context.set_unlocked()
                    self._managers[registry_type] = None
        except Exception:
            logger.exception(
                "An unexpected error occurred in finalize_registration. "
                "Please notify the dsgrid team because registry recovery may be required."
            )
            raise


class RegistryManagerContext:
    """Maintains state for one registry type."""

    def __init__(self, manager: RegistryManagerBase):
        self._manager = manager
        self._has_lock = False
        self._ids = []

    def has_lock(self) -> bool:
        """Return True if the manager has acquired a lock on the remote registry."""
        return self._has_lock

    def set_locked(self):
        """Call when a lock has been acquired on the remote registry."""
        logger.debug("Locks acquired on remote registry for %s", self._manager.__class__.__name__)
        self._has_lock = True

    def set_unlocked(self):
        """Call when all locks have been released on the remote registry."""
        logger.debug("Locks released on remote registry for %s", self._manager.__class__.__name__)
        self._has_lock = False

    @property
    def ids(self):
        """Return a list of config IDs being managed."""
        return self._ids

    @ids.setter
    def ids(self, val):
        """Return a list of config IDs being managed."""
        self._ids = val

    @property
    def manager(self):
        """Return a RegistryManagerBase"""
        return self._manager

    @manager.setter
    def manager(self, val):
        """Set the RegistryManagerBase"""
        self._manager = val

    @property
    def offline_mode(self):
        """Return True if the manager is in offline mode."""
        return self._manager.offline_mode
