import getpass
import logging
from datetime import datetime
from typing import Self

from sqlalchemy import Connection

from dsgrid.exceptions import DSGInvalidParameter
from dsgrid.spark.functions import drop_temp_tables_and_views
from dsgrid.registry.common import RegistrationModel, RegistryType, VersionUpdateType
from dsgrid.registry.registry_interface import RegistryInterfaceBase
from dsgrid.utils.timing import timer_stats_collector, track_timing


logger = logging.getLogger(__name__)


class RegistrationContext:
    """Maintains state information across a multi-config registration process."""

    def __init__(
        self,
        db: RegistryInterfaceBase,
        log_message: str,
        update_type: VersionUpdateType,
        submitter: str | None,
    ):
        self._conn: Connection | None = None
        self._db = db
        self._registration = RegistrationModel(
            timestamp=datetime.now(),
            submitter=submitter or getpass.getuser(),
            log_message=log_message,
            update_type=update_type,
        )
        self._managers: dict[RegistryType, RegistryManagerContext | None] = {
            # This order is required for cleanup in self.finalize().
            RegistryType.PROJECT: None,
            RegistryType.DATASET: None,
            RegistryType.DIMENSION_MAPPING: None,
            RegistryType.DIMENSION: None,
        }

    def __del__(self):
        for registry_type in RegistryType:
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

    def __enter__(self) -> Self:
        self._conn = self._db.engine.connect()
        self._registration = self._db.insert_registration(self._conn, self._registration)
        return self

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        if self._conn is None:
            return
        try:
            if exc_type is None:
                self.finalize(False)
                self._conn.commit()
            else:
                # Order is important. Don't rollback the configs until dataset files are deleted.
                self.finalize(True)
                self._conn.rollback()
        finally:
            self._conn.close()

    @property
    def connection(self) -> Connection:
        """Return the active sqlalchemy connection."""
        assert self._conn is not None
        return self._conn

    @property
    def registration(self) -> RegistrationModel:
        """Return the registration entry for this context."""
        return self._registration

    def add_id(self, registry_type: RegistryType, config_id: str, manager):
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

    def add_ids(self, registry_type: RegistryType, config_ids: list[str], manager):
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
            msg = f"One or more config IDs are already tracked: {registry_type} {diff}"
            raise DSGInvalidParameter(msg)

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
    def finalize(self, error_occurred: bool):
        """Perform final registration actions. If successful, sync all newly-registered configs
        and data with the remote registry. If there was an error, remove all intermediate
        registrations.
        """
        try:
            drop_temp_tables_and_views()
            for registry_type, manager_context in self._managers.items():
                if manager_context is not None:
                    if manager_context.ids:
                        manager_context.manager.finalize_registration(
                            self._conn, set(manager_context.ids), error_occurred
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

    def __init__(self, manager):
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
