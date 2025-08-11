import logging

from dsgrid.exceptions import DSGInvalidRegistryState
from .config_update_checker_base import ConfigUpdateCheckerBase
from .common import DatasetRegistryStatus, ProjectRegistryStatus


logger = logging.getLogger(__name__)


class ProjectUpdateChecker(ConfigUpdateCheckerBase):
    """Handles update checks for projects."""

    _ALLOWED_UPDATE_STATUSES = (
        ProjectRegistryStatus.INITIAL_REGISTRATION,
        ProjectRegistryStatus.IN_PROGRESS,
        ProjectRegistryStatus.COMPLETE,
    )
    _REQUIRES_DATASET_UNREGISTRATION = (
        "dimensions",
        "dimension_mappings",
    )

    def check_preconditions(self):
        if self._old_model.status not in self._ALLOWED_UPDATE_STATUSES:
            msg = f"project status={self._old_model.status} must be one of {self._ALLOWED_UPDATE_STATUSES} in order to update"
            raise DSGInvalidRegistryState(msg)

    def handle_postconditions(self):
        # TODO #191: detect changes to required dimensions for each dataset.
        changes = set(self._REQUIRES_DATASET_UNREGISTRATION).intersection(self._changed_fields)
        if changes:
            for dataset in self._new_model.datasets:
                if dataset.status == DatasetRegistryStatus.REGISTERED:
                    dataset.status = DatasetRegistryStatus.UNREGISTERED
            logger.warning(
                "Set all datasets in %s to unregistered because of changes=%s. "
                "They must be re-submitted.",
                self._new_model.project_id,
                changes,
            )
            if self._new_model.status == ProjectRegistryStatus.COMPLETE:
                self._new_model.status = ProjectRegistryStatus.IN_PROGRESS
                logger.warning(
                    "Set project status to %s because of changes=%s.",
                    self._new_model.status,
                    changes,
                )
