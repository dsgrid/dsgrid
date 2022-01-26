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
        "dimension_associations",
    )

    def check_preconditions(self):
        if self._old_model.status not in self._ALLOWED_UPDATE_STATUSES:
            raise DSGInvalidRegistryState(
                f"project status={self._old_model.status} must be one of {self._ALLOWED_UPDATE_STATUSES} in order to update"
            )

    def handle_postconditions(self):
        if set(self._REQUIRES_DATASET_UNREGISTRATION).intersection(self._changed_fields):
            for dataset in self._new_model.datasets:
                if dataset.status == DatasetRegistryStatus.REGISTERED:
                    dataset.status = DatasetRegistryStatus.UNREGISTERED
            logger.info("Set all datasets in %s to unregistered", self._new_model.project_id)
