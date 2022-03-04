import logging

from .config_update_checker_base import ConfigUpdateCheckerBase


logger = logging.getLogger(__name__)


class DatasetUpdateChecker(ConfigUpdateCheckerBase):
    """Handles update checks for datasets."""

    def check_preconditions(self):
        pass

    def handle_postconditions(self):
        pass
