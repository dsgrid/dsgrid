import logging

from .config_update_checker_base import ConfigUpdateCheckerBase


logger = logging.getLogger(__name__)


class DimensionMappingUpdateChecker(ConfigUpdateCheckerBase):
    """Handles update checks for dimension mappings."""

    def check_preconditions(self):
        pass

    def handle_postconditions(self):
        pass
