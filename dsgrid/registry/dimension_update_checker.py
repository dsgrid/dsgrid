import logging

from .config_update_checker_base import ConfigUpdateCheckerBase


logger = logging.getLogger(__name__)


class DimensionUpdateChecker(ConfigUpdateCheckerBase):
    """Handles update checks for dimensions."""

    def check_preconditions(self):
        pass

    def handle_postconditions(self):
        pass
