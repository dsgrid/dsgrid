import logging
import os

from dsgrid.config.base_config import BaseConfig


logger = logging.getLogger(__name__)
PROJECTDIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


class ProjectConfig(BaseConfig):
    """
    A class for dsgrid project configurations
    """

    # TODO: set up project dimensions data classes
    # TODO: check files and values, etc.
    # TODO: run other quality checks on project config

    def __init__(self, config):
        super().__init__(config)
        self._project_name = self.get("project_name")
        self._project_version = self.get("project_version")
        self._input_datasets = self.get("input_datasets")
        self._dimensions = self._dimensions = self.get("dimensions")

    @property
    def project_name(self):
        """
        """
        return self._project_name

    @property
    def project_version(self):
        """
        """
        return self._project_version

    @property
    def input_datasets(self):
        """
        """
        return self._input_datasets

    @property
    def dimensions(self):
        """
        """
        return self._dimensions

    @property
    def project_dimensions():
        """[summary]
        """
        return None

    @property
    def other_dimensions():
        """
        """
        return None
