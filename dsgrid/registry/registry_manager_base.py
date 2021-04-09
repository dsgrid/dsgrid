"""Base class for all registry managers."""

from pathlib import Path

from dsgrid.filesytem.aws import AwsS3Bucket
from dsgrid.filesytem.local_filesystem import LocalFilesystem


class RegistryManagerBase:
    """Base class for all registry managers."""

    DATASET_REGISTRY_PATH = Path("datasets")
    PROJECT_REGISTRY_PATH = Path("projects")
    DIMENSION_REGISTRY_PATH = Path("dimensions")

    def __init__(self, path, fs_interface):
        if isinstance(fs_interface, AwsS3Bucket):
            self._path = fs_interface.path
        else:
            assert isinstance(fs_interface, LocalFilesystem)
            self._path = path

        self._fs_intf = fs_interface

    @property
    def fs_interface(self):
        """Return the filesystem interface."""
        return self._fs_intf
