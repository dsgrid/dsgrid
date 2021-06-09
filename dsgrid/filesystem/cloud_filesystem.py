"""Abstract implementation for a cloud filesystem"""

import logging
import abc

from .filesystem_interface import FilesystemInterface

logger = logging.getLogger(__name__)


class CloudFilesystemInterface(FilesystemInterface, abc.ABC):
    """Interface to access and edit directories and files on remote cloud filesystem"""

    @abc.abstractmethod
    def check_versions(self, directory):
        """Check for multiple versions and versioning expectations of files.

        Parameters
        ----------
        directory : str
            Directory path
        """

    @abc.abstractmethod
    def list_versions(self, path):
        """List all versions of an S3 file object. Only possible in versioned buckets.

        Parameters
        ----------
        path : str
            Path
        """
