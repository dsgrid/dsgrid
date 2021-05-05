"""Abstract implementation for a cloud filesystem"""

import logging
import abc

from .filesystem_interface import FilesystemInterface

logger = logging.getLogger(__name__)


class CloudFilesystemInterface(FilesystemInterface):
    """Interface to access and edit directories and files on remote cloud filesystem"""

    @abc.abstractmethod
    def copy_file(self, src, dst):
        """Copy a file to a destination.

        Parameters
        ----------
        src : str
            Path to source file
        dst : str
            Path to destination file

        """

    @abc.abstractmethod
    def copy_tree(self, src, dst):
        """Copy src to dst recursively.

        Parameters
        ----------
        src : str
            Source directory
        dst : str
            Destination directory

        """

    @abc.abstractmethod
    def exists(self, path):
        """Return True if path exists.

        Parameters
        ----------
        path : str

        Returns
        -------
        bool

        """

    @abc.abstractmethod
    def listdir(
        self,
        directory,
        files_only=False,
        directories_only=False,
        exclude_hidden=False,
        recursive=True,
    ):
        """List the contents of a directory.

        Parameters
        ----------
        directory : str
        files_only : bool
            only return files
        directories_only : bool
            only return directories
        exclude_hidden : bool
            exclude names starting with "."
        recursive : bool
            recursive list; default=True

        Returns
        -------
        list
            list of str

        """

    @abc.abstractmethod
    def mkdir(self, directory):
        """Make a directory. Do nothing if the directory exists.

        Parameters
        ----------
        directory : str

        """

    @abc.abstractmethod
    def rm_tree(self, directory):
        """Remove all files and directories, recursively.

        Parameters
        ----------
        directory : str

        """

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

    @abc.abstractclassmethod
    def touch(self, filepath):
        """Touch

        Parameters
        ----------
        directory : str
            filepath
        """
