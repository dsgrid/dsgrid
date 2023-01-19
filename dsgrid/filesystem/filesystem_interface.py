"""Defines interface to access all filesystems"""

import abc
from pathlib import Path


class FilesystemInterface(abc.ABC):
    """Interface to access and edit directories and files on a local or remote filesystem"""

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
    def listdir(self, directory, files_only=False, directories_only=False, exclude_hidden=False):
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
    def path(self, path) -> Path:
        """Return an object that meets the interface of pathlib.Path.

        Parameters
        ----------
        path : str

        Returns
        -------
        Path
        """

    @abc.abstractmethod
    def rglob(
        self,
        directory,
        files_only=False,
        directories_only=False,
        exclude_hidden=False,
        pattern="*",
    ):
        """Recursively search a path and return a list of relative paths that match criteria.

        Parameters
        ----------
        directory : str
        files_only : bool, optional
            Return files only, by default False
        directories_only : bool, optional
            Return directories only, by default False
        exclude_hidden : bool, optional
            Exclude hidden files, by default False
        pattern : str, optional
            Search for files with a specific pattern, by default "*"
        """

    @abc.abstractmethod
    def rm_tree(self, directory):
        """Remove all files and directories, recursively.

        Parameters
        ----------
        directory : str

        """

    @abc.abstractmethod
    def touch(self, path):
        """Touch

        Parameters
        ----------
        directory : str
            filepath
        """
