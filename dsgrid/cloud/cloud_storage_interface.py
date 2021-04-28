import abc


class CloudStorageInterface(abc.ABC):
    """Defines interface to synchronize data stored on a cloud storage system."""

    @abc.abstractmethod
    def sync_pull(self, remote_path, local_path):
        """Synchronize data from remote_path to local_path.
        Deletes any files in local_path that do not exist in remote_path.

        """

    @abc.abstractmethod
    def sync_push(self, local_path, remote_path):
        """Synchronize data from local path to remote_path"""

    @abc.abstractmethod
    def lock_exists(self, path):
        """Returns True if a lock exists at path."""

    @abc.abstractmethod
    def make_lock(self, path):
        """Make a lock at path.

        Returns
        -------
        bool
            True if the lock was successfully created, otherwise False

        """

    @abc.abstractmethod
    def remove_lock(self, path):
        """Remove the lock at path."""
