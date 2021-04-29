from .cloud_storage_interface import CloudStorageInterface


class S3StorageInterface(CloudStorageInterface):
    """Interface to S3."""

    def __init__(self, local_path, remote_path):
        self._local_path = local_path
        self._remote_path = remote_path

    def sync_pull(self, remote_path, local_path):
        assert False

    def sync_push(self, local_path, remote_path):
        assert False

    def lock_exists(self, path):
        assert False

    def make_lock(self, path):
        assert False

    def remove_lock(self, path):
        assert False
