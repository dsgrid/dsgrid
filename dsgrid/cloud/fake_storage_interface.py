from .cloud_storage_interface import CloudStorageInterface


class FakeStorageInterface(CloudStorageInterface):
    """Fake interface for tests and local mode."""

    def _sync(self, remote_path, local_path, exclude=None):
        pass

    def check_locks(self, directory):
        pass

    def get_locks(self, directory):
        pass

    def lock_exists(self, path):
        pass

    def make_lock(self, path):
        pass

    def read_lock(self, path):
        pass

    def remove_lock(self, path):
        pass

    def sync_pull(self, remote_path, local_path, exclude=None, delete_local=False):
        pass

    def sync_push(self, remote_path, local_path, exclude=None):
        pass
