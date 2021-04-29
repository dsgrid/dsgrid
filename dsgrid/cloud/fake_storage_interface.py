from .cloud_storage_interface import CloudStorageInterface


class FakeStorageInterface(CloudStorageInterface):
    """Fake interface for tests and local mode."""

    def sync_pull(self, remote_path, local_path):
        pass

    def sync_push(self, local_path, remote_path):
        pass

    def lock_exists(self, path):
        pass

    def make_lock(self, path):
        pass

    def remove_lock(self, path):
        pass
