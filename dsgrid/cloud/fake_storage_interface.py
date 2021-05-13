from contextlib import contextmanager
from .cloud_storage_interface import CloudStorageInterface


class FakeStorageInterface(CloudStorageInterface):
    """Fake interface for tests and local mode."""

    def check_lock_file(self, path):
        pass

    def check_valid_lock_file(self, path):
        pass

    def get_lock_files(self, directory):
        pass

    def has_lock_files(self, directory):
        pass

    @contextmanager
    def make_lock_file(self, path):
        try:
            yield
        finally:
            pass

    def read_lock_file(self, path):
        pass

    def remove_lock_file(self, path):
        pass

    def sync_pull(self, remote_path, local_path, exclude=None, delete_local=False):
        pass

    def sync_push(self, remote_path, local_path, exclude=None):
        pass
