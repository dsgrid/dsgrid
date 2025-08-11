from contextlib import contextmanager
from .cloud_storage_interface import CloudStorageInterface


class FakeStorageInterface(CloudStorageInterface):
    """Fake interface for tests and local mode."""

    def check_lock_file(self, path):
        pass

    def check_valid_lock_file(self, path):
        pass

    def get_lock_files(self, relative_path=None):
        pass

    def has_lock_files(self):
        pass

    @contextmanager
    def make_lock_file_managed(self, path):
        yield

    def make_lock_file(self, path):
        pass

    def read_lock_file(self, path):
        pass

    def remove_lock_file(self, path, force=False):
        pass

    def sync_pull(self, remote_path, local_path, exclude=None, delete_local=False, is_file=False):
        pass

    def sync_push(self, remote_path, local_path, exclude=None):
        pass
