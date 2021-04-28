from .s3_storage_interface import S3StorageInterface
from .fake_storage_interface import FakeStorageInterface


def make_cloud_storage_interface(local_path, remote_path, offline=False):
    """Creates a CloudStorageInterface appropriate for path.

    Parameters
    ----------
    local_path : str
    remote_path : str
    offline : bool
        If True, don't perform any remote syncing operations.

    Returns
    -------
    CloudStorageInterface

    """
    if not offline and remote_path.lower().startswith("s3"):
        return S3StorageInterface(local_path, remote_path)
    return FakeStorageInterface()
