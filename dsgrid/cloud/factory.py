# from .s3_storage_interface import S3StorageInterface
from .fake_storage_interface import FakeStorageInterface

# from dsgrid.common import AWS_PROFILE_NAME


def make_cloud_storage_interface(local_path, remote_path, uuid, user, offline=False):
    """Creates a CloudStorageInterface appropriate for path.

    Parameters
    ----------
    local_path : str
    remote_path : str
    uuid : str
        Unique ID to be used when generating cloud locks.
    user : str
        Username to be used when generating cloud locks.
    offline : bool, optional
        If True, don't perform any remote syncing operations.


    Returns
    -------
    CloudStorageInterface

    """
    if not offline and remote_path.lower().startswith("s3"):
        msg = f"Support for S3 is currently disabled: {remote_path=}"
        raise NotImplementedError(msg)
        # return S3StorageInterface(local_path, remote_path, uuid, user, profile=AWS_PROFILE_NAME)
    return FakeStorageInterface()
