from pathlib import Path

# from .aws import AwsS3Bucket
from .local_filesystem import LocalFilesystem
from .s3_filesystem import S3Filesystem


def make_filesystem_interface(path):
    """Make an instance of FilesystemInterface appropriate for the path.

    Parameters
    ----------
    path : str

    Returns
    -------
    FilesystemInterface

    """
    if isinstance(path, Path):
        path = str(path)
    if path.lower().startswith("s3"):
        if path.startswith("S3"):
            path = "s3" + path[2:]
        fs_intf = S3Filesystem(path)
    else:
        fs_intf = LocalFilesystem()

    return fs_intf