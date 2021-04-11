from pathlib import Path

from dsgrid.filesytem.aws import AwsS3Bucket
from dsgrid.filesytem.local_filesystem import LocalFilesystem


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
        fs_intf = AwsS3Bucket(path)
        # self._path = self._fs_intf.path
    else:
        fs_intf = LocalFilesystem()
        # self._path = path

    return fs_intf
