from pathlib import Path

from .local_filesystem import LocalFilesystem

# from .s3_filesystem import S3Filesystem

# from dsgrid.common import AWS_PROFILE_NAME


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
        msg = f"Support for S3 is currently disabled: {path=}"
        raise NotImplementedError(msg)
        #    path = "s3" + path[2:]
        # fs_intf = S3Filesystem(path, AWS_PROFILE_NAME)
    else:
        fs_intf = LocalFilesystem()

    return fs_intf
