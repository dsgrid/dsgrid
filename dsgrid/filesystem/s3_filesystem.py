"""Implementation for AWS S3 bucket filesystem"""

import boto3
import logging
import re
from s3path import S3Path, register_configuration_parameter

from .cloud_filesystem import CloudFilesystemInterface


logger = logging.getLogger(__name__)


class S3Filesystem(CloudFilesystemInterface):
    """Provides access to an AWS S3 bucket."""

    REGEX_S3_PATH = re.compile(r"s3:\/\/(?P<bucket>[\w-]+)\/?(?P<prefix>.*)?")

    def __init__(self, path, profile):
        match = self.REGEX_S3_PATH.search(str(path))
        assert match, f"Failed to parse AWS S3 bucket: {path}"
        self._bucket = match.groupdict()["bucket"]
        self._relpath = match.groupdict()["prefix"]
        self._uri = str(path)
        self._profile = profile
        self._session = boto3.session.Session(profile_name=self._profile)
        self._client = self._session.client("s3")

        register_configuration_parameter(S3Path("/"), resource=self._session.resource("s3"))

    def _Key(self, path):
        """Get formatted S3 key from provided path for S3Path module"""
        if not path:
            path = ""
        if str(path).startswith(f"/{self._bucket}"):
            path = str(path)[len(f"/{self._bucket}") :]
        if str(path).startswith(self._bucket):
            path = str(path)[len({self._bucket}) :]
        if str(path).startswith(self._uri):
            path = str(path).replace(self._uri, "")
        if str(path).startswith("/"):
            path = str(path)[1:]
        return path

    def check_versions(self, directory):
        assert False, "not supported yet"

    def copy_file(self, src, dst):
        assert False, "not supported yet"

    def copy_tree(self, src, dst):
        assert False, "not supported yet"

    def exists(self, path):
        return self.S3Path(path).exists()

    def listdir(
        self, directory="", files_only=False, directories_only=False, exclude_hidden=False
    ):
        contents = [x for x in self.S3Path(directory).glob("*") if x.name != ""]
        if exclude_hidden:
            # NOTE: this does not currently ignore hidden directories in the path.
            contents = [x for x in contents if not x.name.startswith(".")]
        if files_only:
            return [x.name for x in contents if x.is_file()]
        if directories_only:
            return [x.name for x in contents if x.is_dir()]
        return [x.name for x in contents]

    def list_versions(self, directory, prefix=""):
        assert False, "not supported yet"
        self._s3.list_object_versions(Bucket=self._bucket, Prefix=prefix)

    def mkdir(self, directory):
        key = self._Key(directory)
        self._client.put_object(Bucket=self._bucket, Body="", Key=f"{key}/")
        return None

    @property
    def path(self):
        """Return the full s3 path."""
        return self._path

    def rglob(
        self,
        directory=None,
        files_only=False,
        directories_only=False,
        exclude_hidden=True,
        pattern="*",
    ):
        directory = str(self.S3Path(directory))
        contents = list(self.S3Path(directory).rglob(pattern))
        if exclude_hidden:
            contents = [
                str(x).replace(f"{directory}/", "") for x in contents if not x.name.startswith(".")
            ]
        if files_only:
            return [str(x).replace(f"{directory}/", "") for x in contents if x.is_file()]
        if directories_only:
            return [str(x).replace(f"{directory}/", "") for x in contents if x.is_dir()]
        return [str(x).replace(f"{directory}/", "") for x in contents]

    def rm_tree(self, directory):
        assert False, "not supported yet"

    def S3Path(self, path):
        """Returns S3Path"""
        path = self._Key(path)
        return S3Path(f"/{self._bucket}/{path}")

    def touch(self, filepath):
        self.S3Path(filepath).touch()
