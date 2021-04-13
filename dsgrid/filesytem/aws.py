"""Implementation for AWS S3 bucket"""

import logging
import re
import time

import boto3

from dsgrid.utils.run_command import check_run_command

from dsgrid.filesytem.filesystem_interface import FilesystemInterface


logger = logging.getLogger(__name__)


class AwsS3Bucket(FilesystemInterface):
    """Provides access to an AWS S3 bucket."""

    REGEX_S3_PATH = re.compile(r"s3:\/\/(?P<bucket>[\w-]+)\/(?P<path>.*)")

    """Provides access to an AWS S3 bucket."""

    def __init__(self, path):
        match = self.REGEX_S3_PATH.search(path)
        assert match, f"Failed to parse AWS S3 bucket: {path}"
        self._bucket = match.groupdict("bucket")
        self._path = match.groupdict("path")
        self._session = boto3.session.Session()
        self._client = self._session.client("s3")

    def copy_file(self, src, dst):
        assert False, "not supported yet"

    def copy_tree(self, src, dst):
        assert False, "not supported yet"

    def exists(self, path):
        assert False, "not supported yet"

    def listdir(self, directory, files_only=False, directories_only=False):
        if files_only or directories_only:
            assert False, "limiting output not supported yet"
        result = self._client.list_objects_v2(Bucket=self._bucket, Prefix=directory)
        if result["IsTruncated"]:
            raise Exception(
                f"Received truncated result when listing {directory}. Need to improve logic."
            )
        return [x["Key"] for x in result["Contents"]]

    def mkdir(self, directory):
        assert False, "not supported yet"

    @property
    def path(self):
        """Return the base path in the bucket."""
        return self._path

    def rm_tree(self, directory):
        assert False, "not supported yet"


def sync(src, dst):
    """Sync source to destination."""
    start = time.time()
    sync_command = f"aws s3 sync {src} {dst}"
    logger.info("Running %s", sync_command)
    try:
        check_run_command(sync_command)
        logger.info("Command took %s seconds", time.time() - start)
    except:
        logger.error(
            "Syncing with AWS failed. You may need to run 'aws configure' " "to point to sdi."
        )
        raise
