"""Implementation for AWS S3 bucket filesystem"""

import logging
import re
import time
import os

import boto3

from .cloud_filesystem import CloudFilesystemInterface
from .local_filesystem import LocalRegistryFilesystem
from dsgrid.utils.run_command import check_run_command


from dsgrid.common import AWS_PROFILE_NAME, LOCAL_REGISTRY, REMOTE_REGISTRY
from dsgrid.exceptions import DSGFilesystemInterfaceError

logger = logging.getLogger(__name__)


class S3Filesystem(CloudFilesystemInterface):
    """Provides access to an AWS S3 bucket."""

    REGEX_S3_PATH = re.compile(r"s3:\/\/(?P<bucket>[\w-]+)\/?(?P<prefix>.*)?")

    def __init__(self, path=REMOTE_REGISTRY):
        match = self.REGEX_S3_PATH.search(str(path))
        assert match, f"Failed to parse AWS S3 bucket: {path}"
        self._bucket = match.groupdict()["bucket"]
        self._relpath = match.groupdict()["prefix"]
        self._path = str(path)
        self._profile = AWS_PROFILE_NAME
        self._session = boto3.session.Session(profile_name=self._profile)
        self._client = self._session.client("s3")

    def copy_file(self, src, dst):
        assert False, "not supported yet"

    def copy_tree(self, src, dst):
        assert False, "not supported yet"

    def exists(self, path):
        assert False, "not supported yet"

    def listdir(
        self,
        directory="",
        files_only=False,
        directories_only=False,
        exclude_hidden=True,
        recursive=True,
    ):
        # TODO: Need to add support to exclude_hidden=True
        if recursive:
            result = self._client.list_objects_v2(Bucket=self._bucket, Prefix=directory)
            keys = [x["Key"] for x in result["Contents"]]
        else:
            result = self._client.list_objects_v2(
                Bucket=self._bucket, Prefix=directory, Delimiter="/"
            )
            if "Contents" in result:
                if "CommonPrefixes" in result:
                    keys = [x["Prefix"] for x in result["CommonPrefixes"]] + [
                        x["Key"] for x in result["Contents"]
                    ]
                else:
                    keys = [["Key"] for x in result["Contents"]]
            else:
                keys = [x["Prefix"] for x in result["CommonPrefixes"]]

        if result["IsTruncated"]:
            raise Exception(
                f"Received truncated result when listing {directory}. Need to improve logic."
            )

        if files_only:
            if directories_only:
                raise DSGFilesystemInterfaceError(
                    "Both files_only and directories_only are true. Only one can be true."
                )
            objects = []
            for key in keys:
                if not key.endswith("/"):
                    objects.append(key)
            return objects

        else:
            objects = []
            for key in keys:
                if key.endswith("/"):
                    objects.append(key[:-1])
                else:
                    key_split = key.split("/")
                    for i, k in enumerate(key_split):
                        if i == 0:
                            objects.append(k)
                        else:
                            objects.append(key.split(k)[0] + k)
            if not directories_only:
                return objects
            if directories_only:
                file_objects, dir_objects = set(), set()
                for key in keys:  # TODO: remove some repetition here
                    if not key.endswith("/"):
                        file_objects.add(key)
                for key in objects:
                    if key not in file_objects:
                        dir_objects.add(key)
                return dir_objects
            assert False

    def mkdir(self, directory):
        assert False, "not supported yet"

    @property
    def path(self):
        """Return the full s3 path."""
        return self._path

    def rm_tree(self, directory):
        assert False, "not supported yet"

    def check_versions(self, directory):
        assert False, "not supported yet"

    def listregistry(self, directory):
        assert False, "not supported yet"

    def lock_exists(self, directory):
        assert False, "not supported yet"

    def make_lock(self, directory):
        assert False, "not supported yet"

    def remove_lock(self, directory):
        assert False, "not supported yet"

    @staticmethod
    def sync(src, dst, include_data=False):
        """Base sync function to sync S3 registry with local registry."""
        # TODO: is it possible to prevent this from being called outside of the class?
        start = time.time()
        sync_command = (
            f"aws s3 sync {src} {dst} --profile {AWS_PROFILE_NAME} --exclude '**/.DS_STORE'"
        )
        if not include_data:
            sync_command = sync_command + " --exclude data/*"
        logger.info("Running %s", sync_command)
        check_run_command(sync_command)
        logger.info("Command took %s seconds", time.time() - start)

    @staticmethod
    def sync_pull(local_registry, include_data=False):
        """AWS S3 sync pull files from AWS remote to local.
        Removes local files not in S3 remote.

        Parameters
        ----------
        local_registry : Path
            Local registry path
        include_data : bool, optional
            Flag to include data dir, by default False
        """
        s3_filesystem = S3Filesystem(REMOTE_REGISTRY)
        local_interface = LocalRegistryFilesystem(local_registry)
        local_contents = local_interface.listdir()
        s3_contents = s3_filesystem.listdir()
        if not include_data:
            local_contents = [
                c
                for c in local_contents
                if not str(c).split(local_registry)[1].startswith("/data")
            ]
        for content in local_contents:
            relconent = os.path.relpath(content, local_interface.path)
            if relconent not in s3_contents:
                local_interface.rm(content)
                logger.info("delete: %s", str(content))
        S3Filesystem.sync(s3_filesystem.path, local_interface.path, include_data)

    @staticmethod
    def sync_push(
        remote_registry=REMOTE_REGISTRY, local_registry=LOCAL_REGISTRY, include_data=False
    ):
        """AWS S3 sync push files from local registry to remote.
        Checks for lock files before pushing.

        Parameters
        ----------
        remote_registry : Path, optional
            Remote registry path, by default REMOTE_REGISTRY
        local_registry : Path, optional
            Local registry path, by default LOCAL_REGISTRY
        include_data : bool, optional
            Flag to include data directory, by default False
        """
        # TODO: Must fail if lock files exist.
        # TODO: Prevent pushing hidden/unwanted files (e.g., .DS_STORE)
        s3_path = S3Filesystem(REMOTE_REGISTRY).path
        local_path = LocalRegistryFilesystem(local_registry).path

        S3Filesystem.sync(local_path, s3_path, include_data)

    # @staticmethod
    # def sync_data_pull(s3_data_path, local_data_path):
    #     S3Filesystem.sync(local_path, s3_path, include_data=True)
