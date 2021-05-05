from contextlib import contextmanager
from datetime import datetime
import json
import logging
import os
import time

from .cloud_storage_interface import CloudStorageInterface
from dsgrid.exceptions import DSGRegistryLockError
from dsgrid.filesystem.local_filesystem import LocalFilesystem
from dsgrid.filesystem.s3_filesystem import S3Filesystem
from dsgrid.utils.run_command import check_run_command

logger = logging.getLogger(__name__)


class S3StorageInterface(CloudStorageInterface):
    """Interface to S3."""

    def __init__(self, local_path, remote_path, uuid, user, profile):
        self._local_path = local_path
        self._remote_path = remote_path
        self._uuid = uuid
        self._user = user
        self._local_filesystem = LocalFilesystem()
        self._s3_filesystem = S3Filesystem(remote_path, profile)

    def _sync(self, src, dst, exclude=None):
        start = time.time()
        sync_command = f"aws s3 sync {src} {dst} --profile {self._s3_filesystem.profile}"
        if exclude:
            for x in exclude:
                sync_command = sync_command + f" --exclude {x}"
        logger.info("Running %s", sync_command)
        check_run_command(sync_command)
        logger.info("Command took %s seconds", time.time() - start)

    def check_locks(self, directory):
        filepath = self._s3_filesystem.S3Path(directory)
        if self.lock_exists(filepath):
            lock_file = self.get_locks(filepath)[0]  ## grab only one lock file
            lock_contents = self.read_lock(lock_file)
            if (
                not self._uuid == lock_contents["uuid"]
                and not self._user == lock_contents["username"]
            ):
                raise DSGRegistryLockError(
                    f"Registry path {str(filepath)} is currently locked by {lock_contents['username']}, timestamp={lock_contents['timestamp']}, uuid={lock_contents['uuid']}."
                )

    def get_locks(self, directory):
        contents = list(
            self._s3_filesystem.S3Path(directory).rglob(pattern="*.lock")
        )  # This seems to be slow
        return contents

    def lock_exists(self, directory):
        contents = self.get_locks(directory)
        return contents != []

    @contextmanager
    def make_lock(self, directory):
        try:
            filepath = self._s3_filesystem.S3Path(f"{directory}/registry.lock")
            if filepath.exists():
                lockfile_contents = self.read_lock(filepath)
                username = lockfile_contents["username"]
                uuid = lockfile_contents["uuid"]
                timestamp = lockfile_contents["timestamp"]
                raise DSGRegistryLockError(
                    f"Registry path {str(filepath)} is currently locked by {username}. Lock created as {timestamp} with uuid={uuid}."
                )
            self._s3_filesystem.S3Path(filepath).write_text(
                "{"
                + f'"username": "{self._user}", "uuid": "{self._uuid}", "timestamp": "{datetime.now()}"'
                + "}"
            )
            yield
        finally:
            self.remove_lock(directory)

    def read_lock(self, path):
        lockfile_contents = json.loads(self._s3_filesystem.S3Path(path).read_text())
        return lockfile_contents

    def remove_lock(self, directory, force=False):
        filepath = self._s3_filesystem.S3Path(f"{directory}/registry.lock")
        if filepath.exists():
            lockfile_contents = self.read_lock(filepath)
            if not force:
                if (
                    not self._uuid == lockfile_contents["uuid"]
                    and not self._user == lockfile_contents["username"]
                ):
                    raise DSGRegistryLockError(
                        f"Registry path {str(filepath)} is currently locked by {lockfile_contents['username']}. Lock created as {lockfile_contents['timestamp']} with uuid={lockfile_contents['uuid']}."
                    )
            filepath.unlink()

    def sync_pull(self, remote_path, local_path, exclude=None, delete_local=False):
        self.check_locks(remote_path)
        if delete_local:
            local_contents = self._local_filesystem.rglob(local_path)
            s3_contents = self._s3_filesystem.rglob(remote_path)
            for content in local_contents:
                relconent = os.path.relpath(content, local_path)
                if relconent not in s3_contents:
                    self._local_filesystem.rm(content)
                    logger.info("delete: %s", content)
        self._sync(remote_path, local_path, exclude)

    def sync_push(self, remote_path, local_path, exclude=None):
        self.check_locks(remote_path)
        self._sync(local_path, remote_path, exclude)
