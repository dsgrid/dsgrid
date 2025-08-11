from contextlib import contextmanager
from datetime import datetime
import json
import logging
import os
import sys
from pathlib import Path
import time

from dsgrid.exceptions import DSGMakeLockError, DSGRegistryLockError
from dsgrid.filesystem.local_filesystem import LocalFilesystem
from dsgrid.filesystem.s3_filesystem import S3Filesystem
from dsgrid.utils.run_command import check_run_command
from dsgrid.utils.timing import track_timing, timer_stats_collector
from .cloud_storage_interface import CloudStorageInterface

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

    def _sync(self, src, dst, exclude=None, is_file=False):
        start = time.time()
        aws_exec = self._get_aws_executable()
        cmd = "cp" if is_file else "sync"
        sync_command = f"{aws_exec} s3 {cmd} {src} {dst} --profile {self._s3_filesystem.profile}"
        if exclude:
            for x in exclude:
                sync_command = sync_command + f" --exclude {x}"
        logger.info("Running %s", sync_command)
        check_run_command(sync_command)
        logger.info("Command took %s seconds", time.time() - start)

    @staticmethod
    def _get_aws_executable():
        # subprocess.run cannot find the aws executable on Windows if shell is not True.
        # That's probably an aws cli bug. We can workaround it here.
        return "aws.cmd" if sys.platform == "win32" else "aws"

    def check_lock_file(self, path):
        self.check_valid_lock_file(path)
        filepath = self._s3_filesystem.path(path)
        if filepath.exists():
            lock_contents = self.read_lock_file(filepath)
            if (
                not self._uuid == lock_contents["uuid"]
                or not self._user == lock_contents["username"]
            ):
                msg = f"Registry path {str(filepath)} is currently locked by {lock_contents['username']}, timestamp={lock_contents['timestamp']}, uuid={lock_contents['uuid']}."
                raise DSGRegistryLockError(msg)

    def check_valid_lock_file(self, path):
        path = Path(path)
        # check that lock file is of type .lock
        if path.suffix != ".lock":
            msg = f"Lock file path provided ({path}) must be a valid .lock path"
            raise DSGMakeLockError(msg)
        # check that lock file in expected dirs
        relative_path = Path(path).parent
        if str(relative_path).startswith("s3:/nrel-dsgrid-registry/"):
            relative_path = relative_path.relative_to("s3:/nrel-dsgrid-registry/")
        if str(relative_path).startswith("/"):
            relative_path = relative_path.relative_to("/")
        if relative_path == Path("configs/.locks"):
            pass
        elif relative_path == Path("data/.locks"):
            pass
        else:
            DSGMakeLockError(
                "Lock file path provided must have relative path of configs/.locks or data/.locks"
            )
        return True

    def get_lock_files(self, relative_path=None):
        if relative_path:
            contents = self._s3_filesystem.path(
                f"{self._s3_filesystem.bucket}/{relative_path}"
            ).glob(pattern="**/*.locks")
        else:
            contents = self._s3_filesystem.path(self._s3_filesystem.bucket).glob(
                pattern="**/*.locks"
            )
        return contents

    def has_lock_files(self):
        contents = self.get_lock_files()
        return next(contents, None) is not None

    @contextmanager
    def make_lock_file_managed(self, path):
        try:
            self.make_lock_file(path)
            yield
        finally:
            self.remove_lock_file(path)

    def make_lock_file(self, path):
        self.check_lock_file(path)
        filepath = self._s3_filesystem.path(path)
        lock_content = {
            "username": self._user,
            "uuid": self._uuid,
            "timestamp": str(datetime.now()),
        }
        self._s3_filesystem.path(filepath).write_text(json.dumps(lock_content))

    def read_lock_file(self, path):
        lockfile_contents = json.loads(self._s3_filesystem.path(path).read_text())
        return lockfile_contents

    def remove_lock_file(self, path, force=False):
        filepath = self._s3_filesystem.path(path)
        if filepath.exists():
            lockfile_contents = self.read_lock_file(filepath)
            if not force:
                if (
                    not self._uuid == lockfile_contents["uuid"]
                    and not self._user == lockfile_contents["username"]
                ):
                    msg = f"Registry path {str(filepath)} is currently locked by {lockfile_contents['username']}. Lock created as {lockfile_contents['timestamp']} with uuid={lockfile_contents['uuid']}."
                    raise DSGRegistryLockError(msg)
            if force:
                logger.warning(
                    "Force removed lock file with user=%s and uuid=%s",
                    lockfile_contents["username"],
                    lockfile_contents["uuid"],
                )
            filepath.unlink()

    @track_timing(timer_stats_collector)
    def sync_pull(self, remote_path, local_path, exclude=None, delete_local=False, is_file=False):
        if delete_local:
            local_contents = self._local_filesystem.rglob(local_path)
            s3_contents = {
                str(self._s3_filesystem.path(x).relative_to(self._s3_filesystem.path(remote_path)))
                for x in self._s3_filesystem.rglob(remote_path)
            }
            for content in local_contents:
                relcontent = os.path.relpath(content, local_path)
                if relcontent not in s3_contents:
                    self._local_filesystem.rm(content)
                    logger.info("delete: %s because it is not in %s", relcontent, remote_path)
        self._sync(remote_path, local_path, exclude, is_file)

    @track_timing(timer_stats_collector)
    def sync_push(self, remote_path, local_path, exclude=None):
        self._sync(local_path, remote_path, exclude)
