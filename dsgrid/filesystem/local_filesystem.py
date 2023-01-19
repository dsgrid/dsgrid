"""Implementation for local filesytem"""

import logging
import os
import shutil
from pathlib import Path

from dsgrid.filesystem.filesystem_interface import FilesystemInterface

logger = logging.getLogger(__name__)


class LocalFilesystem(FilesystemInterface):
    """Provides access to the local filesystem."""

    def copy_file(self, src, dst):
        return shutil.copyfile(src, dst)

    def copy_tree(self, src, dst):
        return shutil.copytree(src, dst)

    def exists(self, path):
        return os.path.exists(path)

    def listdir(self, directory, files_only=False, directories_only=False, exclude_hidden=False):
        contents = os.listdir(directory)
        if exclude_hidden:
            contents = [x for x in contents if not x.startswith(".")]
        if files_only:
            return [x for x in contents if os.path.isfile(os.path.join(directory, x))]
        if directories_only:
            return [x for x in contents if os.path.isdir(os.path.join(directory, x))]
        return contents

    def path(self, path) -> Path:
        return Path(path)

    def rglob(
        self,
        directory,
        files_only=False,
        directories_only=False,
        exclude_hidden=False,
        pattern="*",
    ):
        contents = [c for c in Path(directory).rglob(pattern)]
        if exclude_hidden:
            # NOTE: this does not currently ignore hidden directories in the path.
            contents = [x for x in contents if not x.name.startswith(".")]
        if files_only:
            return [x for x in contents if os.path.isfile(x)]
        if directories_only:
            return [x for x in contents if os.path.isdir(x)]
        return contents

    def mkdir(self, directory):
        os.makedirs(directory, exist_ok=True)

    def rm_tree(self, directory):
        return shutil.rmtree(directory)

    def rm(self, path):
        if os.path.exists(path):
            if os.path.isdir(path):
                if os.listdir(path):
                    self.rm_tree(path)
                else:
                    os.removedirs(path)
            elif os.path.isfile(path):
                os.remove(path)
        logger.warning("path %s does not exist", path)

    def touch(self, path):
        Path(path).touch()
