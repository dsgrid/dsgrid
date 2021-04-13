"""Implementation for local filesytem"""

import os
import shutil

from dsgrid.filesytem.filesystem_interface import FilesystemInterface


class LocalFilesystem(FilesystemInterface):
    """Provides access to the local filesystem."""

    def copy_file(self, src, dst):
        return shutil.copyfile(src, dst)

    def copy_tree(self, src, dst):
        return shutil.copytree(src, dst)

    def exists(self, path):
        return os.path.exists(path)

    def listdir(self, directory, files_only=False, directories_only=False):
        contents = os.listdir(directory)
        if files_only:
            return [x for x in contents if os.path.isfile(x)]
        if directories_only:
            return [x for x in contents if os.path.isdir(os.path.join(directory, x))]
        return contents

    def mkdir(self, directory):
        os.makedirs(directory, exist_ok=True)

    def rm_tree(self, directory):
        return shutil.rmtree(directory)
