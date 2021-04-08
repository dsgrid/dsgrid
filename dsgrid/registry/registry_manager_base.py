import re
from pathlib import Path


class RegistryManagerBase:

    DATASET_REGISTRY_PATH = Path("datasets")
    PROJECT_REGISTRY_PATH = Path("projects")
    DIMENSION_REGISTRY_PATH = Path("dimensions")
    REGEX_S3_PATH = re.compile(r"s3:\/\/(?P<bucket>[\w-]+)\/(?P<path>.*)")

    def __init__(self, path):
        self._on_aws = path.name.lower().startswith("s3")
        if self._on_aws:
            match = self.REGEX_S3_PATH.search(path.name)
            assert match, str(match)
            self._bucket = match.groupdict("bucket")
            self._path = match.groupdict("path")
        else:
            self._path = path
