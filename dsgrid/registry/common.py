"""Common definitions for registry components"""

import logging
import re
from collections import namedtuple
from datetime import datetime
from enum import StrEnum
from pathlib import Path


from pydantic import Field

from dsgrid.data_models import DSGBaseModel
from dsgrid.exceptions import DSGInvalidParameter
from dsgrid.utils.versioning import make_version


REGISTRY_LOG_FILE = "dsgrid_registry.log"
# Allows letters, numbers, underscores, spaces, dashes
# Allows letters, numbers, underscores, dashes, spaces
REGEX_VALID_REGISTRY_NAME = re.compile(r"^[\w -]+$")
# Allows letters, numbers, underscores, dashes
REGEX_VALID_REGISTRY_CONFIG_ID_LOOSE = re.compile(r"^[\w/-]+$")
# Allows letters, numbers, underscores.
# dataset_id cannot start with a number because of uses in DatasetExpressionHandler
# It's likely a good rule everywhere else.
REGEX_VALID_REGISTRY_CONFIG_ID_STRICT = re.compile(r"^[a-zA-Z][\w]+$")

REGISTRY_ID_DELIMITER = "__"

logger = logging.getLogger(__name__)


def check_config_id_loose(config_id, tag):
    # Raises ValueError because this is used in Pydantic models.
    if not REGEX_VALID_REGISTRY_CONFIG_ID_LOOSE.search(config_id):
        msg = f"{tag} ID={config_id} is invalid. Restricted to letters, numbers, underscores, and dashes."
        raise ValueError(msg)


def check_config_id_strict(config_id, tag):
    # Raises ValueError because this is used in Pydantic models.
    if not REGEX_VALID_REGISTRY_CONFIG_ID_STRICT.search(config_id):
        msg = (
            f"{tag} ID={config_id} is invalid. Restricted to letters, numbers, and underscores. "
            "Cannot start with a number."
        )
        raise ValueError(msg)


class DatabaseConnection(DSGBaseModel):
    """Input information to connect to a registry database"""

    url: str
    # There attributes are commented-out because the registry is currently only
    # supported in SQLite. If/when we add postgres support, these can be added back.
    # database: str = "dsgrid"
    # hostname: str = "localhost"
    # port: int = 8529
    # username: str = "root"
    # password: str = DEFAULT_DB_PASSWORD

    # @classmethod
    # def from_url(cls, url, **kwargs):
    # """Create a connection from a URL."""
    # regex = re.compile(r"http://(.*):(\d+)")
    # match = regex.search(url)
    # if match is None:
    #    raise DSGInvalidParameter(f"Invalid URL format: {url}")
    # hostname = match.group(1)
    # port = match.group(2)
    # return cls(hostname=hostname, port=port, **kwargs)

    def get_filename(self) -> Path:
        """Return the filename from the URL. Only valid for SQLite databases.

        Raises
        ------
        DSGInvalidParameter
            Raised if the URL does not conform to the SQLite format.
        """
        # All call sites will need to be changed if/when we support Postgres.
        filename = self.try_get_filename()
        if filename is None:
            msg = (
                f"Failed to parse '{self.url}' into a SQLite URL. "
                "The SQLite file path must be specified in the format 'sqlite:///</path/to/db_file.db>'. "
            )
            raise DSGInvalidParameter(msg)
        return filename

    def try_get_filename(self) -> Path | None:
        """Return the filename from the URL, if file-based, otherwise None."""
        regex = re.compile(r"sqlite:\/\/\/(.*)")
        match = regex.search(self.url)
        if not match:
            return None
        return Path(match.group(1))


class DataStoreType(StrEnum):
    """Specifies the type of data store used for the registry."""

    FILESYSTEM = "filesystem"
    DUCKDB = "duckdb"


class RegistryType(StrEnum):
    """Registry types"""

    DATASET = "dataset"
    DIMENSION = "dimension"
    DIMENSION_MAPPING = "dimension_mapping"
    PROJECT = "project"


MODEL_TYPE_TO_ID_FIELD_MAPPING = {
    RegistryType.PROJECT: "project_id",
    RegistryType.DATASET: "dataset_id",
    RegistryType.DIMENSION: "dimension_id",
    RegistryType.DIMENSION_MAPPING: "mapping_id",
}


class RegistryTables(StrEnum):
    """Registry tables"""

    KEY_VALUE = "key_value"
    CURRENT_VERSIONS = "current_versions"
    MODELS = "models"
    REGISTRATIONS = "registrations"
    CONTAINS = "contains"


class DatasetRegistryStatus(StrEnum):
    """Statuses for a dataset within a project"""

    UNREGISTERED = "Unregistered"
    REGISTERED = "Registered"


class ProjectRegistryStatus(StrEnum):
    """Statuses for a project within the DSGRID registry"""

    INITIAL_REGISTRATION = "Initial Registration"
    IN_PROGRESS = "In Progress"
    COMPLETE = "Complete"
    PUBLISHED = "Published"
    DEPRECATED = "Deprecated"


class VersionUpdateType(StrEnum):
    """Types of updates that can be made to projects, datasets, and dimensions"""

    # TODO: we need to find general version update types that can be mapped to
    #   major, minor and patch.
    # i.e., replace input_dataset, fix project_config,
    MAJOR = "major"
    MINOR = "minor"
    PATCH = "patch"


# These keys are used to store references to project/dataset configs and dimensions
# in dictionaries.
ConfigKey = namedtuple("ConfigKey", ["id", "version"])

# Convenience container to be shared among the registry managers.
# Obviates the need to pass parameters to many constructors.
RegistryManagerParams = namedtuple(
    "RegistryManagerParams",
    [
        "base_path",
        "remote_path",
        "use_remote_data",
        "fs_interface",
        "cloud_interface",
        "offline",
        "scratch_dir",
    ],
)


class RegistrationModel(DSGBaseModel):
    """Registration fields required by the ProjectConfig and DatasetConfig"""

    id: int | None = Field(default=None, description="database ID of the registration")
    timestamp: datetime = Field(
        title="timestamp",
        description="Registration timestamp",
    )
    submitter: str = Field(
        title="submitter",
        description="Username that submitted the registration",
    )
    log_message: str | None = Field(
        default=None,
        title="log_message",
        description="Reason for the update",
    )
    update_type: VersionUpdateType = Field(
        title="update_type",
        description="Type of update",
    )


def get_version_from_filename(filename):
    """Return the handle and version from a registry file."""
    regex = re.compile(r"(?P<handle>\w+)-v(?P<version>[\d\.]+).json5")
    match = regex.search(filename)
    assert match, filename
    return match.groupdict("handle"), make_version(match.groupdict("version"))


def make_filename_from_version(handle, version):
    """Make a filename with the handle and version."""
    return f"{handle}-v{version}.json5"


# def update_version(id_handle, update, registry_path):
#    """Determine registration or project version for registration.
#
#    TODO: Current solution is a quick hack. This needs to be better/formalized.
#        - Need smarter version updating / checks; use semvar packages
#        - Set to work with some central version (like S3)
#        - Currently only updating major version
#        - NOTE: not currently utilitzing the update_type in
#                RegistrationModel. Could use this to set
#                major/minor/patch update decisiosns
#
#    Args:
#        registry_type (RegistryType): type of registry (e.g., Project, Dataset)
#        id_handle (str): ID handle is either the project_id or dataset_id
#        update (bool): config registration update setting
#    """
#
#    # TODO: remove when done. project path should be set somewhere else
#    if not os.path.exists(registry_path):
#        raise ValueError(f"Path does not exist: {registry_path}")
#
#    # if config.update is False, then assume major=1, minor=0, patch=0
#    if not update:
#        version = VersionInfo(major=1)
#        registry_file = Path(registry_path) / make_filename_from_version(id_handle, version)
#        # Raise error if v1.0.0 registry exists for project_id
#        if os.path.exists(registry_file):
#            raise ValueError(
#                f'{registry_type} registry for "{registry_file}" already '
#                f"exists. If you want to update the project registration"
#                f" with a new {registry_type} version, then you will need to"
#                f" set update=True in {registry_type} config. Alternatively, "
#                f"if you want to initiate a new dsgrid {registry_type}, you "
#                "will need to specify a new version handle in the "
#                f"{registry_type} config."
#            )
#    # if update is true...
#    else:
#        # list existing project registries
#        existing_versions = []
#        for f in os.listdir(registry_path):
#            handle, version = get_version_from_filename(f)
#            if handle == id_handle:
#                existing_versions.append(version)
#        # check for existing project registries
#        if not existing_versions:
#            raise ValueError(
#                "Registration.update=True, however, no updates can be made "
#                f"because there are no existing registries for {registry_type}"
#                f" ID = {id_handle}. Check project_id or set "
#                f"Registration.update=True in the {registry_type} Config."
#            )
#        # find the latest registry version
#        # NOTE: this is currently based on major verison only
#        last_version = sorted(existing_versions)[-1]
#        old_project_version = make_filename_from_version(id_handle, last_version)
#        old_registry_file = os.path.join(registry_path, old_project_version)
#
#        # deprecate old project registry
#        t = deserialize_registry(old_registry_file)
#        # DT: Can we use an enum here? Spelling/capitalization mistakes could be costly.
#        # Deprecated is a project status.
#        t["status"] = "Deprecated"
#        # DT: can we use version
#        t["version"] = last_version.bump_major()
#        # TODO: deserialize_registry should have returned a Pydantic model
#        serialize_registry(t, make_filename_from_version(id_handle, t["version"]))
#
#    return version
