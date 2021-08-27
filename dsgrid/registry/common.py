"""Common definitions for registry components"""

import itertools
import re
import uuid
from collections import namedtuple
from datetime import datetime
from enum import Enum
from typing import Optional, Union

from pydantic import Field
from semver import VersionInfo

from dsgrid.data_models import DSGBaseModel, DSGEnum
from dsgrid.utils.versioning import make_version


REGISTRY_LOG_FILE = "dsgrid_registry.log"
# Allows letters, numbers, underscores, spaces, dashes
REGEX_VALID_REGISTRY_NAME = re.compile(r"^[\w -]+$")
# Allows letters, numbers, underscores, dashes
REGEX_VALID_REGISTRY_CONFIG_ID_LOOSE = re.compile(r"^[\w-]+$")
# Allows letters, numbers, underscores
REGEX_VALID_REGISTRY_CONFIG_ID_STRICT = re.compile(r"^[\w]+$")


def check_config_id_loose(config_id, tag):
    # Raises ValueError because this is used in Pydantic models.
    if not REGEX_VALID_REGISTRY_CONFIG_ID_LOOSE.search(config_id):
        raise ValueError(
            f"{tag} ID={config_id} is invalid. Restricted to letters, numbers, underscores, and dashes."
        )


def check_config_id_strict(config_id, tag):
    # Raises ValueError because this is used in Pydantic models.
    if not REGEX_VALID_REGISTRY_CONFIG_ID_STRICT.search(config_id):
        raise ValueError(
            f"{tag} ID={config_id} is invalid. Restricted to letters, numbers, and underscores."
        )


class RegistryType(DSGEnum):
    """Registry types"""

    DATASET = "dataset"
    DIMENSION = "dimension"
    DIMENSION_MAPPING = "dimension_mapping"
    PROJECT = "project"


class DatasetRegistryStatus(DSGEnum):
    """Statuses for a dataset within a project"""

    UNREGISTERED = "Unregistered"
    REGISTERED = "Registered"


class ProjectRegistryStatus(DSGEnum):
    """Statuses for a project within the DSGRID registry"""

    INITIAL_REGISTRATION = "Initial Registration"
    IN_PROGRESS = "In Progress"
    COMPLETE = "Complete"
    DEPRECATED = "Deprecated"


class VersionUpdateType(DSGEnum):
    """Types of updates that can be made to projects, datasets, and dimensions"""

    # TODO: we need to find general version update types that can be mapped to
    #   major, minor and patch.
    # i.e., replace input_dataset, fix project_config,
    MAJOR = "major"
    MINOR = "minor"
    PATCH = "patch"


# These keys are used to store references to project/dataset configs and dimensions
# in dictionaries.
# The DimensionKey is useful for comparing whether a project and
# dataset have the same dimension.
ConfigKey = namedtuple("ConfigKey", ["id", "version"])
DimensionKey = namedtuple("DimensionKey", ["type", "id", "version", "trivial"])

# Convenience container to be shared among the registry managers.
# Obviates the need to pass parameters to many constructors.
RegistryManagerParams = namedtuple(
    "RegistryManagerParams",
    ["base_path", "remote_path", "fs_interface", "cloud_interface", "offline", "dry_run"],
)


class ConfigRegistrationModel(DSGBaseModel):
    """Registration fields required by the ProjectConfig and DatasetConfig"""

    version: Union[str, VersionInfo] = Field(
        title="version",
        description="Version resulting from the registration",
    )
    submitter: str = Field(
        title="submitter",
        description="Username that submitted the registration",
    )
    date: datetime = Field(
        title="date",
        description="Registration date",
    )
    log_message: Optional[str] = Field(
        title="log_message",
        description="Reason for the update",
    )


def get_version_from_filename(filename):
    """Return the handle and version from a registry file."""
    regex = re.compile(r"(?P<handle>\w+)-v(?P<version>[\d\.]+).toml")
    match = regex.search(filename)
    assert match, filename
    return match.groupdict("handle"), make_version(match.groupdict("version"))


def make_initial_config_registration(submitter, log_message):
    version = VersionInfo(major=1)
    return ConfigRegistrationModel(
        version=version,
        submitter=submitter,
        date=datetime.now(),
        log_message=log_message,
    )


def make_filename_from_version(handle, version):
    """Make a filename with the handle and version."""
    return f"{handle}-v{version}.toml"


def make_registry_id(fields, delimiter="__"):
    """Make a unique ID by concatenating a list of fields with a UUID.

    Parameters
    ----------
    fields : list
    delimiter : str
        Delimiter used for concatenation

    Returns
    -------
    str

    """
    return delimiter.join(itertools.chain((str(x) for x in fields), [str(uuid.uuid4())]))


# def update_version(id_handle, update, registry_path):
#    """Determine registration or project version for registration.
#
#    TODO: Current solution is a quick hack. This needs to be better/formalized.
#        - Need smarter version updating / checks; use semvar packages
#        - Set to work with some central version (like S3)
#        - Currently only updating major version
#        - NOTE: not currently utilitzing the update_type in
#                ConfigRegistrationModel. Could use this to set
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
