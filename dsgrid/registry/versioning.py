import os
import json5

DATASET_REGISTRY_PATH = "registry/datasets/"
PROJECT_REGISTRY_PATH = "registry/projects/"
DIMENSION_REGISTRY_PATH = "registry/dimensions/"


def versioning(registry_type, id_handle, update):
    """Determine registration or project version for registration.

    TODO: Current solution is a quick hack. This needs to be better/formalized.
        - Need smarter version updating / checks; use semvar packages
        - Set to work with some central version (like S3)
        - Currently only updating major version
        - NOTE: not currently utilitzing the update_type in
                ConfigRegistrationDetails. Could use this to set
                major/minor/patch update decisiosns

    Args:
        registry_type (RegistryType): type of registry (e.g., Project, Dataset)
        id_handle (str): ID handle is either the project_id or dataset_id
        update (bool): config registration update setting
    """

    # get registry path
    if registry_type == "dataset":
        registry_path = DATASET_REGISTRY_PATH
    if registry_type == "project":
        registry_path = PROJECT_REGISTRY_PATH
    if registry_type == "dimension":
        registry_path = DIMENSION_REGISTRY_PATH

    # TODO: remove when done. project path should be set somewhere else
    if not os.path.exists(registry_path):
        msg = f"Path does not exist: {registry_path}"
        raise ValueError(msg)

    # if config.update is False, then assume major=1, minor=0, patch=0
    if not update:
        version = f"{id_handle}-v1.0.0"
        registry_file = f"{registry_path}/{version}.json5"
        # Raise error if v1.0.0 registry exists for project_id
        if os.path.exists(registry_file):
            msg = (
                f'{registry_type} registry for "{registry_file}" already '
                f"exists. If you want to update the project registration"
                f" with a new {registry_type} version, then you will need to"
                f" set update=True in {registry_type} config. Alternatively, "
                f"if you want to initiate a new dsgrid {registry_type}, you "
                "will need to specify a new version handle in the "
                f"{registry_type} config."
            )
            raise ValueError(msg)
    # if update is true...
    else:
        # list existing project registries
        existing_versions = []
        for f in os.listdir(registry_path):
            if f.startswith(id_handle):
                existing_versions.append(int(f.split("-v")[1].split(".")[0]))
        # check for existing project registries
        if len(existing_versions) == 0:
            msg = (
                "Registration.update=True, however, no updates can be made "
                f"because there are no existing registries for {registry_type}"
                f" ID = {id_handle}. Check project_id or set "
                f"Registration.update=True in the {registry_type} Config."
            )
            raise ValueError(msg)
        # find the latest registry version
        # NOTE: this is currently based on major verison only
        last_vmajor_nbr = sorted(existing_versions)[-1]
        old_project_version = f"{id_handle}-v{last_vmajor_nbr}.0.0"
        old_registry_file = f"{registry_path}/{old_project_version}.json5"

        # depricate old project registry
        t = json5.load(old_registry_file)
        t["status"] = "Deprecated"
        with open(old_registry_file.format(**locals()), "w") as f:
            json5.dump(t, f)

        # update version
        # TODO NEED REAL LOGIC FOR THIS!
        #   - Currently assuming only major version is being updated
        major = int(last_vmajor_nbr) + 1
        minor = 0  # TODO: assume 0 for now
        patch = 0  # TODO: assume 0 for now

        version = f"{id_handle}-v{major}.{minor}.{patch}"

    return version
