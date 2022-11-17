from pathlib import Path
import uuid

from dsgrid.cloud.s3_storage_interface import S3StorageInterface
from dsgrid.dimension.base_models import DimensionType
from dsgrid.exceptions import DSGInvalidRegistryState
from dsgrid.tests.common import AWS_PROFILE_NAME, TEST_REMOTE_REGISTRY


def get_joined_file(parts, join_level):
    if join_level == 0:
        return parts[0]
    else:
        return str(Path("").joinpath(*[parts[i] for i in range(join_level + 1)]))


def check_config_dimensions(s3, F, msg):
    parts = Path(F).parts
    for level, f in enumerate(parts):
        if level == 2:
            # L2: /configs/dimensions/{dimension_type}
            if f not in (d.value for d in DimensionType):
                raise DSGInvalidRegistryState(msg.format(file=get_joined_file(parts, 2)))
        if level == 3:
            # L3: /configs/dimensions/{dimension_type}/{dimension_id}__{uuid}
            split_f = str(f).split("__")
            try:
                uuid.UUID(split_f[-1])
            except Exception:
                raise DSGInvalidRegistryState(msg.format(file=get_joined_file(parts, 3)))
        if level == 4:
            # L4: /configs/dimensions/{dimension_type}/{dimension_id}__{uuid}/registry.json5 | ...{version}
            # check that registry.json5 and 1.0.0 intial version dir exists
            dimension_registry_json5 = get_joined_file(parts, 3) + "/registry.json5"
            version_1_folder = get_joined_file(parts, 3) + "/1.0.0"
            for file in (dimension_registry_json5, version_1_folder):
                if not s3._s3_filesystem.path(file).exists():
                    raise DSGInvalidRegistryState(msg.format(file=file))
            # make sure all versions are semver
            files = [
                str(x)
                for x in s3._s3_filesystem.listdir(get_joined_file(parts, 3))
                if x != "registry.json5"
            ]
            for x in files:
                from dsgrid.utils.versioning import handle_version_or_str

                try:
                    handle_version_or_str(x)
                except Exception:
                    raise DSGInvalidRegistryState(msg.format(get_joined_file(parts, 3) / x))
        if level == 5:
            # L5: /configs/dimensions/{dimension_type}/{dimension_id}__{uuid}/{version}/dimension.json5 | ...{dimension}.csv (or json)
            # confirm that dimension.json5 exists
            dimension_json5 = s3._s3_filesystem.path(
                get_joined_file(parts, 4) + "/dimension.json5"
            )
            if not dimension_json5.exists():
                raise DSGInvalidRegistryState(msg.format(file=dimension_json5))
            # confirm that one dimension record is provided (unless dimension type == time)
            if parts[2] != "time":
                files = [
                    x
                    for x in s3._s3_filesystem.listdir(directory=get_joined_file(parts, 4))
                    if x != "dimension.json5"
                ]
                if len(files) != 1:
                    raise DSGInvalidRegistryState(msg.format(file=get_joined_file(parts, 4)))
                # also confirm that dimension record type is csv or json
                if Path(files[0]).suffix not in (".csv", ".json"):
                    raise DSGInvalidRegistryState(
                        msg.format(file=get_joined_file(parts, 4) + "/" + files[0])
                    )


def test_registry_path_expectations():
    """Test/check that registry files are all expected."""
    # TODO: this function is oeprational, however there is lots of logic tweaking to do to reduce path validation redundancy and improve test performance
    s3 = S3StorageInterface(
        local_path="",
        remote_path=TEST_REMOTE_REGISTRY,
        uuid="1",
        user="test",
        profile=AWS_PROFILE_NAME,
    )
    msg = (
        "INVALID TEST_REMOTE_REGISTRY STATE: An invalid file was pushed to dsgrid registry: {file}"
    )
    for level_0 in s3._s3_filesystem.listdir(exclude_hidden=False):
        # L0: Only 3 dirs allowed: /configs, /data, /.locks
        if level_0 not in ("configs", "data", ".locks"):
            raise DSGInvalidRegistryState(msg.format(file=level_0))
        for level_1 in s3._s3_filesystem.listdir(directory=level_0):
            base_level_1_file = Path(level_0) / Path(level_1)
            for F in s3._s3_filesystem.path(level_0 + "/" + level_1).rglob("*"):
                F = F.relative_to(TEST_REMOTE_REGISTRY[4:])
                if level_0 == ".locks":
                    if base_level_1_file.suffix != ".lock":
                        raise DSGInvalidRegistryState(msg.format(file=F))
                elif level_0 == "data":
                    pass  # TODO: Build out /data/ file checks
                elif level_0 == "configs":
                    # L1: make sure dir is of specific category type
                    if level_1 not in ("dimensions", "projects", "datasets", "dimension_mappings"):
                        raise DSGInvalidRegistryState(msg.format(file=base_level_1_file))
                    if level_1 == "dimensions":
                        check_config_dimensions(s3, F, msg)
                    if level_1 == "projects":
                        pass
                        # L2: {project_id}
                        # L3: {project_id}/registry.json5 | {project_id}/{version}
                        # L4: {project_id}/{version}/project.json5
                    if level_1 == "datasets":
                        pass
                        # L2: {dataset_id}
                        # L3: {dataset_id}/registry.json5 | {dataset_id}/{version}
                        # L4: {dataset_id}/{version}/dataset.json5
                    if level_1 == "dimension-mappings":
                        pass
                        # L2: {dimension_mapping_id}__{uuid}
                        # L3: {dimension_mapping_id}__{uuid}/registry.json5 | {dimension_mapping_id}__{uuid}/{version}
                        # L4: {dimension_mapping_id}__{uuid}/{version}/dimension_mapping.json5 | {dimension_mapping_id}__{uuid}/{version}/{dimension_mapping}.csv (or json)
