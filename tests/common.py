"""Common functions used across tests"""

import fileinput
import os
import re
from pathlib import Path

from dsgrid.filesytem.local_filesystem import LocalFilesystem
from dsgrid.config.project_config import ProjectConfigModel


def replace_dimension_mapping_uuids_from_registry(registry_dir, filenames):
    uuids = read_dimension_mapping_uuid_mapping(registry_dir)
    for filename in filenames:
        if filename == filenames[0]:
            replace_dimension_mapping_uuids(filename, uuids)
            import toml

            cfg = ProjectConfigModel(**toml.load(str(filename)))
        # dataset.toml
        elif filename == filenames[1]:
            project_dims = [
                k.dimension_id.split("__")[0] for k in cfg.dimensions.project_dimensions
            ]
            project_dim_uuids = uuids.copy()
            for uid in uuids:
                if uid not in project_dims:
                    project_dim_uuids.pop(uid, None)
            replace_dimension_mapping_uuids(filename, project_dim_uuids)


def read_dimension_mapping_uuid_mapping(registry_dir):
    fs_intf = LocalFilesystem()
    dir_name = Path(registry_dir)
    mappings = {}
    regex = re.compile(r"(?P<from_dimension>\w+)__(?P<to_dimension>\w+)__(?P<uuid>[-0-9a-f]+)$")
    path = dir_name / "dimension_mappings"  # TODO: this path doesn't exist
    for item in fs_intf.listdir(path, directories_only=True, exclude_hidden=True):
        assert os.path.isdir(path / item), str(path / item)
        match = regex.search(item)
        assert match, item
        data = match.groupdict()
        from_dimension = data["from_dimension"]
        to_dimension = data["to_dimension"]
        item_uuid = data["uuid"]
        key = (from_dimension, to_dimension)
        assert key not in mappings, item
        mappings[key] = item_uuid

    return mappings


def replace_dimension_mapping_uuids(filename, uuids):
    regex = re.compile(
        r"mapping_id = \"(?P<from_dimension>\w+)__(?P<to_dimension>\w+)__(?P<uuid>[-0-9a-f]+)\""
    )
    with fileinput.input(files=[filename], inplace=True) as f:
        for line in f:
            match = regex.search(line)
            if match is None:
                print(line, end="")
            else:
                from_dimension = match.groupdict()["from_dimension"]
                to_dimension = match.groupdict()["to_dimension"]
                new_uuid = uuids[(from_dimension, to_dimension)]
                print(f'mapping_id = "{from_dimension}__{to_dimension}__{new_uuid}"')


def replace_dimension_uuids_from_registry(registry_dir, filenames):
    uuids = read_dimension_uuid_mapping(
        registry_dir
    )  # TODO: this is an issue because we don't want to return the supplemental dimensions for the dataset but we do want to return them for the project

    for filename in filenames:
        if filename == filenames[0]:
            replace_dimension_uuids(filename, uuids)
            import toml

            cfg = ProjectConfigModel(**toml.load(str(filename)))
        # dataset.toml
        elif filename == filenames[1]:
            print(filename)
            project_dims = [
                k.dimension_id.split("__")[0] for k in cfg.dimensions.project_dimensions
            ]
            project_dim_uuids = uuids.copy()
            for uid in uuids:
                if uid not in project_dims:
                    project_dim_uuids.pop(uid, None)
            replace_dimension_uuids(filename, project_dim_uuids)


def read_dimension_uuid_mapping(registry_dir):
    fs_intf = LocalFilesystem()
    dir_name = Path(registry_dir)
    mappings = {}
    regex = re.compile(r"(?P<dimension_type>[-\w]+)__(?P<uuid>[-0-9a-f]+)$")
    dim_base_path = dir_name / "configs" / "dimensions"
    for dim_type in fs_intf.listdir(dim_base_path, directories_only=True, exclude_hidden=True):
        dim_path = dim_base_path / dim_type
        for dim in fs_intf.listdir(dim_path, directories_only=True, exclude_hidden=True):
            print(dim_type)
            match = regex.search(dim)
            assert match, dim
            data = match.groupdict()
            dim_type = data["dimension_type"]
            dim_uuid = data["uuid"]
            if dim_type in mappings:
                print("FALSE!")
            assert dim_type not in mappings, dim_type
            mappings[dim_type] = dim_uuid

    return mappings


def replace_dimension_uuids(filename, uuids):
    regex = re.compile(r"dimension_id = \"(?P<dimension_type>[-\w]+)__(?P<uuid>[-0-9a-f]+)\"")

    with fileinput.input(files=[filename], inplace=True) as f:
        for line in f:
            match = regex.search(line)
            if match is None:
                print(line, end="")
            else:
                dimension_type = match.groupdict()["dimension_type"]
                new_uuid = uuids[dimension_type]
                print(f'dimension_id = "{dimension_type}__{new_uuid}"')
