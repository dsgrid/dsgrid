import fileinput
import os
import re
from pathlib import Path


def replace_association_table_uuids_from_registry(registry_dir, filenames):
    uuids = read_association_table_uuid_mapping(registry_dir)
    for filename in filenames:
        replace_association_table_uuids(filename, uuids)


def read_association_table_uuid_mapping(registry_dir):
    dir_name = Path(registry_dir)
    mappings = {}
    regex = re.compile(r"(?P<from_dimension>\w+)__(?P<to_dimension>\w+)__(?P<uuid>[-0-9a-f]+)$")
    path = dir_name / "association_tables"
    for item in os.listdir(path):
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


def replace_association_table_uuids(filename, uuids):
    regex = re.compile(
        r"association_table_id = \"(?P<from_dimension>\w+)__(?P<to_dimension>\w+)__(?P<uuid>[-0-9a-f]+)\""
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
                print(f'association_table_id = "{from_dimension}__{to_dimension}__{new_uuid}"')


def replace_dimension_uuids_from_registry(registry_dir, filenames):
    uuids = read_dimension_uuid_mapping(registry_dir)
    for filename in filenames:
        replace_dimension_uuids(filename, uuids)


def read_dimension_uuid_mapping(registry_dir):
    dir_name = Path(registry_dir)
    mappings = {}
    regex = re.compile(r"(?P<dimension_type>\w+)__(?P<uuid>[-0-9a-f]+)$")
    for dim_type in os.listdir(dir_name / "dimensions"):
        dim_path = dir_name / "dimensions" / dim_type
        if not os.path.isdir(dim_path):
            continue
        for dim in os.listdir(dim_path):
            assert os.path.isdir(dim_path / dim), str(dim_path / dim)
            match = regex.search(dim)
            assert match, dim
            data = match.groupdict()
            dim_type = data["dimension_type"]
            dim_uuid = data["uuid"]
            assert dim_type not in mappings, dim_type
            mappings[dim_type] = dim_uuid

    return mappings


def replace_dimension_uuids(filename, uuids):
    regex = re.compile(r"dimension_id = \"(?P<dimension_type>\w+)__(?P<uuid>[-0-9a-f]+)\"")
    with fileinput.input(files=[filename], inplace=True) as f:
        for line in f:
            match = regex.search(line)
            if match is None:
                print(line, end="")
            else:
                dimension_type = match.groupdict()["dimension_type"]
                new_uuid = uuids[dimension_type]
                print(f'dimension_id = "{dimension_type}__{new_uuid}"')
