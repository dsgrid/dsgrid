"""Contains utility functions to map to/from dimension/mapping names and IDs."""

from dsgrid.registry.dimension_registry_manager import DimensionRegistryManager
from dsgrid.utils.files import dump_data, load_data


def map_dimension_names_to_ids(dimension_mgr: DimensionRegistryManager):
    mapping = {}
    for dim in dimension_mgr.db.dimensions:
        if dim["name"] in mapping:
            assert mapping[dim["name"]] == dim["dimension_id"], dim
        mapping[dim["name"]] = dim["dimension_id"]
    return mapping


def map_dimension_ids_to_names(dimension_mgr):
    mapping = {}
    for dim in dimension_mgr.db.dimensions:
        assert dim["dimension_id"] not in mapping, dim
        mapping[dim["dimension_id"]] = dim["name"]
    return mapping


def map_dimension_mapping_names_to_ids(dimension_mapping_mgr, dim_id_to_name):
    mapping = {}
    for dmap in dimension_mapping_mgr.db.dimension_mappings:
        key = (
            dim_id_to_name[dmap["from_dimension"]["dimension_id"]],
            dim_id_to_name[dmap["to_dimension"]["dimension_id"]],
        )
        if key in mapping:
            assert mapping[key] == dmap["mapping_id"], dmap
        mapping[key] = dmap["mapping_id"]
    return mapping


def replace_dimension_names_with_current_ids(filename, mappings):
    data = load_data(filename)
    assert isinstance(data, dict)

    def perform_replacements(mappings, dimensions):
        changed = False
        for ref in dimensions:
            if "name" in ref:
                ref["dimension_id"] = mappings[ref.pop("name")]
                changed = True
        return changed

    changed = False
    if "dimension_references" in data:
        # This is True for a dataset config file.
        if perform_replacements(mappings, data["dimension_references"]):
            changed = True

    if "dimensions" in data and "base_dimension_references" in data["dimensions"]:
        # This is True for a project config file.
        if perform_replacements(mappings, data["dimensions"]["base_dimension_references"]):
            changed = True
        if perform_replacements(mappings, data["dimensions"]["supplemental_dimension_references"]):
            changed = True

    if "mappings" in data:
        # This is True for a dimension mappings file.
        for mapping in data["mappings"]:
            if perform_replacements(
                mappings, [mapping["from_dimension"], mapping["to_dimension"]]
            ):
                changed = True

    if changed:
        dump_data(data, filename, indent=2)


def replace_dimension_mapping_names_with_current_ids(filename, mappings):
    data = load_data(filename)
    assert isinstance(data, dict)

    def perform_replacements(mappings, references):
        changed = False
        for ref in references:
            if "mapping_names" in ref:
                item = ref.pop("mapping_names")
                ref["mapping_id"] = mappings[(item["from"], item["to"])]
                changed = True
        return changed

    changed = False
    if "dimension_mappings" in data:
        # This is True for a project config file.
        refs = data["dimension_mappings"]["base_to_supplemental_references"]
        if perform_replacements(mappings, refs):
            changed = True

    if "references" in data:
        # This is True for a dataset-to-project dimension mapping reference file.
        if perform_replacements(mappings, data["references"]):
            changed = True

    if changed:
        dump_data(data, filename, indent=2)
