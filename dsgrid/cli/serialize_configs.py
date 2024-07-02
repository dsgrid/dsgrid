"""Serialize a registry config and all dependencies to JSON files."""

import itertools
import json
import logging
from pathlib import Path
from typing import Any, Iterable

import click

from dsgrid.loggers import setup_logging
from dsgrid.config.mapping_tables import MappingTableByNameModel
from dsgrid.config.dimension_config import DimensionConfig
from dsgrid.config.supplemental_dimension import SupplementalDimensionModel
from dsgrid.registry.registry_manager import RegistryManager


logger = logging.getLogger("dsgrid")


@click.group("serialize")
@click.option(
    "--verbose", is_flag=True, default=False, show_default=True, help="Enable verbose log output."
)
@click.pass_obj
def serialize_configs(verbose: bool):
    """Serialize a registry config and all dependencies to a JSON file."""
    level = logging.DEBUG if verbose else logging.INFO
    setup_logging("dsgrid", "dsgrid.log", console_level=level, file_level=level, mode="a")


@click.command()
@click.argument("project_id")
@click.option(
    "-o", "--output", default=".", help="Output directory", callback=lambda *x: Path(x[2])
)
@click.pass_obj
def projects(mgr: RegistryManager, project_id: str, output: Path):
    """Serialize a project and all its dimensions and mappings to a JSON file."""
    project_manager = mgr.project_manager
    dimension_manager = mgr.dimension_manager
    dimension_mapping_manager = mgr.dimension_mapping_manager
    project_config = project_manager.get_by_id(project_id)
    pmodel = project_config.model
    for ref in pmodel.dimensions.base_dimension_references:
        dim = dimension_manager.get_by_id(ref.dimension_id)
        pmodel.dimensions.base_dimensions.append(dim.model)
    pmodel.dimensions.base_dimension_references.clear()
    mappings_by_query_name = {}
    for ref in pmodel.dimension_mappings.base_to_supplemental_references:
        mapping = dimension_mapping_manager.get_by_id(ref.mapping_id)
        to_dim = dimension_manager.get_by_id(mapping.model.to_dimension.dimension_id)
        mappings_by_query_name[to_dim.model.dimension_query_name] = mapping
    for ref in pmodel.dimensions.supplemental_dimension_references:
        dim = dimension_manager.get_by_id(ref.dimension_id)
        if isinstance(dim, DimensionConfig) and _is_all_in_one_dimension(dim):
            # These get auto-generated at registration time.
            continue
        mapping = mappings_by_query_name[dim.model.dimension_query_name]
        supp_dim = _create_supplemental_dimension(dim, mapping)
        pmodel.dimensions.supplemental_dimensions.append(supp_dim)
    for subset in pmodel.dimensions.subset_dimensions:
        subset.create_supplemental_dimension = False  # These were added above.
        for ref in subset.selector_references:
            dim = dimension_manager.get_by_id(ref.dimension_id)
            subset.dimensions.append(dim.model)
        subset.selector_references.clear()

    pmodel.dimensions.supplemental_dimension_references.clear()
    for dset in pmodel.datasets:
        dset.mapping_references.clear()

    data = pmodel.model_dump(mode="json")
    _prune_project_fields(data)
    for dset in data["datasets"]:
        dset.pop("status")
    _prune_dimension_fields(
        itertools.chain(
            data["dimensions"]["base_dimensions"], data["dimensions"]["supplemental_dimensions"]
        )
    )
    _prune_subset_group_fields(data["dimensions"]["subset_dimensions"])
    data["dimensions"].pop("base_dimension_references")
    data["dimensions"].pop("supplemental_dimension_references")
    project_file = output / f"{project_id}.json5"
    with open(project_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    logger.info("Serialized project to %s", project_file)


@click.command()
@click.argument("dataset_id")
@click.option(
    "-o", "--output", default=".", help="Output directory", callback=lambda *x: Path(x[2])
)
@click.pass_obj
def datasets(mgr: RegistryManager, dataset_id: str, output: Path):
    """Serialize a dataset and all its dimensions to a JSON file."""
    dataset_manager = mgr.dataset_manager
    dimension_manager = mgr.dimension_manager
    config = dataset_manager.get_by_id(dataset_id)
    model = config.model
    for ref in model.dimension_references:
        dim = dimension_manager.get_by_id(ref.dimension_id)
        model.dimensions.append(dim.model)
    model.dimension_references.clear()
    data = model.model_dump(mode="json")
    _prune_common_fields(data)
    _prune_dimension_fields(data["dimensions"])
    dataset_file = output / f"{dataset_id}.json5"
    with open(dataset_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    with open(dataset_file, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2)
    logger.info("Serialized dataset to %s", dataset_file)


def _create_supplemental_dimension(dim, mapping) -> SupplementalDimensionModel:
    return SupplementalDimensionModel(
        name=dim.model.name,
        display_name=dim.model.display_name,
        dimension_query_name=dim.model.dimension_query_name,
        dimension_type=dim.model.dimension_type,
        dimension_id=dim.model.dimension_id,
        description=dim.model.description,
        class_name=dim.model.class_name,
        module=dim.model.module,
        version=dim.model.version,
        records=dim.model.records,
        mapping=MappingTableByNameModel(
            mapping_type=mapping.model.mapping_type,
            archetype=mapping.model.archetype,
            description=mapping.model.description,
            from_fraction_tolerance=mapping.model.from_fraction_tolerance,
            to_fraction_tolerance=mapping.model.to_fraction_tolerance,
            records=mapping.model.records,
        ),
    )


def _is_all_in_one_dimension(dimension: DimensionConfig) -> bool:
    return (
        dimension.model.name.startswith("all_")
        and dimension.model.display_name.startswith("All ")
        and len(dimension.model.records) == 1
    )


_COMMON_FIELDS_TO_PRUNE = (
    "_id",
    "_key",
    "_rev",
    "version",
)
_DIMENSION_FIELDS_TO_PRUNE = (
    "dimension_query_name",
    "dimension_class",
    "dimension_id",
    "file",
    "file_hash",
)
_PROJECT_FIELDS_TO_PRUNE = (
    "dimension_mappings",
    "status",
)
_SUBSET_GROUP_FIELDS_TO_PRUNE = (
    "file",
    "record_ids",
    "selectors",
    "selector_references",
)


def _prune_common_fields(data: dict[str, Any]) -> None:
    for field in _COMMON_FIELDS_TO_PRUNE:
        data.pop(field, None)


def _prune_dimension_fields(dimensions: Iterable[dict[str, Any]]) -> None:
    for dimension in dimensions:
        _prune_common_fields(dimension)
        for field in _DIMENSION_FIELDS_TO_PRUNE:
            dimension.pop(field, None)


def _prune_project_fields(data: dict[str, Any]) -> None:
    _prune_common_fields(data)
    for field in _PROJECT_FIELDS_TO_PRUNE:
        data.pop(field, None)


def _prune_subset_group_fields(subsets: Iterable[dict[str, Any]]) -> None:
    for subset in subsets:
        subset.pop("record_ids")
        for field in _SUBSET_GROUP_FIELDS_TO_PRUNE:
            subset.pop(field)
        _prune_dimension_fields(subset["dimensions"])


serialize_configs.add_command(projects)
serialize_configs.add_command(datasets)


if __name__ == "__main__":
    serialize_configs()
