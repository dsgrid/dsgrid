"""Generates tables of the configuration data models."""

import os
from pathlib import Path

import click
from semver import VersionInfo

from dsgrid.config.association_tables import AssociationTableModel
from dsgrid.config.dataset_config import InputSectorDataset, DatasetConfigModel
from dsgrid.config.dimension_mapping_base import (
    DimensionMappingBaseModel,
    DimensionMappingReferenceModel,
    DimensionMappingReferenceListModel,
)
from dsgrid.config.dimensions import DimensionModel, TimeDimensionModel
from dsgrid.config.project_config import (
    DimensionsModel,
    InputDatasetModel,
    InputDatasetsModel,
    DimensionMappingsModel,
    ProjectConfigModel,
)


@classmethod
def modify_schema(cls, field_schema):
    field_schema.update(
        # This is a simplified regex with no prerelease or build.
        # Refer to semver.VersionInfo if a full one is needed.
        pattern=r"^\d+\.\d+\.\d+$",
        examples=["1.0.0"],
    )


# Hack
# This is required to allow Pydantic.BaseModel.schema() to work with models
# that contain VersionInfo.  Choosing to do it here for documentation rather
# than in the main code which could affect normal operation.
VersionInfo.__modify_schema__ = modify_schema


def _get_output(_, __, value):
    return Path(value)


@click.command()
@click.option(
    "-o",
    "--output",
    default="_build/config_tables",
    show_default=True,
    help="output directory",
    callback=_get_output,
)
def make_tables(output):
    os.makedirs(output, exist_ok=True)
    for cls in (
        AssociationTableModel,
        DatasetConfigModel,
        # DimensionMappingBaseModel,
        DimensionMappingReferenceListModel,
        DimensionMappingReferenceModel,
        # DimensionMappingsModel,
        DimensionModel,
        DimensionsModel,
        InputDatasetModel,
        InputDatasetsModel,
        InputSectorDataset,
        ProjectConfigModel,
        TimeDimensionModel,
    ):
        schema = cls.schema()
        title = cls.__name__ + ": " + schema["description"]
        output_file = output / (cls.__name__ + ".csv")
        with open(output_file, "w") as f_out:
            header = ("Property", "Type", "Description", "Required")
            f_out.write("\t".join(header) + "\n")
            required_props = set(schema["required"])
            for prop, vals in schema["properties"].items():
                if vals.get("title") in (prop, None):
                    title = prop
                else:
                    title = vals["title"] + " " + prop
                row = (
                    title,
                    vals.get("type", "Any"),
                    vals["description"],
                    str(prop in required_props),
                )
                f_out.write("\t".join(row))
                f_out.write("\n")

    print(f"Generated config tables in {output}")


if __name__ == "__main__":
    make_tables()
