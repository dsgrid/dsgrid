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
import pydantic


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
    default="source/_build/config_rsts",
    show_default=True,
    help="output directory",
    callback=_get_output,
)
def make_tables(output):
    os.makedirs(output, exist_ok=True)
    for cls in (
        DatasetConfigModel,
        ProjectConfigModel,
        AssociationTableModel,
        DimensionMappingBaseModel,
        DimensionMappingReferenceListModel,
        DimensionMappingReferenceModel,
        DimensionMappingsModel,
        DimensionModel,
        DimensionsModel,
        InputDatasetModel,
        InputDatasetsModel,
        InputSectorDataset,
        DimensionModel,
    ):
        schema = cls.schema(ref_template="{model}")
        field_name = cls.__name__ + ": " + schema["description"]
        output_file = output / (cls.__name__ + ".csv")
        with open(output_file, "w") as f_out:
            header = ("Key", "Type", "Description", "Notes", "Required")
            f_out.write("\t".join(header) + "\n")
            for prop, vals in schema["properties"].items():
                if "dsg_internal" not in vals:
                    if vals["title"] == prop:
                        field_name = "``" + prop + "``"
                    else:
                        field_name = vals["title"] + " " + prop
                    if isinstance(vals.get("type"), pydantic.main.ModelMetaclass):
                        vals["type"] = vals.get("type").__name__
                    if not "required" in vals:
                        required = "True"
                    else:
                        required = str(vals["required"])
                    if not "notes" in vals:
                        notes = ""
                    else:
                        notes = vals["notes"]
                    if "items" in vals:
                        class_name = vals["items"]["$ref"]
                        mod = get_class_path(class_name)
                        dtype = f":meth:`~{mod}.{class_name}`".replace("[", "").replace("]", "")
                    elif "allOf" in vals:
                        dtype = []
                        for i in vals["allOf"]:
                            class_name = i["$ref"]
                            mod = get_class_path(class_name)
                            dtype.append(f":meth:`~{mod}.{class_name}`")
                        dtype = str(dtype).replace("'", "").replace("[", "").replace("]", "")
                    else:
                        dtype = vals.get("type", "Any")
                    if vals["description"].lower().startswith("list of"):
                        dtype = f"[{dtype}]"
                    row = (
                        field_name,
                        dtype,
                        vals["description"],
                        notes,
                        required,
                    )
                    f_out.write("\t".join(row))
                    f_out.write("\n")

    print(f"Generated config tables in {output}")


if __name__ == "__main__":
    make_tables()
