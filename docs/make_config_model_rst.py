"""Generates rst of the root configuration data models."""

import os
from pathlib import Path

import click

from dsgrid.config.dataset_config import DatasetConfigModel
from dsgrid.config.dimension_mapping_base import (
    DimensionMappingBaseModel,
    DimensionMappingReferenceListModel,
)
from dsgrid.config.dimensions import DimensionModel, DateTimeDimensionModel
from dsgrid.config.project_config import (
    DimensionsModel,
    ProjectConfigModel,
)
import pydantic
import pkgutil
import importlib
import dsgrid


dsgrid_modules = []
for importer, modname, ispkg in pkgutil.iter_modules(dsgrid.__path__):
    if not modname.startswith("_"):
        if ispkg:
            for _importer, _modname, _ispkg in pkgutil.iter_modules(
                importlib.import_module(f"dsgrid.{modname}").__path__
            ):
                if not _modname.startswith("_") and modname != _modname:
                    dsgrid_modules.append(f"dsgrid.{modname}.{_modname}")
        else:
            dsgrid_modules.append(f"dsgrid.{modname}")


def get_class_path(cls_name):
    for module in dsgrid_modules:
        try:
            mod = importlib.import_module(module)
            if hasattr(mod, cls_name):
                dsgrid_path = Path(__file__).resolve().parents[1]
                module_file = dsgrid_path / (module.replace(".", "/") + ".py")
                with open(module_file, "r") as f:
                    if f"class {cls_name}" in f.read():
                        return mod.__name__
        except Exception:
            pass


def _get_output(_, __, value):
    return Path(value)


def get_class(cls):
    if cls == "None.None":
        return cls
    elif isinstance(cls, str):
        if "." in cls:
            module = importlib.import_module(".".join(cls.split(".")[:-1]))
            cls = getattr(module, cls.split(".")[-1])
            return cls
    else:
        return cls


def get_field_details(cls):
    cls = get_class(cls)
    schema = cls.schema(ref_template="{model}")
    field_name = cls.__name__ + ": " + schema["description"]
    field_items_list = []
    class_name_list = []
    for prop, vals in schema["properties"].items():
        if "dsg_internal" not in vals:
            if vals["title"] == prop:
                field_name = "``" + prop + "``"
            else:
                field_name = "``" + vals["title"] + "``"

            if isinstance(vals.get("type"), pydantic.main.ModelMetaclass):
                vals["type"] = vals.get("type").__name__

            if cls.__name__ == "DimensionsModel":
                dtype = (
                    ":class:`~dsgrid.config.dimensions.DimensionModel`, "
                    ":class:`~dsgrid.config.dimensions.DateTimeDimensionModel`".replace(
                        "[", ""
                    ).replace("]", "")
                )
                class_name = "DimensionsModel"
                mod = "dsgrid.config.project_config"
                subfields = True
            elif "items" in vals and "$ref" in vals["items"]:
                class_name = vals["items"]["$ref"]
                mod = get_class_path(class_name)
                dtype = f":class:`~{mod}.{class_name}`".replace("[", "").replace("]", "")
                if class_name.endswith("Model"):
                    subfields = True
                else:
                    subfields = False
            elif "allOf" in vals:
                dtype = []
                for i in vals["allOf"]:
                    class_name = i["$ref"]
                    mod = get_class_path(class_name)
                    dtype.append(f":class:`~{mod}.{class_name}`")
                    if class_name.endswith("Model"):
                        subfields = True
                    else:
                        subfields = False
                dtype = str(dtype).replace("'", "").replace("[", "").replace("]", "")
                # TODO: are there any cases when this is a list? Need to confirm.
            # TODO: provide support for "anyOf"? e.g., Union[Dimension, TimeDimension]
            else:
                dtype = vals.get("type", "Any")
                class_name = None
                mod = None
                subfields = None

            if vals.get("description", "").lower().startswith("list of"):
                dtype = f"[{dtype}]"

            field_items = {
                "Field": field_name,
                "Description": vals.get("description", ""),
                "Type": dtype,
                "Value Options": vals.get("options", None),
                "Default": vals.get("default", "None"),
                "Optional": str(vals.get("optional", False)),
                "Requirements": vals.get("requirements", None),
                "Notes": vals.get("notes", None),
                "SubFields": subfields,
            }

            field_items_list.append(field_items)
            class_name_list.append(f"{mod}.{class_name}")

    return field_items_list, class_name_list


def get_subfield_rows(field_items_list, class_name_list, fields, indent_level):
    indent_level = indent_level + 1
    rows = []
    for x, field_items in enumerate(field_items_list):
        _continue = False
        cls = class_name_list[x]
        cls = get_class(cls)
        if x == 0:
            _continue = True
        if x > 0:
            if field_items["Field"] != field_items_list[x - 1]["Field"]:
                _continue = True
            else:
                _continue = False
        if _continue:
            for i, field in enumerate(fields):
                rows.extend(get_row(i, field, fields, field_items, cls, indent_level))
    return rows


def get_row(i, field, fields, field_items, cls, indent_level=1):
    rows = []
    t = " " * 4
    t1 = t
    n = "\n"
    if indent_level > 1:
        t1 = "".join([t1] * indent_level)
    t2 = "".join([t] * (indent_level + 1))

    # treat dimensions.json5 config differently
    if field == "SubFields" and cls == "dsgrid.config.project_config.DimensionsModel":
        rows.append(
            f"{t1}{t}:{field}: The config sub-fields depends on the "
            f":class:`dsgrid.dimensions.base_model.DimensionType`{n}"
            f"{n}{t1}{t}{t}* if ``dimension.type`` != Time "
            "(:class:`~dsgrid.config.dimensions.DateTimeDimensionModel`), "
            f"the config field requirements include:{n}"
            # TODO: find alternative way to not hardcode this path
            f"{n}{t1}{t}{t}{t}.. include:: ../../../../_build/config_rsts/DimensionModel.rst{n}"
            f"{n}{t1}{t}{t}* if ``dimension.type`` == Time (:class:`~dsgrid.config.dimensions."
            f"DateTimeDimensionModel`), the config field requirements include:{n}"
            f"{n}{t1}{t}{t}{t}.. include:: ../../../../_build/config_rsts/DateTimeDimensionModel.rst{n}"
        )

    elif field_items[field]:
        if field == "Field":
            if field_items["Optional"] == "True":
                rows.append(
                    f"{t1}* {field_items[field]}  *(Optional)* :{n}"
                )  # TODO: ask team preference on "optional" placement in docs
            else:
                rows.append(f"{t1}* {field_items[field]}:{n}")
        elif field == "Value Options":
            if isinstance(field_items[field], dict):
                options = []
                for v in field_items[field].items():
                    if v[1] == "None":
                        options.append(f"{n}{t1}{t2}* {v[0]}")
                    else:
                        options.append(f"{n}{t1}{t2}* {v[0]}: {v[1]}")
                options = "".join(options)
                rows.append(f"{t1}{t}:{field}:{options}{n}")
            else:
                rows.append(f"{t1}{t}:{field}: {field_items[field]}{n}")
        elif field == "Requirements" or field == "Notes":
            notes = "".join([f"{n}{t1}{t2}* {v}" for v in field_items[field]])
            rows.append(f"{t1}{t}:{field}:{notes}{n}")
        elif field == "SubFields":
            rows.append(f"{t1}{t}:{field}:{n}")
            field_items_list, class_name_list = get_field_details(cls)
            for row in get_subfield_rows(field_items_list, class_name_list, fields, indent_level):
                rows.append(f"{t}{row}")
        else:
            rows.append(f"{t1}{t}:{field}: {str(field_items[field]).strip()}{n}")
    return rows


@click.command()
@click.option(
    "-o",
    "--output",
    default="_build/config_rsts",
    show_default=True,
    help="output directory",
    callback=_get_output,
)
def make_config_rst(output):
    os.makedirs(output, exist_ok=True)
    for cls in (
        # keep only the config models
        ProjectConfigModel,  # project.json5
        DatasetConfigModel,  # dataset.json5
        DimensionsModel,  # dimensions.json5
        DimensionModel,
        DateTimeDimensionModel,
        DimensionMappingBaseModel,  # dimension mapping json5
        DimensionMappingReferenceListModel,  # dimension mapping references json5
        # TODO: @DT please confirm whether these models below are being used directly by the user or not. If not, I do not think we need to create .rst docs explaining the fields. Do you agree?
        # MappingTableModel,
        # DimensionMappingBaseModel,
        # DimensionMappingReferenceModel,
    ):
        output_file = output / (cls.__name__ + ".rst")
        with open(output_file, "w") as f_out:
            field_items_list, class_name_list = get_field_details(cls)

            for x, field_items in enumerate(field_items_list):
                class_name = class_name_list[x]
                fields = list(field_items.keys())
                for i, field in enumerate(fields):
                    rows = get_row(i, field, fields, field_items, class_name, indent_level=1)
                    for row in rows:
                        f_out.write(row)


if __name__ == "__main__":
    make_config_rst()
