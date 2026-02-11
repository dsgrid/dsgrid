"""Generate dimension record class documentation.

This script generates markdown documentation for all dimension record classes
defined in dsgrid.dimension.standard, organized by dimension type.
"""

import inspect
from enum import Enum
from pathlib import Path

from pydantic import BaseModel
from pydantic.fields import FieldInfo

from dsgrid.dimension import standard as std_module
from dsgrid.dimension.base_models import (
    DimensionRecordBaseModel,
    GeographyDimensionBaseModel,
    MetricDimensionBaseModel,
    ModelYearDimensionBaseModel,
    ScenarioDimensionBaseModel,
    SectorDimensionBaseModel,
    SubsectorDimensionBaseModel,
    WeatherYearDimensionBaseModel,
)

# Map base classes to dimension type labels and display order
_BASE_CLASS_ORDER = [
    (GeographyDimensionBaseModel, "Geography"),
    (SectorDimensionBaseModel, "Sector"),
    (SubsectorDimensionBaseModel, "Subsector"),
    (MetricDimensionBaseModel, "Metric"),
    (ModelYearDimensionBaseModel, "Model Year"),
    (WeatherYearDimensionBaseModel, "Weather Year"),
    (ScenarioDimensionBaseModel, "Scenario"),
]


def _get_own_fields(cls: type[BaseModel]) -> dict[str, FieldInfo]:
    """Return only fields defined directly on cls, not inherited ones."""
    parent_fields = set()
    for base in cls.__mro__[1:]:
        if hasattr(base, "model_fields"):
            parent_fields.update(base.model_fields.keys())
    return {k: v for k, v in cls.model_fields.items() if k not in parent_fields}


def _format_type(annotation) -> str:
    """Format a type annotation for display, linking enums to the enums page."""
    if annotation is type(None):
        return "None"
    if isinstance(annotation, type) and issubclass(annotation, Enum):
        name = annotation.__name__
        return f"[`{name}`](enums.md#{name.lower()})"
    if hasattr(annotation, "__name__"):
        return f"`{annotation.__name__}`"
    return f"`{annotation}`"


def _format_default(field_info: FieldInfo) -> str:
    """Format a field default for display."""
    if field_info.is_required():
        return "*(required)*"
    default = field_info.default
    if default is None:
        return "`None`"
    if isinstance(default, str):
        if len(default) > 50:
            return f'`"{default[:47]}..."`'
        return f'`"{default}"`'
    return f"`{default}`"


def _generate_class_doc(cls: type[BaseModel]) -> str:
    """Generate markdown documentation for a single dimension record class."""
    lines = []
    class_name = cls.__name__
    full_path = f"{cls.__module__}.{class_name}"
    doc = inspect.getdoc(cls) or ""

    lines.append(f"### {class_name}")
    lines.append("")
    lines.append(f"*{full_path}*")
    lines.append("")
    if doc:
        lines.append(doc)
        lines.append("")

    lines.append('<div class="model-fields-table">')
    lines.append("")
    lines.append("| Field | Type | Default | Description |")
    lines.append("|-------|------|---------|-------------|")
    lines.append("| `id` | `str` | *(required)* | Unique identifier within a dimension |")
    lines.append("| `name` | `str` | *(required)* | User-defined name |")
    for field_name, field_info in _get_own_fields(cls).items():
        type_str = _format_type(field_info.annotation)
        default_str = _format_default(field_info)
        desc = (field_info.description or "").replace("\n", " ").strip()
        desc = " ".join(desc.split()).replace("|", "\\|")
        lines.append(f"| `{field_name}` | {type_str} | {default_str} | {desc} |")
    lines.append("")
    lines.append("</div>")

    lines.append("")
    return "\n".join(lines)


def _collect_classes() -> dict[str, list[type[BaseModel]]]:
    """Collect all dimension record classes from standard module, grouped by type."""
    groups: dict[str, list[type[BaseModel]]] = {}

    # Gather all classes from the standard module that are DimensionRecordBaseModel
    # or TimeDimensionBaseModel subclasses
    all_classes = []
    for name, obj in inspect.getmembers(std_module, inspect.isclass):
        if obj.__module__ != std_module.__name__:
            continue
        if issubclass(obj, DimensionRecordBaseModel):
            all_classes.append(obj)

    # Sort into groups by base class
    for base_cls, label in _BASE_CLASS_ORDER:
        matching = [
            cls for cls in all_classes if issubclass(cls, base_cls) and cls is not base_cls
        ]
        if matching:
            # Sort alphabetically within group
            groups[label] = sorted(matching, key=lambda c: c.__name__)

    return groups


def main():
    """Generate dimension record class documentation."""
    docs_dir = Path(__file__).parent.parent
    output_path = docs_dir / "source/software_reference/data_models/dimension_classes.md"

    lines = [
        "# Dimension Record Classes",
        "",
        "These classes define the schema for dimension record files (typically CSVs).",
        "Every dimension config has a `class` field that selects one of these classes.",
        "The class determines what columns are required or optional in the records file.",
        "",
        ":::{note}",
        "Time dimensions do not use record classes. They are configured entirely by",
        "parameters in the config file. See the [Dimensions](dimension_model.md) reference",
        "for time dimension config models (DateTimeDimensionModel, AnnualTimeDimensionModel, etc.).",
        ":::",
        "",
    ]

    groups = _collect_classes()

    section_docs = []
    for label, classes in groups.items():
        section_lines = []
        section_lines.append(f"## {label}")
        section_lines.append("")

        for cls in classes:
            section_lines.append(_generate_class_doc(cls))

        section_docs.append("\n".join(section_lines))

    lines.append("\n---\n\n".join(section_docs))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"  Written to {output_path.relative_to(docs_dir)}")
    print(
        f"\nGenerated documentation for {sum(len(v) for v in groups.values())} dimension classes"
    )
    return 0


if __name__ == "__main__":
    exit(main())
