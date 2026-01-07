"""Generate enum documentation.

This script generates markdown documentation for all dsgrid enumerations.
"""

import sys
from pathlib import Path

# Import the generation function
sys.path.insert(0, str(Path(__file__).parent))
from generate_model_tables import generate_enum_documentation

# Enums to document: (module_path.EnumName)
ENUMS = [
    # From dsgrid.dimension.base_models
    "dsgrid.dimension.base_models.DimensionType",
    # From dsgrid.dimension.time
    "dsgrid.dimension.time.TimeDimensionType",
    "dsgrid.dimension.time.RepresentativePeriodFormat",
    "dsgrid.dimension.time.LeapDayAdjustmentType",
    "dsgrid.dimension.time.TimeIntervalType",
    "dsgrid.dimension.time.MeasurementType",
    # From dsgrid.registry.common
    "dsgrid.registry.common.DatasetRegistryStatus",
    "dsgrid.registry.common.ProjectRegistryStatus",
    # From dsgrid.config.dataset_config
    "dsgrid.config.dataset_config.InputDatasetType",
    "dsgrid.config.dataset_config.DataClassificationType",
    "dsgrid.config.dataset_config.DatasetQualifierType",
    "dsgrid.config.dataset_config.GrowthRateType",
    # From dsgrid.config.dimension_mapping_base
    "dsgrid.config.dimension_mapping_base.DimensionMappingType",
    "dsgrid.config.dimension_mapping_base.DimensionMappingArchetype",
]


def import_enum(enum_path: str):
    """Import an enum from a module path."""
    import importlib

    module_path, enum_name = enum_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, enum_name)


def main():
    """Generate enum documentation."""
    docs_dir = Path(__file__).parent
    output_path = docs_dir / "source/software_reference/data_models/enums.md"

    lines = [
        "# Enums",
        "",
        "Enumeration types used in dsgrid configuration models.",
        "",
    ]

    for enum_path in ENUMS:
        try:
            enum_class = import_enum(enum_path)
            enum_doc = generate_enum_documentation(enum_class)
            lines.append(enum_doc)
        except Exception as e:
            print(f"  Error documenting {enum_path}: {e}")
            import traceback

            traceback.print_exc()
            return 1

    # Write to file
    output_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"  Written to {output_path.relative_to(docs_dir)}")
    print(f"\nGenerated documentation for {len(ENUMS)} enums")
    return 0


if __name__ == "__main__":
    exit(main())
