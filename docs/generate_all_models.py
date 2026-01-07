"""Generate all data model documentation.

This script generates markdown documentation for all dsgrid configuration models.
"""

import sys
from pathlib import Path

# Import the generation function directly instead of using subprocess
sys.path.insert(0, str(Path(__file__).parent))
from generate_model_tables import import_model, generate_model_documentation

# Models to document with their output paths
MODELS = [
    (
        "dsgrid.config.dimensions.DimensionModel",
        "source/software_reference/data_models/dimension_model.md",
    ),
    (
        "dsgrid.config.dataset_config.DatasetConfigModel",
        "source/software_reference/data_models/dataset_model.md",
    ),
    (
        "dsgrid.config.dimension_mapping_base.DimensionMappingBaseModel",
        "source/software_reference/data_models/dimension_mapping_model.md",
    ),
    (
        "dsgrid.config.project_config.ProjectConfigModel",
        "source/software_reference/data_models/project_model.md",
    ),
]


def main():
    """Generate all model documentation."""
    docs_dir = Path(__file__).parent

    # Build a mapping of models to their documentation paths
    # This allows us to link to already-documented models instead of duplicating
    documented_models = {}
    for model_path, output_file in MODELS:
        model = import_model(model_path)
        # Convert to relative path from the data_models directory
        # e.g., "dimension_model.md" instead of full path
        doc_filename = Path(output_file).name
        documented_models[model] = doc_filename

    # Now generate documentation for each model
    for model_path, output_file in MODELS:
        output_path = docs_dir / output_file
        print(f"Generating documentation for {model_path}...")

        try:
            model = import_model(model_path)
            documentation = generate_model_documentation(
                model, include_nested=True, documented_models=documented_models
            )

            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(documentation, encoding="utf-8")
            print(f"  Written to {output_file}")
        except Exception as e:
            print(f"  Error: {e}")
            import traceback

            traceback.print_exc()
            return 1

    print(f"\nGenerated documentation for {len(MODELS)} models")
    return 0


if __name__ == "__main__":
    exit(main())
