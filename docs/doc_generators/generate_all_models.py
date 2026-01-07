"""Generate all data model documentation.

This script generates markdown documentation for all dsgrid configuration models.
"""

from pathlib import Path

from .core import generate_model_documentation, import_model

# Models to document with their output paths
# Format: (page_title, model_path, output_file, additional_models_to_include)
# additional_models_to_include is a list of model paths to document on the same page
MODELS = [
    (
        "Dimensions",
        "dsgrid.config.dimensions.DimensionModel",
        "source/software_reference/data_models/dimension_model.md",
        [
            # All time dimension types
            "dsgrid.config.dimensions.DateTimeDimensionModel",
            "dsgrid.config.dimensions.AnnualTimeDimensionModel",
            "dsgrid.config.dimensions.RepresentativePeriodTimeDimensionModel",
            "dsgrid.config.dimensions.DatetimeExternalTimeZoneDimensionModel",
            "dsgrid.config.dimensions.IndexTimeDimensionModel",
            "dsgrid.config.dimensions.NoOpTimeDimensionModel",
            # Dimension reference
            "dsgrid.config.dimensions.DimensionReferenceModel",
        ],
    ),
    (
        "Dataset Config",
        "dsgrid.config.dataset_config.DatasetConfigModel",
        "source/software_reference/data_models/dataset_model.md",
        [],
    ),
    (
        "Dimension Mappings",
        "dsgrid.config.dimension_mapping_base.DimensionMappingBaseModel",
        "source/software_reference/data_models/dimension_mapping_model.md",
        [
            "dsgrid.config.dimension_mapping_base.DimensionMappingReferenceModel",
        ],
    ),
    (
        "Project Config",
        "dsgrid.config.project_config.ProjectConfigModel",
        "source/software_reference/data_models/project_model.md",
        [],
    ),
]


def main():
    """Generate all model documentation."""
    docs_dir = Path(__file__).parent.parent  # Go up to docs/ directory

    # Build a mapping of models to their documentation paths
    # This allows us to link to already-documented models instead of duplicating
    documented_models = {}

    # First pass: collect all models that will be documented
    for page_title, model_path, output_file, additional_models in MODELS:
        doc_filename = Path(output_file).name

        # Add main model
        model = import_model(model_path)
        documented_models[model] = doc_filename

        # Add additional models that will be on the same page
        for additional_model_path in additional_models:
            additional_model = import_model(additional_model_path)
            documented_models[additional_model] = doc_filename

    # Second pass: generate documentation for each page
    for page_title, model_path, output_file, additional_models in MODELS:
        output_path = docs_dir / output_file
        print(f"Generating documentation for {model_path}...")

        try:
            # Start with page title
            documentation = f"# {page_title}\n\n"

            # Generate main model documentation at H2 level
            model = import_model(model_path)
            model_doc = generate_model_documentation(
                model, include_nested=True, documented_models=documented_models, heading_level=2
            )
            documentation += model_doc

            # Add additional models to the same page (also at H2 level)
            for additional_model_path in additional_models:
                print(f"  + Including {additional_model_path}")
                additional_model = import_model(additional_model_path)
                additional_doc = generate_model_documentation(
                    additional_model,
                    include_nested=True,
                    documented_models=documented_models,
                    heading_level=2,
                )
                documentation += "\n\n" + additional_doc

            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(documentation, encoding="utf-8")
            print(f"  Written to {output_file}")
        except Exception as e:
            print(f"  Error: {e}")
            import traceback

            traceback.print_exc()
            return 1

    total_models = sum(1 + len(additional) for _, _, _, additional in MODELS)
    print(f"\nGenerated documentation for {total_models} models across {len(MODELS)} pages")
    return 0


if __name__ == "__main__":
    exit(main())
