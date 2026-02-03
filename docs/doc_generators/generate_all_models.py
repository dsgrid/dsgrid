"""Generate all data model documentation.

This script generates markdown documentation for all dsgrid configuration models.
"""

from pathlib import Path

from .core import extract_all_nested_models, generate_model_documentation, import_model

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
    # First occurrence wins - if a model is documented on one page, all other
    # pages will link to it instead of re-documenting it
    documented_models = {}

    # First pass: collect all models that will be documented (including nested)
    for page_title, model_path, output_file, additional_models in MODELS:
        doc_filename = Path(output_file).name

        # Add main model and all its nested models (only if not already documented)
        model = import_model(model_path)
        if model not in documented_models:
            documented_models[model] = doc_filename
        for nested_model in extract_all_nested_models(model):
            if nested_model not in documented_models:
                documented_models[nested_model] = doc_filename

        # Add additional models and all their nested models (only if not already documented)
        for additional_model_path in additional_models:
            additional_model = import_model(additional_model_path)
            if additional_model not in documented_models:
                documented_models[additional_model] = doc_filename
            for nested_model in extract_all_nested_models(additional_model):
                if nested_model not in documented_models:
                    documented_models[nested_model] = doc_filename

    # Second pass: generate documentation for each page
    for page_title, model_path, output_file, additional_models in MODELS:
        output_path = docs_dir / output_file
        doc_filename = Path(output_file).name
        print(f"Generating documentation for {model_path}...")

        try:
            # Start with page title and horizontal line
            documentation = f"# {page_title}\n\n---\n\n"

            # Collect all unique models to document on this page in order encountered
            # Only include models that should be documented on THIS page
            models_to_document = []
            seen = set()

            def add_model_if_new(m):
                """Add model to list if not already seen AND if it belongs on this page."""
                if m not in seen and documented_models.get(m) == doc_filename:
                    models_to_document.append(m)
                    seen.add(m)

            # Add main model first
            model = import_model(model_path)
            add_model_if_new(model)

            # Then all its nested models (in alphabetical order for consistency)
            for nested_model in sorted(extract_all_nested_models(model), key=lambda m: m.__name__):
                add_model_if_new(nested_model)

            # Add additional models and their nested models
            for additional_model_path in additional_models:
                additional_model = import_model(additional_model_path)
                add_model_if_new(additional_model)

                # Then its nested models (in alphabetical order)
                for nested_model in sorted(
                    extract_all_nested_models(additional_model), key=lambda m: m.__name__
                ):
                    add_model_if_new(nested_model)

            # Generate documentation for each model (flat, at H2 level, no nested)
            # The documented_models dict ensures nested models link to their documentation
            # instead of being duplicated
            model_docs = []
            for model_to_doc in models_to_document:
                model_doc = generate_model_documentation(
                    model_to_doc,
                    include_nested=False,  # Flat structure - no nested tables
                    documented_models=documented_models,
                    heading_level=2,
                )
                model_docs.append(model_doc)

            # Join all model docs with horizontal bars
            documentation += "\n\n---\n\n".join(model_docs)

            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(documentation, encoding="utf-8")
            print(f"  Written to {output_file}")
        except Exception as e:
            print(f"  Error: {e}")
            import traceback

            traceback.print_exc()
            return 1

    print(f"\nGenerated documentation for {len(MODELS)} pages")
    return 0


if __name__ == "__main__":
    exit(main())
