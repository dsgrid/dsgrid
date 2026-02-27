# Documentation Generators

This package contains tools for automatically generating MyST markdown documentation from Pydantic models and Python enumerations.

## Structure

- **`core.py`**: Core library functions for generating markdown documentation
  - `generate_model_documentation()` - Generate docs for Pydantic models
  - `generate_enum_documentation()` - Generate docs for Python enums
  - `import_model()` - Import Pydantic models by module path
  - `import_enum()` - Import enums by module path
  - Helper functions for field/validator/type processing

- **`generate_enums.py`**: Runner script that generates documentation for all dsgrid enumerations into a single `enums.md` file
  - **Configuration**: Edit the `ENUMS` list to add/remove enums to document

- **`generate_all_models.py`**: Runner script that generates documentation for all dsgrid configuration models across multiple pages
  - **Configuration**: Edit the `MODELS` list to configure which models appear on which pages. Each entry specifies:
    - Page title (H1 heading)
    - Main model path
    - Output file path
    - Additional models to include on the same page

## Build Integration

Both runner scripts are called automatically during Sphinx builds via the `setup()` hook in `source/conf.py`:
1. `generate_enums.py` runs first
2. `generate_all_models.py` runs second

This ensures documentation is always up-to-date with the Python source code.

## Key Features

- **Deduplication**: Models documented at the top level are linked to (not duplicated) when referenced elsewhere
- **Cross-file linking**: Models can link to other models on different pages
- **Enum field support**: EnumValue fields (description, frequency, etc.) are automatically extracted and displayed
- **Type-aware processing**: Different handling for DSGEnum vs StrEnum, str-based enums, etc.
- **Automatic table generation**: Field tables, validator tables, and enum constant tables
