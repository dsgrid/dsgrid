"""Generate MyST markdown tables for Pydantic models.

This script generates clean, scannable documentation tables for Pydantic models.
It can be called on individual models or recursively generate docs for nested models.
"""

import inspect
import sys
from pathlib import Path
from typing import Any, get_args, get_origin, Union
import importlib

import click
from pydantic import BaseModel
from pydantic.fields import FieldInfo


def get_type_string(field_type: Any) -> str:
    """Convert a Python type annotation to a readable string.

    Parameters
    ----------
    field_type : Any
        The type annotation to convert

    Returns
    -------
    str
        Human-readable type string
    """
    # Handle None type
    if field_type is type(None):
        return "None"

    # Get the origin for generic types (List, Dict, Union, etc.)
    origin = get_origin(field_type)
    args = get_args(field_type)

    if origin is Union:
        # Handle Optional and Union types
        type_strs = [get_type_string(arg) for arg in args]
        # Escape pipes for markdown tables
        return " \\| ".join(type_strs)
    elif origin is list:
        if args:
            return f"list[{get_type_string(args[0])}]"
        return "list"
    elif origin is dict:
        if args:
            key_type = get_type_string(args[0])
            val_type = get_type_string(args[1])
            return f"dict[{key_type}, {val_type}]"
        return "dict"
    elif origin is tuple:
        if args:
            arg_strs = [get_type_string(arg) for arg in args]
            return f"tuple[{', '.join(arg_strs)}]"
        return "tuple"
    elif hasattr(field_type, "__name__"):
        # For regular classes and Pydantic models
        name = field_type.__name__
        if hasattr(field_type, "__module__") and field_type.__module__.startswith("dsgrid"):
            # Link to other dsgrid models
            return f"[{name}](#{name.lower()})"
        return f"`{name}`"
    else:
        return str(field_type)


def get_default_string(field_info: FieldInfo) -> str:
    """Get a readable default value string.

    Parameters
    ----------
    field_info : FieldInfo
        The Pydantic field info

    Returns
    -------
    str
        Human-readable default value string
    """
    if field_info.is_required():
        return "*(required)*"

    default = field_info.default
    if default is None:
        return "`None`"
    elif isinstance(default, str):
        if len(default) > 50:
            return f'`"{default[:47]}..."`'
        return f'`"{default}"`'
    elif isinstance(default, (list, dict, tuple)):
        return f"`{repr(default)}`"
    else:
        return f"`{default}`"


def get_field_description(field_info: FieldInfo) -> str:
    """Get the field description.

    Parameters
    ----------
    field_info : FieldInfo
        The Pydantic field info

    Returns
    -------
    str
        The field description, or empty string if none
    """
    return field_info.description or ""


def is_pydantic_model(obj: Any) -> bool:
    """Check if an object is a Pydantic model class.

    Parameters
    ----------
    obj : Any
        The object to check

    Returns
    -------
    bool
        True if obj is a Pydantic model class
    """
    try:
        return inspect.isclass(obj) and issubclass(obj, BaseModel)
    except TypeError:
        return False


def extract_nested_models(model: type[BaseModel]) -> set[type[BaseModel]]:
    """Extract all nested Pydantic models from a model's fields.

    Parameters
    ----------
    model : type[BaseModel]
        The Pydantic model to analyze

    Returns
    -------
    set[type[BaseModel]]
        Set of nested Pydantic model classes
    """
    nested = set()

    for field_name, field_info in model.model_fields.items():
        field_type = field_info.annotation

        # Check the type itself
        if is_pydantic_model(field_type):
            nested.add(field_type)

        # Check Union/Optional arguments
        args = get_args(field_type)
        for arg in args:
            if is_pydantic_model(arg):
                nested.add(arg)
            # Check nested generics (e.g., List[Model])
            inner_args = get_args(arg)
            for inner_arg in inner_args:
                if is_pydantic_model(inner_arg):
                    nested.add(inner_arg)

    return nested


def generate_fields_table(model: type[BaseModel]) -> str:
    """Generate a markdown table for a model's fields.

    Parameters
    ----------
    model : type[BaseModel]
        The Pydantic model to document

    Returns
    -------
    str
        Markdown table string
    """
    lines = ["| Name | Type | Default | Description |", "|------|------|---------|-------------|"]

    for field_name, field_info in model.model_fields.items():
        # Skip internal fields
        if field_name.startswith("_"):
            continue

        type_str = get_type_string(field_info.annotation)
        default_str = get_default_string(field_info)
        desc = get_field_description(field_info)

        # Clean up description: remove newlines and extra whitespace, escape pipes
        desc = " ".join(desc.split())
        desc = desc.replace("|", "\\|")

        lines.append(f"| `{field_name}` | {type_str} | {default_str} | {desc} |")

    return "\n".join(lines)


def generate_validators_table(model: type[BaseModel], heading_level: int = 2) -> str:
    """Generate a markdown table for a model's validators.

    Parameters
    ----------
    model : type[BaseModel]
        The Pydantic model
    heading_level : int, default=2
        Heading level for the "Validators" section
        The Pydantic model to document

    Returns
    -------
    str
        Markdown table string, or empty string if no validators
    """
    validators = []

    # Get model validators - Pydantic v2 approach
    try:
        if hasattr(model, "__pydantic_decorators__"):
            decorators = model.__pydantic_decorators__

            # Field validators
            if hasattr(decorators, "field_validators") and decorators.field_validators:
                for field_name, validator_list in decorators.field_validators.items():
                    # Handle if it's a dict or other structure
                    if hasattr(validator_list, "items"):
                        for mode, field_validators in validator_list.items():
                            if not isinstance(field_validators, (list, tuple)):
                                field_validators = [field_validators]
                            for validator in field_validators:
                                func = getattr(validator, "func", validator)
                                func_name = (
                                    func.__name__ if hasattr(func, "__name__") else str(func)
                                )
                                doc = getattr(func, "__doc__", None) or "No description"
                                doc = doc.strip().split("\n")[0]
                                validators.append((func_name, field_name, doc))
                    else:
                        # It's already a list of validators
                        if not isinstance(validator_list, (list, tuple)):
                            validator_list = [validator_list]
                        for validator in validator_list:
                            func = getattr(validator, "func", validator)
                            func_name = func.__name__ if hasattr(func, "__name__") else str(func)
                            doc = getattr(func, "__doc__", None) or "No description"
                            doc = doc.strip().split("\n")[0]
                            validators.append((func_name, field_name, doc))

            # Model validators
            if hasattr(decorators, "model_validators") and decorators.model_validators:
                for mode, model_validators in decorators.model_validators.items():
                    if not isinstance(model_validators, (list, tuple)):
                        model_validators = [model_validators]
                    for validator in model_validators:
                        func = getattr(validator, "func", validator)
                        func_name = func.__name__ if hasattr(func, "__name__") else str(func)
                        doc = getattr(func, "__doc__", None) or "No description"
                        doc = doc.strip().split("\n")[0]
                        validators.append((func_name, "*(model)*", doc))
    except Exception:
        # If validator extraction fails, just skip it
        pass

    if not validators:
        return ""

    lines = [
        "",
        f"{'#' * heading_level} Validators",
        "",
        "| Name | Applies To | Description |",
        "|------|------------|-------------|",
    ]

    for name, applies_to, description in validators:
        # Escape pipe characters
        description = description.replace("|", "\\|")
        lines.append(f"| `{name}` | `{applies_to}` | {description} |")

    return "\n".join(lines)


def generate_model_documentation(
    model: type[BaseModel],
    include_nested: bool = True,
    processed: set[type[BaseModel]] = None,
    heading_level: int = 1,
    documented_models: dict[type[BaseModel], str] = None,
) -> str:
    """Generate complete markdown documentation for a Pydantic model.

    Parameters
    ----------
    model : type[BaseModel]
        The Pydantic model to document
    include_nested : bool, default=True
        Whether to recursively document nested models
    processed : set[type[BaseModel]], optional
        Set of already processed models (used internally for recursion)
    heading_level : int, default=1
        Heading level for the model name (1=H1 for top-level, 2=H2 for nested)
    documented_models : dict[type[BaseModel], str], optional
        Mapping of top-level models to their documentation file paths.
        Used to create links instead of duplicating documentation.

    Returns
    -------
    str
        Complete markdown documentation
    """
    if processed is None:
        processed = set()

    if documented_models is None:
        documented_models = {}

    if model in processed:
        return ""

    processed.add(model)

    # Get model docstring
    doc = inspect.getdoc(model) or ""

    # Use heading_level for model name, heading_level+1 for sections
    lines = [
        f"{'#' * heading_level} {model.__name__}",
        "",
    ]

    if doc:
        lines.extend([doc, ""])

    lines.extend(
        [
            f"{'#' * (heading_level + 1)} Fields",
            "",
            generate_fields_table(model),
        ]
    )

    # Add validators if present
    validators_table = generate_validators_table(model, heading_level + 1)
    if validators_table:
        lines.append(validators_table)

    lines.append("")

    # Document nested models at heading_level + 1
    if include_nested:
        nested_models = extract_nested_models(model)
        # Sort by name for consistent output
        for nested_model in sorted(nested_models, key=lambda m: m.__name__):
            # Check if this nested model is already documented at top level
            if nested_model in documented_models:
                # Create a link instead of duplicating documentation
                doc_path = documented_models[nested_model]
                lines.append(f"{'#' * (heading_level + 1)} {nested_model.__name__}")
                lines.append("")
                lines.append(f"See [{nested_model.__name__}]({doc_path}) for details.")
                lines.append("")
            else:
                # Document the nested model inline
                nested_doc = generate_model_documentation(
                    nested_model, True, processed, heading_level + 1, documented_models
                )
                if nested_doc:
                    lines.append(nested_doc)

    return "\n".join(lines)


def import_model(model_path: str) -> type[BaseModel]:
    """Import a Pydantic model from a module path.

    Parameters
    ----------
    model_path : str
        Full path to the model, e.g., 'dsgrid.config.dataset_config.DatasetConfigModel'

    Returns
    -------
    type[BaseModel]
        The imported Pydantic model class
    """
    module_path, class_name = model_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    model = getattr(module, class_name)

    if not is_pydantic_model(model):
        msg = f"{model_path} is not a Pydantic model"
        raise ValueError(msg)

    return model


@click.command()
@click.argument("model_path")
@click.option(
    "-o",
    "--output",
    type=click.Path(),
    help="Output file path. If not provided, prints to stdout.",
)
@click.option(
    "--no-nested",
    is_flag=True,
    help="Don't recursively document nested models.",
)
def main(model_path: str, output: str | None, no_nested: bool):
    """Generate markdown documentation for a Pydantic model.

    MODEL_PATH should be the full Python path to the model, e.g.:
    dsgrid.config.dataset_config.DatasetConfigModel
    """
    try:
        model = import_model(model_path)
        documentation = generate_model_documentation(model, include_nested=not no_nested)

        if output:
            output_path = Path(output)
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(documentation, encoding="utf-8")
            click.echo(f"Documentation written to {output_path}")
        else:
            click.echo(documentation)

    except Exception as e:
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
