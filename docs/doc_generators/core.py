"""Core documentation generation functions for Pydantic models and Enums.

This module provides the low-level functions for generating MyST markdown
documentation from Pydantic models and Python enumerations.
"""

import importlib
import inspect
from enum import Enum
from typing import Any, get_args, get_origin

from pydantic import BaseModel
from pydantic.fields import FieldInfo


def get_type_string(field_type: Any, documented_models: dict = None) -> str:
    """Convert a Python type annotation to a readable string.

    Parameters
    ----------
    field_type : Any
        The type annotation to convert
    documented_models : dict, optional
        Mapping of models to their documentation file paths

    Returns
    -------
    str
        Human-readable type string
    """
    if documented_models is None:
        documented_models = {}

    # Handle None type
    if field_type is type(None):
        return "None"

    # Get the origin for generic types (List, Dict, Union, etc.)
    origin = get_origin(field_type)
    args = get_args(field_type)

    if origin is type(None) or origin is type(type(None)):
        # Handle Union types
        type_strs = [get_type_string(arg, documented_models) for arg in args]
        # Escape pipes for markdown tables
        return " \\| ".join(type_strs)
    elif origin is list:
        if args:
            return f"list[{get_type_string(args[0], documented_models)}]"
        return "list"
    elif origin is dict:
        if args:
            key_type = get_type_string(args[0], documented_models)
            val_type = get_type_string(args[1], documented_models)
            return f"dict[{key_type}, {val_type}]"
        return "dict"
    elif origin is tuple:
        if args:
            arg_strs = [get_type_string(arg, documented_models) for arg in args]
            return f"tuple[{', '.join(arg_strs)}]"
        return "tuple"
    elif hasattr(field_type, "__name__"):
        # For regular classes and Pydantic models
        name = field_type.__name__
        if hasattr(field_type, "__module__") and field_type.__module__.startswith("dsgrid"):
            # Check if it's an Enum - link to enums page
            if inspect.isclass(field_type) and issubclass(field_type, Enum):
                # Link to the enums page with anchor
                return f"[{name}](enums.md#{name.lower()})"
            # Check if it's a documented model - link to its page
            elif field_type in documented_models:
                doc_path = documented_models[field_type]
                # Link to the file with anchor (H2 heading)
                return f"[{name}]({doc_path}#{name.lower()})"
            # Otherwise link to local anchor
            else:
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


def extract_all_nested_models(model: type[BaseModel]) -> set[type[BaseModel]]:
    """Recursively extract ALL nested Pydantic models from a model.

    Parameters
    ----------
    model : type[BaseModel]
        The Pydantic model to analyze

    Returns
    -------
    set[type[BaseModel]]
        Set of all nested Pydantic model classes (including deeply nested)
    """
    all_nested = set()
    to_process = {model}
    processed = set()

    while to_process:
        current = to_process.pop()
        if current in processed:
            continue
        processed.add(current)

        # Get direct nested models
        direct_nested = extract_nested_models(current)
        for nested in direct_nested:
            if nested not in all_nested and nested != model:
                all_nested.add(nested)
                to_process.add(nested)

    return all_nested


def generate_fields_table(model: type[BaseModel], documented_models: dict = None) -> str:
    """Generate a markdown table for a model's fields.

    Parameters
    ----------
    model : type[BaseModel]
        The Pydantic model to document
    documented_models : dict, optional
        Mapping of models to their documentation file paths

    Returns
    -------
    str
        Markdown table string
    """
    if documented_models is None:
        documented_models = {}

    lines = ["| Name | Type | Default | Description |", "|------|------|---------|-------------|"]

    for field_name, field_info in model.model_fields.items():
        # Skip internal fields
        if field_name.startswith("_"):
            continue

        type_str = get_type_string(field_info.annotation, documented_models)
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
        The Pydantic model to document
    heading_level : int, default=2
        Heading level for the "Validators" section

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

    # Get full module path
    full_path = f"{model.__module__}.{model.__name__}"

    # Use heading_level for model name, heading_level+1 for sections
    lines = [
        f"{'#' * heading_level} {model.__name__}",
        "",
    ]

    # Add full path for H1 or H2 (top-level models on a page)
    # Don't show for H3+ (nested models)
    if heading_level <= 2:
        lines.extend([f"*{full_path}*", ""])

    if doc:
        lines.extend([doc, ""])

    lines.extend(
        [
            f"{'#' * (heading_level + 1)} Fields",
            "",
            generate_fields_table(model, documented_models),
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
                # Skip - the table already has a link to it
                continue
            else:
                # Document the nested model inline
                nested_doc = generate_model_documentation(
                    nested_model, True, processed, heading_level + 1, documented_models
                )
                if nested_doc:
                    lines.append(nested_doc)

    return "\n".join(lines)


def generate_enum_documentation(enum_class: type[Enum]) -> str:
    """Generate markdown documentation for an Enum.

    Parameters
    ----------
    enum_class : type[Enum]
        The Enum class to document

    Returns
    -------
    str
        Markdown documentation for the enum
    """
    doc = inspect.getdoc(enum_class) or ""

    # Filter out inherited str class documentation
    # Enums that inherit from str (either via StrEnum or (str, Enum)) get str's __doc__
    # Check if str is in the base classes
    if str in enum_class.__mro__:
        # Only use the docstring if it's explicitly defined on the class itself
        # (not inherited from str)
        if enum_class.__doc__ is None or enum_class.__doc__ == str.__doc__:
            doc = ""
        else:
            doc = enum_class.__doc__

    # Get full module path
    full_path = f"{enum_class.__module__}.{enum_class.__name__}"

    lines = [
        f"## {enum_class.__name__}",
        "",
        f"*{full_path}*",
        "",
    ]

    if doc:
        lines.extend([doc, ""])

    # Check if any enum members have additional attributes (from EnumValue)
    # We need to check the actual __dict__ of the member to get only the extra attributes
    extra_attrs = set()
    has_description = False

    # Get all enum member names to exclude them from attributes
    enum_member_names = {member.name for member in enum_class}

    for member in enum_class:
        # Check for description
        if hasattr(member, "description") and member.description:
            has_description = True

        # Get attributes that are not callables and not standard enum attributes
        for attr in dir(member):
            if attr.startswith("_"):
                continue
            if attr in ("name", "value", "description"):
                continue
            # Skip if this attribute name is actually another enum member
            if attr in enum_member_names:
                continue

            attr_val = getattr(member, attr, None)
            # Skip methods, functions, and other callables
            if callable(attr_val):
                continue
            # Skip if the attribute value is an enum member
            if isinstance(attr_val, Enum):
                continue

            # Only include if it's an actual data attribute
            extra_attrs.add(attr)

    # Build table header based on what attributes are present
    if has_description and extra_attrs:
        # All fields: Constant, Value, Description, and other attributes
        header_cols = ["Constant", "Value", "Description"] + sorted(extra_attrs)
    elif has_description:
        # Just Constant, Value, and Description
        header_cols = ["Constant", "Value", "Description"]
    elif extra_attrs:
        # Constant, Value, and other attributes (no description)
        header_cols = ["Constant", "Value"] + sorted(extra_attrs)
    else:
        # Just Constant and Value (basic enum)
        header_cols = ["Constant", "Value"]

    # Create table header
    lines.extend(
        [
            "| " + " | ".join(header_cols) + " |",
            "|" + "|".join(["-" * (len(col) + 2) for col in header_cols]) + "|",
        ]
    )

    # Create table rows
    for member in enum_class:
        # Show the enum member name and its actual value
        value = member.value
        # Format the value appropriately (string values get quotes)
        if isinstance(value, str):
            value_str = f"`'{value}'`"
        else:
            value_str = f"`{value}`"

        row = [f"`{member.name}`", value_str]

        # Add description if present in header
        if has_description:
            desc = getattr(member, "description", None) or ""
            # Clean up description: remove newlines and extra whitespace, escape pipes
            desc = " ".join(desc.split()).replace("|", "\\|")
            row.append(desc)

        # Add other attributes if present in header
        for attr in sorted(extra_attrs):
            attr_value = getattr(member, attr, None)
            if attr_value is not None:
                # Format the attribute value
                if isinstance(attr_value, str):
                    attr_str = attr_value
                else:
                    attr_str = str(attr_value)
                # Clean and escape
                attr_str = " ".join(attr_str.split()).replace("|", "\\|")
                row.append(attr_str)
            else:
                row.append("")

        lines.append("| " + " | ".join(row) + " |")

    lines.append("")
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


def import_enum(enum_path: str) -> type[Enum]:
    """Import an enum from a module path.

    Parameters
    ----------
    enum_path : str
        Full path to the enum, e.g., 'dsgrid.dimension.time.TimeDimensionType'

    Returns
    -------
    type[Enum]
        The imported Enum class
    """
    module_path, enum_name = enum_path.rsplit(".", 1)
    module = importlib.import_module(module_path)
    return getattr(module, enum_name)
