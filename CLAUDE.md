# Python Coding Conventions

This document defines the coding conventions for the Python client codebase. These rules should be followed for all new code and when modifying existing code.

## Type Annotations

- **Do not** import `List`, `Dict`, `Optional`, `Union`, `Tuple`, `Set` from the `typing` module for standard container types.
- Use built-in generics instead:
  - `list[str]` instead of `List[str]`
  - `dict[str, Any]` instead of `Dict[str, Any]`
  - `tuple[int, str]` instead of `Tuple[int, str]`
  - `set[int]` instead of `Set[int]`
- Use union syntax with `|` instead of `Optional` or `Union`:
  - `str | None` instead of `Optional[str]`
  - `int | str` instead of `Union[int, str]`
- The `Any` type should still be imported from `typing` when needed.

## Path Operations

- Prefer `pathlib.Path` over `os.path` for filesystem path operations.
- Import `pathlib` or `from pathlib import Path` at the top of the file.
- Use `Path` methods like `.exists()`, `.is_file()`, `.read_text()`, etc.

## Imports

- All imports should be at the top of the file.
- Do not import modules inside functions unless there's a specific reason (e.g., circular imports, optional dependencies).
- Group imports in the following order:
  1. Standard library imports
  2. Third-party imports
  3. Local application imports

## Tests

- Use pytest.
- Write free functions. Do not write test classes.

## Docstrings

- Use NumPy-style docstrings for all public functions, classes, and modules.

```python
def example_function(param1: str, param2: int | None = None) -> dict[str, Any]:
    """Short description of the function.

    Longer description if needed, explaining the function's behavior
    in more detail.

    Parameters
    ----------
    param1 : str
        Description of param1.
    param2 : int | None, optional
        Description of param2, by default None.

    Returns
    -------
    dict[str, Any]
        Description of the return value.

    Raises
    ------
    ValueError
        When param1 is empty.

    Examples
    --------
    >>> example_function("test", 42)
    {'result': 'test-42'}
    """
```

## Exception Messages

- **Do not** use f-strings directly in exception raises.
- Assign the message to a variable first:

```python
# Correct
msg = f"Invalid value: {value!r}"
raise ValueError(msg)

# Incorrect
raise ValueError(f"Invalid value: {value!r}")
```

This convention improves debuggability and allows for easier message modification.

## Example

```python
"""Module docstring describing the module's purpose."""

import json
import subprocess
from pathlib import Path
from typing import Any

import requests


def process_file(file_path: Path, options: dict[str, Any] | None = None) -> list[str]:
    """Process a file and return results.

    Parameters
    ----------
    file_path : Path
        Path to the file to process.
    options : dict[str, Any] | None, optional
        Processing options, by default None.

    Returns
    -------
    list[str]
        List of processed lines.

    Raises
    ------
    FileNotFoundError
        If the file does not exist.
    """
    if not file_path.exists():
        msg = f"File not found: {file_path}"
        raise FileNotFoundError(msg)

    return file_path.read_text().splitlines()
```
