"""Python wrapper for Rust-based pattern finding functionality."""

import importlib
from typing import Any, cast

from dsgrid.rust_ext.find_minimal_patterns import find_minimal_patterns_from_file

try:
    _minimal_patterns = cast(Any, importlib.import_module("dsgrid.minimal_patterns"))
    Pattern = _minimal_patterns.Pattern
    PatternConfig = _minimal_patterns.PatternConfig
except ImportError as e:
    msg = (
        "Failed to import minimal_patterns Rust extension. "
        "Make sure the package was built with maturin: `pip install -e .` or `maturin develop`"
    )
    raise ImportError() from e

__all__ = ["Pattern", "PatternConfig", "find_minimal_patterns_from_file"]
