"""Python wrapper for Rust-based pattern finding functionality."""

from dsgrid.rust_ext.find_minimal_patterns import find_minimal_patterns_from_file

try:
    from dsgrid.minimal_patterns import Pattern, PatternConfig
except ImportError as e:
    msg = (
        "Failed to import minimal_patterns Rust extension. "
        "Make sure the package was built with maturin: `pip install -e .` or `maturin develop`"
    )
    raise ImportError() from e

__all__ = ["Pattern", "PatternConfig", "find_minimal_patterns_from_file"]
