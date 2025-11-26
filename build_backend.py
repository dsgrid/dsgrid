"""Custom build backend that wraps maturin but allows fallback when Rust is unavailable.

This allows `pip install -e .` to succeed even without a Rust toolchain installed,
while still building the Rust extension when building wheels for distribution.
"""

import shutil
import sys
import warnings

# Check if Rust toolchain is available
RUST_AVAILABLE = shutil.which("cargo") is not None


def _warn_no_rust():
    """Warn that Rust extension will not be built."""
    msg = (
        "Rust toolchain (cargo) not found. The dsgrid Rust extension (minimal_patterns) "
        "will not be built. Some functionality will be unavailable. "
        "To build the Rust extension, install Rust from https://rustup.rs/"
    )
    # Use warnings module - this is typically shown by pip
    warnings.warn(msg, UserWarning, stacklevel=2)
    # Also print directly to stderr for visibility
    print(f"\n{'='*70}", file=sys.stderr)
    print("WARNING: Rust toolchain not found.", file=sys.stderr)
    print("Building dsgrid without Rust extension (minimal_patterns).", file=sys.stderr)
    print("Some functionality will be unavailable.", file=sys.stderr)
    print(f"{'='*70}\n", file=sys.stderr)
    # Flush to ensure it appears
    sys.stderr.flush()


# For wheel builds (CI), always use maturin
def build_wheel(wheel_directory, config_settings=None, metadata_directory=None):
    """Build wheel - requires Rust for distribution builds."""
    import maturin

    return maturin.build_wheel(wheel_directory, config_settings, metadata_directory)


def build_sdist(sdist_directory, config_settings=None):
    """Build source distribution."""
    import maturin

    return maturin.build_sdist(sdist_directory, config_settings)


def get_requires_for_build_wheel(config_settings=None):
    """Get requirements for building wheel."""
    return ["maturin>=1.0,<2.0"]


def get_requires_for_build_sdist(config_settings=None):
    """Get requirements for building sdist."""
    return ["maturin>=1.0,<2.0"]


# For editable installs, allow fallback without Rust
def build_editable(wheel_directory, config_settings=None, metadata_directory=None):
    """Build editable install - can work without Rust."""
    if RUST_AVAILABLE:
        import maturin

        return maturin.build_editable(wheel_directory, config_settings, metadata_directory)
    else:
        _warn_no_rust()
        # Fall back to setuptools for editable install without Rust extension
        import setuptools.build_meta

        return setuptools.build_meta.build_editable(
            wheel_directory, config_settings, metadata_directory
        )


def get_requires_for_build_editable(config_settings=None):
    """Get requirements for editable install."""
    if RUST_AVAILABLE:
        return ["maturin>=1.0,<2.0"]
    else:
        return ["setuptools>=61.0"]
