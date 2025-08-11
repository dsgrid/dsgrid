"""Utility functions for versioning"""

from semver import VersionInfo


def handle_version_or_str(version):
    """Return VersionInfo if version is a str."""
    if isinstance(version, str):
        return make_version(version)
    return version


def make_version(version):
    """Convert the string version to a VersionInfo object.

    Parameters
    ----------
    version : str

    Returns
    -------
    VersionInfo

    Raises
    ------
    ValueError
        Raised if parsing fails.

    """
    try:
        return VersionInfo.parse(version)
    except Exception as exc:
        msg = f"Failed to create VersionInfo: {exc}"
        raise ValueError(msg) from exc

    return version
