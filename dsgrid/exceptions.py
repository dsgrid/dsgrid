"""DSGrid exceptions"""

class DSGBaseException(Exception):
    """Base class for all dsgrid exceptions."""


class DSGInvalidField(DSGBaseException):
    """Raised if a field is missing or invalid."""


class DSGInvalidEnumeration(DSGBaseException):
    """Raised if a type is not stored or is invalid."""
