"""DSGrid exceptions"""

class DSGBaseException(Exception):
    """Base class for all dsgrid exceptions."""


class DSGInvalidField(DSGBaseException):
    """Raised if a field is missing or invalid."""


class DSGInvalidDimension(DSGBaseException):
    """Raised if a type is not stored or is invalid."""


class DSGInvalidDimensionMapping(DSGBaseException):
    """Raised if a mapping is not stored or is invalid."""
