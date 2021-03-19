"""DSGrid exceptions"""


class DSGBaseException(Exception):
    """Base class for all dsgrid exceptions."""


class DSGInvalidField(DSGBaseException):
    """Raised if a field is missing or invalid."""


class DSGInvalidDimension(DSGBaseException):
    """Raised if a type is not stored or is invalid."""


class DSGInvalidDimensionMapping(DSGBaseException):
    """Raised if a mapping is not stored or is invalid."""


class DSGRuntimeError(Exception):
    """Raised if there was a generic runtime error."""


class DSGProjectConfigError(Exception):
    """
    Error for bad project configuration inputs
    """


class DSGDatasetConfigError(Exception):
    """
    Error for bad dataset configuration inputs
    """


class DSGValueNotStored(DSGBaseException):
    """Raised if a value is not stored."""


class ConfigWarning(Warning):
    """
    Warning for unclear or default configuration inputs
    """


class FileInputError(Exception):
    """
    Error during input file checks.
    """


class FileInputWarning(Warning):
    """
    Warning during input file checks.
    """


class JSONError(Exception):
    """
    Error with JSON file
    """
