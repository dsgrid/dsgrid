"""dsgrid exceptions and warnings"""


class DSGBaseException(Exception):
    """Base class for all dsgrid exceptions."""


class DSGBaseWarning(Warning):
    """Base class for all dsgrid warnings."""


class DSGInvalidField(DSGBaseException):
    """Raised if a field is missing or invalid."""


class DSGInvalidDimension(DSGBaseException):
    """Raised if a type is not stored or is invalid."""


class DSGInvalidDimensionMapping(DSGBaseException):
    """Raised if a mapping is not stored or is invalid."""


class DSGRuntimeError(DSGBaseException):
    """Raised if there was a generic runtime error."""


class DSGProjectConfigError(DSGBaseException):
    """Error for bad project configuration inputs"""


class DSGDatasetConfigError(DSGBaseException):
    """Error for bad dataset configuration inputs"""


class DSGValueNotStored(DSGBaseException):
    """Raised if a value is not stored."""


class DSGConfigWarning(DSGBaseWarning):
    """Warning for unclear or default configuration inputs"""


class DSGFileInputError(DSGBaseException):
    """Error during input file checks."""


class DSGFileInputWarning(DSGBaseWarning):
    """Warning during input file checks."""


class DSGJSONError(DSGBaseException):
    """Error with JSON file"""
