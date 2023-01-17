"""dsgrid exceptions and warnings"""


class DSGBaseException(Exception):
    """Base class for all dsgrid exceptions."""


class DSGBaseWarning(Warning):
    """Base class for all dsgrid warnings."""


class DSGInvalidDataset(DSGBaseException):
    """Raised if a dataset is invalid."""


class DSGInvalidField(DSGBaseException):
    """Raised if a field is missing or invalid."""


class DSGInvalidParameter(DSGBaseException):
    """Raised if a parameter is invalid."""


class DSGInvalidFile(DSGBaseException):
    """Raised if a file cannot be read. Possible reason is that the write operation failed."""


class DSGInvalidDimension(DSGBaseException):
    """Raised if a type is not stored or is invalid."""


class DSGInvalidDimensionAssociation(DSGBaseException):
    """Raised if an association is not stored or is invalid."""


class DSGInvalidDimensionMapping(DSGBaseException):
    """Raised if a mapping is not stored or is invalid."""


class DSGInvalidQuery(DSGBaseException):
    """Raised if a query is invalid."""


class DSGMissingDimensionMapping(DSGBaseException):
    """Raised if a mapping is not provided."""


class DSGInvalidOperation(DSGBaseException):
    """Raised if a requested user operation is invalid."""


class DSGRuntimeError(DSGBaseException):
    """Raised if there was a generic runtime error."""


class DSGProjectConfigError(DSGBaseException):
    """Error for bad project configuration inputs"""


class DSGDatasetConfigError(DSGBaseException):
    """Error for bad dataset configuration inputs"""


class DSGDuplicateValueRegistered(Warning):
    """Raised if the user attempts to register a duplicate value."""


class DSGValueNotRegistered(DSGBaseException):
    """Raised if a value is not registered."""


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


class DSGFilesystemInterfaceError(DSGBaseException):
    """Error with FileSystemInterface command"""


class DSGRegistryLockError(DSGBaseException):
    """Error with a locked registry"""


class DSGMakeLockError(DSGBaseException):
    """Error when making registry lock"""


class DSGInvalidRegistryState(DSGBaseException):
    """Invalid Registry State"""
