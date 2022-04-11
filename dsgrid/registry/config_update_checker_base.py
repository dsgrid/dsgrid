import abc
import logging

from dsgrid.exceptions import DSGInvalidRegistryState, DSGInvalidOperation


logger = logging.getLogger(__name__)


class ConfigUpdateCheckerBase(abc.ABC):
    """Base class for updating all config models"""

    def __init__(self, old_model, new_model):
        self._old_model = old_model
        self._new_model = new_model
        assert type(self._old_model) == type(self._new_model)
        self._type = type(self._old_model)
        self._changed_fields = set()

    def _check_common(self):
        for field, attrs in self._type.__fields__.items():
            old = getattr(self._old_model, field)
            new = getattr(self._new_model, field)
            if old != new:
                if not attrs.field_info.extra.get("updateable", True):
                    raise DSGInvalidOperation(f"{self._type}.{field} cannot be updated")
                self._changed_fields.add(field)
                logger.info("%s %s changed from %s to %s.", self._type, field, old, new)
        # FUTURE: We could recurse into each dsgrid pydantic model and check each individual
        # field. Would also need to handle lists and dicts of models.
        # This would allow more precise control of changed fields and much better logging.

    @abc.abstractmethod
    def check_preconditions(self):
        """Check preconditions for performing an update.

        Raises
        ------
        DSGInvalidRegistryState
            Raised if a precondition is violated.

        """

    @abc.abstractmethod
    def handle_postconditions(self):
        """Handle any required postconditions."""

    def run(self):
        """Run all checks.

        Raises
        ------
        DSGInvalidOperation
            Raised if the user is changing an immutable field.
        DSGInvalidRegistryState
            Raised if a precondition is violated.

        """
        self.check_preconditions()
        self._check_common()
        self.handle_postconditions()
