import abc
import os


class ConfigBase(abc.ABC):
    """Base class for all config classes"""

    def __init__(self, model):
        self._model = model

    @staticmethod
    @abc.abstractmethod
    def model_class():
        """Return the data model class backing the config"""

    @classmethod
    def load(cls, config_file):
        """Load the config from a file.

        Parameters
        ---------
        config_file : str

        Returns
        -------
        ConfigBase

        """
        model = cls.model_class().load(config_file)
        return cls(model)

    @property
    def model(self):
        """Return the data model for the config.

        Returns
        -------
        DSGBaseModel

        """
        return self._model
