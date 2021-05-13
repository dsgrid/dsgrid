import abc
from pathlib import Path

from dsgrid.data_models import serialize_model
from dsgrid.utils.files import dump_data


class ConfigBase(abc.ABC):
    """Base class for all config classes"""

    def __init__(self, model):
        self._model = model

    @classmethod
    def load(cls, config_file, *args, **kwargs):
        """Load the config from a file.

        Parameters
        ---------
        config_file : str

        Returns
        -------
        ConfigBase

        """
        # Subclasses can reimplement this method if they need more arguments.
        return cls._load(config_file)

    @classmethod
    def _load(cls, config_file):
        model = cls.model_class().load(config_file)
        return cls(model)

    @staticmethod
    @abc.abstractmethod
    def config_filename():
        """Return the config filename.

        Returns
        -------
        str

        """

    @property
    @abc.abstractmethod
    def config_id(self):
        """Return the configuration ID.

        Returns
        -------
        str

        """

    @property
    def model(self):
        """Return the data model for the config.

        Returns
        -------
        DSGBaseModel

        """
        return self._model

    @staticmethod
    @abc.abstractmethod
    def model_class():
        """Return the data model class backing the config"""

    def serialize(self, path):
        """Serialize the configuration to a path.

        path : str
            Directory

        """
        filename = Path(path) / self.config_filename()
        dump_data(serialize_model(self.model), filename)
        return filename
