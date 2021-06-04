import abc
import os
import shutil
from pathlib import Path

from dsgrid.data_models import serialize_model
from dsgrid.exceptions import DSGInvalidOperation
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

    def serialize(self, path, force=False):
        """Serialize the configuration to a path.

        path : str
            Directory
        force : bool
            If True, overwrite files.

        """
        filename = Path(path) / self.config_filename()
        if filename.exists() and not force:
            raise DSGInvalidOperation(f"{filename} exists. Set force=True to overwrite.")
        dump_data(serialize_model(self.model), filename)
        return filename


class ConfigWithDataFilesBase(ConfigBase, abc.ABC):
    """Intermediate-level base class to provide serialization of data files."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._src_dir = None

    @classmethod
    def load(cls, config_file):
        config = super().load(config_file)
        config.src_dir = os.path.dirname(config_file)
        return config

    def serialize(self, path, force=False):
        # Serialize the data alongside the config file at path and point the
        # config to that file to ensure that the config will be Pydantic-valid when loaded.
        model_data = serialize_model(self.model)
        orig_file = getattr(self.model, "filename")

        # Leading directories from the original are not relevant in the registry.
        dst_record_file = Path(path) / Path(orig_file).name
        filename = path / self.config_filename()

        for path in (dst_record_file, filename):
            if path.exists() and not force:
                raise DSGInvalidOperation(f"{path} exists. Set force=True to overwrite.")

        shutil.copyfile(self._src_dir / self.model.filename, dst_record_file)
        # We have to make this change in the serialized dict instead of
        # model because Pydantic will fail the assignment due to not being
        # able to find the path.
        model_data["file"] = Path(self.model.filename).name
        dump_data(model_data, filename)
        return filename

    @property
    def src_dir(self):
        """Return the directory containing the config file. Data files inside the config file
        are relative to this.

        """
        return self._src_dir

    @src_dir.setter
    def src_dir(self, src_dir):
        """Set the source directory. Must be the directory containing the config file."""
        self._src_dir = Path(src_dir)
