import abc
import logging
from pathlib import Path
from typing import Type

import json5

from dsgrid.exceptions import DSGInvalidOperation
from dsgrid.spark.types import (
    DataFrame,
)
from dsgrid.utils.spark import models_to_dataframe


logger = logging.getLogger(__name__)


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
        return cls._load(config_file, *args, **kwargs)

    @classmethod
    def load_from_model(cls, model):
        """Load the config from a model.

        Parameters
        ---------
        model : DSGBaseModel

        Returns
        -------
        ConfigBase

        """
        return cls(model)

    @classmethod
    def _load(cls, config_file):
        model = cls.model_class().load(config_file)
        return cls(model)

    @staticmethod
    @abc.abstractmethod
    def config_filename() -> str:
        """Return the config filename.

        Returns
        -------
        str

        """

    @property
    @abc.abstractmethod
    def config_id(self) -> str:
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
    def model_class() -> Type:
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
            msg = f"{filename} exists. Set force=True to overwrite."
            raise DSGInvalidOperation(msg)
        filename.write_text(self.model.model_dump_json(indent=2))
        return filename


class ConfigWithRecordFileBase(ConfigBase, abc.ABC):
    """Intermediate-level base class to provide serialization of record files."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def get_records_dataframe(self) -> DataFrame:
        """Return the records in a spark dataframe. Cached on first call."""
        # id provides uniqueness and the config_id could help inspect what's in cache in case we
        # ever need that.
        # Spark doesn't allow dashes in the table name.
        table_name = f"{self.config_id}__{id(self)}".replace("-", "_")
        df = models_to_dataframe(self.model.records, table_name=table_name)
        logger.debug("Loaded %s records dataframe", self.config_id)
        return df

    @classmethod
    def load(cls, config_file):
        config = super().load(config_file)
        return config

    def serialize(self, path, force=False):
        dst_config_file = path / self.config_filename()
        records_file = path / "records.csv"
        for filename in (dst_config_file, records_file):
            if filename.exists() and not force:
                msg = f"{filename} exists. Set force=True to overwrite."
                raise DSGInvalidOperation(msg)

        self.get_records_dataframe().toPandas().to_csv(records_file, index=False)
        model_data = self.model.serialize()
        model_data["file"] = records_file.name
        dst_config_file.write_text(json5.dumps(model_data, indent=2))
        return dst_config_file
