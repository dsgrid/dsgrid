import csv
import logging
import os
from pathlib import Path
from typing import List, Optional, Union

from pydantic import Field, validator

from dsgrid.config.dimension_mapping_base import (
    DimensionMappingBaseModel,
    DimensionMappingDatasetToProjectBaseModel,
    DimensionMappingPreRegisteredBaseModel,
)
from dsgrid.data_models import DSGBaseModel
from dsgrid.utils.files import compute_file_hash
from dsgrid.utils.utilities import convert_record_dicts_to_classes
from .config_base import ConfigWithRecordFileBase


logger = logging.getLogger(__name__)


class MappingTableRecordModel(DSGBaseModel):
    """Represents one record in dimension mapping record files. Maps one dimension to another."""

    from_id: str = Field(
        title="from_id",
        description="Source mapping",
    )
    to_id: Union[str, None] = Field(
        title="to_id",
        description="Destination mapping",
    )
    from_fraction: float = Field(
        title="from_fraction",
        description="Fraction of from_id to map to to_id",
        default=1.0,
    )

    @validator("from_id", "to_id")
    def check_to_id(cls, val):
        if val == "":
            return None
        return val


class MappingTableByNameModel(DimensionMappingPreRegisteredBaseModel):
    """Attributes for a dimension mapping table for soon-to-be registered dimensions by name.
    This will be converted to a MappingTableModel as soon as the dimensions are registered.
    """

    filename: str = Field(
        title="filename",
        alias="file",
        description="Filename containing association table records.",
    )


class DatasetBaseToProjectMappingTableModel(DimensionMappingDatasetToProjectBaseModel):
    """Attributes for a dimension mapping table to map soon-to-be-registered dataset base
    dimensions to a project's dimensions. This will be converted to a MappingTableModel as soon as
    the dimensions are registered.
    """

    filename: str = Field(
        title="filename",
        alias="file",
        description="Filename containing association table records.",
    )


class DatasetBaseToProjectMappingTableListModel(DSGBaseModel):
    """Represents the config file passed to register-and-submit-dataset command."""

    mappings: List[DatasetBaseToProjectMappingTableModel]


class MappingTableModel(DimensionMappingBaseModel):
    """Attributes for a dimension mapping table"""

    filename: Optional[str] = Field(
        title="filename",
        alias="file",
        description="Filename containing association table records. Only assigned for user input "
        "and output purposes. The registry database stores records in the mapping JSON document.",
    )
    file_hash: Optional[str] = Field(
        title="file_hash",
        description="Hash of the contents of the file, computed by dsgrid.",
        dsg_internal=True,
    )
    records: List = Field(
        title="records",
        description="dimension mapping records in filename that get loaded at runtime",
        dsg_internal=True,
        default=[],
    )

    @validator("filename")
    def check_filename(cls, filename):
        """Validate record file"""
        if filename is not None and not os.path.isfile(filename):
            raise ValueError(f"{filename} does not exist")
        return filename

    @validator("file_hash")
    def compute_file_hash(cls, file_hash, values):
        """Compute file hash."""
        if "filename" not in values:
            return file_hash  # this means filename validator fail
        return file_hash or compute_file_hash(values["filename"])

    @validator("records", always=True)
    def add_records(cls, records, values):
        """Add records from the file."""
        if "filename" not in values:
            return []  # this means filename validator fail

        if records:
            if isinstance(records[0], dict):
                records = convert_record_dicts_to_classes(records, MappingTableRecordModel)
            return records

        filename = Path(values["filename"])
        if not filename.name.endswith(".csv"):
            raise ValueError(f"only CSV is supported: {filename}")

        with open(filename, encoding="utf8") as f_in:
            return convert_record_dicts_to_classes(csv.DictReader(f_in), MappingTableRecordModel)

    def dict(self, *args, **kwargs):
        return super().dict(*args, **self._handle_kwargs(**kwargs))

    def json(self, *args, **kwargs):
        return super().json(*args, **self._handle_kwargs(**kwargs))

    @staticmethod
    def _handle_kwargs(**kwargs):
        exclude = {"file", "filename"}
        if "exclude" in kwargs and kwargs["exclude"] is not None:
            kwargs["exclude"].union(exclude)
        else:
            kwargs["exclude"] = exclude
        return kwargs

    @classmethod
    def from_pre_registered_model(
        cls, model: DimensionMappingPreRegisteredBaseModel, from_dimension, to_dimension
    ):
        return MappingTableModel(
            mapping_type=model.mapping_type,
            archetype=model.archetype,
            from_dimension=from_dimension,
            to_dimension=to_dimension,
            description=model.description,
            filename=model.filename,
            from_fraction_tolerance=model.from_fraction_tolerance,
            to_fraction_tolerance=model.to_fraction_tolerance,
        )


class MappingTableConfig(ConfigWithRecordFileBase):
    """Provides an interface to an MappingTableModel"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._dataframe = None

    @staticmethod
    def config_filename():
        return "dimension_mapping.json5"

    @property
    def config_id(self):
        return self.model.mapping_id

    @staticmethod
    def model_class():
        return MappingTableModel

    def get_unique_from_ids(self):
        """Return the unique from IDs in an association table's records.

        Returns
        -------
        set
            set of str

        """
        return {x.from_id for x in self.model.records}

    def get_unique_to_ids(self):
        """Return the unique to IDs in an association table's records.

        Returns
        -------
        set
            set of str

        """
        return {x.to_id for x in self.model.records}
