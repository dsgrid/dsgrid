import csv
import logging
import os
import shutil
from pathlib import Path
from typing import Dict, List, Optional, Union

from pydantic import Field, validator
from pyspark.sql import DataFrame, Row, SparkSession

from .config_base import ConfigWithDataFilesBase
from .dimension_mapping_base import DimensionMappingBaseModel
from dsgrid.data_models import serialize_model_data, DSGBaseModel
from dsgrid.exceptions import DSGInvalidOperation
from dsgrid.utils.files import compute_file_hash, dump_data


logger = logging.getLogger(__name__)


class AssociationTableRecordModel(DSGBaseModel):
    """Represents one record in association table record files. Maps one dimension to another."""

    from_id: str = Field(
        title="from_id",
        description="Source mapping",
    )
    to_id: str = Field(
        title="to_id",
        description="Destination mapping",
    )


class AssociationTableModel(DimensionMappingBaseModel):
    """Attributes for an association table"""

    filename: str = Field(
        title="filename",
        alias="file",
        description="filename containing association table records",
    )
    file_hash: Optional[str] = Field(
        title="file_hash",
        description="hash of the contents of the file, computed by dsgrid",
        dsg_internal=True,
    )
    records: Optional[List] = Field(
        title="records",
        description="dimension mapping records in filename that get loaded at runtime",
        dsg_internal=True,
        default=[],
    )

    @validator("filename")
    def check_filename(cls, filename):
        """Validate record file"""
        if not os.path.exists(filename):
            raise ValueError(f"{filename} does not exist")
        return filename

    @validator("file_hash")
    def compute_file_hash(cls, file_hash, values):
        """Compute file hash."""
        return file_hash or compute_file_hash(values["filename"])

    @validator("records", always=True)
    def add_records(cls, records, values):
        """Add records from the file."""
        if records:
            raise ValueError("records should not be defined in the dimension mapping config")

        records = []
        filename = Path(values["filename"])
        if filename.name.endswith(".csv"):
            with open(filename) as f_in:
                reader = csv.DictReader(f_in)
                for row in reader:
                    record = AssociationTableRecordModel(**row)
                    records.append(record)
        else:
            raise ValueError(f"only CSV is supported: {filename}")

        return records

    def dict(self, by_alias=True, exclude=None):
        if exclude is None:
            exclude = set()
        exclude.add("records")
        data = super().dict(by_alias=by_alias, exclude=exclude)
        return serialize_model_data(data)


class AssociationTableConfig(ConfigWithDataFilesBase):
    """Provides an interface to an AssociationTableModel"""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._dataframe = None

    @staticmethod
    def config_filename():
        return "dimension_mapping.toml"

    @property
    def config_id(self):
        return self.model.mapping_id

    @staticmethod
    def model_class():
        return AssociationTableModel

    def get_unique_from_ids(self):
        """Return the unique from IDs in a dimension's records.

        Returns
        -------
        set
            set of str

        """
        return {x.from_id for x in self.model.records}

    def get_unique_to_ids(self):
        """Return the unique to IDs in a dimension's records.

        Returns
        -------
        set
            set of str

        """
        return {x.to_id for x in self.model.records}
