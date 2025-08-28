from typing import Self
from pydantic import Field, field_validator, model_validator

from dsgrid.data_models import DSGBaseModel
from dsgrid.dimension.base_models import DimensionType
from dsgrid.utils.utilities import check_uniqueness


SUPPORTED_CSV_TYPES = set(
    (
        "BOOLEAN",
        "INT",
        "INTEGER",
        "TINYINT",
        "SMALLINT",
        "BIGINT",
        "FLOAT",
        "DOUBLE",
        "TIMESTAMP_TZ",
        "TIMESTAMP_NTZ",
        "STRING",
        "TEXT",
        "VARCHAR",
    )
)


class Column(DSGBaseModel):
    name: str = Field(description="Name of the column")
    dimension_type: DimensionType | None = Field(
        default=None,
        description="Dimension represented by the data in the column. Optional if this is a "
        "time column or pivoted column. Required if the column represents a stacked dimension "
        "but an alternate name is being used, such as 'county' instead of 'geography'. "
        "dsgrid will rename any column that is set at runtime.",
    )


class CsvColumn(Column):
    data_type: str | None = Field(
        description="Type of the data in the column. If None, infer the type."
    )

    @field_validator("data_type")
    @classmethod
    def check_data_type(cls, data_type: str | None) -> str | None:
        if data_type is None:
            return None

        type_upper = data_type.upper()
        if type_upper not in SUPPORTED_CSV_TYPES:
            supported_data_types = sorted(SUPPORTED_CSV_TYPES)
            msg = f"{data_type=} is not one of {supported_data_types=}"
            raise ValueError(msg)
        return type_upper


class CsvSchema(DSGBaseModel):
    """Defines the format of a CSV data file."""

    columns: list[CsvColumn]

    @model_validator(mode="after")
    def check_consistency(self) -> Self:
        if len(self.columns) < 1:
            return self
        has_data_type = self.columns[0].data_type is not None
        for column in self.columns[1:]:
            has_data_type_ = column.data_type is not None
            if has_data_type_ != has_data_type:
                msg = (
                    "All columns must define data_type or no columns can can define "
                    f"data_type: {self.columns}"
                )
                raise ValueError(msg)
        check_uniqueness([(x.name for x in self.columns)], "column names")
        return self

    def defines_data_types(self) -> bool:
        """Return True if the columns define data types."""
        return self.columns[0].data_type is not None

    def get_data_type_mapping(self) -> dict[str, str] | None:
        """Return the mapping of column to data type. Return None if data types are not defined."""
        if not self.defines_data_types():
            return None
        return {x.name: x.data_type for x in self.columns}  # type: ignore


class ParquetSchema(DSGBaseModel):
    """Defines the format of a Parquet data file."""

    columns: list[Column]

    @model_validator(mode="after")
    def check_consistency(self) -> Self:
        check_uniqueness([(x.name for x in self.columns)], "column names")
        return self


def get_column_renames(schema: CsvSchema | ParquetSchema) -> dict[str, str]:
    """Return a mapping of columns to rename."""
    mapping: dict[str, str] = {}
    for column in schema.columns:
        if column.dimension_type is not None and column.name != column.dimension_type.value:
            mapping[column.name] = column.dimension_type.value
    return mapping
