"""Pydantic-model <-> Ibis-table conversion helpers.

Reflects pydantic model fields into a PySpark-style schema and builds an
Ibis table from a list of model instances. Lives in its own module so
the reflection logic (which is intentionally a small, fragile surface)
can grow tests without touching the larger runtime-session file.
"""

import enum
from types import UnionType
from typing import Any, Type, Union, cast, get_args, get_origin

import ibis

from dsgrid.data_models import DSGBaseModel
from dsgrid.ibis.backend import get_runtime_backend
from dsgrid.ibis.session import (
    PYTHON_TO_SPARK_TYPES,
    StructField,
    StructType,
    get_runtime_session,
)
from dsgrid.utils.timing import timer_stats_collector, track_timing


@track_timing(timer_stats_collector)
def models_to_dataframe(models: list[DSGBaseModel], table_name: str | None = None) -> ibis.Table:
    """Converts a list of Pydantic models to a table.

    Parameters
    ----------
    models : list
    table_name : str | None
        If set, a unique ID to use as the cached table name. Return from cache if already stored.
    """
    session = get_runtime_session()
    if table_name is not None and get_runtime_backend().has_table(table_name):
        return get_runtime_backend().table(table_name)

    assert models
    cls = type(models[0])
    rows = []
    struct_fields: list[Any] = []
    for i, model in enumerate(models):
        dct = {}
        for f in cls.model_fields:
            val = getattr(model, f)
            if isinstance(val, enum.Enum):
                val = val.value
            if i == 0:
                if val is None:
                    python_type = cls.model_fields[f].annotation
                    origin = get_origin(python_type)
                    if origin is Union or origin is UnionType:
                        python_type = get_type_from_union(python_type)
                        # else: will likely fail below
                        # Need to add more logic to detect the actual type or add to
                        # PYTHON_TO_SPARK_TYPES.
                else:
                    python_type = type(val)
                python_type = cast(type[Any], python_type)
                spark_type = PYTHON_TO_SPARK_TYPES[python_type]()
                struct_fields.append(StructField(f, spark_type, nullable=True))
            dct[f] = val
        rows.append(tuple(dct.values()))

    schema: Any = StructType(struct_fields)
    df = session.createDataFrame(rows, schema=schema)

    if table_name is not None:
        get_runtime_backend().create_view(table_name, df)

    return df


def get_type_from_union(python_type) -> Type:
    """Return the Python type from a Union.

    Only works if it is Union of NoneType and something.

    Raises
    ------
    NotImplementedError
        Raised if the code does know how to determine the type.
    """
    args = get_args(python_type)
    if issubclass(args[0], enum.Enum):
        python_type = type(next(iter(args[0])).value)
    else:
        types = [x for x in args if not issubclass(x, type(None))]
        if not types:
            msg = f"Unhandled Union type: {python_type=} {args=}"
            raise NotImplementedError(msg)
        elif len(types) > 1:
            msg = f"Unhandled Union type: {types=}"
            raise NotImplementedError(msg)
        else:
            python_type = types[0]

    return python_type
