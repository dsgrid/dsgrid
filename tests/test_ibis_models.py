from enum import Enum
from typing import Optional, Union

import pytest

from dsgrid.ibis.models import get_type_from_union
from dsgrid.time.types import DayType


class Example(Enum):
    ONE = "one"


def test_get_type_from_union():
    assert get_type_from_union(Optional[str]) is str
    assert get_type_from_union(Optional[DayType]) is str


def test_get_type_from_union_invalid():
    with pytest.raises(NotImplementedError, match="Unhandled Union type"):
        get_type_from_union(Union[str, int, None])


def test_get_type_from_enum_union():
    assert get_type_from_union(Optional[Example]) is str


def test_get_type_from_union_ignores_argument_order():
    """Python keeps NoneType wherever the caller wrote it, so order must not matter.

    ``Optional[Example]`` yields ``(Example, NoneType)`` but ``None | Example``
    yields ``(NoneType, Example)``. Both name the same type.
    """
    assert get_type_from_union(None | Example) is str
    assert get_type_from_union(Union[None, Example]) is str
    assert get_type_from_union(Union[None, str]) is str


def test_get_type_from_union_rejects_enum_with_another_type():
    """An Enum alongside a second non-None type is ambiguous, not an enum union."""
    with pytest.raises(NotImplementedError, match="Unhandled Union type"):
        get_type_from_union(Union[Example, int])


def test_get_type_from_union_no_non_none_types():
    with pytest.raises(NotImplementedError, match="Unhandled Union type"):
        get_type_from_union(Union[None])
