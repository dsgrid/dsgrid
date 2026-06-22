from enum import Enum
from typing import Optional, Union

import pytest

from dsgrid.ibis.models import get_type_from_union
from dsgrid.time.types import DayType


def test_get_type_from_union():
    assert get_type_from_union(Optional[str]) is str
    assert get_type_from_union(Optional[DayType]) is str


def test_get_type_from_union_invalid():
    with pytest.raises(NotImplementedError, match="Unhandled Union type"):
        get_type_from_union(Union[str, int, None])


def test_get_type_from_enum_union():
    class Example(Enum):
        ONE = "one"

    assert get_type_from_union(Optional[Example]) is str
