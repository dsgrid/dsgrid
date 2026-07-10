"""Filter-operator definitions for dsgrid dimension filters.

This module is the single source of truth for the column-filter operator
vocabulary that may appear in a dimension filter's ``operator`` field. Every
per-operator representation (the Ibis expression it builds, the shape of
value it expects, its family bucket) lives on a :class:`FilterOperatorSpec`,
and the operator set, the pydantic validator, and the dispatch all derive
from :data:`FILTER_OPERATOR_SPECS`.
"""

from dataclasses import dataclass
from typing import Any, Callable

import ibis

from dsgrid.exceptions import DSGInvalidField


@dataclass(frozen=True)
class FilterOperatorSpec:
    """A column-filter operator that dsgrid users may name in a dimension filter.

    Parameters
    ----------
    name
        The user-facing token written in ``operator`` (e.g. ``"startswith"``).
    family
        Coarse bucket: ``"string"``, ``"null"``, ``"membership"``, or
        ``"range"``.
    value_kind
        Expected shape of ``value``: ``"scalar"`` (one value), ``"none"``
        (value ignored), ``"list"`` (list/tuple/set of values), or
        ``"bounds"`` (two-element lower/upper pair).
    apply
        Builds the boolean Ibis expression from ``(column, checked_value)``.
    """

    name: str
    family: str
    value_kind: str
    apply: Callable[[Any, Any], Any]


FILTER_OPERATOR_SPECS: tuple[FilterOperatorSpec, ...] = (
    FilterOperatorSpec("contains", "string", "scalar", lambda col, v: col.contains(v)),
    FilterOperatorSpec("startswith", "string", "scalar", lambda col, v: col.startswith(v)),
    FilterOperatorSpec("endswith", "string", "scalar", lambda col, v: col.endswith(v)),
    FilterOperatorSpec("like", "string", "scalar", lambda col, v: col.like(v)),
    FilterOperatorSpec("rlike", "string", "scalar", lambda col, v: col.re_search(v)),
    FilterOperatorSpec("isNull", "null", "none", lambda col, v: col.isnull()),
    FilterOperatorSpec("isNotNull", "null", "none", lambda col, v: col.notnull()),
    FilterOperatorSpec("isin", "membership", "list", lambda col, v: col.isin(list(v))),
    FilterOperatorSpec("between", "range", "bounds", lambda col, v: col.between(v[0], v[1])),
)


_BY_NAME: dict[str, FilterOperatorSpec] = {spec.name: spec for spec in FILTER_OPERATOR_SPECS}

FILTER_OPERATOR_NAMES: frozenset[str] = frozenset(_BY_NAME)


def spec_for_filter_operator(name: str) -> FilterOperatorSpec:
    """Look up a :class:`FilterOperatorSpec` by user-facing name.

    Raises
    ------
    DSGInvalidField
        If ``name`` is not a supported filter operator.
    """
    spec = _BY_NAME.get(name)
    if spec is None:
        msg = f"operator={name} is not supported. Allowed={sorted(FILTER_OPERATOR_NAMES)}"
        raise DSGInvalidField(msg)
    return spec


def check_filter_value(spec: FilterOperatorSpec, value: Any) -> Any:
    """Validate ``value`` against the shape ``spec`` expects and return it.

    Raises
    ------
    DSGInvalidField
        If ``value`` does not have the shape the operator requires.
    """
    match spec.value_kind:
        case "scalar":
            return value
        case "none":
            return None
        case "list":
            if not isinstance(value, list | tuple | set):
                msg = f"value must be a list, tuple, or set for operator={spec.name!r}"
                raise DSGInvalidField(msg)
            return value
        case "bounds":
            if not isinstance(value, list | tuple) or len(value) != 2:
                msg = (
                    "value must be a two-element list or tuple of (lower, upper) for "
                    f"operator={spec.name!r}"
                )
                raise DSGInvalidField(msg)
            return value
        case _:
            msg = f"Bug: unhandled value_kind={spec.value_kind!r} for operator={spec.name!r}"
            raise NotImplementedError(msg)


def apply_filter_operator(
    df: ibis.Table, column: str, operator: str, value: Any, negate: bool
) -> ibis.Table:
    """Filter ``df`` where ``column`` matches ``operator`` applied to ``value``.

    Parameters
    ----------
    df : ibis.Table
    column : str
        Name of the column to filter on.
    operator : str
        A name in :data:`FILTER_OPERATOR_NAMES`.
    value : Any
        The operator's operand, in the shape its ``value_kind`` requires.
    negate : bool
        If True, keep the rows the operator would reject.

    Raises
    ------
    DSGInvalidField
        If ``operator`` is not supported or ``value`` has the wrong shape.
    """
    spec = spec_for_filter_operator(operator)
    checked = check_filter_value(spec, value)
    expr = spec.apply(df[column], checked)
    if negate:
        expr = ~expr
    return df.filter(expr)
