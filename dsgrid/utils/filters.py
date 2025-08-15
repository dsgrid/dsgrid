import numpy as np
from dsgrid.exceptions import DSGInvalidParameter

ACCEPTED_OPS = ["==", "!=", "contains", "not contains"]


def transform_and_validate_filters(filter_expressions):
    """
    Validate whether an operation exists, return tranformed/formatted filters

    Parameters
    ------------
    filter_expressions : set(str)
       - each expression takes the format 'field operation value'
       - 'field' and 'value' are case-insenstive and can accept spaces since expression is split by 'operation'

    Returns
    --------
    transformed_filters : list
        list of validated and transformed filtering fields
    """

    transformed_filters = []
    for expr_str in filter_expressions:
        check_ops = []
        op = None
        for opp in ACCEPTED_OPS:
            check_ops.append(opp in expr_str)
            if opp in expr_str:
                op = opp

        if np.sum(check_ops) < 1:
            msg = f"invalid operation detected, valid ops: {ACCEPTED_OPS}"
            raise DSGInvalidParameter(msg)
        elif np.sum(check_ops) > 2:
            msg = f"too many operations detected, choose one of the valid ops: {ACCEPTED_OPS}"
            raise DSGInvalidParameter(msg)

        fields = [x.strip() for x in expr_str.split(op) if x != ""]

        if len(fields) < 2:
            msg = (
                f"filter expression: '{expr_str}' contains too few arguments, must be in the format 'field operation value' "
                "(ex: 'Submitter == username')"
            )
            raise DSGInvalidParameter(msg)
        elif len(fields) > 2:
            msg = (
                f"filter expression: '{expr_str}' contains too many arguments, must be in the format 'field operation value', "
                "(ex: 'Submitter == username')"
            )
            raise DSGInvalidParameter(msg)

        field = fields[0]
        value = fields[1]
        transformed_filters.append(
            [field.lower(), op, value.lower()]
        )  # to accept case-insensitive comparison

    return transformed_filters


def matches_filters(row, field_to_index, transformed_filters):
    """
    Validate field name in transformed filter_expressions, return TRUE for rows matching all filters

    Parameters
    ------------
    row : str
        row in `list` registry table (manager.show())
    field_to_index : dict
        key = column names, val = column index, in registry table (or manager.show())
    transformed_filters : list
        transformed/formatted fields for filtering rows

    Returns
    --------
    bool
        return TRUE for rows matching all filters
    """

    field_to_index_lower = dict(
        (k.lower(), v) for k, v in field_to_index.items()
    )  # to accept case-insensitive comparison

    for tfilter in transformed_filters:
        [field, op, value] = tfilter
        if field not in field_to_index_lower:
            msg = f"field='{field}' is not a valid column name, valid fields: {list(field_to_index.keys())}"
            raise DSGInvalidParameter(msg)
        obj_val = row[field_to_index_lower[field]].lower()  # to accept case-insensitive comparison
        if not matches_filter(val=obj_val, op=op, required_value=value):
            return False
    return True


def matches_filter(val, op, required_value):
    """
    check if table content matches filtered value

    Parameters
    ------------
    val : str
        value from registry table to be compared
    op : str
        filtering operation to be performed
    required_value : str
        value to match from filter_expression

    Returns
    --------
    bool
        return TRUE if vtable content matches filtered value, FALSE otherwise
    """

    if op == "==":
        return val == required_value
    elif op == "!=":
        return val != required_value
    elif op == "contains":
        return required_value in val
    elif op == "not contains":
        return required_value not in val
    else:
        assert False, op
