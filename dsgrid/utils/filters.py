from dsgrid.exceptions import DSGInvalidParameter
import numpy as np

accepted_ops = ["==", "!=", "contains", "not contains"]


def transform_and_validate_filters(filter_expressions):
    """
    Validate whether an operation exists, return tranformed/formatted filters;
    filter_expressions:
       - (str, set) combined input from flag -f or --filter
       - each expression takes the format 'field operation value'
       - 'field' and 'value' are case-insenstive and can accept spaces since expression is split by 'operation'
    """
    transformed_filters = []
    for expr_str in filter_expressions:
        check_ops = []
        op = None
        for opp in accepted_ops:
            check_ops.append(opp in expr_str)
            if opp in expr_str:
                op = opp

        if np.sum(check_ops) != 1:
            raise DSGInvalidParameter(f"invalid operation detected, valid ops: {accepted_ops}")

        fields = [x.rstrip().lstrip() for x in expr_str.split(op)]

        if len(fields) < 2:
            raise DSGInvalidParameter(
                "filter expression: '%s' contains too few arguments, must be in the format 'field operation value' "
                "(ex: 'Submitter == my_username')" % expr_str
            )
        elif len(fields) > 2:
            raise DSGInvalidParameter(
                "filter expression: '%s' contains too many arguments, must be in the format 'field operation value', "
                "(ex: 'Submitter == my_username')" % expr_str
            )

        field = fields[0].title()  # to accept case-insensitive comparison
        field = "ID" if field == "Id" else field
        value = fields[1].lower()  # to accept case-insensitive comparison
        transformed_filters.append([field, op, value])

    return transformed_filters


def matches_filters(row, field_to_index, transformed_filters):
    """
    Validate field name in transformed filter_expressions, return rows matching all filters

    row: row in `list` registry table (manager.show())
    field_to_index: (dict) key = column names, val = column index, in registry table (or manager.show())
    transformed_filters: (list) transformed/formatted fields for filtering rows
    """

    for tfilter in transformed_filters:
        [field, op, value] = tfilter
        if field not in field_to_index:
            raise DSGInvalidParameter(
                f"field='{field}' is not a valid column name, valid fields: {list(field_to_index.keys())}"
            )
        obj_val = row[field_to_index[field]].lower()  # to accept case-insensitive comparison
        if not matches_filter(val=obj_val, op=op, required_value=value):
            return False
    return True


def matches_filter(val, op, required_value):
    """
    val:

    """
    if op == "==":
        return val == required_value
    elif op == "contains":
        return required_value in val
    elif op == "not contains":
        return required_value not in val
    elif op == "!=":
        return val != required_value
