import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)
RowType = Any


def replace_multiple(row: RowType, target, source_sep, target_sep, *pattern_repl):
    """
    Transform location perils from source to target perils.

    Args:
        row (RowType):
        target (_type_): values to be transformed
        source_sep (_type_): delimeter in source values
        target_sep (_type_): delimeter in target values

    Returns:
        pd.Series: transformed values
    """
    if isinstance(target, pd.Series):
        perils = target.apply(lambda x: [p.strip() for p in str(x).split(source_sep.strip("'"))])
    else:
        return target

    # Create list of replacements
    pattern_repl_list = list(zip(pattern_repl[::2], pattern_repl[1::2]))

    def transform_peril(peril_list):
        result = []
        for peril in peril_list:
            for pattern, repl in pattern_repl_list:
                # Replace if our list of replacements contains the peril
                if peril == pattern.strip("'"):
                    result.append(repl.strip("'"))
                    break
            else:
                result.append(peril)
        return target_sep.strip("'").join(result)

    result = perils.apply(transform_peril)
    return result


def replace_double(row: RowType, first_column, second_column, *triplets):
    """
    Transform `target` values using two columns.

    Args:
        row (RowType): The row of data.
        first_column: A pd.Series column to be transformed.
        second_column: A pd.Series column to be transformed.

    Returns:
        pd.Series: Transformed values.
    """
    mappings = list(zip(triplets[::3], triplets[1::3], triplets[2::3]))

    if isinstance(first_column, pd.Series) and isinstance(second_column, pd.Series):
        def apply_mapping(val, ctx):
            for from_val, via_val, to_val in mappings:
                if val == from_val.strip("'") and ctx == via_val.strip("'"):
                    return to_val.strip("'")
            return val
        return pd.Series([
            apply_mapping(val, ctx)
            for val, ctx in zip(first_column, second_column)
        ], index=first_column.index)

    return first_column
