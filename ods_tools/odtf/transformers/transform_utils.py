import logging
from typing import Any

import pandas as pd

logger = logging.getLogger(__name__)
RowType = Any


def replace_multiple(row: RowType, first_column, second_column, *triplets):
    """
    Transform `target` values using additional `context` values
    and a list of (from, via, to) mappings.

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
