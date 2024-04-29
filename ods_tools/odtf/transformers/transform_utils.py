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
