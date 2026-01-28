import pandas as pd
from dataclasses import asdict


def dataclass_list_to_dataframe(dataclass_list):
    return pd.DataFrame([asdict(c) for c in dataclass_list])


def hash_summary_level_fields(summary_level_fields):
    """
    Hash the summary level fields, ignoring ordering.
    """
    summary_level_fields = sorted(summary_level_fields)
    return str(summary_level_fields)
