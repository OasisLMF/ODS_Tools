import numpy as np
import numba as nb
import os

oasis_int = np.dtype(os.environ.get('OASIS_INT', 'i4'))
oasis_float = np.dtype(os.environ.get('OASIS_FLOAT', 'f4'))
nb_oasis_int = nb.from_dtype(oasis_int)

DEFAULT_RANDOM_SEED = 8762

DEFAULT_CONFIG = {
    "group_fill_perspective": False,
    "group_event_set_fields": [
        "event_set_id",
        "event_set_description",
        "event_occurrence_id",
        "event_occurrence_description",
        "event_occurrence_max_periods",
        "model_supplier_id",
        "model_name_id",
        "model_description",
        "model_version"
    ],
    "group_period_seed": 2479,
    "group_mean": False,
    "group_secondary_uncertainty": False
}

GEPT_OUTPUT = [
    ('groupset_id', oasis_int, '%d'),
    ('SummaryId', oasis_int, '%d'),
    ('EPCalc', oasis_int, '%d'),
    ('EPType', oasis_int, '%d'),
    ('ReturnPeriod', oasis_float, '%.6f'),
    ('Loss', oasis_float, '%.6f'),
]

GEPT_dtype = {c[0]: c[1] for c in GEPT_OUTPUT}
GEPT_headers = [c[0] for c in GEPT_OUTPUT]


GALT_OUTPUT = [
    ('groupset_id', oasis_int, '%d'),
    ('SummaryId', oasis_int, '%d'),
    ('LossType', oasis_int, '%d'),
    ('MeanLoss', oasis_float, '%.6f'),
    ('SDLoss', oasis_float, '%.6f'),
]

GALT_dtype = {c[0]: c[1] for c in GALT_OUTPUT}
GALT_headers = [c[0] for c in GALT_OUTPUT]
