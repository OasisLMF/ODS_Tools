import numpy as np
import numba as nb
import os
import pyarrow as pa
import re

oasis_int = np.dtype(os.environ.get('OASIS_INT', 'i4'))
oasis_float = np.dtype(os.environ.get('OASIS_FLOAT', 'f4'))
nb_oasis_int = nb.from_dtype(oasis_int)


def pa_type_from_format(dtype, fmt):
    '''
    Convert dtype and format string to pyarrow schema field
    Mapping rules:
      %[+ ]?[diu]        → int32/int64/uint32/uint64 matching numpy dtype
      %[+ ]?[0.]?[.X]?f → decimal128(16+X, X) for exact X-decimal output
      anything else      → decimal128(22, 6)   (fallback, equivalent to %.6f)
    '''
    if re.fullmatch(r'%[+ ]?[diu]', fmt):
        if dtype.kind == 'i':
            pa_t = pa.int32() if dtype.itemsize <= 4 else pa.int64()
        elif dtype.kind == 'u':
            pa_t = pa.uint32() if dtype.itemsize <= 4 else pa.uint64()
        else:
            pa_t = pa.float64()
    else:
        m = re.search(r'\.(\d+)l?f', fmt)
        prec = int(m.group(1)) if m else 6
        pa_t = pa.decimal128(16 + prec, prec)
    return pa_t


def schema_from_output_list(output_list):
    fields = []
    for name, dtype, fmt in output_list:
        pa_dtype = pa_type_from_format(np.dtype(dtype), fmt)
        fields.append(pa.field(name, pa_dtype))
    return pa.schema(fields)


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
GEPT_schema = schema_from_output_list(GEPT_OUTPUT)


GALT_OUTPUT = [
    ('groupset_id', oasis_int, '%d'),
    ('SummaryId', oasis_int, '%d'),
    ('LossType', oasis_int, '%d'),
    ('MeanLoss', oasis_float, '%.6f'),
    ('SDLoss', oasis_float, '%.6f'),
]

GALT_dtype = {c[0]: c[1] for c in GALT_OUTPUT}
GALT_headers = [c[0] for c in GALT_OUTPUT]
GALT_schema = schema_from_output_list(GALT_OUTPUT)

GPQT_OUTPUT = [
    ('GroupPeriod', oasis_int, '%d'),
    ('groupeventset_id', 'category', '%d'),
    ('outputset_id', 'category', '%d'),
    ('EventId', oasis_int, '%d'),
    ('Quantile', oasis_float, '%.6f'),
]
GPQT_dtype = {c[0]: c[1] for c in GPQT_OUTPUT}
GPQT_headers = [c[0] for c in GPQT_OUTPUT]

GPLT_OUTPUT = [
    ('groupset_id', oasis_int, '%d'),
    ('groupeventset_id', oasis_int, '%d'),
    ('GroupPeriod', oasis_int, '%d'),
    ('SummaryId', oasis_int, '%d'),
    ('EventId', oasis_int, '%d'),
    ('LossType', oasis_int, '%d'),
    ('Loss', oasis_float, '%.6f'),
]

GPLT_dtype = {c[0]: c[1] for c in GPLT_OUTPUT}
GPLT_headers = [c[0] for c in GPLT_OUTPUT]
GPLT_schema = schema_from_output_list(GPLT_OUTPUT)
