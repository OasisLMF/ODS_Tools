"""An unavailable backend_dtype falls back to pd_dtype, and the warning names the one that was asked for."""
import logging

import pandas as pd

from ods_tools.oed import OedExposure


def test_unavailable_backend_dtype_warning_names_requested_dtype(caplog):
    location = pd.DataFrame({'PortNumber': ['1'], 'AccNumber': ['1'], 'LocNumber': ['1'], 'CountryCode': ['GB'],
                             'LocPerilsCovered': ['WTC'], 'BuildingTIV': [1000.0], 'LocCurrency': ['GBP']})
    with caplog.at_level(logging.WARNING):
        exposure = OedExposure(location=location, backend_dtype='no_such_dtype')
    assert exposure.backend_dtype == 'pd_dtype'
    assert 'backend_dtype no_such_dtype is not available' in caplog.text
    assert 'falling back to pd_dtype' in caplog.text
