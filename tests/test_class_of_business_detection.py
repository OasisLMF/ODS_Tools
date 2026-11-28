"""OedExposure.check when no class of business fits the exposure (#174).

Detection fails when every class of business has a required field missing or an n/a field present. That is a
required_fields error, so check() follows the required_fields on_error action instead of always raising.
"""
import logging

import pandas as pd
import pytest

from ods_tools.oed import OedExposure
from ods_tools.oed.common import ClassOfBusiness, OdsException
from ods_tools.oed.exposure import ClassOfBusinessNotDetected

LOCATION = {'PortNumber': ['1'], 'AccNumber': ['1'], 'LocNumber': ['1'], 'CountryCode': ['GB'],
            'LocPerilsCovered': ['WTC'], 'BuildingTIV': [1000.0], 'LocCurrency': ['GBP']}


def exposure(drop=(), **kwargs):
    location = pd.DataFrame({k: v for k, v in LOCATION.items() if k not in drop})
    return OedExposure(location=location, **kwargs)


def undetectable(**kwargs):
    # LocCurrency is required for property and marine cargo, LocNumber is n/a for cyber and liability
    return exposure(drop=['LocCurrency'], **kwargs)


def config(on_error):
    return [{'name': 'required_fields', 'on_error': on_error}]


@pytest.mark.parametrize('validation_config', [None, config('raise')])
def test_raises_when_required_fields_raise(validation_config):
    with pytest.raises(ClassOfBusinessNotDetected, match='LocCurrency missing'):
        undetectable(validation_config=validation_config).check()


def test_not_detected_is_an_ods_exception():
    with pytest.raises(OdsException):
        undetectable().check()


def test_log_warns_and_validates_as_property(caplog):
    exp = undetectable(validation_config=config('log'))
    with caplog.at_level(logging.WARNING):
        assert exp.check() == []
    assert 'Could not detect the class of business' in caplog.text
    assert 'missing required column LocCurrency' in caplog.text
    assert exp.class_of_business is None  # the fallback is not kept as the detected class of business


def test_return_gives_the_missing_field():
    invalid = undetectable(validation_config=config('return')).check()
    assert any('LocCurrency' in item['msg'] for item in invalid)


def test_check_argument_overrides_exposure_config():
    exp = undetectable(validation_config=config('raise'))
    assert exp.check(config('return')) != []


@pytest.mark.parametrize('validation_config', [config('ignore'), [{'name': 'valid_values', 'on_error': 'raise'}]])
def test_ignored_or_unlisted_required_fields_is_silent(validation_config, caplog):
    with caplog.at_level(logging.WARNING):
        assert undetectable(validation_config=validation_config).check() == []
    assert 'Could not detect the class of business' not in caplog.text


def test_detectable_exposure_unchanged():
    exp = exposure(validation_config=config('raise'))
    assert exp.check() == []
    assert exp.class_of_business == ClassOfBusiness.prop
