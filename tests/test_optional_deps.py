"""
Tests for the optional dependency handling (ods-tools[combine]) and the combine settings schema
"""
import json
import logging
import sys
import tempfile
from pathlib import Path
from unittest import mock

import pytest

# WORKAROUND - this line to to force an import from the installed ods_tools package
# from `python3.xx/site-packages/ods_tools` instead of the local module
sys.path.append(sys.path.pop(0))

import ods_tools.oed as oed_package
from ods_tools.combine.settings import CombineSettingsSchema
from ods_tools.main import combine as main_combine
from ods_tools.oed import OedSchema, OdsException
from ods_tools.oed import oed_schema

example_path = Path(Path(__file__).parent.parent, "ods_tools", "combine", "examples")


@pytest.fixture
def numba_not_installed(monkeypatch):
    """make numba (and anything importing it) unimportable"""
    monkeypatch.delattr(oed_package, 'peril_jit', raising=False)
    with mock.patch.dict(sys.modules, {'numba': None, 'ods_tools.oed.peril_jit': None}):
        yield


def test_combine_settings__valid_config():
    is_valid, errors = CombineSettingsSchema().validate({'group_number_of_periods': 1000})
    assert is_valid
    assert errors == {}


def test_combine_settings__invalid_config_raises():
    with pytest.raises(OdsException, match='group_number_of_periods'):
        CombineSettingsSchema().validate({'group_number_of_periods': 'not_an_int'})


def test_combine_settings__invalid_config_no_raise():
    config = {'group_number_of_periods': 'not_an_int', 'group_period_seed': 'not_an_int'}
    is_valid, errors = CombineSettingsSchema().validate(config, raise_error=False)
    assert not is_valid
    assert set(errors) == {'group_number_of_periods', 'group_period_seed'}


def test_combine_settings__missing_required_field():
    is_valid, errors = CombineSettingsSchema().validate({}, raise_error=False)
    assert not is_valid
    assert errors == {'required': ["'group_number_of_periods' is a required property"]}


def test_main_combine__runs():
    analysis_dirs = [str(example_path / 'inputs' / i) for i in ['1', '2']]
    with tempfile.TemporaryDirectory() as tmp_dir:
        config_file = Path(tmp_dir, 'config.json')
        config_file.write_text(json.dumps({'group_number_of_periods': 100, 'group_mean': True}))
        output_dir = Path(tmp_dir, 'output')
        output_dir.mkdir()

        main_combine(config_file=str(config_file), analysis_dirs=analysis_dirs, output_dir=str(output_dir))

        assert any(output_dir.iterdir())


def test_main_combine__missing_requirements(caplog):
    with mock.patch.dict(sys.modules, {'ods_tools.combine.combine': None}):
        with caplog.at_level(logging.ERROR):
            main_combine(config_file='unused.json', analysis_dirs=[], output_dir='unused')

    assert "pip install ods-tools[combine]" in caplog.text


def test_oed_schema__peril_jit_names_forwarded():
    from ods_tools.oed import peril_jit
    for name in oed_schema._PERIL_JIT_NAMES:
        assert getattr(oed_schema, name) is getattr(peril_jit, name)


def test_oed_schema__unknown_attribute():
    with pytest.raises(AttributeError):
        oed_schema.not_a_real_attribute


def test_oed_schema__nb_perils_dict():
    schema = OedSchema.from_oed_schema_info(None)
    perils_dict = schema.nb_perils_dict
    assert set(perils_dict.keys()) == set(schema.schema['perils']['covered'])


def test_oed_schema__numba_not_installed(numba_not_installed):
    with pytest.raises(OdsException, match="pip install ods-tools\\[combine\\]"):
        oed_schema.jit_peril_filtering
