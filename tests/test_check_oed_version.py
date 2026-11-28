"""Tests for scripts/check-oed-version.py, the release check on OED_VERSION (ODS_Tools#275)."""
import importlib.util
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parent.parent / 'scripts' / 'check-oed-version.py'
spec = importlib.util.spec_from_file_location('check_oed_version', SCRIPT)
check_oed_version = importlib.util.module_from_spec(spec)
spec.loader.exec_module(check_oed_version)
check = check_oed_version.check

RELEASES = [(3, 4, 1), (4, 0, 0), (5, 0, 0)]


@pytest.mark.parametrize('release_tag', ['5.0.11', '5.1.0rc1', 'v5.0.0'])
def test_latest_matching_major_passes(release_tag):
    assert check(release_tag, '5.0.0', RELEASES) == []


def test_major_mismatch_fails():
    problems = check('6.0.0', '5.0.0', RELEASES)
    assert len(problems) == 1
    assert 'should default to OED 6.x.x' in problems[0]


def test_unreleased_oed_version_fails():
    problems = check('5.0.11', '5.0.1', RELEASES)
    assert any('not a published' in p for p in problems)


def test_newer_oed_release_with_same_major_fails():
    problems = check('5.0.11', '5.0.0', RELEASES + [(5, 1, 0)])
    assert problems == ['OED 5.1.0 has been released, set OED_VERSION to it']


def test_newer_release_of_another_major_is_ignored():
    assert check('5.0.11', '5.0.0', RELEASES + [(6, 0, 0)]) == []


@pytest.mark.parametrize('release_tag, oed_version', [('latest', '5.0.0'), ('5.0.11', 'five')])
def test_bad_versions_fail(release_tag, oed_version):
    assert check(release_tag, oed_version, RELEASES) != []


def test_reads_oed_version_from_package():
    from ods_tools.oed.oed_schema import OED_VERSION
    assert check_oed_version.read_oed_version() == OED_VERSION
