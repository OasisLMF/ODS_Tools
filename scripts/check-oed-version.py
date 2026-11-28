#!/usr/bin/env python3
"""Check the default OED schema version (OED_VERSION in ods_tools/oed/oed_schema.py) before a release.

ods-tools X.y.z should load OED X.*.* by default (ODS_Tools#275). The check fails when:
- OED_VERSION's major version differs from the release tag's;
- OED_VERSION is not a published ODS_OpenExposureData release, so the wheel would not contain it;
- a newer OED release with the same major version exists, so OED_VERSION has fallen behind.

Usage: scripts/check-oed-version.py RELEASE_TAG
Set GITHUB_TOKEN to avoid the GitHub API's anonymous rate limit.
"""
import json
import os
import re
import sys
import urllib.request
from pathlib import Path

OED_REPO = 'OasisLMF/ODS_OpenExposureData'
SCHEMA_MODULE = Path(__file__).resolve().parent.parent / 'ods_tools' / 'oed' / 'oed_schema.py'
VERSION_RE = re.compile(r'^(\d+)\.(\d+)\.(\d+)$')


def read_oed_version(path=SCHEMA_MODULE):
    match = re.search(r"^OED_VERSION\s*=\s*['\"]([^'\"]+)['\"]", path.read_text(), re.MULTILINE)
    if not match:
        raise SystemExit(f"OED_VERSION not found in {path}")
    return match.group(1)


def oed_releases():
    """Version tuples of the published (not pre-release) OED releases."""
    releases, page = [], 1
    while True:
        req = urllib.request.Request(f'https://api.github.com/repos/{OED_REPO}/releases?per_page=100&page={page}')
        if os.environ.get('GITHUB_TOKEN'):
            req.add_header('Authorization', f"token {os.environ['GITHUB_TOKEN']}")
        with urllib.request.urlopen(req) as response:
            data = json.load(response)
        if not data:
            return releases
        releases += [as_tuple(r['tag_name']) for r in data
                     if not r['prerelease'] and not r['draft'] and VERSION_RE.match(r['tag_name'].lstrip('v'))]
        page += 1


def as_tuple(version):
    return tuple(int(part) for part in VERSION_RE.match(version.lstrip('v')).groups())


def check(release_tag, oed_version, releases):
    """Return a list of problems, empty if OED_VERSION is right for this release."""
    release_match = re.match(r'^v?(\d+)\.\d+\.\d+(rc\d+)?$', release_tag)
    if not release_match:
        return [f"release tag '{release_tag}' is not a version"]
    if not VERSION_RE.match(oed_version):
        return [f"OED_VERSION '{oed_version}' is not a version"]

    release_major, oed = int(release_match.group(1)), as_tuple(oed_version)
    problems = []
    if oed[0] != release_major:
        problems.append(f"OED_VERSION {oed_version} has major version {oed[0]}, but ods-tools {release_tag} should "
                        f"default to OED {release_major}.x.x")
    if oed not in releases:
        problems.append(f"OED_VERSION {oed_version} is not a published {OED_REPO} release, so the package won't contain it")
    newer = sorted(r for r in releases if r[0] == oed[0] and r > oed)
    if newer:
        problems.append(f"OED {'.'.join(map(str, newer[-1]))} has been released, set OED_VERSION to it")
    return problems


def main(argv):
    if len(argv) != 2:
        raise SystemExit(__doc__)
    oed_version = read_oed_version()
    problems = check(argv[1], oed_version, oed_releases())
    for problem in problems:
        print(f"ERROR: {problem} (ods_tools/oed/oed_schema.py)")
    if not problems:
        print(f"OED_VERSION {oed_version} is the latest OED release for ods-tools {argv[1]}")
    return 1 if problems else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
