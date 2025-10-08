import io
import os
import re
import setuptools
from urllib.error import HTTPError

import setuptools.command.install as orig

import urllib.request
import json


SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))

OED_VERSION = '4.0.0'
# ORD_VERSION =


def get_readme():
    with io.open(os.path.join(SCRIPT_DIR, 'README.md'), encoding='utf-8') as readme:
        return readme.read()


def get_version():
    """
    Return package version as listed in `__version__` in `init.py`.
    """
    with io.open(os.path.join(SCRIPT_DIR, 'ods_tools', '__init__.py'), encoding='utf-8') as init_py:
        return re.search('__version__ = [\'"]([^\'"]+)[\'"]', init_py.read()).group(1)


def get_install_requirements():
    with io.open(os.path.join(SCRIPT_DIR, 'requirements.in'), encoding='utf-8') as reqs:
        return reqs.readlines()


def get_extra_requirements():
    with io.open(os.path.join(SCRIPT_DIR, 'requirements-extra.in'), encoding='utf-8') as extrareqs:
        return extrareqs.readlines()


class DownloadSpecODS(orig.install):
    """A custom command to download a JSON ODS spec during installation.

        Example Install:
            pip install -v . --install-option="--local-oed-spec=<path>" .

    """
    description = 'Download a ODS JSON spec file from a release URL.'
    user_options = orig.install.user_options + [
        ('local-oed-spec=', None, 'Override to build package with extracted spec (filepath)'),
    ]

    def __init__(self, *args, **kwargs):
        self.filename = 'OpenExposureData_Spec.json'
        self.ods_repo = 'OasisLMF/ODS_OpenExposureData'
        self.oed_version = OED_VERSION
        self.url = f'https://github.com/{self.ods_repo}/releases/download/'
        orig.install.__init__(self, *args, **kwargs)

    def initialize_options(self):
        orig.install.initialize_options(self)
        self.local_oed_spec = None

    def finalize_options(self):
        print("Local OED Spec:", str(self.local_oed_spec))
        if self.local_oed_spec is not None:
            if not os.path.isfile(self.local_oed_spec):
                raise ValueError(f"Local OED Spec '{self.local_oed_spec}' not found")
        orig.install.finalize_options(self)

    def run(self):
        if self.local_oed_spec:
            # Install with local json spec
            print('OED Version: Local File')
            print(f'Install from path: {self.local_oed_spec}')
            with open(self.local_oed_spec, 'r') as f:
                data = json.load(f)
                data['version'] = f'Local-file-install: {self.local_oed_spec}'
        else:
            # Install all releases
            print(f'Install all versions from url: {self.url}')
            tags = self.get_all_tags()
            data = {}
            for tag in tags:
                try:
                    url = self.url + f"{tag}/{self.filename}"
                    response = urllib.request.urlopen(url)
                    data[tag] = json.loads(response.read())
                    data[tag]['version'] = tag
                except HTTPError:
                    print(f'No OED associated with {tag}: {url}')

        download_path = os.path.join(self.build_lib, 'ods_tools', 'data', self.filename)
        with open(download_path, 'w+') as f:
            json.dump(data, f)
        orig.install.run(self)

    def get_all_tags(self):
        """Fetch all release tags from GitHub API."""
        tags = []
        page = 1
        while True:
            api_url = f"https://api.github.com/repos/{self.ods_repo}/tags?per_page=100&page={page}"
            with urllib.request.urlopen(api_url) as response:
                data = json.load(response)
            if not data:
                break
            tags.extend([t['name'] for t in data if 'rc' not in t['name']])
            page += 1
        return tags


version = get_version()
readme = get_readme()
reqs = get_install_requirements()
extra_reqs = get_extra_requirements()

setuptools.setup(
    name="ods_tools",
    version=version,
    include_package_data=True,
    package_data={
        "": ["*.md"],                # Copy in readme
        "ods_tools": ["data/*", "odtf/data/**/*"]  # Copy spec JSON/CSV and YAML mappings
    },
    entry_points={
        'console_scripts': [
            'ods_tools=ods_tools.main:main',
        ]
    },
    author='Oasis LMF',
    author_email="support@oasislmf.org",
    packages=setuptools.find_packages(exclude=('tests', 'tests.*', 'tests.*.*')),
    package_dir={'ods_tools': 'ods_tools'},
    python_requires='>=3.8',
    install_requires=reqs,
    extras_require={
        'extra': extra_reqs,
    },
    description='Tools to manage ODS files',
    long_description=readme,
    long_description_content_type='text/markdown',
    url='https://github.com/OasisLMF/OpenDataStandards',
    cmdclass={
        'install': DownloadSpecODS,
    },
)
