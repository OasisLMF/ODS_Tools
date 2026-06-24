import os
import json
import urllib.request
from urllib.error import HTTPError

from setuptools import setup
from setuptools.command.build_py import build_py
from setuptools.command.editable_wheel import editable_wheel


class DownloadSpecODSBase:
    """Base class for supporting downloading OEDSpec"""
    description = 'Download a ODS JSON spec file from a release URL.'

    def __init__(self, *args, **kwargs):
        self.filename = 'OpenExposureData_{}Spec.json'
        self.ods_repo = 'OasisLMF/ODS_OpenExposureData'
        self.url = f'https://github.com/{self.ods_repo}/releases/download/'
        self.github_token = os.environ.get('GITHUB_TOKEN', None)
        self.src_path_attr = None
        self.skip_if_present = False

    def run(self):
        print(f'Install all versions from url: {self.url}')
        tags = self.get_all_tags()
        for tag in tags:
            try:
                download_path = os.path.join(getattr(self, self.src_path_attr), 'ods_tools', 'data', self.filename.format(tag))

                if self.skip_if_present and os.path.isfile(download_path):
                    print(f'Tag: {tag} already present, skipping.')
                    continue

                url = self.url + f"{tag}/{self.filename.format('')}"
                req = urllib.request.Request(url)
                if self.github_token:
                    req.add_header('Authorization', f'token {self.github_token}')

                response = urllib.request.urlopen(req)
                data = json.loads(response.read())
                data['version'] = tag

                with open(download_path, 'w+') as f:
                    json.dump(data, f)

            except HTTPError:
                print(f'No OED associated with {tag}: {url}')

    def get_all_tags(self):
        """Fetch all release tags from GitHub API."""
        tags = []
        page = 1
        while True:
            api_url = f"https://api.github.com/repos/{self.ods_repo}/tags?per_page=100&page={page}"
            req = urllib.request.Request(api_url)
            if self.github_token:
                req.add_header('Authorization', f'token {self.github_token}')

            with urllib.request.urlopen(req) as response:
                data = json.load(response)
            if not data:
                break
            tags.extend([t['name'] for t in data if 'rc' not in t['name']])
            page += 1
        return tags


class DownloadSpecODSEditable(DownloadSpecODSBase, editable_wheel):
    """Custom editable_wheel command that downloads OED spec JSON files during build."""

    def __init__(self, *args, **kwargs):
        DownloadSpecODSBase.__init__(self, *args, **kwargs)
        editable_wheel.__init__(self, *args, **kwargs)
        self.src_path_attr = 'project_dir'
        self.skip_if_present = True

    def run(self):
        DownloadSpecODSBase.run(self)
        editable_wheel.run(self)


class DownloadSpecODS(DownloadSpecODSBase, build_py):
    """Custom build_py command that downloads OED spec JSON files during build."""

    def __init__(self, *args, **kwargs):
        DownloadSpecODSBase.__init__(self, *args, **kwargs)
        build_py.__init__(self, *args, **kwargs)
        self.src_path_attr = 'build_lib'

    def run(self):
        build_py.run(self)
        DownloadSpecODSBase.run(self)


setup(cmdclass={
    'build_py': DownloadSpecODS,
    'editable_wheel': DownloadSpecODSEditable,
})
