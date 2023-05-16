from unittest import TestCase, main
from unittest.mock import patch
from pandas import DataFrame, read_excel, read_csv

from ods_tools.oed.required_fields.files_map import FilesMap, FileType
from ods_tools.oed.required_fields.field import Field
import json


class TestFilesMap(TestCase):

    def setUp(self) -> None:
        self.json_data = json.load(open('../oed.json'))

    def tearDown(self) -> None:
        pass

    def test___init__(self):
        print(self.json_data)


if __name__ == '__main__':
    main()
