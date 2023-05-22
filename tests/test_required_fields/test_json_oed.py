from unittest import TestCase, main
from unittest.mock import patch
from pandas import DataFrame, read_excel, read_csv

from ods_tools.oed.required_fields.files_map import FilesMap, FileType
from ods_tools.oed.required_fields.field import Field
from ods_tools.oed.required_fields.json_oed import JsonOed
import json


class TestFilesMap(TestCase):

    def setUp(self) -> None:
        self.file_path: str = "../oed.json"
        with open(self.file_path) as json_file:
            self.json_data: dict = json.load(json_file)

    def tearDown(self) -> None:
        pass

    def test___init__(self):
        test = JsonOed()

        self.assertEqual(test._json_data, {})

    def test_from_file(self):
        test = JsonOed.from_file(file_path=self.file_path)

        self.assertEqual(test._json_data, self.json_data)
        self.assertEqual(type(test), JsonOed)

    def test_from_dict(self):
        test = JsonOed.from_dict(json_dict=self.json_data)

        self.assertEqual(test._json_data, self.json_data)
        self.assertEqual(type(test), JsonOed)

    def test_testing(self):
        test = JsonOed.from_file(file_path=self.file_path)

        outcome = test.get_file_fields(file_type=FileType.Acc)
        field = Field.from_dict(field_dict=outcome["portnumber"])
        print(field)


if __name__ == '__main__':
    main()
