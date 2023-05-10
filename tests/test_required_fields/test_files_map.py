from unittest import TestCase, main
from unittest.mock import patch
from pandas import DataFrame, read_excel, read_csv

from ods_tools.oed.required_fields.files_map import FilesMap, FileType
from ods_tools.oed.required_fields.field import Field


class TestFilesMap(TestCase):

    def setUp(self) -> None:
        self.Acc = 'Acc'
        self.Loc = 'Loc'
        self.ReinsScope = 'ReinsScope'
        self.schema_path = "../../OpenExposureData_Spec.xlsx"
        self.locations_path = "../locations.csv"
        self.schema_data: DataFrame = read_excel(self.schema_path, sheet_name="OED CR Field Appendix")
        self.test = FilesMap(schema_path="../../OpenExposureData_Spec.xlsx")

    def tearDown(self) -> None:
        pass

    def get_filter(self, file_name: str) -> DataFrame:
        return self.schema_data.loc[self.schema_data['File Name'] == file_name]

    @patch("ods_tools.oed.required_fields.files_map.read_excel")
    @patch("ods_tools.oed.required_fields.files_map.FilesMap.populate_files")
    def test___init__(self, mock_populate_files, mock_read_excel):
        test = FilesMap(schema_path=self.schema_path)

        mock_populate_files.assert_called_once_with()
        self.assertEqual(test.schema_path, self.schema_path)
        self.assertEqual(test.schema_data, mock_read_excel.return_value)
        self.assertEqual(test.files, {})

    @patch("ods_tools.oed.required_fields.files_map.FileFieldReference")
    def test_populate_files(self, mock_file_field_reference):
        self.test.populate_files()

        self.assertEqual(mock_file_field_reference.call_count, 3)

        self.assertEqual(
            mock_file_field_reference.call_args_list[0][1]["schema_data"].equals(self.get_filter(self.Acc)),
            True
        )
        self.assertEqual(mock_file_field_reference.call_args_list[0][1]["name"], self.Acc)

        self.assertEqual(
            mock_file_field_reference.call_args_list[1][1]["schema_data"].equals(self.get_filter(self.Loc)),
            True
        )
        self.assertEqual(mock_file_field_reference.call_args_list[1][1]["name"], self.Loc)

        self.assertEqual(
            mock_file_field_reference.call_args_list[2][1]["schema_data"].equals(self.get_filter(self.ReinsScope)),
            True
        )
        self.assertEqual(mock_file_field_reference.call_args_list[2][1]["name"], self.ReinsScope)

    def test_get_missing_fields(self):
        locations_data: DataFrame = read_csv(self.locations_path)
        outcome = self.test.get_missing_fields(file_type=FileType(self.ReinsScope), file_data=locations_data)

        self.assertEqual({}, outcome)

        # insert a False parent that does not exist in the data loaded
        self.test.files[FileType(self.ReinsScope).value].code_refs["CR6"] = [
            Field(
                file_name=self.ReinsScope,
                input_field_name="test",
                type_description="test",
                required_field="test",
                data_type="varchar(250)",
                allow_blanks="TRUE",
                default="test"
            )
        ]

        # insert a False dependency for AccNumber field that does not exist in the data loaded
        self.test.files[FileType(self.ReinsScope).value].code_refs["CR6-01-1"].append(
            Field(
                file_name=self.ReinsScope,
                input_field_name="test_two",
                type_description="test",
                required_field="test",
                data_type="varchar(250)",
                allow_blanks="TRUE",
                default="test"
            )
        )
        expected_outcome = {
            "test": [
                "PortNumber",
                "AccNumber",
                "LocNumber",
                "CountryCode"
            ],
            "test_two": [
                "AccNumber"
            ]
        }

        outcome = self.test.get_missing_fields(file_type=FileType(self.ReinsScope), file_data=locations_data)
        self.assertEqual(expected_outcome, outcome)


if __name__ == '__main__':
    main()
