from unittest import TestCase, main

from ods_tools.oed.required_fields.field import Field, DataType
import json


class TestField(TestCase):

    def setUp(self) -> None:
        self.json_data = json.load(open('../oed.json'))
        self.data = {
            'File Name': 'Acc',
            'Input Field Name': 'AccDedType1Building',
            'Type & Description': 'Account building deductible type',
            'Required Field': 'CR1-01-1',
            'Data Type': 'tinyint',
            'Allow blanks?': 'YES',
            'Default': 0
        }
        self.test = Field(
            file_name="Acc",
            input_field_name="AccDedType1Building",
            type_description="Account building deductible type",
            required_field="CR1-01-1",
            data_type="tinyint",
            allow_blanks="YES",
            default=0
        )

    def tearDown(self) -> None:
        pass

    def test___init__(self):
        self.assertEqual(self.test.file_name, "Acc")
        self.assertEqual(self.test.input_field_name, "AccDedType1Building")
        self.assertEqual(self.test.type_description, "Account building deductible type")
        self.assertEqual(self.test.required_field, "CR1-01-1")
        self.assertEqual(self.test.data_type, DataType.TINYINT)
        self.assertEqual(self.test.allow_blanks, "YES")
        self.assertEqual(self.test.default, 0)

    def test_from_dict(self):
        field = Field.from_dict(field_dict=self.data)
        self.assertEqual(field.file_name, "Acc")
        self.assertEqual(field.input_field_name, "AccDedType1Building")
        self.assertEqual(field.type_description, "Account building deductible type")
        self.assertEqual(field.required_field, "CR1-01-1")
        self.assertEqual(field.data_type, DataType.TINYINT)
        self.assertEqual(field.allow_blanks, "YES")
        self.assertEqual(field.default, 0)

    def test_from_schema(self):
        for i in self.json_data["input_fields"]["null"].keys():
            print(i)

        # print(self.json_data["input_fields"]["Acc"].keys())

    def test_parent(self):
        self.assertEqual(self.test.parent, "CR1")

    def test_python_data_type(self):
        self.assertEqual(self.test.python_data_type, int)


if __name__ == '__main__':
    main()
