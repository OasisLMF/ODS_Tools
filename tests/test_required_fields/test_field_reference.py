import pandas

from ods_tools.oed.required_fields.field_reference import FileFieldReference
from unittest.mock import patch
from pandas import DataFrame
from unittest import TestCase, main


class TestFileFieldReference(TestCase):

    def setUp(self) -> None:
        self.df = DataFrame(data=DATA, columns=COLUMNS)
        self.name = "Acc"
        self.test = FileFieldReference(schema_data=self.df, name=self.name)

    def tearDown(self) -> None:
        pass

    @patch("ods_tools.oed.required_fields.field_reference.FileFieldReference.populate")
    def test_init(self, mock_populate) -> None:
        test = FileFieldReference(schema_data=self.df, name=self.name)
        if not test.schema_data.equals(self.df):
            raise AssertionError("DataFrames are not equal")

        self.assertEqual(self.name, test.name)
        self.assertEqual({}, test.name_refs)
        self.assertEqual({}, test.code_refs)
        mock_populate.assert_called_once_with()

    def test_populate(self) -> None:
        self.test.populate()

        self.assertEqual(self.test.name_refs["AccDedType1Building"].required_field, "CR1-01-1")

        # assert that the id of the field object is the same as the id of the field object in the code_refs dict
        self.assertEqual(id(self.test.name_refs["AccDedType1Building"]), id(self.test.code_refs["CR1-01-1"][0]))
        self.assertEqual(id(self.test.name_refs["AccDed1Building"]), id(self.test.code_refs["CR1-01-1"][1]))

    def test_get_field_by_name(self) -> None:
        field = self.test.get_field_by_name("AccDedType1Building")
        self.assertEqual(field.required_field, "CR1-01-1")

    def test_get_fields_by_code(self) -> None:
        fields = self.test.get_fields_by_code("CR1-01-1")
        self.assertEqual(len(fields), 2)
        self.assertEqual(fields[0].input_field_name, "AccDedType1Building")
        self.assertEqual(fields[1].input_field_name, "AccDed1Building")

    def test_check_value(self):
        self.assertEqual(self.test.check_value(field_name="AccDedType1Building", data=1), True)
        self.assertEqual(self.test.check_value(field_name="AccDedType1Building", data="test"), False)



    # def test_total(self):
    #     data = pandas.read_excel("../../OpenExposureData_Spec.xlsx",
    #                              sheet_name="OED CR Field Appendix")
    #     test = FileFieldReference(schema_data=data, name=self.name)
    #     test.populate()


DATA = [
    ['Acc', 'AccPeril', 'Perils for account financial terms', 'CR1', 'varchar(250)', 'YES', 'n/a'],
    ['Acc', 'AccDedType1Building', 'Account building deductible type', 'CR1-01-1', 'tinyint', 'YES', 0],
    ['Acc', 'AccDed1Building', 'Account building deductible', 'CR1-01-1', 'float', 'YES', 0],
    ['Acc', 'AccMinDed1Building', 'Account minimum building deductible', 'CR1-01-2', 'float', 'YES', 0],
    ['Acc', 'AccMaxDed1Building', 'Account maximum building deductible', 'CR1-01-3', 'float', 'YES', 0],
    ['Acc', 'AccDedType2Other', 'Account other building deductible type', 'CR1-02-1', 'tinyint', 'YES', 0],
    ['Acc', 'AccDed2Other', 'Account other building deductible', 'CR1-02-1', 'float', 'YES', 0],
    ['Acc', 'AccMinDed2Other', 'Account minimum other building deductible', 'CR1-02-2', 'float', 'YES', 0],
    ['Acc', 'AccMaxDed2Other', 'Account maximum other building deductible', 'CR1-02-3', 'float', 'YES', 0],
    ['Acc', 'AccDedType3Contents', 'Account contents deductible type', 'CR1-03-1', 'tinyint', 'YES', 0],
    ['Acc', 'AccDed3Contents', 'Account contents deductible', 'CR1-03-1', 'float', 'YES', 0],
    ['Acc', 'AccMinDed3Contents', 'Account minimum contents deductible', 'CR1-03-2', 'float', 'YES', 0],
    ['Acc', 'AccMaxDed3Contents', 'Account maximum contents deductible', 'CR1-03-3', 'float', 'YES', 0],
    ['Acc', 'AccDedType4BI', 'Account BI deductible type', 'CR1-04-1', 'tinyint', 'YES', 0],
    ['Acc', 'AccDed4BI', 'Account BI deductible', 'CR1-04-1', 'float', 'YES', 0],
    ['Acc', 'AccMinDed4BI', 'Account minimum BI deductible', 'CR1-04-2', 'float', 'YES', 0],
    ['Acc', 'AccMaxDed4BI', 'Account maximum BI deductible', 'CR1-04-3', 'float', 'YES', 0],
    ['Acc', 'AccDedType5PD', 'Account PD deductible type', 'CR1-05-1', 'tinyint', 'YES', 0],
    ['Acc', 'AccDed5PD', 'Account PD deductible', 'CR1-05-1', 'float', 'YES', 0],
    ['Acc', 'AccMinDed5PD', 'Account minimum PD deductible', 'CR1-05-2', 'float', 'YES', 0],
    ['Acc', 'AccMaxDed5PD', 'Account maximum PD deductible', 'CR1-05-3', 'float', 'YES', 0]
]

COLUMNS = ['File Name', 'Input Field Name', 'Type & Description', 'Required Field', 'Data Type', 'Allow blanks?', 'Default']


if __name__ == '__main__':
    main()
