from ods_tools.oed.required_fields.field_reference import FileFieldReference
from pandas import DataFrame
from unittest import TestCase, main


class TestFileFieldReference(TestCase):

    def setUp(self) -> None:
        self.df = DataFrame(data=DATA, columns=COLUMNS)
        self.name = "Acc"
        self.test = FileFieldReference(schema_data=self.df, name=self.name)

    def tearDown(self) -> None:
        pass

    def test_init(self) -> None:
        if not self.df.equals(self.df):
            raise AssertionError("DataFrames are not equal")
        self.assertEqual(self.name, self.test.name)




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
