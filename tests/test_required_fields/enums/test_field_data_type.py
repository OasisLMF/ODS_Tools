from unittest import TestCase, main

from ods_tools.oed.required_fields.enums.field_data_type import DataType


class TestDataType(TestCase):

    def setUp(self) -> None:
        self.varchar_250 = 'varchar(250)'
        self.tiny_int = 'tinyint'
        self.float = 'float'
        self.varchar_30 = 'varchar(30)'
        self.varchar_20 = 'varchar(20)'
        self.int = 'int'
        self.nvarchar_40 = 'nvarchar(40)'
        self.nvarchar_20 = 'nvarchar(20)'
        self.varchar_40 = 'varchar(40)'
        self.char_2 = 'char(2)'

        self.all_data_types = [
            self.varchar_250,
            self.tiny_int,
            self.float,
            self.varchar_30,
            self.varchar_20,
            self.int,
            self.nvarchar_40,
            self.nvarchar_20,
            self.varchar_40,
            self.char_2
        ]

    def tearDown(self) -> None:
        pass

    def test___init__(self):
        for data_type in self.all_data_types:
            self.assertEqual(DataType(data_type).value, data_type)

        with self.assertRaises(ValueError):
            DataType('invalid')

    def test_from_string(self):
        for data_type in self.all_data_types:
            self.assertEqual(DataType.from_string(data_type).value, data_type)

        with self.assertRaises(ValueError):
            DataType.from_string('invalid')

    def test_python_type(self):
        self.assertEqual(DataType.VARCHAR_250.python_type, str)
        self.assertEqual(DataType.TINYINT.python_type, int)
        self.assertEqual(DataType.FLOAT.python_type, float)
        self.assertEqual(DataType.VARCHAR_30.python_type, str)
        self.assertEqual(DataType.VARCHAR_20.python_type, str)
        self.assertEqual(DataType.INT.python_type, int)
        self.assertEqual(DataType.NVARCHAR_40.python_type, str)
        self.assertEqual(DataType.NVARCHAR_20.python_type, str)
        self.assertEqual(DataType.VARCHAR_40.python_type, str)
        self.assertEqual(DataType.CHAR_2.python_type, str)

        with self.assertRaises(ValueError):
            _ = DataType('invalid').python_type

    def test_check_value(self):
        self.assertEqual(DataType.VARCHAR_40.check_value('test'), True)
        self.assertEqual(DataType.VARCHAR_40.check_value('this is a longer string than 40 characters'), False)

        self.assertEqual(DataType.TINYINT.check_value(42), True)
        self.assertEqual(DataType.TINYINT.check_value('not a number'), False)

        self.assertEqual(DataType.VARCHAR_40.check_value(42), False)


if __name__ == '__main__':
    main()
