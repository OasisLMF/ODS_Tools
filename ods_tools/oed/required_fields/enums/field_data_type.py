"""
This file defines the DataType enum class to handle the type of data in a field from the OED required fields file.
"""
from enum import Enum
from typing import Union


class DataType:

    VARCHAR = "varchar"
    NVARCHAR = "nvarchar"
    CHAR = "char"
    INT = "int"
    FLOAT = "float"
    TINYINT = "tinyint"
    SMALL_DATETIME = "smalldatetime"

    FIELD_DATA_TYPES = [
        VARCHAR,
        NVARCHAR,
        CHAR,
        INT,
        FLOAT,
        TINYINT,
        SMALL_DATETIME
    ]

    def __init__(self, data_string: str) -> None:
        self._data_string = data_string
        self.check_data_type(data_string=data_string)

    @staticmethod
    def check_data_type(data_string: str) -> None:
        data_type = data_string.split("(")[0]
        if data_type not in DataType.FIELD_DATA_TYPES:
            raise ValueError(f"Invalid data type: {data_type}")

    def check_value(self, input_value: Union[str, int, float]) -> bool:
        """
        This method checks if the input value is of the correct type for the data type.

        Args:
            input_value: The value to check.

        Returns: True if the input value is of the correct type for the data type, False otherwise.
        """
        if isinstance(input_value, self.python_type):
            if self.value.startswith('varchar') or self.value.startswith('nvarchar') or self.value.startswith('char'):
                length = int(self.value[self.value.index("(") + 1:self.value.index(")")])
                return len(input_value) <= length
            else:
                return True
        else:
            return False

    @classmethod
    def from_string(cls, data_type: str) -> "DataType":
        """
        This method returns a DataType enum object from a string.

        Args:
            data_type: A string representing the data type.

        Returns: A DataType enum object.
        """
        try:
            return cls(data_type)
        except ValueError:
            raise ValueError(f"Invalid data type: {data_type}")

    @property
    def python_type(self) -> type:
        if self.value.startswith('varchar') or self.value.startswith('nvarchar') or self.value.startswith('char'):
            return str
        elif self.value == 'tinyint' or self.value == 'int':
            return int
        elif self.value == 'float':
            return float
        else:
            raise ValueError(f"No Python type defined for data type: {self.value}")

    @property
    def value(self) -> str:
        return self._data_string
