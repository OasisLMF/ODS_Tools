"""
This file defines the DataType enum class to handle the type of data in a field from the OED required fields file.
"""
from enum import Enum
from typing import Union


class DataType(Enum):
    """
    This class defines the DataType enum class to handle the type of data in a field from the OED required fields file.

    Attributes:
        VARCHAR_250: A string of up to 250 characters.
        TINYINT: An integer between 0 and 255.
        FLOAT: A floating point number.
        VARCHAR_30: A string of up to 30 characters.
        VARCHAR_20: A string of up to 20 characters.
        INT: An integer.
        NVARCHAR_40: A string of up to 40 characters.
        NVARCHAR_20: A string of up to 20 characters.
        VARCHAR_40: A string of up to 40 characters.
        CHAR_2: A string of 2 characters.
    """
    VARCHAR_250 = 'varchar(250)'
    TINYINT = 'tinyint'
    FLOAT = 'float'
    VARCHAR_30 = 'varchar(30)'
    VARCHAR_20 = 'varchar(20)'
    INT = 'int'
    NVARCHAR_40 = 'nvarchar(40)'
    NVARCHAR_20 = 'nvarchar(20)'
    VARCHAR_40 = 'varchar(40)'
    CHAR_2 = 'char(2)'

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