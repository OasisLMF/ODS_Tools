"""
This file defines the Field class to handle the fields from the OED required fields file.
"""
from typing import Dict, Optional, Union

from ods_tools.oed.required_fields.enums.field_data_type import DataType


class Field:
    """
    A class to represent a field from the OED required fields file.

    Attributes:
        file_name (str): The name of the file the field is in.
        input_field_name (str): The name of the field in the file.
        type_description (str): The description of the field.
        required_field (str): The code of the field.
        data_type (DataType): The data type of the field.
        allow_blanks (str): Whether or not the field allows blanks.
        default (Optional[Union[int, float, str]]): The default value of the field.
    """

    def __init__(self, file_name: str, input_field_name: str, type_description: str, required_field: str,
                 data_type: str, allow_blanks: str, default: Optional[Union[int, float, str]]) -> None:
        """
        The constructor for the Field class.

        Args:
            file_name (str): The name of the file the field is in.
            input_field_name (str): The name of the field in the file.
            type_description (str): The description of the field.
            required_field (str): The code of the field.
            data_type (str): The data type of the field.
            allow_blanks (str): Whether or not the field allows blanks.
            default (Optional[Union[int, float, str]]): The default value of the field.
        """
        self.file_name = file_name
        self.input_field_name = input_field_name
        self.type_description = type_description
        self.required_field = required_field
        self.data_type: DataType = DataType.from_string(data_type=data_type)
        self.allow_blanks = allow_blanks
        self.default: Union[int, float, str] = default

    def __repr__(self) -> str:
        """
        A method to represent the Field object as a string.

        Returns: the string representation of the Field object.
        """
        return f"Field<{self.input_field_name} {self.allow_blanks}>"

    def __str__(self) -> str:
        """
        A method to represent the Field object as a string.

        Returns: the string representation of the Field object.
        """
        return f"Field<{self.input_field_name} {self.allow_blanks}>"

    @staticmethod
    def from_dict(field_dict: Dict[str, str]) -> "Field":
        """
        A static method that creates a Field object from a dictionary.

        Args:
            field_dict (Dict[str, str]): The dictionary to create the Field object from.

        Returns: the Field object created from the dictionary.
        """
        return Field(file_name=field_dict["File Name"],
                     input_field_name=field_dict["Input Field Name"],
                     type_description=field_dict["Type & Description"],
                     required_field=field_dict["Required Field"],
                     data_type=field_dict["Data Type"],
                     allow_blanks=field_dict["Allow blanks?"],
                     default=field_dict["Default"])

    @property
    def parent(self) -> str:
        return self.required_field.split("-")[0]

    @property
    def python_data_type(self) -> type:
        return self.data_type.python_type
