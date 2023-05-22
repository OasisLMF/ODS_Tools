"""
This file defines the FileFieldReference class to handle references to the OED required fields file.
"""
from typing import Dict, List, Union, Optional

from pandas import DataFrame

from ods_tools.oed.required_fields.field import Field
from ods_tools.oed.required_fields.json_oed import JsonOed
from ods_tools.oed.required_fields.enums.file_type import FileType


class FileFieldReference:
    """
    A class to represent a reference to the required fields file.

    Attributes:
        schema_data (DataFrame): The data from the OED required fields file.
        name (str): The name of the file the data is referring to.
        name_refs (Dict[str, Field]): A dictionary of the fields in the file, keyed by their input field name.
        code_refs (Dict[str, List[Field]]): A dictionary of the fields in the file, keyed by their required field code.
    """
    def __init__(self, file_type: FileType) -> None:
        """
        The constructor for the FileFieldReference class.

        Args:
            schema_data (DataFrame): The data from the OED required fields file.
            name (str): The name of the file the data is referring to.
        """
        self.file_type: FileType = file_type
        self.name_refs: Dict[str, Field] = {}
        self.code_refs: Dict[str, List[Field]] = {}

    def _add_field_from_dict(self, field_data: dict) -> None:
        """
        Adds a field to the name_refs and code_refs dictionaries from field data.

        :param field_data: The data for the field to add.
        :return: None
        """
        field: Field = Field.from_dict(field_data)
        if self.name_refs.get(field.input_field_name) is None:
            self.name_refs[field.input_field_name] = field

        if self.code_refs.get(field.required_field) is None:
            self.code_refs[field.required_field] = []

        self.code_refs[field.required_field].append(field)

    def populate_from_json(self, json_data: JsonOed) -> None:
        """
        Populates the name_refs and code_refs dictionaries from a JsonOed object.

        :param json_data: The JsonOed object to populate from.
        :return: None
        """
        fields: Dict[str, dict] = json_data.get_file_fields(file_type=self.file_type)

        for field_name in fields.keys():
            self._add_field_from_dict(field_data=fields[field_name])

    def populate_from_dataframe(self, data_frame: DataFrame) -> None:
        """
        A method to populate the name_refs and code_refs dictionaries.

        Returns: None
        """
        for row in data_frame.to_dict('records'):
            self._add_field_from_dict(field_data=row)

    def get_field_by_name(self, name: str) -> Optional[Field]:
        """
        A method to get a field from the name_refs dictionary.

        Args:
            name (str): The name of the field to get.

        Returns: The field with the given name.
        """
        return self.name_refs.get(name)

    def get_fields_by_code(self, code: str) -> List[Field]:
        """
        A method to get a list of fields from the code_refs dictionary.

        Args:
            code (str): The code of the fields to get.

        Returns: A list of fields with the given code.
        """
        return self.code_refs.get(code, [])

    def check_value(self, field_name: str, data: Union[str, int, float]) -> bool:
        """
        A method to check if the data type of the given field name matches the given data.

        Args:
            field_name (str): The name of the field to check.
            data (Union[str, int, float]): The data to check the field against.

        Returns: True if the data type of the field matches the data, False otherwise.
        """
        return self.name_refs[field_name].data_type.check_value(input_value=data)
