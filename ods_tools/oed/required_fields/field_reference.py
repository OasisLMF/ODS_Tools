"""
This file defines the FileFieldReference class to handle references to the OED required fields file.
"""
from typing import Dict, List, Union, Optional

from ods_tools.oed.required_fields.field import Field


class FileFieldReference:
    """
    A class to represent a reference to the required fields file.

    Attributes:
        name_refs (Dict[str, Field]): A dictionary of the fields in the file, keyed by their input field name.
        code_refs (Dict[str, List[Field]]): A dictionary of the fields in the file, keyed by their required field code.
    """

    def __init__(self) -> None:
        """
        The constructor for the FileFieldReference class.
        """
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

    def populate_from_json(self, json_data: dict) -> None:
        """
        Populates the name_refs and code_refs dictionaries from a JsonOed object.

        :param json_data: The JsonOed object to populate from.
        :return: None
        """
        fields: List[dict] = json_data["cr_field"]

        for field in fields:
            self._add_field_from_dict(field_data=field)

    def populate_from_dataframe(self, data_frame: "DataFrame") -> None:
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

    def get_missing_fields(self, file_data: "DataFrame") -> Dict[str, List[str]]:
        """
        Get a list of missing fields from a file_data DataFrame.

        :param file_data: The DataFrame containing the file data
        :return: Fields that are missing and the fields that are dependent on them
        """
        column_set = set(list(file_data))
        missing_fields_cache = {}

        for name in list(file_data):
            field = self.get_field_by_name(name=name)

            if field is not None:
                columns = self.get_fields_by_code(code=field.required_field)
                parent = self.get_fields_by_code(code=field.parent)

                for column in columns + parent:
                    if column.input_field_name not in column_set:
                        dependent_columns = missing_fields_cache.get(column.input_field_name, [])
                        dependent_columns.append(field.input_field_name)
                        missing_fields_cache[column.input_field_name] = dependent_columns

        return missing_fields_cache

    def check_value(self, field_name: str, data: Union[str, int, float]) -> bool:
        """
        A method to check if the data type of the given field name matches the given data.

        Args:
            field_name (str): The name of the field to check.
            data (Union[str, int, float]): The data to check the field against.

        Returns: True if the data type of the field matches the data, False otherwise.
        """
        return self.name_refs[field_name].data_type.check_value(input_value=data)
