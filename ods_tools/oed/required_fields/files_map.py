"""
This file defines the FilesMap class to handle the files from the OED required fields file.
"""
from typing import Dict, List

from pandas import DataFrame, read_excel

from ods_tools.oed.required_fields.enums.file_type import FileType
from ods_tools.oed.required_fields.field_reference import FileFieldReference


class FilesMap:
    """
    This class is responsible for handling the data from a file in relation to the OED required fields file.

    Attributes:
        schema_path (str): The path to the OED required fields file.
        schema_data (DataFrame): The data from the OED required fields file.
        files (Dict[str, FileFieldReference]): A dictionary of the files in the OED required fields file,
                                               keyed by their name.
    """
    def __init__(self, schema_path: str) -> None:
        """
        The constructor for the FilesMap class.

        :param schema_path: The path to the OED required fields file.
        """
        self.schema_path: str = schema_path
        self.schema_data: DataFrame = read_excel(schema_path, sheet_name="OED CR Field Appendix")
        self.files: Dict[str, FileFieldReference] = {}
        self.populate_files()

    def populate_files(self) -> None:
        """
        Populate the files dictionary with FileFieldReference objects.

        :return: None
        """
        for i in FileType:
            filtered_df = self.schema_data.loc[self.schema_data['File Name'] == i.value]
            self.files[i.value] = FileFieldReference(schema_data=filtered_df, name=i.value)

    def get_missing_fields(self, file_type: FileType, file_data: DataFrame) -> Dict[str, List[str]]:
        """
        Get a list of missing fields from a file_data DataFrame.

        :param file_type: The type of file being checked
        :param file_data: The DataFrame containing the file data
        :return: Fields that are missing and the fields that are dependent on them
        """
        column_set = set(list(file_data))
        missing_fields_cache = {}

        for name in list(file_data):
            field = self.files[file_type.value].get_field_by_name(name=name)

            if field is not None:
                columns = self.files[file_type.value].get_fields_by_code(code=field.required_field)
                parent = self.files[file_type.value].get_fields_by_code(code=field.parent)

                for column in columns + parent:
                    if column.input_field_name not in column_set:
                        dependent_columns = missing_fields_cache.get(column.input_field_name, [])
                        dependent_columns.append(field.input_field_name)
                        missing_fields_cache[column.input_field_name] = dependent_columns

        return missing_fields_cache
