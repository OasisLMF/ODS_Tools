import json
from typing import Dict
from ods_tools.oed.required_fields.enums.file_type import FileType


class JsonOed:

    def __init__(self) -> None:
        self._json_data: dict = {}

    def get_file_fields(self, file_type: FileType) -> Dict[str, dict]:
        return self.input_fields[file_type.value]

    @staticmethod
    def from_file(file_path: str) -> "JsonOed":
        """
        A static method that creates a JsonOed object from a json file.

        Args:
            file_path (str): The path to the json file.

        Returns: The JsonOed object created from the json file.
        """
        json_oed = JsonOed()
        with open(file_path) as json_file:
            json_oed._json_data = json.load(json_file)
        return json_oed

    @staticmethod
    def from_dict(json_dict: dict) -> "JsonOed":
        """
        A static method that creates a JsonOed object from a dictionary.

        Args:
            json_dict (dict): The dictionary to create the JsonOed object from.

        Returns: The JsonOed object created from the dictionary.
        """
        json_oed = JsonOed()
        json_oed._json_data = json_dict
        return json_oed

    @property
    def input_fields(self) -> Dict[str, dict]:
        """
        A property containing the input fields from the json file.

        Returns: The input fields from the json file.
        """
        return self._json_data["input_fields"]
