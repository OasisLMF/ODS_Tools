from typing import Dict, Optional


class Field:
    def __init__(self, file_name: str, input_field_name: str, type_description: str,
                 required_field: str, data_type: str, allow_blanks: str, default: Optional[str]) -> None:
        self.file_name = file_name
        self.input_field_name = input_field_name
        self.type_description = type_description
        self.required_field = required_field
        self.data_type = data_type
        self.allow_blanks = allow_blanks
        self.default = default

    @staticmethod
    def from_dict(field_dict: Dict[str, str]) -> "Field":
        return Field(file_name=field_dict["File Name"],
                     input_field_name=field_dict["Input Field Name"],
                     type_description=field_dict["Type & Description"],
                     required_field=field_dict["Required Field"],
                     data_type=field_dict["Data Type"],
                     allow_blanks=field_dict["Allow blanks?"],
                     default=field_dict["Default"])
