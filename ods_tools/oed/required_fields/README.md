# Required Fields

This module maps the fields in data to see if other fields that are required are missing.

## Usage
We can check the field dependencies of a file with the following code:
    
```python
from pandas import DataFrame, read_excel, read_csv
from ods_tools.oed.required_fields.files_map import FilesMap, FileType


locations_data: DataFrame = read_csv("locations.csv")
file_map = FilesMap(schema_path="../../OpenExposureData_Spec.xlsx")
outcome = file_map.get_missing_fields(file_type=FileType("ReinsScope"), file_data=locations_data)
```
The outcome is a dictionary of missing fields and the fields that depend on them. For instance, let us alter the 
data so that we can see some missing fields. We can insert a parent field that we know is not in the data under 
the name "test" with the code below:

```python
from ods_tools.oed.required_fields.field import Field
from ods_tools.oed.required_fields.files_map import FileType

# insert a False parent that does not exist in the data loaded
file_map.files[FileType("ReinsScope").value].code_refs["CR6"] = [
    Field(
        file_name="ReinsScope",
        input_field_name="test",
        type_description="test",
        required_field="test",
        data_type="varchar(250)",
        allow_blanks="TRUE",
        default="test"
    )
]

# insert a False dependency for AccNumber field that does not exist in the data loaded
file_map.files[FileType("ReinsScope").value].code_refs["CR6-01-1"].append(
    Field(
        file_name="ReinsScope",
        input_field_name="test_two",
        type_description="test",
        required_field="test",
        data_type="varchar(250)",
        allow_blanks="TRUE",
        default="test"
    )
)
```

Now, we can run the code again and see the following missing fields:

```json
{
    "test": [
        "PortNumber",
        "AccNumber",
        "LocNumber",
        "CountryCode"
    ],
    "test_two": [
        "AccNumber"
    ]
}
```
Here we can see that "test" has all of the fields as it is a parent that is missing, and that "test_two" is missing
only "AccNumber" as it is a dependency of "test_two".
