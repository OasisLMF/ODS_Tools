from pandas import DataFrame


class FileFieldReference:

    def __init__(self, schema_data: DataFrame, name: str) -> None:
        self.schema_data: DataFrame = schema_data
        self.name: str = name

    def _populate(self) -> None:
        for row in self.schema_data.to_dict('records'):
            print(row)
