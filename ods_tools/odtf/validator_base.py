import logging
from typing import (
    Any,
    Dict,
    Generic,
    List,
    Optional,
    TypedDict,
    TypeVar,
    Union,
)

import yaml

logger = logging.getLogger(__name__)

DataType = TypeVar("DataType")
GroupedDataType = TypeVar("GroupedDataType")
GroupedDataType_cov = TypeVar("GroupedDataType_cov", covariant=True)


class ValidationResultEntry(TypedDict, total=False):
    field: Optional[str]
    groups: Optional[Dict[str, Any]]
    value: Optional[Any]
    error: Optional[str]


class ValidationResult(TypedDict):
    name: str
    operator: str
    entries: List[ValidationResultEntry]


class ValidationLogEntry(TypedDict):
    file_type: str
    format: str
    validation_file: Optional[str]
    validations: List[ValidationResult]


class ValidatorConfigEntry:
    def __init__(self, validator_name, config):
        self.validator_name = validator_name
        self.fields = config.get("fields", [])
        self.operator = config.get("operator", "sum")
        self.group_by = config.get("group_by", None)

    def __eq__(self, other):
        return all(
            [
                self.validator_name == other.validator_name,
                self.fields == other.fields,
                self.operator == other.operator,
                self.group_by == other.group_by,
            ]
        )


class ValidatorConfig:
    def __init__(self, path=None, raw_config=None):
        if raw_config:
            self.raw_config = raw_config
        else:
            self.path = path

            with open(self.path) as f:
                self.raw_config = yaml.load(f, yaml.Loader)

        self.entries = [
            ValidatorConfigEntry(k, v)
            for k, v in self.raw_config.get("entries", {}).items()
        ]

    def __eq__(self, other):
        return self.entries == other.entries


class BaseValidator(Generic[DataType, GroupedDataType]):
    def __init__(
        self,
        path: List[str] = None
    ):
        self.path = path

    def load_config(
        self, fmt, version, file_type
    ) -> Union[None, ValidatorConfig]:
        if self.path is None:
            logger.warning(
                "No validation given"
            )
            return None

        return ValidatorConfig(self.path)

    def run(self, data: DataType, fmt: str, version: str, file_type: str, enable_logging: bool = False):
        config = self.load_config(fmt, version, file_type)

        result: ValidationLogEntry = {
            "file_type": file_type,
            "format": fmt,
            "validation_file": config.path if config else None,
            "validations": [],
        }
        if config:
            for entry in config.entries:
                result["validations"].append(self.run_entry(data, entry))

        if enable_logging:
            logger.info(yaml.safe_dump([result]))
        return result

    def group_data(
        self, data: DataType, group_by: List[str], entry: ValidatorConfigEntry
    ) -> GroupedDataType_cov:  # pragma: no cover
        raise NotImplementedError()

    def sum(
        self,
        data: Union[DataType, GroupedDataType],
        entry: ValidatorConfigEntry,
    ) -> List[ValidationResultEntry]:  # pragma: no cover
        raise NotImplementedError()

    def count(
        self,
        data: Union[DataType, GroupedDataType],
        entry: ValidatorConfigEntry,
    ) -> List[ValidationResultEntry]:  # pragma: no cover
        raise NotImplementedError()

    def count_unique(
        self,
        data: Union[DataType, GroupedDataType],
        entry: ValidatorConfigEntry,
    ) -> List[ValidationResultEntry]:  # pragma: no cover
        raise NotImplementedError()

    def run_entry(
        self, data: DataType, entry: ValidatorConfigEntry
    ) -> ValidationResult:
        if entry.group_by is not None:
            data = self.group_data(data, entry.group_by, entry)

        if entry.operator == "sum":
            return ValidationResult(
                name=entry.validator_name,
                operator=entry.operator,
                entries=self.sum(data, entry),  # types: ignore
            )
        elif entry.operator == "count":
            return ValidationResult(
                name=entry.validator_name,
                operator=entry.operator,
                entries=self.count(data, entry),  # types: ignore
            )
        elif entry.operator == "count-unique":
            return ValidationResult(
                name=entry.validator_name,
                operator=entry.operator,
                entries=self.count_unique(data, entry),  # types: ignore
            )
        else:
            return ValidationResult(
                name=entry.validator_name,
                operator=entry.operator,
                entries=[{"error": "Unknown operator"}],
            )
