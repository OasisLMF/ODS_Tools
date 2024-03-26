import logging
import math
import os
import re
from functools import reduce
from operator import and_, or_
from typing import Any, Dict, Iterable, Union

import pandas as pd
from numpy import nan

from ..connector.base import BaseConnector
from ..mapping.base import (
    BaseMapping,
    DirectionalMapping,
    ColumnConversions,
    TransformationEntry
)
from ..transformers import run
from ..transformers.transform import (
    GroupWrapper,
    RowType,
    TransformerMapping,
    default_match,
    default_search,
    transform_loc_perils
)
from ..notset import NotSet, NotSetType
from ..validator_pandas import PandasValidator
from .base import BaseRunner
#
# Group Wrappers
#
if tuple(map(int, pd.__version__.split('.'))) >= (2, 1, 9):
    pd.set_option('future.no_silent_downcasting', True)
logger = logging.getLogger(__name__)


class PandasGroupWrapper(GroupWrapper):
    """
    Base class for the pandas implementation for any and all groups
    """

    def in_operator(self, x, y):
        return reduce(or_, (x == c for c in y), False)

    def not_in_operator(self, x, y):
        return reduce(and_, (x != c for c in y), True)


class PandasAnyWrapper(PandasGroupWrapper):
    """
    Pandas specific implementation of the ``any`` expression
    """

    def check_fn(self, values):
        return reduce(or_, values, False)


class PandasAllWrapper(PandasGroupWrapper):
    """
    Pandas specific implementation of the ``all`` expression
    """

    def check_fn(self, values):
        return reduce(and_, values, True)


#
# Transformer overrides
#


def logical_and_transformer(row, lhs, rhs):
    return lhs & rhs


def logical_or_transformer(row, lhs, rhs):
    return lhs | rhs


def logical_not_transformer(row, value):
    try:
        return not bool(value)
    except ValueError:
        # assume we are dealing with series or
        # dataframe is we get a value error
        return value.apply(lambda v: not bool(v))


def in_transformer(row, lhs, rhs):
    if hasattr(lhs, "is_in"):
        return lhs.is_in(rhs)
    else:
        return reduce(or_, map(lambda s: lhs == s, rhs))


def not_in_transformer(row, lhs, rhs):
    if hasattr(lhs, "is_not_in"):
        return lhs.is_not_in(rhs)
    else:
        return reduce(and_, map(lambda s: lhs != s, rhs))


#
# String Manipulations
#


class StrReplace:
    def __init__(self, series_type):
        self.series_type = series_type

    def __call__(self, row: RowType, target, *pattern_repl):
        result = target
        patterns = (re.compile(f'^{re.escape(p)}$') for i, p in enumerate(pattern_repl) if i % 2 == 0)
        repls = (r for i, r in enumerate(pattern_repl) if i % 2 != 0)

        if isinstance(result, self.series_type):
            result = result.astype(str)
            for pattern, repl in zip(patterns, repls):
                result = result.str.replace(pattern, repl, regex=True)
        else:
            for pattern, repl in zip(patterns, repls):
                result = pattern.sub(repl, str(result))

        return result


class StrMatch:
    def __init__(self, series_type):
        self.series_type = series_type

    def __call__(self, row: RowType, target, pattern: re.Pattern):
        if isinstance(target, self.series_type):
            return target.astype(str).str.match(pattern)
        else:
            return default_match(row, target, pattern)


class StrSearch:
    def __init__(self, series_type):
        self.series_type = series_type

    def __call__(self, row: RowType, target, pattern: re.Pattern):
        if isinstance(target, self.series_type):
            return target.astype(str).str.contains(pattern)
        else:
            return default_search(row, target, pattern)


class StrJoin:
    def __init__(self, series_type):
        self.series_type = series_type

    def to_str(self, obj):
        return (
            obj.astype(str) if isinstance(obj, self.series_type) else str(obj)
        )

    def concat(self, left, right):
        left_is_series = isinstance(left, self.series_type)
        right_is_series = isinstance(right, self.series_type)

        if left_is_series or not right_is_series:
            # if the left it already a series or if the right isn't a series
            # the strings will be concatenated in the correct order
            return self.to_str(left) + self.to_str(right)
        else:
            # if right is a series and left isnt force the join to prepend left
            return self.to_str(right).apply(lambda x: self.to_str(left) + x)

    def join(self, left, join, right):
        return self.concat(self.concat(left, join), right)

    def __call__(self, row: RowType, join, *elements):
        if not elements:
            return ""
        elif len(elements) == 1:
            return self.to_str(elements[0])
        else:
            return reduce(
                lambda reduced, element: self.join(reduced, join, element),
                elements[1:],
                elements[0],
            )


class ConversionError:
    def __init__(self, value=None, reason=None):
        self.reason = reason
        self.value = value


def type_converter(to_type, nullable, null_values):
    def _converter(value):
        try:
            if nullable and (
                value in null_values
                or (isinstance(value, float) and math.isnan(value))
                or value is None
            ):
                return None
            return to_type(value)
        except Exception as e:
            return ConversionError(value, e)

    return _converter


class PandasRunner(BaseRunner):
    """
    Default implementation for a pandas like runner
    """

    name = "Pandas"

    row_value_conversions = {
        "int": lambda col, nullable, null_values: col.astype(object).apply(
            type_converter((lambda v: int(float(v))), nullable, null_values),
        ),
        "float": lambda col, nullable, null_values: col.astype(object).apply(
            type_converter(float, nullable, null_values),
        ),
        "string": lambda col, nullable, null_values: col.astype(object).apply(
            type_converter(str, nullable, null_values),
        ),
    }

    dataframe_type = pd.DataFrame
    series_type = pd.Series

    def coerce_row_types(self, row, conversions: ColumnConversions):
        coerced_row = NotSet

        for column in row.columns:
            conversion = conversions.get(column)
            if not conversion:
                coerced_column = row[column]
                bad_rows = None
            else:
                coerced_column = self.row_value_conversions[conversion.type](
                    row[column],
                    conversion.nullable,
                    conversion.null_values,
                )
                bad_rows = coerced_column.apply(
                    lambda v: isinstance(v, ConversionError),
                )

                for error, (idx, entry) in zip(
                    coerced_column[bad_rows], row[bad_rows].iterrows()
                ):
                    self.log_type_coercion_error(
                        entry.to_dict(),
                        column,
                        error.value,
                        conversion.type,
                        error.reason,
                    )

                coerced_column = coerced_column[~bad_rows]

            if isinstance(coerced_row, NotSetType):
                coerced_row = coerced_column.to_frame(column)
            else:
                coerced_row[column] = coerced_column

            # remove the bad rows from the input row and the coerced row
            # so that no bad rows arent processed anymore and bad rows
            # arent included in the final coerced value
            if bad_rows is not None and len(bad_rows):
                row = row[~bad_rows]
                coerced_row = coerced_row[~bad_rows]  # type: ignore

        return coerced_row

    def create_series(self, index, value):
        return self.series_type(value, index=index)

    def get_dataframe(self, extractor: BaseConnector) -> pd.DataFrame:
        """
        Builds a dataframe from the extractors data

        :param extractor: The extractor providing the input data

        :return: The created dataframe
        """
        return pd.DataFrame(extractor.extract(), dtype="object")

    def combine_column(
        self,
        row,
        current_column_value: Union[pd.Series, NotSetType],
        entry: TransformationEntry,
    ):
        """
        Combines the current column value with the result of the
        transformation. If the current value is ``NotSet`` the value of the
        current transformation will be calculated and applied.

        :param row: The row loaded from the extractor
        :param current_column_value: Series representing the current
            transformed value
        :param entry: The transformation to apply

        :return: The combined column value
        """
        if not isinstance(current_column_value, NotSetType):
            # remove any entries that have already been processed
            row = row[
                self.create_series(
                    current_column_value.index, False
                ).combine_first(self.create_series(row.index, True))
            ]

        if isinstance(row, NotSetType) or len(row) == 0:
            return current_column_value

        new_column_value = self.apply_transformation_entry(row, entry)

        if isinstance(current_column_value, NotSetType):
            return new_column_value
        elif isinstance(new_column_value, NotSetType):
            return current_column_value
        else:
            return current_column_value.combine_first(new_column_value)

    def assign(
        self,
        input_row: pd.DataFrame,
        output_row: Union[pd.DataFrame, NotSetType],
        **assignments,
    ):
        """
        Helper function for assigning a series to a dataframe. Some
        implementations of pandas are less efficient if we start with an empty
        dataframe so here we allow for `None` to be passed and create the
        initial dataframe from the first assigned series.

        :param input_row: The row loaded from the extractor
        :param output_row: The data frame to assign to or None
        :param assignments: The assignments to apply to the dataframe

        :return: The updated dataframe
        """
        for name, series in assignments.items():
            if isinstance(series, NotSetType):
                series = self.create_series(input_row.index, nan)

            if isinstance(output_row, NotSetType):
                output_row = series.to_frame(name=name)
            else:
                series.name = name
                output_row, series = output_row.align(
                    series, axis=0, fill_value=NotSet
                )
                output_row = output_row.infer_objects(copy=False)
                output_row = output_row.assign(**{name: series})

        return output_row

    def apply_transformation_entry(
        self,
        input_df: pd.DataFrame,
        entry: TransformationEntry,
    ) -> Union[pd.Series, NotSetType]:
        """
        Applies a single transformation to the dataset returning the result
        as a series.

        :param input_df: The dataframe loaded from the extractor
        :param entry: The transformation to apply

        :return: The transformation result
        """
        transformer_mapping: TransformerMapping = {
            "logical_and": logical_and_transformer,
            "logical_or": logical_or_transformer,
            "logical_not": logical_not_transformer,
            "is_in": in_transformer,
            "not_in": not_in_transformer,
            "any": lambda r, values: PandasAnyWrapper(values),
            "all": lambda r, values: PandasAllWrapper(values),
            "str_replace": StrReplace(self.series_type),
            "str_match": StrMatch(self.series_type),
            "str_search": StrSearch(self.series_type),
            "str_join": StrJoin(self.series_type),
            "transform_loc_perils": transform_loc_perils,
        }

        # process the when clause to get a filter series
        filter_series = run(
            input_df, entry.when_tree or entry.when, transformer_mapping
        )

        if isinstance(filter_series, self.series_type):
            # if we have a series treat it as a row mapping
            filtered_input = input_df[filter_series]
        elif filter_series:
            # if the filter series is normal value that resolves to true
            # return all rows
            filtered_input = input_df
        else:
            # if the filter series is normal value that resolves to false
            # return no rows, this should never happen so raise a warning.
            logger.warning(
                f"A transformer when clause resolves to false in all cases "
                f"({entry.when})."
            )
            return NotSet

        result = run(
            filtered_input,
            entry.transformation_tree or entry.transformation,
            transformer_mapping,
        )
        if isinstance(result, self.series_type):
            return result
        else:
            return self.create_series(input_df.index, result)

    def transform(
        self, extractor: BaseConnector, mapping: BaseMapping
    ) -> Iterable[Dict[str, Any]]:
        transformations = mapping.get_transformations()

        validator = PandasValidator(search_paths=([os.path.dirname(self.config.path)] if self.config.path else []))

        logger.info(f"Running transformation set {transformations[0].input_format} -> {transformations[-1].output_format}")
        total_rows = 0
        runner_config = self.config.config.get('runner', None)
        batch_size = runner_config.get('batch_size', 10000)

        for batch in pd.read_csv(extractor.file_path, chunksize=batch_size, low_memory=False):
            # Check if all the columns necessary for the transformations are present. If not, drop the transformations we can't run
            missing_columns = set(transformations[0].types.keys()) - set(batch.columns)
            updated_transformations = [
                DirectionalMapping(
                    input_format=t.input_format,
                    output_format=t.output_format,
                    transformation_set={k: v for k, v in t.transformation_set.items() if k not in missing_columns},
                    types=t.types,
                    null_values=t.null_values,
                )
                for t in transformations
            ]

            validator.run(self.coerce_row_types(batch, updated_transformations[0].types),
                          mapping.input_format.name, mapping.input_format.version, mapping.file_type)

            transformed = batch
            for transformation in updated_transformations:
                transformed = self.apply_transformation_set(transformed, transformation, missing_columns)

            validator.run(transformed, mapping.output_format.name, mapping.output_format.version, mapping.file_type)
            total_rows += len(batch)
            logger.info(f"Processed {len(batch)} rows in the current batch (total: {total_rows})")

            yield from (r.to_dict() for idx, r in transformed.iterrows())
