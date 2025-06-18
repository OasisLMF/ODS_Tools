import logging
import json
from operator import add, mul, sub, truediv
from packaging import version
from .transformers.transform_utils import (
    AllWrapper, AnyWrapper, StrJoin, StrMatch, StrReplace, StrSearch, ConversionError,
    replace_double, replace_multiple, safe_lookup, logical_and_transformer, logical_or_transformer,
    logical_not_transformer, not_in_transformer, in_transformer, type_converter
)

import pandas as pd
from .transformers import run
from .notset import NotSet, NotSetType
from .validator import Validator

if version.parse(pd.__version__) >= version.parse("2.1.9"):
    pd.set_option('future.no_silent_downcasting', True)
logger = logging.getLogger(__name__)


class PandasRunner():
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

    def __init__(self, config):
        self.config = config
        self.logging = config.get('logging', False)

    def run(self, extractor, mapper, loader):
        """
        Runs the transformation process and sends the data to the data loader

        :param extractor: The data connection to extract data from
        :param mapping: Mapping object describing the transformations to apply
        :param loader: The data connection to load data to
        """
        loader.load(self.transform(extractor, mapper))

    def transform(self, extractor, mapper):
        """
        Orchestrates the data transformation.
        It receives the data in batches from the extractor, calculates which
        transformations can be applied to it, applies them, and yields the
        transformed rows.

        :param extractor: The data extractor object (e.g., CSV or database connector).
        :param mapping: The mapping object defining transformations.
        :return: An iterable of dictionaries, each representing a transformed row.
        """
        validator = Validator(mapper.validation)
        batch_size = self.config.get('batch_size', 100000)
        total_rows = 0
        transformation = None

        try:
            for batch in extractor.fetch_data(batch_size):
                # Calculates the set of transformations from the columns in the
                # first batch (to avoid double-querying)
                if transformation is None:
                    available_columns = set(batch.columns)
                    transformation = mapper.get_transform(available_columns=available_columns)
                    self.log("Running transformation set [{extractor.name}]", "info")

                try:
                    validation = validator.run(self.coerce_df_types(batch, transformation.types), stage=0)
                    self.log(validation, "warning")
                except Exception as e:
                    self.log(f"Validation failed: {e}", "warning")

                batch = self.apply_transformation_set(batch, transformation)

                try:
                    validation = validator.run(batch, stage=1)
                    self.log(validation, "warning")
                except Exception as e:
                    self.log(f"Validation failed: {e}", "warning")

                # Log the transformation progress
                total_rows += len(batch)
                self.log(f"Processed {len(batch)} rows in the current batch (total: {total_rows})", "info")

                yield from (r.to_dict() for idx, r in batch.iterrows())
        except FileNotFoundError:
            logger.error(f"File not found: {extractor.file_path}")
        except Exception as e:
            logger.error(f"Error processing batch: {e}")

    def apply_transformation_set(self, input_df, transformations):
        """
        Applies all the transformations to produce the output

        :param input_df: The dataframe to transform
        :param transformations: The full set of column conversions and transformation sets to apply to the dataframe

        :return: The transformed dataframe
        """
        input_df = self.coerce_df_types(input_df, transformations.types)
        results = {}

        for column, transformation in transformations.transformation_set.items():
            column_result = self.apply_column_transformation(input_df, transformation)
            if not isinstance(column_result, NotSetType):
                results[column] = column_result

        df = pd.concat(results, axis=1)
        return df.fillna("")  # Can remove if prefer having nans to , in output?

    def apply_column_transformation(self, input_df, entry_list):
        """
        Applies all the transformations for a single output column

        :param input_df: The dataframe to transform
        :param entry_list: A list of all the transformations to apply to generate the output

        :return: The transformation result
        """
        result = None

        for entry in entry_list:
            new_series = self.apply_transformation_entry(input_df, entry)

            if isinstance(new_series, NotSetType):
                continue
            if result is None:
                result = new_series
            else:
                result = result.combine_first(new_series)

        return result if result is not None else NotSet

    def apply_transformation_entry(self, input_df, entry):
        """
        Applies a single transformation to the dataset returning the result
        as a series.

        :param input_df: The dataframe loaded from the extractor
        :param entry: The transformation to apply

        :return: The transformation result
        """
        transformer_mapping = {
            "logical_and": logical_and_transformer,
            "logical_or": logical_or_transformer,
            "logical_not": logical_not_transformer,
            "is_in": in_transformer,
            "not_in": not_in_transformer,
            "any": lambda r, values: AnyWrapper(values),
            "all": lambda r, values: AllWrapper(values),
            "str_replace": StrReplace(),
            "str_match": StrMatch(),
            "str_search": StrSearch(),
            "str_join": StrJoin(),
            "add": lambda r, lhs, rhs: add(lhs, rhs),
            "subtract": lambda r, lhs, rhs: sub(lhs, rhs),
            "multiply": lambda r, lhs, rhs: mul(lhs, rhs),
            "divide": lambda r, lhs, rhs: truediv(lhs, rhs),
            "eq": lambda r, lhs, rhs: lhs == rhs,
            "not_eq": lambda r, lhs, rhs: lhs != rhs,
            "gt": lambda r, lhs, rhs: lhs > rhs,
            "gte": lambda r, lhs, rhs: lhs >= rhs,
            "lt": lambda r, lhs, rhs: lhs < rhs,
            "lte": lambda r, lhs, rhs: lhs <= rhs,
            "lookup": safe_lookup,
            "replace_multiple": replace_multiple,
            "replace_double": replace_double,
        }

        # process the when clause to get a filter series
        filter_series = run(
            input_df, entry.when_tree or entry.when, transformer_mapping
        )

        if isinstance(filter_series, pd.Series):
            # if we have a series treat it as a row mapping
            filtered_input = input_df[filter_series]
        elif filter_series:
            # if the filter series is normal value that resolves to true
            # return all rows
            filtered_input = input_df
        else:
            # if the filter series is normal value that resolves to false
            # return no rows, this should never happen so raise a warning.
            self.log(f"A transformer when clause resolves to false in all cases ({entry.when}).", "warning")
            return NotSet

        result = run(
            filtered_input,
            entry.transformation_tree or entry.transformation,
            transformer_mapping,
        )
        if isinstance(result, pd.Series):
            return result
        else:
            return pd.Series(result, index=input_df.index)

    def coerce_df_types(self, df, conversions):
        coerced_df = NotSet

        for column in df.columns:
            conversion = conversions.get(column)
            if not conversion:
                coerced_column = df[column]
                bad_rows = None
            else:
                coerced_column = self.row_value_conversions[conversion.type](
                    df[column],
                    conversion.nullable,
                    conversion.null_values,
                )
                bad_rows = coerced_column.apply(
                    lambda v: isinstance(v, ConversionError),
                )

                for error, (idx, entry) in zip(
                    coerced_column[bad_rows], df[bad_rows].iterrows()
                ):
                    self.log_type_coercion_error(
                        entry.to_dict(),
                        column,
                        error.value,
                        conversion.type,
                        error.reason,
                    )

                coerced_column = coerced_column[~bad_rows]

            if isinstance(coerced_df, NotSetType):
                coerced_df = coerced_column.to_frame(column)
            else:
                coerced_df[column] = coerced_column

            # remove the bad rows from the input row and the coerced row
            # so that no bad rows arent processed anymore and bad rows
            # arent included in the final coerced value
            if bad_rows is not None and len(bad_rows):
                df = df[~bad_rows]
                coerced_df = coerced_df[~bad_rows]  # type: ignore
        return coerced_df

    def log(self, message, severity):
        if self.logging:
            log_method = getattr(logger, severity.lower(), logger.info)
            log_method(message)

    @classmethod
    def log_type_coercion_error(cls, row, column, value, to_type, reason):
        """
        Logs a failure of a row type coercion

        :param row: The input row that failed
        :param column: The name of the column in which the error occurred
        :param value: The value of the failing column
        :param to_type: The type the coercion was attempting
        :param reason: The error message
        """
        logger.warning(
            f"Cannot coerce {column} ({value}) to {to_type}. "
            f"Reason: {reason}. Row: {json.dumps(row)}."
        )
        raise Exception
