from lark import Token
import pandas as pd
from operator import and_, or_
import re
from functools import reduce, total_ordering
import math


@total_ordering
class GroupWrapper():
    def __init__(self, values):
        self.values = values

    def __eq__(self, other):
        return self.check_fn(self.eq_operator(v, other) for v in self.values)

    def check_fn(self, checks):
        raise NotImplementedError()

    def eq_operator(self, lhs, rhs):
        return lhs == rhs

    def __lt__(self, other):
        return self.check_fn(self.lt_operator(v, other) for v in self.values)

    def gt_operator(self, lhs, rhs):
        return lhs > rhs

    def gte_operator(self, lhs, rhs):
        return lhs >= rhs

    def lt_operator(self, lhs, rhs):
        return lhs < rhs

    def lte_operator(self, lhs, rhs):
        return lhs <= rhs

    def is_in(self, other):
        return self.check_fn(self.in_operator(v, other) for v in self.values)

    def is_not_in(self, other):
        return self.check_fn(
            self.not_in_operator(v, other) for v in self.values
        )

    def in_operator(self, x, y):
        return reduce(or_, (x == c for c in y), False)

    def not_in_operator(self, x, y):
        return reduce(and_, (x != c for c in y), True)


class AnyWrapper(GroupWrapper):
    def check_fn(self, values):
        return reduce(or_, values, False)


class AllWrapper(GroupWrapper):
    def check_fn(self, values):
        return reduce(and_, values, True)


class StrReplace:
    def __call__(self, row, target, *pattern_repl):
        result = target
        patterns = (re.compile(f'^{re.escape(p)}$') for i, p in enumerate(pattern_repl) if i % 2 == 0)
        repls = (r for i, r in enumerate(pattern_repl) if i % 2 != 0)

        if isinstance(result, pd.Series):
            result = result.astype(str)
            for pattern, repl in zip(patterns, repls):
                result = result.str.replace(pattern, repl, regex=True)
        else:
            for pattern, repl in zip(patterns, repls):
                result = pattern.sub(repl, str(result))

        return result


class StrMatch:
    def __call__(self, row, target, pattern: re.Pattern):
        if isinstance(target, pd.Series):
            return target.astype(str).str.match(pattern)
        else:
            return default_match(row, target, pattern)


class StrSearch:
    def __call__(self, row, target, pattern: re.Pattern):
        if isinstance(target, pd.Series):
            return target.astype(str).str.contains(pattern)
        else:
            return default_search(row, target, pattern)


class StrJoin:
    def to_str(self, obj):
        return (
            obj.astype(str) if isinstance(obj, pd.Series) else str(obj)
        )

    def concat(self, left, right):
        left_is_series = isinstance(left, pd.Series)
        right_is_series = isinstance(right, pd.Series)

        if left_is_series or not right_is_series:
            # if the left it already a series or if the right isn't a series
            # the strings will be concatenated in the correct order
            return self.to_str(left) + self.to_str(right)
        else:
            # if right is a series and left isnt force the join to prepend left
            return self.to_str(right).apply(lambda x: self.to_str(left) + x)

    def join(self, left, join, right):
        return self.concat(self.concat(left, join), right)

    def __call__(self, row, join, *elements):
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


def replace_multiple(row, target, source_sep, target_sep, *pattern_repl):
    """
    Transform location perils from source to target perils.

    Args:
        row (RowType):
        target (_type_): values to be transformed
        source_sep (_type_): delimeter in source values
        target_sep (_type_): delimeter in target values

    Returns:
        pd.Series: transformed values
    """
    if isinstance(target, pd.Series):
        perils = target.apply(lambda x: [p.strip() for p in str(x).split(source_sep.strip("'"))])
    else:
        return target

    # Create list of replacements
    pattern_repl_list = list(zip(pattern_repl[::2], pattern_repl[1::2]))

    def transform_peril(peril_list):
        result = []
        for peril in peril_list:
            for pattern, repl in pattern_repl_list:
                # Replace if our list of replacements contains the peril
                if peril == pattern.strip("'"):
                    result.append(repl.strip("'"))
                    break
            else:
                result.append(peril)
        return target_sep.strip("'").join(result)

    result = perils.apply(transform_peril)
    return result


def replace_double(row, first_column, second_column, *triplets):
    """
    Transform `target` values using two columns.

    Args:
        row (RowType): The row of data.
        first_column: A pd.Series column to be transformed.
        second_column: A pd.Series column to be transformed.

    Returns:
        pd.Series: Transformed values.
    """
    mappings = list(zip(triplets[::3], triplets[1::3], triplets[2::3]))

    if isinstance(first_column, pd.Series) and isinstance(second_column, pd.Series):
        def apply_mapping(val, ctx):
            for from_val, via_val, to_val in mappings:
                if val == from_val.strip("'") and ctx == via_val.strip("'"):
                    return to_val.strip("'")
            return val
        return pd.Series([
            apply_mapping(val, ctx)
            for val, ctx in zip(first_column, second_column)
        ], index=first_column.index)

    return first_column


def safe_lookup(r, name):
    if isinstance(name, Token):
        name = name.value
    if name == 'True':
        return True
    elif name == 'False':
        return False
    return r.get(name, name)


def default_match(row, target, pattern):
    """
    Checks if a pattern matches the target. The pattern can be either a string
    or regular expression, if a string is used it is the same as
    `pattern == target`.

    :param row: The row being checked (not used)
    :param target: The value to perform the check on
    :param pattern: The pattern to find match the target

    :return: True if the pattern matches the pattern, False otherwise
    """
    if isinstance(pattern, str):
        return str(target) == pattern
    else:
        return bool(pattern.fullmatch(str(target)))


def default_search(row, target, pattern):
    """
    Checks if a pattern is in the target. The pattern can be either a string
    or regular expression, if a string is used it is the same as
    `pattern in target`.

    :param row: The row being checked (not used)
    :param target: The value to perform the check on
    :param pattern: The pattern to find in the target

    :return: True if the pattern matches the pattern, False otherwise
    """
    if isinstance(pattern, str):
        return pattern in target
    else:
        return bool(pattern.search(target))


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
