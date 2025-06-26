import logging
import re
from functools import partial
from typing import Any

from lark import Transformer as _LarkTransformer
from lark import Tree
from lark import exceptions as lark_exceptions
from lark import v_args

from .errors import UnexpectedCharacters
from .grammar import parser


RowType = Any
logger = logging.getLogger(__name__)


@v_args(inline=True)
class BaseTreeTransformer(_LarkTransformer):
    """
    Tree transformer class without the transforms added
    """

    def string(self, value=""):
        """
        Parses a string from the transformer language and performs any
        necessary escaping. `value` has a default value to account for the
        empty string case.

        :param value: The value to parse

        :return: The parsed value
        """
        # process any escape characters
        return self.string_escape_re.sub(r"\1", value)

    def regex(self, value=""):
        """
        Generates a regex from teh provided string

        :param value: The pattern

        :return: The regex object
        """
        return re.compile(self.string(value))

    def iregex(self, value=""):
        """
        Generates a case insensitive regex from teh provided string

        :param value: The pattern

        :return: The regex object
        """
        return re.compile(self.string(value), flags=re.IGNORECASE)

    def boolean(self, value):
        if isinstance(value, bool):
            return value
        return str(value).lower() == "true"

    def null(self, value):
        return None

    def number(self, value):
        try:
            return int(value)
        except ValueError:
            return float(value)


def run(row, expression, transformer_mapping):
    """
    Runs a transformation expression on a row

    :param row: The row to transform
    :param expression: The transformation to perform
    :param transformer_mapping: Overrides for the transformer operations

    :return: The transformed result
    """
    if not isinstance(expression, (str, Tree)):
        return expression

    return transform(
        row, parse(expression), transformer_mapping=transformer_mapping
    )


def transform(row, tree, transformer_mapping):
    """
    Performs the transformation on the row

    :param row: The row to transform
    :param tree: The parsed tree for the expression
    :param transformer_mapping: Overrides for the transformer operations

    :return: The transformation result
    """
    transformer_class = create_transformer_class(row, transformer_mapping)
    transformer = transformer_class()

    return transformer.transform(tree)


def parse(expression):
    """
    Parse an expression from the transformation language

    :param expression: The expression to pass

    :return: The parsd expression tree
    """
    if not isinstance(expression, str):
        return expression

    try:
        return parser.parse(expression)
    except lark_exceptions.UnexpectedCharacters as e:
        raise UnexpectedCharacters(
            expression, expression[e.pos_in_stream], e.column
        )


def create_transformer_class(row, transformer_mapping):
    """
    Creates a transformer class from the provided mapping overrides.

    :param row: The row to transform
    :param transformer_mapping: The overrides for the transform functions

    :return: The new transformer class
    """
    def mapped_function(name, *args, **kwargs):
        return transformer_mapping[name](row, *args, **kwargs)

    @v_args(inline=True)
    class TreeTransformer(BaseTreeTransformer):
        lookup = partial(mapped_function, "lookup")
        add = partial(mapped_function, "add")
        subtract = partial(mapped_function, "subtract")
        multiply = partial(mapped_function, "multiply")
        divide = partial(mapped_function, "divide")
        eq = partial(mapped_function, "eq")
        not_eq = partial(mapped_function, "not_eq")
        is_in = partial(mapped_function, "is_in")
        not_in = partial(mapped_function, "not_in")
        gt = partial(mapped_function, "gt")
        gte = partial(mapped_function, "gte")
        lt = partial(mapped_function, "lt")
        lte = partial(mapped_function, "lte")
        logical_not = partial(mapped_function, "logical_not")
        logical_or = partial(mapped_function, "logical_or")
        logical_and = partial(mapped_function, "logical_and")
        any = partial(mapped_function, "any")
        all = partial(mapped_function, "all")
        str_join = partial(mapped_function, "str_join")
        str_replace = partial(mapped_function, "str_replace")
        str_match = partial(mapped_function, "str_match")
        str_search = partial(mapped_function, "str_search")
        replace_multiple = partial(mapped_function, "replace_multiple")
        replace_double = partial(mapped_function, "replace_double")
        array = v_args(inline=False)(list)
        string_escape_re = re.compile(r"`([`'])")

    return TreeTransformer
