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
        lookup = staticmethod(partial(mapped_function, "lookup"))
        add = staticmethod(partial(mapped_function, "add"))
        subtract = staticmethod(partial(mapped_function, "subtract"))
        multiply = staticmethod(partial(mapped_function, "multiply"))
        divide = staticmethod(partial(mapped_function, "divide"))
        eq = staticmethod(partial(mapped_function, "eq"))
        not_eq = staticmethod(partial(mapped_function, "not_eq"))
        is_in = staticmethod(partial(mapped_function, "is_in"))
        not_in = staticmethod(partial(mapped_function, "not_in"))
        gt = staticmethod(partial(mapped_function, "gt"))
        gte = staticmethod(partial(mapped_function, "gte"))
        lt = staticmethod(partial(mapped_function, "lt"))
        lte = staticmethod(partial(mapped_function, "lte"))
        logical_not = staticmethod(partial(mapped_function, "logical_not"))
        logical_or = staticmethod(partial(mapped_function, "logical_or"))
        logical_and = staticmethod(partial(mapped_function, "logical_and"))
        any = staticmethod(partial(mapped_function, "any"))
        all = staticmethod(partial(mapped_function, "all"))
        str_join = staticmethod(partial(mapped_function, "str_join"))
        str_replace = staticmethod(partial(mapped_function, "str_replace"))
        str_match = staticmethod(partial(mapped_function, "str_match"))
        str_search = staticmethod(partial(mapped_function, "str_search"))
        replace_multiple = staticmethod(partial(mapped_function, "replace_multiple"))
        replace_double = staticmethod(partial(mapped_function, "replace_double"))
        array = v_args(inline=False)(list)
        string_escape_re = re.compile(r"`([`'])")

    return TreeTransformer
