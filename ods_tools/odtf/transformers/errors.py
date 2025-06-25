from ..errors import ConverterError


class UnexpectedCharacters(ConverterError):
    """
    Error raised when there's an unexpected character in the transformation.
    """

    def __init__(self, expression, char, position):
        super().__init__(
            f"Unexpected character in '{expression}':"
            f" '{char}' at position {position}"
        )
