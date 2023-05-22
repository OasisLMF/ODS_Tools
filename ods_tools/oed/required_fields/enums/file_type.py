"""
This file defines the
"""
from enum import Enum


class FileType(Enum):
    """
    Defines the file types supported.

    Attributes:
        Acc: The account file.
        Loc: The location file.
        ReinsScope: The reinsurance scope file.
        ReinsInfo: The reinsurance info file.
        null: The null file.
    """
    Acc = "Acc"
    Loc = "Loc"
    ReinsScope = "ReinsScope"
    ReinsInfo = "ReinsInfo"
    null = "null"
