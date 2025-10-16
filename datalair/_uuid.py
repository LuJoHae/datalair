"""
This module defines a custom `UUID` class that validates 16-digit hexadecimal strings and provides
an associated utility function to generate random UUIDs.

Classes:
    UUID(str):
        Inherits from `str` and validates that a given input is a 16-character hexadecimal string.
        It includes a strict regular expression pattern for validation.

Methods (of UUID):
    __new__(cls, value: str) -> UUID:
        Validates and constructs a UUID object from the given 16-character hexadecimal string.

Attributes (of UUID):
    _pattern (Pattern): A compiled regex pattern for validating 16-character hexadecimal strings.

Functions:
    generate_random_uuid() -> UUID:
        Generates a random 16-character hexadecimal UUID object.

Dependencies:
    - re: Provides regular expression support for UUID validation.
    - random.choices: Used for generating random hexadecimal strings.
"""

import re
from random import choices
from typing import Self


class UUID(str):
    """
    Represents a 16-digit hexadecimal string with validation.

    This class is used to validate and encapsulate a string that represents a
    16-digit hexadecimal value. It provides strict checks to ensure only valid
    16-character hexadecimal strings are accepted. It inherits from the built-in
    `str` class.

    Attributes:
        _pattern (Pattern): Regular expression pattern used to validate if the
            provided string is a valid 16-digit hexadecimal string.
    """
    _pattern = re.compile(r"^[0-9a-fA-F]{16}$")

    def __new__(cls, value: str) -> Self:
        if not isinstance(value, str):
            raise TypeError("Hex16String must be created from a string")
        if not cls._pattern.match(value):
            raise ValueError(f"'{value}' is not a valid 16-digit hexadecimal string")
        return str.__new__(cls, value)


def generate_random_uuid() -> UUID:
    """
    Generates a random UUID using a pseudo-random selection of hexadecimal digits.

    The function constructs a UUID object by randomly selecting 16 hexadecimal
    characters. This is achieved through the `choices` function, which ensures high
    entropy selection. The resulting string is then converted into a UUID object.

    Returns:
        UUID: A UUID object generated from the random hexadecimal string.
    """
    return UUID("".join(choices("0123456789abcdef", k=16)))
