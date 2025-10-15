import re
from random import choices
from typing import Self


class UUID(str):
    _pattern = re.compile(r"^[0-9a-fA-F]{16}$")

    def __new__(cls, value: str) -> Self:
        if not isinstance(value, str):
            raise TypeError("Hex16String must be created from a string")
        if not cls._pattern.match(value):
            raise ValueError(f"'{value}' is not a valid 16-digit hexadecimal string")
        return str.__new__(cls, value)


def generate_random_uuid() -> UUID:
    return UUID("".join(choices("0123456789abcdef", k=16)))
