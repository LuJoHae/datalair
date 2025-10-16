import datalair
import pytest
from typing import Any

def test_uuid() -> None:
    uuid = datalair.UUID("0123456789ABCDEF")
    assert isinstance(uuid, datalair.UUID)


def test_uuid_type_error() -> None:
    with pytest.raises(TypeError):
        bad_hex: Any = 0
        _ = datalair.UUID(bad_hex)


def test_uuid_value_error_length() -> None:
    with pytest.raises(ValueError):
        _ = datalair.UUID("0123456789ABCDEF0")


def test_uuid_value_error_range() -> None:
    with pytest.raises(ValueError):
        _ = datalair.UUID("0123456789ABCDEG")
