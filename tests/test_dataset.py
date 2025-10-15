import datalair

def test_uuid() -> None:
    uuid = datalair.UUID("0123456789ABCDEF")
    assert isinstance(uuid, datalair.UUID)