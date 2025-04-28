import datalair

def test_uuid():
    uuid = datalair.UUID("0123456789ABCDEF")
    assert isinstance(uuid, datalair.UUID)