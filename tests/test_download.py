import hashlib
import datalair
from pathlib import Path

def hash_file(path: str | Path, algo: str = "sha256") -> str:
    """Compute the hash of a file using the given algorithm."""
    h = hashlib.new(algo)
    with open(path, "rb") as f:
        # read in 8KB chunks to avoid loading huge files into memory
        for chunk in iter(lambda: f.read(8192), b""):
            h.update(chunk)
    return h.hexdigest()


def test_download_supplementary_from_geo(tmp_path: Path) -> None:
    datalair.download_supplementary_from_geo("GSE25134", tmp_path)
    assert len(list(tmp_path.iterdir())) == 3
    data_filepath = tmp_path.joinpath("GSE25134_RAW.tar")
    assert data_filepath.exists()
    assert hash_file(data_filepath)=="863480a571f6177bd42e6f66cb87db6183a5ab2b81756302038173b54e1b84c9"