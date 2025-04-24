import pandas as pd

from datalair import Store, dataset, DatasetDict
import tempfile
import pandas
from pathlib import Path

def test_create_store():
    with tempfile.TemporaryDirectory() as temp_dir:
        store = Store(Path(temp_dir).joinpath("store"))
        store.create()
        store.assert_ok_satus()


def test_zenodo():
    @dataset(uuid="82b0593c70732101")
    def zenodo_dataset() -> str:
        return DatasetDict({"zenodo-test": "https://zenodo.org/records/14330132/files/dataset_dummy_nodes.csv"})

    with tempfile.TemporaryDirectory() as temp_dir:
        store = Store(Path(temp_dir).joinpath("store"))
        store.create()
        store.assert_ok_satus()
        store.save(zenodo_dataset)
        store.assert_ok_satus()

        data = store.load(zenodo_dataset)
        assert type(data) == DatasetDict
        assert isinstance(data["zenodo-test"], pd.DataFrame)

        data = store.load(zenodo_dataset, mode="filepath")
        assert type(data) == DatasetDict
        assert isinstance(data["zenodo-test"], Path)


def test_geo():
    @dataset(uuid="82b0593c70732101")
    def geo_dataset() -> str:
        return DatasetDict({"geo-test": "GSE33126"})

    with tempfile.TemporaryDirectory() as temp_dir:
        store = Store(Path(temp_dir).joinpath("store"))
        store.create()
        store.assert_ok_satus()
        store.save(geo_dataset)
        store.assert_ok_satus()

        data = store.load(geo_dataset)
        assert type(data) == DatasetDict
        assert "GSE33126_non-normalized-expr" in list(data.keys())
        assert isinstance(data["GSE33126_non-normalized-expr"], pd.DataFrame), type(data["GSE33126_non-normalized-expr"])

        data = store.load(geo_dataset, mode="filepath")
        assert type(data) == DatasetDict
        assert isinstance(data["GSE33126_non-normalized-expr"], Path)


def test_file_copy():
    @dataset(uuid="82b0593c70732101")
    def zenodo_dataset() -> str:
        return DatasetDict({"test": Path("./tests/test_store.py")})

    with tempfile.TemporaryDirectory() as temp_dir:
        store = Store(Path(temp_dir).joinpath("store"))
        store.create()
        store.assert_ok_satus()
        store.save(zenodo_dataset)
        store.assert_ok_satus()

        data = store.load(zenodo_dataset, mode="filepath")
        assert isinstance(data["test"], Path)
        assert data["test"].exists()
