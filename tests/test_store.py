import pandas as pd
from datalair import Store, dataset, DatasetDict
import tempfile
from pathlib import Path


def test_create_store():
    with tempfile.TemporaryDirectory() as temp_dir:
        store = Store(Path(temp_dir).joinpath("store"))
        store.create()
        store.assert_ok_satus()


def test_zenodo():
    @dataset(uuid="0000000000000000")
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
        assert "zenodo-test.csv" in list(data.keys()), list(data.keys())
        assert isinstance(data["zenodo-test.csv"], pd.DataFrame)

        data = store.load(zenodo_dataset, mode="filepath")
        assert type(data) == DatasetDict
        assert "zenodo-test.csv" in list(data.keys()), list(data.keys())
        assert isinstance(data["zenodo-test.csv"], Path)


def test_geo():
    @dataset(uuid="0000000000000000")
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
        assert "GSE33126_non-normalized-expr.txt.gz" in list(data.keys())
        assert isinstance(data["GSE33126_non-normalized-expr.txt.gz"], pd.DataFrame), type(data["GSE33126_non-normalized-expr.txt.gz"])

        data = store.load(geo_dataset, mode="filepath")
        assert type(data) == DatasetDict
        assert isinstance(data["GSE33126_non-normalized-expr.txt.gz"], Path)


def test_arrayexpress():
    @dataset(uuid="0000000000000000")
    def arrayexpress_dataset() -> str:
        return DatasetDict({"arrayexpress-test": "E-MTAB-7143"})

    with tempfile.TemporaryDirectory() as temp_dir:
        store = Store(Path(temp_dir).joinpath("store"))
        store.create()
        store.assert_ok_satus()
        store.save(arrayexpress_dataset)
        store.assert_ok_satus()

        data = store.load(arrayexpress_dataset, mode="filepath")
        assert type(data) == DatasetDict
        assert "E-MTAB-7143.sdrf.txt" in list(data.keys())
        assert isinstance(data["E-MTAB-7143.sdrf.txt"], Path)


def test_file_copy():
    @dataset(uuid="0000000000000000")
    def zenodo_dataset() -> str:
        return DatasetDict({"test": Path("./tests/test_store.py")})

    with tempfile.TemporaryDirectory() as temp_dir:
        store = Store(Path(temp_dir).joinpath("store"))
        store.create()
        store.assert_ok_satus()
        store.save(zenodo_dataset)
        store.assert_ok_satus()

        data = store.load(zenodo_dataset, mode="filepath")
        assert isinstance(data["test.py"], Path)
        assert data["test.py"].exists()


def test_dataset_dependency_resolution():
    @dataset(uuid="0000000000000000")
    def original_dataset():
        return DatasetDict({"test-original": Path("./tests/test_store.py")})

    @dataset(uuid="0000000000000001")
    def new_dataset(old_dataset=original_dataset):
        return DatasetDict({"test-new": old_dataset()})

    with tempfile.TemporaryDirectory() as temp_dir:
        store = Store(Path(temp_dir).joinpath("store"))
        store.create()
        store.assert_ok_satus()
        store.save(new_dataset)
        store.assert_ok_satus()

        data = store.load(new_dataset, mode="filepath")
        assert "test-new.pkl" in data.keys(), data.keys()
        assert isinstance(data["test-new.pkl"], Path)
        assert data["test-new.pkl"].exists()
