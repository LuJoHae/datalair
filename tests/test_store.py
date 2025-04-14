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
