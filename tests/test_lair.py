import tempfile
from pathlib import Path

import numpy as np

import datalair


def test_uuid() -> None:
    uuid = datalair.UUID("0123456789ABCDEF")
    assert isinstance(uuid, datalair.UUID)


def test_dataset() -> None:
    class DatasetOne(datalair.Dataset):
        uuid = datalair.generate_random_uuid()

        def derive(self, lair: datalair.Lair) -> None:
            dirpath = lair.get_path(self)
            x = np.linspace(0, 1, 10)
            np.save(dirpath.joinpath("my_array.npy"), x)

    class DatasetTwo(datalair.Dataset):
        uuid = datalair.generate_random_uuid()

        def derive(self, lair: datalair.Lair) -> None:
            dirpath = lair.get_path(self)
            dataset_one = DatasetOne()
            lair.derive(dataset_one)
            filepaths = lair.get_dataset_filepaths(dataset_one)
            assert "my_array.npy" in filepaths
            x = np.load(filepaths["my_array.npy"])
            y = 2 * x
            np.save(dirpath.joinpath("my_array_2.npy"), y)

    with tempfile.TemporaryDirectory() as lair_dir:
        lair = datalair.Lair(path=Path(lair_dir).joinpath("lair"))
        lair.create()
        lair.assert_ok_satus()
        dataset_one = DatasetOne()
        lair.assert_dataset_missing(dataset_one)
        lair.derive(dataset_one)
        lair.assert_dataset_exists(dataset_one)
        filepaths = lair.get_dataset_filepaths(dataset_one)
        assert isinstance(filepaths, dict), type(filepaths)
        assert "my_array.npy" in filepaths
        y = np.load(filepaths["my_array.npy"])
        assert np.all(y == np.linspace(0, 1, 10))

    with tempfile.TemporaryDirectory() as lair_dir:
        lair = datalair.Lair(path=Path(lair_dir).joinpath("lair"))
        lair.create()
        lair.assert_ok_satus()
        dataset_two = DatasetTwo()
        lair.assert_dataset_missing(dataset_two)
        lair.assert_ok_satus()
        assert issubclass(type(dataset_two), datalair.Dataset)
        lair.derive(dataset_two)
        lair.assert_dataset_exists(dataset_two)
        lair.assert_ok_satus()
        filepaths = lair.get_dataset_filepaths(dataset_two)
        assert isinstance(filepaths, dict), type(filepaths)
        assert "my_array_2.npy" in filepaths
        y = np.load(filepaths["my_array_2.npy"])
        assert np.all(y == 2 * np.linspace(0, 1, 10))
