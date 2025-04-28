from __future__ import annotations
import dataclasses
from abc import ABC, abstractmethod
from pathlib import Path
import re
import json
import os
import shutil
from enum import Enum
from random import choices
from platform import platform, python_version, python_implementation
import sys
from psutil import virtual_memory
from importlib.metadata import distributions
from datetime import datetime
import dill



class UUID(str):
    _pattern = re.compile(r'^[0-9a-fA-F]{16}$')

    def __new__(cls, value):
        if not isinstance(value, str):
            raise TypeError("Hex16String must be created from a string")
        if not cls._pattern.match(value):
            raise ValueError(f"'{value}' is not a valid 16-digit hexadecimal string")
        return str.__new__(cls, value)


def generate_random_uuid() -> UUID:
    return UUID(''.join(choices('0123456789abcdef', k=16)))


class Dataset(ABC):

    @abstractmethod
    def derive(self, lair) -> None:
        raise NotImplementedError()

    @property
    @abstractmethod
    def uuid(self, output_directory: Path) -> UUID:
        raise NotImplementedError()



class LairStatus(Enum):
    NOT_EXIST = "not_exist"
    NOT_DIRECTORY = "not_directory"
    CONFIG_MISSING = "config_missing"
    MALFORMED = "malformed"
    OK = "ok"


class Lair:
    def __init__(self, path: Path = Path("store")):
        self._path = Path(path).absolute().resolve()

    def __str__(self):
        return f"Store at \"{self._path}\""

    def get_path(self, dataset: Dataset | None =None):
        if dataset is None:
            return self._path
        elif issubclass(type(dataset), Dataset):
            return self._path.joinpath(str(dataset.uuid))
        else:
            raise TypeError("Dataset must be a subclass of Dataset!")

    def _get_config_filepath(self):
        return self.get_path().joinpath("__config__.json")

    def _create_config_file(self):
        config = dict(config="")
        json.dump(config, open(self._get_config_filepath(), "w"))

    def dataset_exists(self, dataset):
        if not issubclass(type(dataset), Dataset):
            raise TypeError("dataset must be a subclass of Dataset! But is {}".format(type(dataset)))
        if not self.get_path(dataset).exists():
            return False
        if not self.get_path(dataset).is_dir():
            return False
        return True

    def status(self):
        if not self.get_path().exists():
            return LairStatus.NOT_EXIST
        if not self._get_config_filepath().exists():
            return LairStatus.MALFORMED
        return LairStatus.OK

    def assert_ok_satus(self):
        assert self.status() is LairStatus.OK,(
            "Store {store} is not ok! It's status is {status}"
            .format(store=self, status=self.status()))

    def assert_dataset_missing(self, dataset):
        self.assert_ok_satus()
        assert self.dataset_exists(dataset) is False, "Dataset {dataset} already exists!".format(dataset=dataset)

    def assert_dataset_exists(self, dataset):
        self.assert_ok_satus()
        assert self.dataset_exists(dataset) is True, "Dataset {dataset} does not exist!".format(dataset=dataset)

    def create(self):
        # if self.exists():
        #     raise FileExistsError("Store already exists at {path}".format(path=self.get_path()))
        os.makedirs(self.get_path(), exist_ok=False)
        self._create_config_file()

    def create_if_not_exist(self):
        if self.status() == LairStatus.NOT_EXIST:
            self.create()

    def delete(self, force=False):
        if not force:
            match self.status():
                case LairStatus.OK:
                    if bool(os.listdr(self.get_path())):
                        raise IOError("Store has elements in it!")
                case LairStatus.NOT_EXIST:
                    raise IOError("Store does not exist!")
                case _:
                    raise IOError("File the path of store points to is not a store!")
        shutil.rmtree(self.get_path())


    def check_store_permissions(self):
        """The first three digits (400) mean that the path points to a directory and the last three digits (700 to 777) encode the file permissions."""
        return 0o040700 <= self.get_path().stat().st_mode <= 0o040777

    def __enter__(self):
        self.assert_ok_satus()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def delete_from_store(self, dataset: Dataset):
        self.assert_ok_satus()
        self.get_path(dataset).rmdir()


    def save_dataset_metadata(self, dataset: Dataset):
        metadata = {
            "python": {
                "version": sys.version,
                "implementation": python_implementation(),
                "version": python_version(),
                "path": sys.path,
                "packages": [f"{package.metadata['Name']}=={package.metadata['Version']}" for package in
                             distributions()]
            },
            "hardware+OS": {
                "platform": platform(),
                "total_memory_of_machine": virtual_memory().total,
                "datetime": str(datetime.now())
            }

        }
        json.dump(metadata, open(self.get_path(dataset).joinpath("__metadata__.json"), "w"))


    def save_dataset_implementation(self, dataset: Dataset):
        with open(self.get_path(dataset).joinpath('__dataset__.pkl'), 'wb') as f:
            dill.dump(dataset, file=f)


    def make_dataset_dir(self, dataset: Dataset) -> Path:
        self.assert_dataset_missing(dataset)
        dir_path = self.get_path(dataset)
        dir_path.mkdir(parents=False, exist_ok=False)

        return dir_path

    def derive(self, dataset):
        self.assert_ok_satus()
        assert issubclass(type(dataset), Dataset), type(dataset)
        self.assert_dataset_missing(dataset)
        self.make_dataset_dir(dataset)
        self.save_dataset_metadata(dataset)
        self.save_dataset_implementation(dataset)
        self.assert_dataset_exists(dataset)
        try:
            dataset.derive(self)
        except Exception as e:
            self.delete(dataset)
            raise e

    def get_dataset_filepaths(self, dataset: Dataset) -> dict[str, Path]:
        self.assert_ok_satus()
        self.assert_dataset_exists(dataset)
        return {filepath.name: filepath for filepath in self.get_path(dataset).iterdir()
                if filepath.name not in ("__metadata__.json", "__dataset__.pkl")}