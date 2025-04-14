import os
import shutil
import sys
import pickle
import json
from unittest import case

import requests
import logging
from random import choices
from enum import Enum
from shutil import move, rmtree
from logging import getLogger
from psutil import virtual_memory
from re import match
from importlib.metadata import distributions
from datetime import datetime
from pathlib import Path
from functools import partial
from inspect import signature
from platform import platform, python_version, python_implementation
from tqdm import tqdm


# supported file formats
from anndata import read_h5ad, AnnData
from pandas import DataFrame, read_csv
from pandas import read_hdf as read_h5pd

logger = getLogger('datastore')


class DatasetFunction:
    def __init__(self, function, uuid):
        self._function = function
        self._signature = signature(function)
        if isinstance(uuid, int):
            assert 0 <= uuid <= 16 ** 16, "uuid must be between 0 and 16**16"
            uuid = "{:016x}".format(uuid)
        assert bool(match(r'^[0-9a-f]{16}$', uuid)), "uuid must be a 16 digit hexadecimal string"
        self._dataset_uuid = uuid

    def __call__(self, *args, **kwargs):
        return self._function(*args, **kwargs)

    def __name__(self):
        return self._function.__name__

    def get_signature(self):
        return self._signature

    def get_uuid(self):
        return self._dataset_uuid

    def get_metadata(self):
        return {
            "dataset_uuid": self._dataset_uuid,
            "function_name": self._function.__name__,
            "function_hash": hash(self._function),
            "function_signature": repr(self._signature)
        }


def dataset(uuid: str | int):
    return partial(DatasetFunction, uuid=uuid)

def generate_random_uuid():
    return ''.join(choices('0123456789abcdef', k=16))

def download_file(url: str, filepath: Path):
    # Send GET request with streaming
    response = requests.get(url, stream=True)
    total_size = int(response.headers.get('content-length', 0))
    block_size = 1024  # Size of chunks to download

    # Progress bar
    with open(str(filepath), 'wb') as file, tqdm(
            desc=str(filepath),
            total=total_size,
            unit='iB',
            unit_scale=True,
            unit_divisor=1024,
    ) as bar:
        for data in response.iter_content(chunk_size=block_size):
            file.write(data)
            bar.update(len(data))


def save_dataset_file(dataset_, filename, path):
    match dataset_:
        case str():
            regex_match = match(r"^https://zenodo.org/records/[0-9]{8}/files/\w*.(\w*)", dataset_)
            if regex_match is not None:
                download_file(dataset_, path.joinpath(filename + ".{}".format(regex_match.groups()[0])))
            else:
                raise NotImplementedError
        case list() | int() | float() | bool() | dict():
            pickle.dump(dataset_, open(path.joinpath(filename + ".pkl"), mode="wb"))
        case AnnData():
            dataset_.write_h5ad(path.joinpath(filename + ".h5ad"))
        case DataFrame():
            dataset_.to_hdf(path.joinpath(filename + ".h5pd"), key=filename, mode='w')
        case _:
            raise IOError("don't know {} of type {}".format(dataset_, type(dataset_)))


def get_dataset_file_handle(filepath):
    match filepath.suffix:
        case ".h5ad":
            return open(filepath, mode="b")
        case ".txt" | ".csv" | ".tsv":
            return open(filepath, mode="r")
        case _:
            raise IOError("don't know {} of type {}".format(filepath, type(filepath)))


def load_dataset_file(filepath):
    match filepath.suffix:
        case ".pkl":
            dataset_ = pickle.load(open(filepath, mode="rb"))
        case ".h5ad":
            dataset_ = read_h5ad(filepath)
        case ".h5pd":
            dataset_ = read_h5pd(filepath)
        case ".json":
            dataset_ = json.load(open(filepath, mode="r"))
        case ".csv":
            dataset_ = read_csv(filepath)
        case _:
            raise IOError("don't know file type {} | {}".format(filepath.suffix, filepath))
    return dataset_


class DatasetDict(dict):
    def __setitem__(self, key, value):
        if not isinstance(key, str):
            raise TypeError("key must be a string")
        if key in self:
            raise KeyError(f"Key '{key}' already exists. Overwriting is not allowed.")
        super().__setitem__(key, value)


class StoreStatus(Enum):
    NOT_EXIST = "not_exist"
    NOT_DIRECTORY = "not_directory"
    CONFIG_MISSING = "config_missing"
    MALFORMED = "malformed"
    OK = "ok"

class Store:
    def __init__(self, path: Path = Path("store")):
        self._path = Path(path).absolute().resolve()

    def __str__(self):
        return f"Store at \"{self._path}\""

    def get_path(self, dataset_function=None):
        if dataset_function is None:
            return self._path
        elif isinstance(dataset_function, DatasetFunction):
            return self._path.joinpath(str(dataset_function._dataset_uuid))
        else:
            raise TypeError("Dataset_function must be a dataset function!")

    def _get_config_filepath(self):
        return self.get_path().joinpath("__config__.json")

    def _create_config_file(self):
        config = dict(config="")
        json.dump(config, open(self._get_config_filepath(), "w"))

    def dataset_exists(self, dataset_function):
        if not isinstance(dataset_function, DatasetFunction):
            raise TypeError("dataset_function must be a DatasetFunction!")
        if not self.get_path(dataset_function).exists():
            return False
        if not self.get_path(dataset_function).is_dir():
            return False
        return True

    def status(self):
        if not self.get_path().exists():
            return StoreStatus.NOT_EXIST
        if not self._get_config_filepath().exists():
            return StoreStatus.MALFORMED
        return StoreStatus.OK

    def assert_ok_satus(self):
        assert self.status() is StoreStatus.OK,(
            "Store {store} is not ok! It's status is {status}"
            .format(store=self, status=self.status()))

    def create(self):
        # if self.exists():
        #     raise FileExistsError("Store already exists at {path}".format(path=self.get_path()))
        os.makedirs(self.get_path(), exist_ok=False)
        self._create_config_file()

    def create_if_not_exist(self):
        if self.status() == StoreStatus.NOT_EXIST:
            self.create()

    def delete(self, force=False):
        if not force:
            match self.status():
                case StoreStatus.OK:
                    if bool(os.listdr(self.get_path())):
                        raise IOError("Store has elements in it!")
                case StoreStatus.NOT_EXIST:
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

    def delete_from_store(self, dataset_function):
        self.assert_ok_satus()
        if not self.dataset_exists(dataset_function):
            raise FileExistsError("Store does not contain {dataset}".format(dataset=dataset_function))
        os.remove(self.get_path(dataset_function))

    def _resolve_dependencies(self, dataset_function):
        for dependency in dataset_function.get_signature().parameters.values():
            dependency = dependency.default
            if not isinstance(dependency, DatasetFunction):
                raise ValueError(
                    "The dependency of {dataset_function} called {dependency} is not a dataset function!"
                    .format(dataset_function=dataset_function, dependency=dependency))
            if self.dataset_exists(dependency):
                logger.info("Dependency {name} ({uuid}) already exists!".format(name=dependency.__name__,
                                                                                uuid=dependency.get_uuid))
            else:
                self.save_to_store(dependency)

    def _get_temporary_dataset_directory_path(self):
        return self.get_path().joinpath("__tmp_dataset__")

    def _initialize_temporary_dataset_directory(self):
        tmp_dataset_directory = self._get_temporary_dataset_directory_path()
        if tmp_dataset_directory.exists():
            rmtree(tmp_dataset_directory, ignore_errors=True)
        os.mkdir(tmp_dataset_directory)
        return tmp_dataset_directory

    def _save_dataset_metadata(self, dataset_function):
        metadata = {
            "dataset_function": dataset_function.get_metadata(),
            "environment": {
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
        }
        json.dump(metadata, open(self._get_temporary_dataset_directory_path().joinpath("__metadata__.json"), "w"))

    def _save_datasets_to_temporary_dataset_directory(self, datasets: DatasetDict):
        for key, dataset_ in datasets.items():
            save_dataset_file(dataset_, filename=key, path=self._get_temporary_dataset_directory_path())

    def _replace_with_temporary_dataset_directory(self, dataset_function):
        rmtree(self.get_path(dataset_function), ignore_errors=True)
        move(self._get_temporary_dataset_directory_path(), self.get_path(dataset_function))

    def save(self, dataset_function, overwrite=False):
        self.assert_ok_satus()
        if self.dataset_exists(dataset_function) and not overwrite:
            raise FileExistsError(
                "A dataset with the uuid {uuid} already exists!"
                .format(uuid=dataset_function.get_uuid()))
        self._resolve_dependencies(dataset_function=dataset_function)
        self._initialize_temporary_dataset_directory()
        self._save_dataset_metadata(dataset_function)
        datasets = dataset_function()
        self._save_datasets_to_temporary_dataset_directory(datasets)
        self._replace_with_temporary_dataset_directory(dataset_function)

    def load(self, dataset_function, mode="data"):
        self.assert_ok_satus()
        datasets = DatasetDict()
        for file in self.get_path(dataset_function).glob("*"):
            if file.name == "__metadata__.json":
                continue
            match mode:
                case "data":
                    datasets["{}".format(file.stem)] = load_dataset_file(file)
                case "handle":
                    datasets["{}".format(file.stem)] = get_dataset_file_handle(file)
                case "filepath":
                    datasets["{}".format(file.stem)] = file.resolve()
                case _:
                    raise ValueError("Invalid mode {}!".format(mode))
        if datasets == DatasetDict():
            raise FileExistsError("Dataset does not exist yet in store!")
        return datasets