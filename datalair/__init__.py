from __future__ import annotations
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
from ftplib import FTP
import requests
from tqdm import tqdm
from typing import Optional, Self, Type, Literal
from types import TracebackType


class UUID(str):
    _pattern = re.compile(r'^[0-9a-fA-F]{16}$')

    def __new__(cls, value: str) -> Self:
        if not isinstance(value, str):
            raise TypeError("Hex16String must be created from a string")
        if not cls._pattern.match(value):
            raise ValueError(f"'{value}' is not a valid 16-digit hexadecimal string")
        return str.__new__(cls, value)


def generate_random_uuid() -> UUID:
    return UUID(''.join(choices('0123456789abcdef', k=16)))


class Dataset(ABC):
    _dataset_name: str | UUID
    _namespace: Optional[str]
    _name: str | UUID

    def __init__(self, namespace: Optional[str] = None):
        if hasattr(self, "_self"):
            return

        if hasattr(self, "uuid"):
            self._dataset_name = self.uuid
        else:
            self._dataset_name = self.__class__.__name__

        self._namespace: Optional[str] = namespace


        if self._namespace is None:
            self._name = self._dataset_name
        else:
            self._name = "-".join([self._namespace, self._dataset_name])

    @abstractmethod
    def derive(self, lair: Lair) -> None:
        raise NotImplementedError()


class LairStatus(Enum):
    NOT_EXIST = "not_exist"
    NOT_DIRECTORY = "not_directory"
    CONFIG_MISSING = "config_missing"
    MALFORMED = "malformed"
    OK = "ok"


class Lair:
    def __init__(self, path: Path | str = Path("./lair"), archive_path: Optional[Path | str] = None):
        self._path: Path = Path(path).absolute().resolve()
        self._archive_path: Optional[Path] = None
        if archive_path is not None:
            self._archive_path = Path(archive_path).absolute().resolve()

    def __str__(self) -> str:
        return f"Store at \"{self._path}\""

    def get_path(self, dataset: Optional[Dataset] = None) -> Path:
        if dataset is None:
            return self._path
        elif issubclass(type(dataset), Dataset):
            return self._path.joinpath(str(dataset._name))
        else:
            raise TypeError("Dataset must be a subclass of Dataset!")

    def _get_config_filepath(self) -> Path:
        return self.get_path().joinpath("__config__.json")

    def _create_config_file(self) -> None:
        config = dict(config="")
        json.dump(config, open(self._get_config_filepath(), "w"))

    def dataset_exists(self, dataset: Dataset) -> bool:
        if not issubclass(type(dataset), Dataset):
            raise TypeError("dataset must be a subclass of Dataset! But is {}".format(type(dataset)))
        if not self.get_path(dataset).exists():
            return False
        if not self.get_path(dataset).is_dir():
            return False
        return True

    def status(self) -> LairStatus:
        if not self.get_path().exists():
            return LairStatus.NOT_EXIST
        if not self._get_config_filepath().exists():
            return LairStatus.MALFORMED
        return LairStatus.OK

    def assert_ok_satus(self) -> None:
        assert self.status() is LairStatus.OK,(
            "Store {store} is not ok! It's status is {status}"
            .format(store=self, status=self.status()))

    def assert_dataset_missing(self, dataset: Dataset) -> None:
        self.assert_ok_satus()
        assert self.dataset_exists(dataset) is False, "Dataset {dataset} already exists!".format(dataset=dataset)

    def assert_dataset_exists(self, dataset: Dataset) -> None:
        self.assert_ok_satus()
        assert self.dataset_exists(dataset) is True, "Dataset {dataset} does not exist!".format(dataset=dataset)

    def create(self) -> None:
        # if self.exists():
        #     raise FileExistsError("Store already exists at {path}".format(path=self.get_path()))
        os.makedirs(self.get_path(), exist_ok=False)
        self._create_config_file()

    def create_if_not_exist(self) -> None:
        if self.status() == LairStatus.NOT_EXIST:
            self.create()

    def delete(self, force: bool = False) -> None:
        if not force:
            match self.status():
                case LairStatus.OK:
                    if bool(os.listdir(self.get_path())):
                        raise IOError("Store has elements in it!")
                case LairStatus.NOT_EXIST:
                    raise IOError("Store does not exist!")
                case _:
                    raise IOError("File the path of store points to is not a store!")
        shutil.rmtree(self.get_path())


    def check_store_permissions(self) -> bool:
        """The first three digits (400) mean that the path points to a directory and the last three digits (700 to 777)
        encode the file permissions."""
        return 0o040700 <= self.get_path().stat().st_mode <= 0o040777

    def __enter__(self) -> Self:
        self.assert_ok_satus()
        return self


    def __exit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType],
    ) -> Literal[False]:
        return False

    def delete_from_store(self, dataset: Dataset) -> None:
        self.assert_ok_satus()
        shutil.rmtree(self.get_path(dataset))


    def save_dataset_metadata(self, dataset: Dataset) -> None:
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


    def save_dataset_implementation(self, dataset: Dataset) -> None:
        with open(self.get_path(dataset).joinpath('__dataset__.pkl'), 'wb') as f:
            dill.dump(dataset, file=f)


    def make_dataset_dir(self, dataset: Dataset) -> Path:
        self.assert_dataset_missing(dataset)
        dir_path = self.get_path(dataset)
        dir_path.mkdir(parents=False, exist_ok=False)

        return dir_path

    def derive(self, dataset: Dataset) -> None:
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
            self.delete_from_store(dataset)
            raise e

    def safe_derive(self, dataset: Dataset, overwrite: bool = False) -> None:
        self.assert_ok_satus()
        assert issubclass(type(dataset), Dataset), type(dataset)
        if self.dataset_exists(dataset) and not overwrite:
            return None
        elif self.dataset_exists(dataset) and overwrite:
            self.delete_from_store(dataset)
            self.derive(dataset)
        else:
            self.derive(dataset)

    def get_dataset_filepaths(self, dataset: Dataset) -> dict[str, Path]:
        self.assert_ok_satus()
        self.assert_dataset_exists(dataset)
        return {filepath.name: filepath for filepath in self.get_path(dataset).iterdir()
                if filepath.name not in ("__metadata__.json", "__dataset__.pkl")}

    def delete_all_empty_datasets_from_store(self, dry_run: bool = False) -> None:
        """This is a cleanup function to remove all the datasets from the lair which failed to be derived."""
        self.assert_ok_satus()
        for dataset_dir in self.get_path().iterdir():
            if not dataset_dir.is_dir():
                continue
            if {filepath.name for filepath in dataset_dir.iterdir()}\
                    .difference({"__dataset__.pkl",  "__metadata__.json"}) == set():
                if dry_run:
                    print(f"Need to delete {dataset_dir}")
                else:
                    shutil.rmtree(dataset_dir)


def download_file(url: str, filepath: Path) -> None:
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


def download_supplementary_from_geo(gse_id: str, local_dir: Path) -> None:
    ftp_host = "ftp.ncbi.nlm.nih.gov"
    ftp_dir = "/geo/series/{}nnn/{}/suppl/".format(gse_id[:-3], gse_id)

    # Connect to FTP
    ftp = FTP(ftp_host)
    ftp.login()
    ftp.cwd(ftp_dir)

    files = ftp.nlst()

    os.makedirs(local_dir, exist_ok=True)
    for filename in files:
        local_filepath = local_dir.joinpath(filename)
        with open(local_filepath, 'wb') as f:
            ftp.retrbinary(f'RETR {filename}', f.write)
            print(f'Downloaded: {filename}')

    ftp.quit()


def download_files_from_arrayexpress(arrayexpress_id: str, local_dir: Path) -> None:

    ftp_host = "ftp.ebi.ac.uk"
    ftp_dir = "/biostudies/fire/E-MTAB-/{}/{}/Files".format(arrayexpress_id[-3:], arrayexpress_id)
    ftp = FTP(ftp_host)
    ftp.login()
    ftp.cwd(ftp_dir)
    files = ftp.nlst()

    os.makedirs(local_dir, exist_ok=True)
    for filename in files:
        local_filepath = local_dir.joinpath(filename)
        with open(local_filepath, 'wb') as f:
            ftp.retrbinary(f'RETR {filename}', f.write)
            print(f'Downloaded: {filename}')

    ftp.quit()