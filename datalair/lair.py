import json
import os
import shutil
from enum import Enum
from pathlib import Path

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

    def get_path(self, dataset_function=None):
        if dataset_function is None:
            return self._path
        elif isinstance(dataset_function, Dataset):
            return self._path.joinpath(str(dataset_function._dataset_uuid))
        else:
            raise TypeError("Dataset_function must be a dataset function!")

    def _get_config_filepath(self):
        return self.get_path().joinpath("__config__.json")

    def _create_config_file(self):
        config = dict(config="")
        json.dump(config, open(self._get_config_filepath(), "w"))

    def dataset_exists(self, dataset_function):
        if not isinstance(dataset_function, Dataset):
            raise TypeError("dataset_function must be a DatasetFunction!")
        if not self.get_path(dataset_function).exists():
            return False
        if not self.get_path(dataset_function).is_dir():
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

    def delete_from_store(self, dataset_function):
        self.assert_ok_satus()
        if not self.dataset_exists(dataset_function):
            raise FileExistsError("Store does not contain {dataset}".format(dataset=dataset_function))
        os.remove(self.get_path(dataset_function))