

import json
import os
import shutil
import sys
from datetime import datetime
from enum import Enum
from importlib.metadata import distributions
from pathlib import Path
from platform import platform, python_implementation, python_version
from types import TracebackType
from typing import Literal, Optional, Self, Type

import dill
from psutil import virtual_memory

from ._dataset import Dataset


class LairStatus(Enum):
    """
    Represents the possible statuses of a 'Lair' directory and its configuration.

    Defines an enumeration for representing various statuses that a 'Lair'
    directory and its associated configuration can have. This is primarily
    used to indicate the state or validity of a directory with respect to its
    structure and configuration file.

    Attributes:
        NOT_EXIST: Indicates that the directory does not exist.
        NOT_DIRECTORY: Indicates that the specified path exists but is not a directory.
        CONFIG_MISSING: Indicates that the required configuration file is missing in the directory.
        MALFORMED: Indicates that the configuration file exists but is improperly formatted.
        OK: Indicates that the directory and its configuration file are valid and properly formatted.
    """
    NOT_EXIST = "not_exist"
    NOT_DIRECTORY = "not_directory"
    CONFIG_MISSING = "config_missing"
    MALFORMED = "malformed"
    OK = "ok"


class Lair:
    """
    This class represents a data storage system called "Lair" for managing structured
    datasets. It provides functionality to check, create, delete, and manage datasets,
    as well as maintain metadata and configurations. Additionally, it supports operations
    to ensure data integrity and applicable directory permissions.

    Detailed description of the class, its purpose, and usage.

    Attributes:
        _path (Path): Absolute path to the main storage directory for the lair.
        _archive_path (Optional[Path]): Absolute path to an optional archive directory
            for storing supplementary data.
    """
    def __init__(
        self, path: Path | str = Path("./lair"), archive_path: Optional[Path | str] = None
    ):
        """Initializes an instance of the class with specified paths.

        The constructor sets up the main path and optional archive path, converting
        them to absolute and resolved Path objects. If the archive_path is not
        provided, it remains as None.

        Args:
            path (Path | str): The main path, which is used and stored as an
                absolute, resolved Path object. Defaults to './lair'.
            archive_path (Optional[Path | str]): The optional archive path, which,
                if provided, is stored as an absolute, resolved Path object. Defaults
                to None.
        """
        self._path: Path = Path(path).absolute().resolve()
        self._archive_path: Optional[Path] = None
        if archive_path is not None:
            self._archive_path = Path(archive_path).absolute().resolve()

    def __str__(self) -> str:
        """
        Returns a string representation of the object.

        The method provides a meaningful string that represents the object. This
        can be used for debugging or display purposes. The returned string
        includes the object's specific state or data.

        Returns:
            str: A string representation of the object.
        """
        return f'Store at "{self._path}"'

    def get_path(self, dataset: Optional[Dataset] = None) -> Path:
        """
        Gets the path associated with the dataset.

        This method retrieves either the default path or the path corresponding to
        the given dataset. If no dataset is provided, the default path is returned.
        If a dataset is provided and it is a subclass of `Dataset`, the dataset's
        specific path is concatenated to the default path. If the provided dataset
        parameter is not a subclass of `Dataset`, a `TypeError` is raised.

        Args:
            dataset (Optional[Dataset]): The dataset object which defines a specific
                path. If not provided, defaults to `None`, returning the default path.

        Returns:
            Path: The resulting path, either the default path or the combined path
            for the given dataset.

        Raises:
            TypeError: If the provided dataset is not a subclass of `Dataset`.
        """
        if dataset is None:
            return self._path
        elif issubclass(type(dataset), Dataset):
            return self._path.joinpath(str(dataset._name))
        else:
            raise TypeError("Dataset must be a subclass of Dataset!")

    def _get_config_filepath(self) -> Path:
        """
        Retrieves the file path to the configuration file.

        This method constructs and returns the file path to the
        configuration file named "__config__.json". It utilizes the
        `get_path` method to retrieve the base path and appends the
        specific file name to it.

        Returns:
            Path: A Path object representing the configuration file's location.
        """
        return self.get_path().joinpath("__config__.json")

    def _create_config_file(self) -> None:
        """
        Writes a default configuration file in JSON format to the disk.

        This method generates a configuration dictionary with a default value
        and writes it to a file specified by `_get_config_filepath`. It ensures
        that the configuration file is created and saved in the appropriate
        location with the required structure.

        Args:
            None

        Raises:
            None

        Returns:
            None
        """
        config = dict(config="")
        json.dump(config, open(self._get_config_filepath(), "w"))

    def dataset_exists(self, dataset: Dataset) -> bool:
        """
        Checks whether the dataset exists and is a valid directory.

        This method verifies if the provided `dataset` is a subclass of `Dataset`, checks the
        existence of the dataset's path, and confirms that the path is a directory.

        Args:
            dataset: The dataset object to validate.

        Returns:
            bool: True if the dataset exists and is a directory, False otherwise.

        Raises:
            TypeError: If the provided dataset is not a subclass of `Dataset`.
        """
        if not issubclass(type(dataset), Dataset):
            raise TypeError(
                "dataset must be a subclass of Dataset! But is {}".format(type(dataset))
            )
        if not self.get_path(dataset).exists():
            return False
        if not self.get_path(dataset).is_dir():
            return False
        return True

    def status(self) -> LairStatus:
        """
        Determines the current status of the lair by checking the existence and validity
        of its path and configuration file.

        The method evaluates the state of the lair based on the presence of its path
        and a valid configuration file. If the path does not exist, the lair status
        is considered as NOT_EXIST. If the path exists but the configuration file is
        missing, it is marked as MALFORMED. Otherwise, the lair is deemed OK.

        Returns:
            LairStatus: The status of the lair, which can be one of the following:
                - LairStatus.NOT_EXIST: The lair path does not exist.
                - LairStatus.MALFORMED: The lair exists but the configuration file
                  is missing.
                - LairStatus.OK: The lair is properly set up and its path and
                  configuration are intact.
        """
        if not self.get_path().exists():
            return LairStatus.NOT_EXIST
        if not self._get_config_filepath().exists():
            return LairStatus.MALFORMED
        return LairStatus.OK

    def assert_ok_satus(self) -> None:
        """
        Asserts that the current status of the store is LairStatus.OK.

        This method verifies the status of the store object to ensure it is in a valid
        and acceptable state (LairStatus.OK). If the status does not match, an assertion
        error is raised with a detailed error message containing the store information
        and its current status.

        Raises:
            AssertionError: If the store status is not LairStatus.OK. The error message
            includes details about the store and its current status.
        """
        assert self.status() is LairStatus.OK, (
            "Store {store} is not ok! It's status is {status}".format(
                store=self, status=self.status()
            )
        )

    def assert_dataset_missing(self, dataset: Dataset) -> None:
        """
        Asserts that the specified dataset does not exist. If the dataset exists, an assertion
        error is raised with an appropriate error message. This method ensures its operation
        by first checking the system status is valid.

        Args:
            dataset (Dataset): The dataset to verify for non-existence.
        """
        self.assert_ok_satus()
        assert self.dataset_exists(dataset) is False, "Dataset {dataset} already exists!".format(
            dataset=dataset
        )

    def assert_dataset_exists(self, dataset: Dataset) -> None:
        """
        Asserts that a specified dataset exists in the current context.

        This method verifies the existence of the provided dataset by checking internal
        conditions or performing necessary validations. It ensures the dataset is present
        and accessible, raising an assertion error if the dataset is not found.

        Args:
            dataset (Dataset): The dataset object to validate for existence.

        Raises:
            AssertionError: If the specified dataset does not exist.
        """
        self.assert_ok_satus()
        assert self.dataset_exists(dataset) is True, "Dataset {dataset} does not exist!".format(
            dataset=dataset
        )

    def create(self) -> None:
        """
        Creates a new store directory and its configuration file.

        This method is responsible for creating the necessary directory structure
        and associated configuration file for a store. It ensures the store does
        not already exist and creates the directory at the specified path. The
        configuration file is created within the newly created directory.

        Raises:
            FileExistsError: If the directory for the store already exists.
        """
        # if self.exists():
        #     raise FileExistsError("Store already exists at {path}".format(path=self.get_path()))
        os.makedirs(self.get_path(), exist_ok=False)
        self._create_config_file()

    def create_if_not_exist(self) -> None:
        """
        Checks the current status and creates the instance if it does not exist.

        This method verifies the status of the instance, and if its status is
        identified as `NOT_EXIST`, it proceeds to create the instance to ensure its
        existence. The method does not return any value.

        Raises:
            Any exception that may occur during the status check or instance creation.

        Returns:
            None
        """
        if self.status() == LairStatus.NOT_EXIST:
            self.create()

    def delete(self, force: bool = False) -> None:
        """
        Deletes the store directory at the current path.

        This method removes the directory associated with the store object. If the
        `force` parameter is not set to True, the deletion is subject to checks based
        on the current status of the store. The method ensures the store is empty or
        does not contain elements before performing deletion, and it raises an
        appropriate error if the store does not exist or is not valid.

        Args:
            force (bool): If True, forces the deletion of the store directory without
                performing status checks or validations. Defaults to False.

        Raises:
            IOError: If the store contains elements and `force` is False.
            IOError: If the store does not exist and `force` is False.
            IOError: If the path refers to a file that is not a valid store and
                `force` is False.
        """
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
        """
        Checks if the store permissions are valid.

        This method evaluates the permissions of a file or directory at the path
        returned by `get_path()`. It ensures the permissions fall within the
        range of valid store permissions.

        Returns:
            bool: True if the permissions are valid, False otherwise.
        """
        return 0o040700 <= self.get_path().stat().st_mode <= 0o040777

    def __enter__(self) -> Self:
        """
        Manages the resource context and ensures the resource is in an acceptable
        state upon entering the context. This method should be used with a context
        manager to guarantee that the resource status is validated before it is used.

        Returns:
            Self: An instance of the class this method belongs to, ensuring the
            context is correctly initialized for usage.
        """
        self.assert_ok_satus()
        return self

    def __exit__(
        self,
        exc_type: Optional[Type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[TracebackType],
    ) -> Literal[False]:
        """
        Handles the context manager exit by explicitly returning False to indicate that exceptions
        should not be suppressed during context management.

        Args:
            exc_type: The class of the exception that occurred, if any; otherwise, None.
            exc_val: The exception instance that occurred, if any; otherwise, None.
            exc_tb: The traceback object associated with the exception, if any; otherwise, None.

        Returns:
            Literal[False]: Always returns False, which specifies that exceptions will not
            be suppressed and will propagate normally.
        """
        return False

    def delete_from_store(self, dataset: Dataset) -> None:
        """
        Deletes a dataset from the storage.

        This method removes the specified dataset directory from the file system,
        ensuring it is no longer available in the storage.

        Args:
            dataset (Dataset): The dataset to be deleted.
        """
        self.assert_ok_satus()
        shutil.rmtree(self.get_path(dataset))

    def save_dataset_metadata(self, dataset: Dataset) -> None:
        """
        Saves metadata of the given dataset including information about the Python environment,
        hardware, and operating system.

        This method captures Python-related metadata (version, implementation, active packages),
        and system information (platform, memory, current date and time) and saves it in JSON
        format as part of the dataset.

        Args:
            dataset (Dataset): The dataset for which the metadata will be saved.

        """
        metadata = {
            "python": {
                "version": sys.version,
                "implementation": python_implementation(),
                "python-version": python_version(),
                "path": sys.path,
                "packages": [
                    f"{package.metadata['Name']}=={package.metadata['Version']}"
                    for package in distributions()
                ],
            },
            "hardware+OS": {
                "platform": platform(),
                "total_memory_of_machine": virtual_memory().total,
                "datetime": str(datetime.now()),
            },
        }
        json.dump(metadata, open(self.get_path(dataset).joinpath("__metadata__.json"), "w"))

    def save_dataset_implementation(self, dataset: Dataset) -> None:
        """
        Saves the given dataset implementation to a specified file path.

        This method serializes the provided dataset object and saves it as a
        pickle file using the dill module. The file is stored in a directory
        defined by the path generated for the dataset. It overwrites the contents
        if a file with the same name already exists.

        Args:
            dataset (Dataset): The dataset object to be serialized and saved.
        """
        with open(self.get_path(dataset).joinpath("__dataset__.pkl"), "wb") as f:
            dill.dump(dataset, file=f)

    def make_dataset_dir(self, dataset: Dataset) -> Path:
        """
        Creates a directory for the specified dataset if it does not already exist.

        The method first verifies that the dataset is not already marked as existing. Once confirmed, it determines the
        directory path corresponding to the dataset and creates the directory. The created directory path is then returned.

        Args:
            dataset: An instance of the Dataset class representing the dataset for which the directory is to be created.

        Returns:
            Path: The path to the newly created directory.

        Raises:
            FileExistsError: If the directory already exists.
            AssertionError: If the dataset is marked as missing.
        """
        self.assert_dataset_missing(dataset)
        dir_path = self.get_path(dataset)
        dir_path.mkdir(parents=False, exist_ok=False)

        return dir_path

    def derive(self, dataset: Dataset) -> None:
        """
        Derives a new dataset by following a structured workflow and validates its creation.

        This method performs several steps such as checking the dataset's status and existence,
        creating necessary directories, saving metadata and implementation details, and invoking
        the dataset-specific `derive` implementation. In case of errors during the derivation
        process, it cleans up the dataset store to maintain consistency.

        Args:
            dataset (Dataset): The dataset object to be derived. It is expected to
                be a subclass of the Dataset base class.

        Raises:
            AssertionError: If the dataset is not of the correct type or if its
                status or existence in the store cannot be verified.
            Exception: If any error occurs during the dataset derivation process. The
                raised exception will propagate after executing necessary cleanup.
        """
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
        """
        Executes a safe derivation process for the provided dataset, ensuring that existing datasets are not overwritten
        unless explicitly specified. This method combines checks for pre-existing datasets, handling deletion if necessary,
        and invokes dataset derivation logic. The overwrite parameter controls whether an existing dataset should be
        overwritten.

        Args:
            dataset: The dataset to be processed. Must be an instance or subclass of the Dataset type.
            overwrite: A boolean flag indicating whether to overwrite the existing dataset in the store. Defaults to False.
        """
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
        """
        Returns a dictionary mapping file names to their respective file paths for a given dataset,
        excluding specific metadata and dataset files.

        Args:
            dataset (Dataset): The dataset object for which file paths are to be retrieved.

        Returns:
            dict[str, Path]: A dictionary where keys are the file names and values are the file paths
            for the respective files within the dataset directory.

        """
        self.assert_ok_satus()
        self.assert_dataset_exists(dataset)
        return {
            filepath.name: filepath
            for filepath in self.get_path(dataset).iterdir()
            if filepath.name not in ("__metadata__.json", "__dataset__.pkl")
        }

    def get_archive_filepaths(self) -> Optional[dict[str, Path]]:
        """
        Retrieves a dictionary mapping filenames to their corresponding file paths from the archive path.

        This method ensures that the system is in a valid state before proceeding. If
        the `_archive_path` attribute is `None`, the method returns `None`. Otherwise,
        it constructs and returns a dictionary where each key is a filename and the
        value is the corresponding file path from the specified archive directory.

        Returns:
            Optional[dict[str, Path]]: A dictionary mapping filenames to their
            corresponding `Path` objects from the archive path, or `None` if the
            archive path is not set.
        """
        self.assert_ok_satus()
        if self._archive_path is None:
            return None
        return {
            filepath.name: filepath
            for filepath in self._archive_path.iterdir()
        }

    def delete_all_empty_datasets_from_store(self, dry_run: bool = False) -> None:
        """
        Deletes all empty datasets from the store directory. A dataset is considered empty
        if its directory only contains the files '__dataset__.pkl' and '__metadata__.json'.
        The method can optionally run in dry-run mode to only log the directories that
        would have been deleted without actually deleting them.

        Args:
            dry_run (bool): If True, prints the dataset directories that would be
                deleted instead of deleting them.
        """
        self.assert_ok_satus()
        for dataset_dir in self.get_path().iterdir():
            if not dataset_dir.is_dir():
                continue
            if {filepath.name for filepath in dataset_dir.iterdir()}.difference(
                {"__dataset__.pkl", "__metadata__.json"}
            ) == set():
                if dry_run:
                    print(f"Need to delete {dataset_dir}")
                else:
                    shutil.rmtree(dataset_dir)
