"""
This module defines the `Dataset` abstract base class, which serves as a blueprint for creating
specific types of datasets. It enforces the structure and methods that must be implemented by
derived classes.

Classes:
    Dataset(ABC):
        Represents an abstract base class for managing datasets, providing functionality to
        initialize dataset names, manage optional namespaces, and enforce the `derive`
        method to be implemented in subclasses.

Attributes (of Dataset):
    _dataset_name (str | UUID): Stores the dataset name, derived from the class name or `uuid`.
    _namespace (Optional[str]): An optional namespace to uniquely identify the dataset.
    _name (str | UUID): Combined namespace and dataset name, or just the dataset name if no namespace is specified.

Methods:
    __init__(self, namespace: Optional[str] = None):
        Initializes the dataset with an optional namespace.

    derive(self, lair: "Lair") -> None:
        An abstract method to be implemented by subclasses to define custom functionality.

Dependencies:
    - abc (Abstract Base Classes): Used to enforce the `derive` method.
    - typing: For type hints.
    - UUID: A utility for handling dataset identifiers.
"""

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional

from ._uuid import UUID

if TYPE_CHECKING:
    from ._lair import Lair


class Dataset(ABC):
    """
    Represents an abstract base class for a dataset.

    This class defines the structure and required functionality for
    derived classes that represent datasets. It initializes and manages
    the naming and optional namespace for the dataset. Derived classes
    must implement the `derive` method to specify their own behavior.

    Attributes:
        _dataset_name (str | UUID): Stores the dataset name, which is either
            derived from the `uuid` attribute or the class name.
        _namespace (Optional[str]): Optional namespace for the dataset, used
            to create a unique dataset name when provided.
        _name (str | UUID): Fully formed name for the dataset, combining the
            namespace and dataset name when a namespace is provided.
    """
    _dataset_name: str | UUID
    _namespace: Optional[str]
    _name: str | UUID

    def __init__(self, namespace: Optional[str] = None):
        """
        Initializes the object with a dataset name and an optional namespace. If a
        namespace is provided, it is combined with the dataset name to form the name
        of the object. Otherwise, the dataset name is used as the name directly.
        The dataset name is determined either by the presence of a `uuid` attribute
        or by the class name of the object.

        :param namespace: An optional string used to qualify the dataset name.
        :type namespace: Optional[str]
        """
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
    def derive(self, lair: "Lair") -> None:
        """
        Derives specific functionality based on the provided lair instance.

        This abstract method serves as a template for subclasses to implement a
        customized behavior tailored to the provided lair. The method must be
        overridden in any subclass to ensure proper execution of the desired operations.

        :param lair: The lair instance that this method will utilize to perform its
            custom implementation. The specific behavior and outcome are determined
            by subclass implementations.
        :type lair: Lair

        :return: This method does not return a value as it is expected to perform its
            operations in place or influence the state of related objects. The
            implementation must handle any required side effects or processing.
        :rtype: None
        """
        raise NotImplementedError()
