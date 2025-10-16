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
        Initializes a class instance. This method sets up internal attributes
        for the dataset name and namespace. It ensures proper naming of the
        instance by either using the 'uuid' attribute or the class name. If
        a namespace is provided, it prefixes the dataset name with the namespace
        joined by a hyphen. This initialization ensures that duplicate instances
        are not created by checking for specific pre-existing attributes.

        Args:
            namespace (Optional[str]): The namespace to be prefixed to the dataset
                name. If no namespace is provided, the dataset name remains
                unprefixed.
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
        Defines an abstract method for deriving behavior specific to a particular
        implementation of the class. This method must be overridden by subclasses
        to provide concrete functionality.

        Args:
            lair: Input parameter of type "Lair" that provides the necessary context or
                data required for the derivation process.
        """
        raise NotImplementedError()
