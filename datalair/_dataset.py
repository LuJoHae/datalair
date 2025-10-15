from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional

from ._uuid import UUID

if TYPE_CHECKING:
    from ._lair import Lair


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
    def derive(self, lair: "Lair") -> None:
        raise NotImplementedError()
