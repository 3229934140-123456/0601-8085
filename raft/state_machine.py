from abc import ABC, abstractmethod
from typing import Any


class StateMachine(ABC):
    @abstractmethod
    def apply(self, command: Any) -> Any:
        pass

    @abstractmethod
    def snapshot(self) -> bytes:
        pass

    @abstractmethod
    def restore(self, snapshot: bytes) -> None:
        pass
