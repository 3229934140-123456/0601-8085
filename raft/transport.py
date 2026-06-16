import threading
import queue
from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
from dataclasses import dataclass


@dataclass
class RaftMessage:
    src: str
    dst: str
    msg_type: str
    payload: Dict[str, Any]


class Transport(ABC):
    @abstractmethod
    def send(self, msg: RaftMessage) -> None:
        pass

    @abstractmethod
    def recv(self, node_id: str, timeout: float = None) -> Optional[RaftMessage]:
        pass

    @abstractmethod
    def register_node(self, node_id: str) -> None:
        pass


class InMemoryTransport(Transport):
    def __init__(self):
        self._nodes: Dict[str, queue.Queue] = {}
        self._lock = threading.Lock()

    def register_node(self, node_id: str) -> None:
        with self._lock:
            if node_id not in self._nodes:
                self._nodes[node_id] = queue.Queue()

    def send(self, msg: RaftMessage) -> None:
        with self._lock:
            if msg.dst in self._nodes:
                self._nodes[msg.dst].put(msg)

    def recv(self, node_id: str, timeout: float = None) -> Optional[RaftMessage]:
        with self._lock:
            if node_id not in self._nodes:
                return None
            q = self._nodes[node_id]
        try:
            return q.get(timeout=timeout)
        except queue.Empty:
            return None
