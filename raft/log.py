import threading
from dataclasses import dataclass, field
from typing import List, Any, Optional


@dataclass
class LogEntry:
    term: int
    index: int
    command: Any

    def __repr__(self):
        return f"LogEntry(term={self.term}, index={self.index}, cmd={self.command})"


class LogStore:
    def __init__(self):
        self._entries: List[LogEntry] = []
        self._lock = threading.RLock()
        self._last_applied = -1

    def append(self, entries: List[LogEntry]) -> int:
        with self._lock:
            if not entries:
                return self.last_index()
            for entry in entries:
                if entry.index <= self.last_index():
                    self._entries = self._entries[:entry.index]
                    break
            self._entries.extend(entries)
            return self.last_index()

    def get(self, index: int) -> Optional[LogEntry]:
        with self._lock:
            if index < 0 or index >= len(self._entries):
                return None
            return self._entries[index]

    def last_index(self) -> int:
        with self._lock:
            if not self._entries:
                return -1
            return self._entries[-1].index

    def last_term(self) -> int:
        with self._lock:
            if not self._entries:
                return 0
            return self._entries[-1].term

    def term_at(self, index: int) -> int:
        with self._lock:
            if index < 0 or index >= len(self._entries):
                return 0
            return self._entries[index].term

    def entries_from(self, start_index: int) -> List[LogEntry]:
        with self._lock:
            if start_index < 0:
                return list(self._entries)
            if start_index >= len(self._entries):
                return []
            return list(self._entries[start_index:])

    def truncate_from(self, index: int) -> None:
        with self._lock:
            if index < len(self._entries):
                self._entries = self._entries[:index]

    def length(self) -> int:
        with self._lock:
            return len(self._entries)
