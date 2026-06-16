import threading
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Callable, Set, Deque


class EventType(Enum):
    PUT = "PUT"
    DELETE = "DELETE"
    COMPACTED = "COMPACTED"


@dataclass
class WatchEvent:
    type: EventType
    key: str
    value: any = None
    prev_value: any = None
    revision: int = 0


@dataclass
class Watcher:
    watch_id: int
    key: Optional[str] = None
    prefix: Optional[str] = None
    start_revision: int = 0
    callback: Callable[[WatchEvent], None] = None
    queue: deque = field(default_factory=deque)
    compacted: bool = False


class WatchManager:
    def __init__(self, kv_store, max_history: int = 1000):
        self._kv_store = kv_store
        self._max_history = max_history
        self._event_history: Deque[WatchEvent] = deque(maxlen=max_history)
        self._watchers: Dict[int, Watcher] = {}
        self._next_watch_id = 0
        self._lock = threading.Lock()
        self._kv_store.register_watch_callback(self._on_event)

    def _on_event(self, event: WatchEvent):
        with self._lock:
            self._event_history.append(event)
            for watcher in self._watchers.values():
                if self._match(watcher, event):
                    watcher.queue.append(event)
                    if watcher.callback:
                        try:
                            watcher.callback(event)
                        except Exception:
                            pass

    def _match(self, watcher: Watcher, event: WatchEvent) -> bool:
        if event.type == EventType.COMPACTED:
            return True
        if watcher.key:
            return event.key == watcher.key
        if watcher.prefix:
            return event.key.startswith(watcher.prefix)
        return True

    def _get_min_revision(self) -> int:
        if not self._event_history:
            return 0
        return self._event_history[0].revision

    def watch(self, key: Optional[str] = None, prefix: Optional[str] = None,
              start_revision: int = 0,
              callback: Callable[[WatchEvent], None] = None) -> int:
        with self._lock:
            watch_id = self._next_watch_id
            self._next_watch_id += 1

            watcher = Watcher(
                watch_id=watch_id,
                key=key,
                prefix=prefix,
                start_revision=start_revision,
                callback=callback,
                queue=deque(),
            )
            self._watchers[watch_id] = watcher

            self._catch_up_watcher(watcher)

            return watch_id

    def _catch_up_watcher(self, watcher: Watcher):
        if watcher.start_revision <= 0:
            return

        min_rev = self._get_min_revision()
        if not self._event_history:
            return

        if watcher.start_revision < min_rev:
            compacted_event = WatchEvent(
                type=EventType.COMPACTED,
                key="",
                revision=min_rev,
            )
            watcher.queue.append(compacted_event)
            watcher.compacted = True
            if watcher.callback:
                try:
                    watcher.callback(compacted_event)
                except Exception:
                    pass

        for event in self._event_history:
            if event.revision >= watcher.start_revision:
                if self._match(watcher, event):
                    watcher.queue.append(event)
                    if watcher.callback:
                        try:
                            watcher.callback(event)
                        except Exception:
                            pass

    def unwatch(self, watch_id: int) -> bool:
        with self._lock:
            if watch_id in self._watchers:
                del self._watchers[watch_id]
                return True
            return False

    def poll(self, watch_id: int, timeout: float = 0) -> Optional[WatchEvent]:
        with self._lock:
            watcher = self._watchers.get(watch_id)
            if not watcher:
                return None
            if watcher.queue:
                return watcher.queue.popleft()

        if timeout <= 0:
            return None

        deadline = time.time() + timeout
        while time.time() < deadline:
            time.sleep(0.01)
            with self._lock:
                watcher = self._watchers.get(watch_id)
                if not watcher:
                    return None
                if watcher.queue:
                    return watcher.queue.popleft()
        return None

    def poll_all(self, watch_id: int, timeout: float = 0) -> List[WatchEvent]:
        events = []
        first = self.poll(watch_id, timeout)
        if first is None:
            return events
        events.append(first)
        while True:
            ev = self.poll(watch_id, 0)
            if ev is None:
                break
            events.append(ev)
        return events

    def get_event_history(self, start_revision: int = 0,
                          key: Optional[str] = None,
                          prefix: Optional[str] = None) -> List[WatchEvent]:
        with self._lock:
            result = []
            for event in self._event_history:
                if event.revision < start_revision:
                    continue
                if key and event.key != key:
                    continue
                if prefix and not event.key.startswith(prefix):
                    continue
                result.append(event)
            return result

    def get_watch_count(self) -> int:
        with self._lock:
            return len(self._watchers)
