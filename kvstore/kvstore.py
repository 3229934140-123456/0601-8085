import json
import threading
from enum import Enum
from dataclasses import dataclass, field
from typing import Dict, Optional, Any, List, Callable

from raft import StateMachine


class CommandType(Enum):
    PUT = "put"
    DELETE = "delete"
    PUT_IF_ABSENT = "put_if_absent"
    COMPARE_AND_SWAP = "cas"
    TXN = "txn"


class EventType(Enum):
    PUT = "PUT"
    DELETE = "DELETE"


@dataclass
class KVEntry:
    value: Any
    version: int = 0
    lease_id: Optional[str] = None
    create_revision: int = 0
    mod_revision: int = 0


@dataclass
class KVCommand:
    type: CommandType
    key: Optional[str] = None
    value: Any = None
    expected_version: Optional[int] = None
    lease_id: Optional[str] = None
    ops: List[Dict] = None


@dataclass
class KVResult:
    success: bool
    value: Any = None
    error: str = None
    prev_value: Any = None


@dataclass
class WatchEvent:
    type: EventType
    key: str
    value: Any = None
    prev_value: Any = None
    revision: int = 0


class KVStore(StateMachine):
    def __init__(self):
        self._data: Dict[str, KVEntry] = {}
        self._revision = 0
        self._lock = threading.Lock()
        self._watch_callbacks: List[Callable[[WatchEvent], None]] = []

    def apply(self, command: Any) -> Any:
        if isinstance(command, dict):
            cmd_type = command.get("type")
            if isinstance(cmd_type, str):
                command = KVCommand(type=CommandType(cmd_type), **{k: v for k, v in command.items() if k != "type"})
            else:
                command = KVCommand(type=cmd_type, **{k: v for k, v in command.items() if k != "type"})

        if not isinstance(command, KVCommand):
            return KVResult(success=False, error="invalid command")

        with self._lock:
            self._revision += 1
            revision = self._revision

            if command.type == CommandType.PUT:
                return self._do_put(command.key, command.value, command.lease_id, revision)
            elif command.type == CommandType.DELETE:
                return self._do_delete(command.key, revision)
            elif command.type == CommandType.PUT_IF_ABSENT:
                return self._do_put_if_absent(command.key, command.value, command.lease_id, revision)
            elif command.type == CommandType.COMPARE_AND_SWAP:
                return self._do_cas(command.key, command.expected_version, command.value, command.lease_id, revision)
            elif command.type == CommandType.TXN:
                return self._do_txn(command.ops, revision)
            else:
                return KVResult(success=False, error=f"unknown command type: {command.type}")

    def _do_put(self, key: str, value: Any, lease_id: Optional[str], revision: int) -> KVResult:
        existing = self._data.get(key)
        prev_value = existing.value if existing else None

        if existing:
            entry = KVEntry(
                value=value,
                version=existing.version + 1,
                lease_id=lease_id if lease_id else existing.lease_id,
                create_revision=existing.create_revision,
                mod_revision=revision,
            )
        else:
            entry = KVEntry(
                value=value,
                version=1,
                lease_id=lease_id,
                create_revision=revision,
                mod_revision=revision,
            )

        self._data[key] = entry
        self._notify_watch(WatchEvent(
            type=EventType.PUT,
            key=key,
            value=value,
            prev_value=prev_value,
            revision=revision,
        ))
        return KVResult(success=True, prev_value=prev_value)

    def _do_delete(self, key: str, revision: int) -> KVResult:
        if key not in self._data:
            return KVResult(success=True, prev_value=None)

        existing = self._data.pop(key)
        self._notify_watch(WatchEvent(
            type=EventType.DELETE,
            key=key,
            prev_value=existing.value,
            revision=revision,
        ))
        return KVResult(success=True, prev_value=existing.value)

    def _do_put_if_absent(self, key: str, value: Any, lease_id: Optional[str], revision: int) -> KVResult:
        if key in self._data:
            return KVResult(success=False, error="key exists", prev_value=self._data[key].value)

        entry = KVEntry(
            value=value,
            version=1,
            lease_id=lease_id,
            create_revision=revision,
            mod_revision=revision,
        )
        self._data[key] = entry
        self._notify_watch(WatchEvent(
            type=EventType.PUT,
            key=key,
            value=value,
            prev_value=None,
            revision=revision,
        ))
        return KVResult(success=True, prev_value=None)

    def _do_cas(self, key: str, expected_version: Optional[int], value: Any,
                lease_id: Optional[str], revision: int) -> KVResult:
        existing = self._data.get(key)

        if expected_version is None:
            if existing is not None:
                return KVResult(success=False, error="key exists", prev_value=existing.value)
        else:
            if existing is None or existing.version != expected_version:
                actual_ver = existing.version if existing else None
                return KVResult(success=False, error=f"version mismatch: expected {expected_version}, got {actual_ver}")

        return self._do_put(key, value, lease_id, revision)

    def _do_txn(self, ops: List[Dict], revision: int) -> KVResult:
        if not ops:
            return KVResult(success=True)

        results = []
        for op in ops:
            op_type = op.get("type")
            if op_type == "put":
                res = self._do_put(op["key"], op.get("value"), op.get("lease_id"), revision)
            elif op_type == "delete":
                res = self._do_delete(op["key"], revision)
            elif op_type == "put_if_absent":
                res = self._do_put_if_absent(op["key"], op.get("value"), op.get("lease_id"), revision)
            elif op_type == "cas":
                res = self._do_cas(op["key"], op.get("expected_version"), op.get("value"), op.get("lease_id"), revision)
            else:
                return KVResult(success=False, error=f"unknown op type: {op_type}")

            if not res.success:
                return KVResult(success=False, error=f"txn failed at op: {op_type}, {res.error}")
            results.append(res)

        return KVResult(success=True, value=results)

    def get(self, key: str) -> Optional[KVEntry]:
        with self._lock:
            entry = self._data.get(key)
            if entry:
                return KVEntry(
                    value=entry.value,
                    version=entry.version,
                    lease_id=entry.lease_id,
                    create_revision=entry.create_revision,
                    mod_revision=entry.mod_revision,
                )
            return None

    def get_all(self) -> Dict[str, KVEntry]:
        with self._lock:
            return {k: KVEntry(
                value=v.value,
                version=v.version,
                lease_id=v.lease_id,
                create_revision=v.create_revision,
                mod_revision=v.mod_revision,
            ) for k, v in self._data.items()}

    def get_revision(self) -> int:
        with self._lock:
            return self._revision

    def register_watch_callback(self, cb: Callable[[WatchEvent], None]) -> None:
        with self._lock:
            self._watch_callbacks.append(cb)

    def unregister_watch_callback(self, cb: Callable[[WatchEvent], None]) -> None:
        with self._lock:
            if cb in self._watch_callbacks:
                self._watch_callbacks.remove(cb)

    def _notify_watch(self, event: WatchEvent) -> None:
        callbacks = list(self._watch_callbacks)
        for cb in callbacks:
            try:
                cb(event)
            except Exception:
                pass

    def snapshot(self) -> bytes:
        with self._lock:
            data = {
                "revision": self._revision,
                "data": {
                    k: {
                        "value": v.value,
                        "version": v.version,
                        "lease_id": v.lease_id,
                        "create_revision": v.create_revision,
                        "mod_revision": v.mod_revision,
                    }
                    for k, v in self._data.items()
                }
            }
            return json.dumps(data).encode("utf-8")

    def restore(self, snapshot: bytes) -> None:
        with self._lock:
            data = json.loads(snapshot.decode("utf-8"))
            self._revision = data["revision"]
            self._data = {}
            for k, v in data["data"].items():
                self._data[k] = KVEntry(
                    value=v["value"],
                    version=v["version"],
                    lease_id=v["lease_id"],
                    create_revision=v["create_revision"],
                    mod_revision=v["mod_revision"],
                )
