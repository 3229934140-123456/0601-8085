import time
import threading
import uuid
from dataclasses import dataclass
from typing import Optional, Callable

from .kvstore import KVCommand, CommandType, KVResult


@dataclass
class LockResult:
    success: bool
    lease_id: Optional[str] = None
    error: str = None
    stop_keepalive: Optional[Callable] = None


class LockManager:
    def __init__(self, raft_node, kv_store, lease_manager):
        self.raft_node = raft_node
        self.kv_store = kv_store
        self.lease_manager = lease_manager

    def acquire(self, lock_name: str, ttl: float = 10.0,
                timeout: float = 0) -> LockResult:
        lock_key = f"locks/{lock_name}"
        holder_id = str(uuid.uuid4())

        deadline = time.time() + timeout if timeout > 0 else None

        while True:
            if deadline and time.time() > deadline:
                return LockResult(success=False, error="timeout")

            lease_result = self.lease_manager.grant(ttl)
            if not lease_result.get("success"):
                return LockResult(success=False, error=f"lease grant failed: {lease_result.get('error')}")

            lease_id = lease_result["lease_id"]

            cmd = KVCommand(
                type=CommandType.PUT_IF_ABSENT,
                key=lock_key,
                value={"holder": holder_id, "lease_id": lease_id},
                lease_id=lease_id,
            )

            result = None
            event = threading.Event()

            def callback(res):
                nonlocal result
                result = res
                event.set()

            self.raft_node.submit_command(cmd, callback)
            event.wait(timeout=5.0)

            if result is None:
                self.lease_manager.revoke(lease_id)
                if deadline:
                    time.sleep(0.05)
                    continue
                return LockResult(success=False, error="raft timeout")

            if isinstance(result, KVResult) and result.success:
                self.lease_manager.lease_sm.add_key_to_lease(lease_id, lock_key)
                stop_keepalive = self.lease_manager.start_keepalive_daemon(lease_id)
                return LockResult(success=True, lease_id=lease_id, stop_keepalive=stop_keepalive)
            else:
                self.lease_manager.revoke(lease_id)
                if timeout == 0:
                    err = result.error if hasattr(result, 'error') else 'lock held by others'
                    return LockResult(success=False, error=err)
                time.sleep(0.05)

    def release(self, lock_name: str, lease_id: str) -> bool:
        lock_key = f"locks/{lock_name}"

        entry = self.kv_store.get(lock_key)
        if not entry:
            return True

        if entry.lease_id != lease_id:
            return False

        delete_result = None
        delete_event = threading.Event()

        def on_delete(res):
            nonlocal delete_result
            delete_result = res
            delete_event.set()

        cmd = KVCommand(type=CommandType.DELETE, key=lock_key)
        self.raft_node.submit_command(cmd, on_delete)
        delete_event.wait(timeout=5.0)

        self.lease_manager.revoke(lease_id)

        if delete_result and hasattr(delete_result, 'success'):
            return delete_result.success
        return delete_result is not None

    def is_locked(self, lock_name: str) -> bool:
        lock_key = f"locks/{lock_name}"
        return self.kv_store.get(lock_key) is not None

    def get_holder(self, lock_name: str) -> Optional[dict]:
        lock_key = f"locks/{lock_name}"
        entry = self.kv_store.get(lock_key)
        if entry:
            return entry.value
        return None


class DistributedLock:
    def __init__(self, lock_manager: LockManager, name: str, ttl: float = 10.0):
        self.lock_manager = lock_manager
        self.name = name
        self.ttl = ttl
        self._lease_id: Optional[str] = None
        self._locked = False
        self._stop_keepalive: Optional[Callable] = None

    def acquire(self, timeout: float = 0) -> bool:
        if self._locked:
            return True

        result = self.lock_manager.acquire(self.name, self.ttl, timeout)
        if result.success:
            self._lease_id = result.lease_id
            self._stop_keepalive = result.stop_keepalive
            self._locked = True
            return True
        return False

    def release(self) -> bool:
        if not self._locked:
            return True

        if self._stop_keepalive:
            self._stop_keepalive()
            self._stop_keepalive = None

        success = self.lock_manager.release(self.name, self._lease_id)
        if success:
            self._locked = False
            self._lease_id = None
        return success

    def simulate_crash(self) -> None:
        if self._stop_keepalive:
            self._stop_keepalive()
            self._stop_keepalive = None

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

    @property
    def locked(self) -> bool:
        return self._locked

    @property
    def lease_id(self) -> Optional[str]:
        return self._lease_id
