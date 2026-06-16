import threading
import time
from typing import Optional, Callable, Any

from kvstore import WatchEvent


class LockServiceClient:
    def __init__(self, server):
        self._server = server

    def put(self, key: str, value: Any, lease_id: str = None) -> bool:
        return self._server.put(key, value, lease_id)

    def get(self, key: str) -> Optional[Any]:
        return self._server.get(key)

    def delete(self, key: str) -> bool:
        return self._server.delete(key)

    def lease_grant(self, ttl: float, lease_id: str = None) -> dict:
        return self._server.lease_grant(ttl, lease_id)

    def lease_revoke(self, lease_id: str) -> dict:
        return self._server.lease_revoke(lease_id)

    def lease_keepalive(self, lease_id: str) -> dict:
        return self._server.lease_keepalive(lease_id)

    def lock(self, name: str, ttl: float = 10.0):
        return self._server.get_lock(name, ttl)

    def watch(self, key: str = None, prefix: str = None,
              start_revision: int = 0,
              callback: Callable[[WatchEvent], None] = None) -> int:
        return self._server.watch(key=key, prefix=prefix,
                                  start_revision=start_revision,
                                  callback=callback)

    def unwatch(self, watch_id: int) -> bool:
        return self._server.unwatch(watch_id)


class LockLeaseWatcher:
    def __init__(self, client, lock_name: str, ttl: float = 10.0):
        self.client = client
        self.lock_name = lock_name
        self.ttl = ttl
        self._lease_id = None
        self._keepalive_stop = None
        self._lock = threading.Lock()

    def acquire(self, timeout: float = 0) -> bool:
        with self._lock:
            if self._lease_id:
                return True

            result = self.client._server.lock_manager.acquire(
                self.lock_name, self.ttl, timeout
            )
            if result.success:
                self._lease_id = result.lease_id
                self._keepalive_stop = self.client._server.lease_manager.start_keepalive_daemon(
                    self._lease_id
                )
                return True
            return False

    def release(self) -> bool:
        with self._lock:
            if not self._lease_id:
                return True

            if self._keepalive_stop:
                self._keepalive_stop()
                self._keepalive_stop = None

            success = self.client._server.lock_release(self.lock_name, self._lease_id)
            if success:
                self._lease_id = None
            return success

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()

    @property
    def locked(self) -> bool:
        return self._lease_id is not None

    @property
    def lease_id(self) -> Optional[str]:
        return self._lease_id
