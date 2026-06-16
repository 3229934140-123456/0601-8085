import time
import uuid
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Callable, Set
from enum import Enum


class LeaseCommandType(Enum):
    GRANT = "lease_grant"
    REVOKE = "lease_revoke"
    KEEPALIVE = "lease_keepalive"
    LEASE_EXPIRE = "lease_expire"


@dataclass
class LeaseCommand:
    type: LeaseCommandType
    lease_id: Optional[str] = None
    ttl: float = 0


@dataclass
class Lease:
    lease_id: str
    ttl: float
    expire_time: float
    keys: Set[str] = field(default_factory=set)

    def is_expired(self) -> bool:
        return time.time() > self.expire_time

    def remaining_ttl(self) -> float:
        return max(0, self.expire_time - time.time())


class LeaseStateMachine:
    def __init__(self, kv_store, on_expire_callback: Callable[[str, List[str]], None] = None):
        self._leases: Dict[str, Lease] = {}
        self._kv_store = kv_store
        self._on_expire_callback = on_expire_callback
        self._lock = threading.Lock()

    def apply(self, command: any) -> any:
        if isinstance(command, dict):
            cmd_type = command.get("type")
            if isinstance(cmd_type, str):
                command = LeaseCommand(type=LeaseCommandType(cmd_type),
                                       **{k: v for k, v in command.items() if k != "type"})
            else:
                command = LeaseCommand(type=cmd_type,
                                       **{k: v for k, v in command.items() if k != "type"})

        if not isinstance(command, LeaseCommand):
            return {"success": False, "error": "invalid lease command"}

        with self._lock:
            if command.type == LeaseCommandType.GRANT:
                return self._do_grant(command.lease_id, command.ttl)
            elif command.type == LeaseCommandType.REVOKE:
                return self._do_revoke(command.lease_id)
            elif command.type == LeaseCommandType.KEEPALIVE:
                return self._do_keepalive(command.lease_id)
            elif command.type == LeaseCommandType.LEASE_EXPIRE:
                return self._do_expire(command.lease_id)
            else:
                return {"success": False, "error": f"unknown command: {command.type}"}

    def _do_grant(self, lease_id: Optional[str], ttl: float) -> Dict:
        if not lease_id:
            lease_id = str(uuid.uuid4())

        if lease_id in self._leases:
            return {"success": False, "error": "lease already exists", "lease_id": lease_id}

        expire_time = time.time() + ttl
        lease = Lease(
            lease_id=lease_id,
            ttl=ttl,
            expire_time=expire_time,
        )
        self._leases[lease_id] = lease
        return {"success": True, "lease_id": lease_id, "ttl": ttl}

    def _do_revoke(self, lease_id: str) -> Dict:
        if lease_id not in self._leases:
            return {"success": False, "error": "lease not found"}

        lease = self._leases.pop(lease_id)
        keys = list(lease.keys)

        if self._on_expire_callback:
            self._on_expire_callback(lease_id, keys)

        return {"success": True, "lease_id": lease_id, "keys": keys}

    def _do_keepalive(self, lease_id: str) -> Dict:
        if lease_id not in self._leases:
            return {"success": False, "error": "lease not found"}

        lease = self._leases[lease_id]
        lease.expire_time = time.time() + lease.ttl

        return {"success": True, "lease_id": lease_id, "ttl": lease.remaining_ttl()}

    def _do_expire(self, lease_id: str) -> Dict:
        if lease_id not in self._leases:
            return {"success": False, "error": "lease not found"}

        lease = self._leases.pop(lease_id)
        keys = list(lease.keys)

        if self._on_expire_callback:
            self._on_expire_callback(lease_id, keys)

        return {"success": True, "lease_id": lease_id, "keys": keys, "expired": True}

    def get_lease(self, lease_id: str) -> Optional[Lease]:
        with self._lock:
            lease = self._leases.get(lease_id)
            if lease:
                return Lease(
                    lease_id=lease.lease_id,
                    ttl=lease.ttl,
                    expire_time=lease.expire_time,
                    keys=set(lease.keys),
                )
            return None

    def get_all_leases(self) -> Dict[str, Lease]:
        with self._lock:
            return {
                k: Lease(
                    lease_id=v.lease_id,
                    ttl=v.ttl,
                    expire_time=v.expire_time,
                    keys=set(v.keys),
                )
                for k, v in self._leases.items()
            }

    def add_key_to_lease(self, lease_id: str, key: str) -> bool:
        with self._lock:
            if lease_id not in self._leases:
                return False
            self._leases[lease_id].keys.add(key)
            return True

    def remove_key_from_lease(self, lease_id: str, key: str) -> bool:
        with self._lock:
            if lease_id not in self._leases:
                return False
            self._leases[lease_id].keys.discard(key)
            return True

    def get_expired_leases(self) -> List[str]:
        with self._lock:
            now = time.time()
            return [lid for lid, lease in self._leases.items() if lease.expire_time <= now]


class LeaseManager:
    def __init__(self, raft_node, kv_store, lease_sm=None):
        self.raft_node = raft_node
        self.kv_store = kv_store
        if lease_sm is None:
            self.lease_sm = LeaseStateMachine(kv_store, self._on_lease_expire)
        else:
            self.lease_sm = lease_sm
            self.lease_sm._on_expire_callback = self._on_lease_expire
        self._running = False
        self._check_thread = None
        self._lock = threading.Lock()

    def start(self):
        self._running = True
        self._check_thread = threading.Thread(target=self._expiry_check_loop, daemon=True)
        self._check_thread.start()

    def stop(self):
        self._running = False
        if self._check_thread:
            self._check_thread.join(timeout=1.0)

    def _expiry_check_loop(self):
        while self._running:
            time.sleep(0.1)
            if not self.raft_node.is_leader():
                continue

            expired = self.lease_sm.get_expired_leases()
            for lease_id in expired:
                self._submit_expire(lease_id)

    def _submit_expire(self, lease_id: str):
        cmd = LeaseCommand(
            type=LeaseCommandType.LEASE_EXPIRE,
            lease_id=lease_id,
        )
        self.raft_node.submit_command(cmd)

    def _on_lease_expire(self, lease_id: str, keys: List[str]):
        from .kvstore import KVCommand, CommandType
        for key in keys:
            cmd = KVCommand(type=CommandType.DELETE, key=key)
            self.raft_node.submit_command(cmd)

    def grant(self, ttl: float, lease_id: str = None) -> dict:
        cmd = LeaseCommand(
            type=LeaseCommandType.GRANT,
            lease_id=lease_id,
            ttl=ttl,
        )

        result = {"success": False, "error": "timeout"}
        event = threading.Event()

        def callback(res):
            nonlocal result
            result = res
            event.set()

        self.raft_node.submit_command(cmd, callback)
        event.wait(timeout=5.0)
        return result

    def revoke(self, lease_id: str) -> dict:
        cmd = LeaseCommand(
            type=LeaseCommandType.REVOKE,
            lease_id=lease_id,
        )

        result = {"success": False, "error": "timeout"}
        event = threading.Event()

        def callback(res):
            nonlocal result
            result = res
            event.set()

        self.raft_node.submit_command(cmd, callback)
        event.wait(timeout=5.0)
        return result

    def keepalive(self, lease_id: str) -> dict:
        cmd = LeaseCommand(
            type=LeaseCommandType.KEEPALIVE,
            lease_id=lease_id,
        )

        result = {"success": False, "error": "timeout"}
        event = threading.Event()

        def callback(res):
            nonlocal result
            result = res
            event.set()

        self.raft_node.submit_command(cmd, callback)
        event.wait(timeout=5.0)
        return result

    def start_keepalive_daemon(self, lease_id: str, interval: float = None):
        lease = self.lease_sm.get_lease(lease_id)
        if not lease:
            return None

        if interval is None:
            interval = lease.ttl / 3.0

        stop_event = threading.Event()

        def _daemon():
            while not stop_event.is_set():
                time.sleep(interval)
                if stop_event.is_set():
                    break
                try:
                    self.keepalive(lease_id)
                except Exception:
                    pass

        t = threading.Thread(target=_daemon, daemon=True)
        t.start()

        def stop():
            stop_event.set()

        return stop
