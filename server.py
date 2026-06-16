import threading
from typing import List, Optional

from raft import RaftNode, RaftState, InMemoryTransport
from kvstore import (
    CombinedStateMachine,
    LeaseManager,
    LockManager,
    WatchManager,
    DistributedLock,
)


class LockServiceServer:
    def __init__(self, node_id: str, peer_ids: List[str],
                 transport: InMemoryTransport = None):
        self.node_id = node_id
        self.peer_ids = peer_ids

        if transport is None:
            transport = InMemoryTransport()
        self.transport = transport

        self.state_machine = CombinedStateMachine()
        self.raft = RaftNode(
            node_id=node_id,
            peers=peer_ids,
            transport=transport,
            state_machine=self.state_machine,
        )

        self.lease_manager = LeaseManager(self.raft, self.state_machine.kv_store, self.state_machine.lease_sm)
        self.lock_manager = LockManager(self.raft, self.state_machine.kv_store, self.lease_manager)
        self.watch_manager = WatchManager(self.state_machine.kv_store)

        self._started = False
        self._lock = threading.Lock()

    def start(self):
        with self._lock:
            if self._started:
                return
            self.raft.start()
            self.lease_manager.start()
            self._started = True

    def stop(self):
        with self._lock:
            if not self._started:
                return
            self.lease_manager.stop()
            self.raft.stop()
            self._started = False

    def is_leader(self) -> bool:
        return self.raft.is_leader()

    def get_state(self) -> RaftState:
        return self.raft.get_state()

    def get_current_term(self) -> int:
        return self.raft.get_current_term()

    def put(self, key: str, value, lease_id: str = None) -> bool:
        from kvstore import KVCommand, CommandType
        cmd = KVCommand(type=CommandType.PUT, key=key, value=value, lease_id=lease_id)

        result = None
        event = threading.Event()

        def callback(res):
            nonlocal result
            result = res
            event.set()

        self.raft.submit_command(cmd, callback)
        event.wait(timeout=5.0)

        if result is None:
            return False
        return result.success if hasattr(result, 'success') else False

    def get(self, key: str):
        entry = self.state_machine.kv_store.get(key)
        return entry.value if entry else None

    def delete(self, key: str) -> bool:
        from kvstore import KVCommand, CommandType
        cmd = KVCommand(type=CommandType.DELETE, key=key)

        result = None
        event = threading.Event()

        def callback(res):
            nonlocal result
            result = res
            event.set()

        self.raft.submit_command(cmd, callback)
        event.wait(timeout=5.0)

        if result is None:
            return False
        return result.success if hasattr(result, 'success') else False

    def txn(self, ops: list) -> dict:
        from kvstore import KVCommand, CommandType
        cmd = KVCommand(type=CommandType.TXN, ops=ops)

        result = None
        event = threading.Event()

        def callback(res):
            nonlocal result
            result = res
            event.set()

        self.raft.submit_command(cmd, callback)
        event.wait(timeout=5.0)

        if result is None:
            return {"success": False, "error": "timeout"}
        if hasattr(result, 'success'):
            return {"success": result.success, "error": result.error, "value": result.value}
        return {"success": False, "error": "unknown"}

    def lease_grant(self, ttl: float, lease_id: str = None) -> dict:
        return self.lease_manager.grant(ttl, lease_id)

    def lease_revoke(self, lease_id: str) -> dict:
        return self.lease_manager.revoke(lease_id)

    def lease_keepalive(self, lease_id: str) -> dict:
        return self.lease_manager.keepalive(lease_id)

    def lock_acquire(self, lock_name: str, ttl: float = 10.0,
                     timeout: float = 0) -> Optional[str]:
        result = self.lock_manager.acquire(lock_name, ttl, timeout)
        return result.lease_id if result.success else None

    def lock_release(self, lock_name: str, lease_id: str) -> bool:
        return self.lock_manager.release(lock_name, lease_id)

    def is_locked(self, lock_name: str) -> bool:
        return self.lock_manager.is_locked(lock_name)

    def get_lock(self, name: str, ttl: float = 10.0) -> DistributedLock:
        return DistributedLock(self.lock_manager, name, ttl)

    def watch(self, key: str = None, prefix: str = None,
              start_revision: int = 0, callback=None) -> int:
        return self.watch_manager.watch(
            key=key, prefix=prefix,
            start_revision=start_revision,
            callback=callback,
        )

    def unwatch(self, watch_id: int) -> bool:
        return self.watch_manager.unwatch(watch_id)

    def watch_poll(self, watch_id: int, timeout: float = 0):
        return self.watch_manager.poll(watch_id, timeout)

    def watch_poll_all(self, watch_id: int, timeout: float = 0):
        return self.watch_manager.poll_all(watch_id, timeout)


class LockServiceCluster:
    def __init__(self, node_count: int = 3):
        self.node_count = node_count
        self.node_ids = [f"node-{i}" for i in range(node_count)]
        self.transport = InMemoryTransport()
        self.nodes: List[LockServiceServer] = []

        for i in range(node_count):
            node_id = self.node_ids[i]
            peers = [pid for pid in self.node_ids if pid != node_id]
            node = LockServiceServer(node_id, peers, self.transport)
            self.nodes.append(node)

    def start(self):
        for node in self.nodes:
            node.start()

    def stop(self):
        for node in self.nodes:
            node.stop()

    def get_leader(self) -> Optional[LockServiceServer]:
        for node in self.nodes:
            if node.is_leader():
                return node
        return None

    def wait_for_leader(self, timeout: float = 5.0) -> Optional[LockServiceServer]:
        import time
        deadline = time.time() + timeout
        while time.time() < deadline:
            leader = self.get_leader()
            if leader:
                return leader
            time.sleep(0.05)
        return None
