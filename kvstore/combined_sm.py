import json
from typing import Any

from raft import StateMachine
from .kvstore import KVStore, KVCommand, CommandType as KVCommandType
from .lease import LeaseStateMachine, LeaseCommand, LeaseCommandType


class CombinedStateMachine(StateMachine):
    def __init__(self):
        self.kv_store = KVStore()
        self.lease_sm = LeaseStateMachine(self.kv_store, self._on_lease_expire)
        self._pending_expire_keys = []

    def _on_lease_expire(self, lease_id: str, keys: list):
        self._pending_expire_keys.extend(keys)

    def apply(self, command: Any) -> Any:
        if isinstance(command, dict):
            cmd_type = command.get("type")
            if cmd_type in [t.value for t in KVCommandType]:
                cmd = KVCommand(type=KVCommandType(cmd_type),
                                **{k: v for k, v in command.items() if k != "type"})
                result = self.kv_store.apply(cmd)
                self._update_lease_keys(cmd, result)
                return result
            elif cmd_type in [t.value for t in LeaseCommandType]:
                cmd = LeaseCommand(type=LeaseCommandType(cmd_type),
                                   **{k: v for k, v in command.items() if k != "type"})
                return self.lease_sm.apply(cmd)

        if isinstance(command, KVCommand):
            result = self.kv_store.apply(command)
            self._update_lease_keys(command, result)
            return result
        elif isinstance(command, LeaseCommand):
            return self.lease_sm.apply(command)

        return {"success": False, "error": "unknown command type"}

    def _update_lease_keys(self, cmd: KVCommand, result):
        if not hasattr(result, 'success') or not result.success:
            return

        if cmd.type == KVCommandType.PUT or cmd.type == KVCommandType.PUT_IF_ABSENT:
            if cmd.lease_id:
                if hasattr(result, 'prev_lease_id') and result.prev_lease_id and result.prev_lease_id != cmd.lease_id:
                    self.lease_sm.remove_key_from_lease(result.prev_lease_id, cmd.key)
                self.lease_sm.add_key_to_lease(cmd.lease_id, cmd.key)
        elif cmd.type == KVCommandType.DELETE:
            if hasattr(result, 'prev_lease_id') and result.prev_lease_id:
                self.lease_sm.remove_key_from_lease(result.prev_lease_id, cmd.key)

    def snapshot(self) -> bytes:
        data = {
            "kv": self.kv_store.snapshot().decode("utf-8"),
            "leases": {
                lid: {
                    "lease_id": l.lease_id,
                    "ttl": l.ttl,
                    "expire_time": l.expire_time,
                    "keys": list(l.keys),
                }
                for lid, l in self.lease_sm.get_all_leases().items()
            }
        }
        return json.dumps(data).encode("utf-8")

    def restore(self, snapshot: bytes) -> None:
        data = json.loads(snapshot.decode("utf-8"))
        self.kv_store.restore(data["kv"].encode("utf-8"))
        from .lease import Lease
        self.lease_sm._leases = {}
        for lid, ld in data.get("leases", {}).items():
            lease = Lease(
                lease_id=ld["lease_id"],
                ttl=ld["ttl"],
                expire_time=ld["expire_time"],
                keys=set(ld.get("keys", [])),
            )
            self.lease_sm._leases[lid] = lease
