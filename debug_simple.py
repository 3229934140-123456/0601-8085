import sys
import os
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from raft import RaftNode, RaftState, InMemoryTransport, RaftMessage
from raft.log import LogEntry


class DebugTransport(InMemoryTransport):
    def __init__(self):
        super().__init__()
        self._msg_log = []

    def send(self, msg: RaftMessage):
        self._msg_log.append(("send", msg.src, msg.dst, msg.msg_type))
        super().send(msg)

    def get_msg_count(self):
        return len(self._msg_log)

    def print_msgs(self, limit=20):
        print(f"  最近消息记录 (共 {len(self._msg_log)} 条):")
        for direction, src, dst, typ in self._msg_log[-limit:]:
            print(f"    {src} -> {dst}: {typ}")


def test_raft_step_by_step():
    print("=" * 60)
    print("逐步测试 Raft")
    print("=" * 60)

    transport = DebugTransport()

    from kvstore import KVStore
    sm0 = KVStore()
    sm1 = KVStore()

    n0 = RaftNode("n0", ["n1"], transport, sm0,
                   election_timeout_min=0.5, election_timeout_max=1.0,
                   heartbeat_interval=0.1)
    n1 = RaftNode("n1", ["n0"], transport, sm1,
                   election_timeout_min=0.5, election_timeout_max=1.0,
                   heartbeat_interval=0.1)

    n0.start()
    n1.start()

    try:
        print("\n步骤 1: 等待 Leader 选举")
        time.sleep(1.5)

        print(f"  n0 状态: {n0.get_state().value}, term={n0.get_current_term()}")
        print(f"  n1 状态: {n1.get_state().value}, term={n1.get_current_term()}")
        transport.print_msgs()

        leader = None
        if n0.is_leader():
            leader = n0
        elif n1.is_leader():
            leader = n1

        if not leader:
            print("  ERROR: 没有 Leader！")
            return

        print(f"\n步骤 2: Leader={leader.node_id}")

        print(f"\n步骤 3: 提交一条命令")
        from kvstore import KVCommand, CommandType
        cmd = KVCommand(type=CommandType.PUT, key="foo", value="bar")

        before_msgs = transport.get_msg_count()
        log_index = leader.submit_command(cmd)
        print(f"  提交后日志索引: {log_index}")
        print(f"  提交后发送消息数: {transport.get_msg_count() - before_msgs}")

        time.sleep(0.5)

        print(f"\n步骤 4: 检查各节点状态")
        print(f"  n0: commit_idx={n0._commit_index}, applied={n0._last_applied}, log_len={n0._log.length()}")
        print(f"  n1: commit_idx={n1._commit_index}, applied={n1._last_applied}, log_len={n1._log.length()}")
        transport.print_msgs()

        print(f"\n步骤 5: 检查 KV 状态")
        print(f"  n0 foo={sm0.get('foo')}")
        print(f"  n1 foo={sm1.get('foo')}")

        print(f"\n步骤 6: Leader 的 match_index 和 next_index")
        if leader.node_id == "n0":
            print(f"  match_index: {n0._match_index}")
            print(f"  next_index: {n0._next_index}")
        else:
            print(f"  match_index: {n1._match_index}")
            print(f"  next_index: {n1._next_index}")

    finally:
        n0.stop()
        n1.stop()


if __name__ == "__main__":
    test_raft_step_by_step()
