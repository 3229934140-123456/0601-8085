import sys
import os
import time
import threading

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from raft import RaftNode, RaftState, InMemoryTransport, RaftMessage
from raft.log import LogEntry


def test_basic_raft():
    print("=" * 60)
    print("测试基础 Raft 功能")
    print("=" * 60)

    transport = InMemoryTransport()
    node_ids = ["n0", "n1", "n2"]

    from kvstore import KVStore
    sm0 = KVStore()
    sm1 = KVStore()
    sm2 = KVStore()

    nodes = [
        RaftNode("n0", ["n1", "n2"], transport, sm0,
                 election_timeout_min=0.3, election_timeout_max=0.6,
                 heartbeat_interval=0.1),
        RaftNode("n1", ["n0", "n2"], transport, sm1,
                 election_timeout_min=0.3, election_timeout_max=0.6,
                 heartbeat_interval=0.1),
        RaftNode("n2", ["n0", "n1"], transport, sm2,
                 election_timeout_min=0.3, election_timeout_max=0.6,
                 heartbeat_interval=0.1),
    ]

    for node in nodes:
        node.start()

    try:
        print("\n等待 Leader 选举...")
        leader = None
        for _ in range(50):
            for node in nodes:
                if node.is_leader():
                    leader = node
                    break
            if leader:
                break
            time.sleep(0.1)

        if leader:
            print(f"Leader 选出: {leader.node_id}, term={leader.get_current_term()}")
        else:
            print("ERROR: 没有选出 Leader")
            for node in nodes:
                print(f"  {node.node_id}: state={node.get_state()}, term={node.get_current_term()}")
            return

        print(f"\n各节点状态:")
        for node in nodes:
            print(f"  {node.node_id}: state={node.get_state().value}, term={node.get_current_term()}")

        print("\n测试提交命令...")
        from kvstore import KVCommand, CommandType

        result = None
        event = threading.Event()

        def callback(res):
            nonlocal result
            result = res
            event.set()
            print(f"  回调被调用: success={res.success if hasattr(res, 'success') else 'N/A'}")

        cmd = KVCommand(type=CommandType.PUT, key="foo", value="bar")
        log_index = leader.submit_command(cmd, callback)
        print(f"  命令已提交，日志索引: {log_index}")

        print("  等待提交完成...")
        for i in range(30):
            time.sleep(0.2)
            print(f"  检查 #{i}: leader commit_idx={leader._commit_index}, applied={leader._last_applied}")
            for node in nodes:
                if node.node_id != leader.node_id:
                    print(f"    {node.node_id}: last_log_idx={node._log.last_index()}, state={node.get_state().value}")
            if event.is_set():
                break

        if result:
            print(f"\n  命令执行结果: {result}")
        else:
            print("\n  命令超时未执行")

        print("\n检查各节点 KV 状态:")
        for i, node in enumerate(nodes):
            val = [sm0, sm1, sm2][i].get("foo")
            print(f"  {node.node_id}: foo={val.value if val else None}")

    finally:
        for node in nodes:
            node.stop()


if __name__ == "__main__":
    test_basic_raft()
