import sys
import os
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def test_minimal():
    print("极简测试...")

    from raft import RaftNode, InMemoryTransport
    from kvstore import KVStore

    transport = InMemoryTransport()
    sm = KVStore()

    node = RaftNode("n0", [], transport, sm,
                    election_timeout_min=0.5, election_timeout_max=1.0,
                    heartbeat_interval=0.1)

    print("启动节点...")
    node.start()
    time.sleep(0.5)

    print(f"状态: {node.get_state()}")
    print(f"是否 Leader: {node.is_leader()}")

    print("\n尝试提交命令...")
    from kvstore import KVCommand, CommandType

    try:
        cmd = KVCommand(type=CommandType.PUT, key="test", value="hello")
        print("  调用 submit_command...")
        result = node.submit_command(cmd)
        print(f"  submit_command 返回: {result}")
    except Exception as e:
        print(f"  异常: {e}")
        traceback.print_exc()

    time.sleep(0.5)

    print(f"\ncommit_index: {node._commit_index}")
    print(f"last_applied: {node._last_applied}")
    print(f"log length: {node._log.length()}")

    val = sm.get("test")
    print(f"KV 中 test = {val}")

    node.stop()
    print("\n完成")


if __name__ == "__main__":
    test_minimal()
