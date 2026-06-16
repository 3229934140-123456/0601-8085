import sys
import os
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))


def test_single_node():
    print("测试单节点 Raft...")

    from raft import RaftNode, InMemoryTransport, RaftState
    from kvstore import KVStore

    transport = InMemoryTransport()
    sm = KVStore()

    node = RaftNode("n0", [], transport, sm,
                    election_timeout_min=0.2, election_timeout_max=0.4,
                    heartbeat_interval=0.05)

    print("启动节点...")
    node.start()

    for i in range(20):
        time.sleep(0.1)
        state = node.get_state()
        print(f"  第 {i*0.1:.1f}s: state={state.value}")
        if state == RaftState.LEADER:
            print("  ✓ 成为 Leader！")
            break

    if node.is_leader():
        print("\n提交命令...")
        from kvstore import KVCommand, CommandType

        import threading
        result = None
        event = threading.Event()

        def callback(res):
            nonlocal result
            result = res
            event.set()
            print(f"  [回调] success={res.success}")

        cmd = KVCommand(type=CommandType.PUT, key="test", value="hello")
        idx = node.submit_command(cmd, callback)
        print(f"  提交索引: {idx}")

        if event.wait(timeout=2.0):
            print(f"  ✓ 命令已提交并应用")
        else:
            print(f"  ✗ 命令超时")
            print(f"    commit_index={node._commit_index}")
            print(f"    last_applied={node._last_applied}")
            print(f"    log_len={node._log.length()}")

        val = sm.get("test")
        print(f"  KV 中 test = {val.value if val else None}")
    else:
        print("\n✗ 未能成为 Leader")

    node.stop()
    print("\n完成")


if __name__ == "__main__":
    test_single_node()
