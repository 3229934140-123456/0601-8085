import sys
import os
import time
import threading

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from server import LockServiceCluster


def test_raft_election():
    print("测试 Raft 选举...")
    cluster = LockServiceCluster(node_count=3)
    cluster.start()

    try:
        leader = cluster.wait_for_leader(timeout=5.0)
        if leader:
            print(f"  Leader 选举成功: {leader.node_id}")
            print(f"  Term: {leader.get_current_term()}")
            return True
        else:
            print("  Leader 选举失败")
            return False
    finally:
        cluster.stop()


def test_simple_put():
    print("\n测试简单 PUT 操作...")
    cluster = LockServiceCluster(node_count=3)
    cluster.start()

    try:
        leader = cluster.wait_for_leader(timeout=5.0)
        if not leader:
            print("  没有 Leader")
            return False

        print(f"  Leader: {leader.node_id}")

        print("  提交 PUT 命令...")
        from kvstore import KVCommand, CommandType

        result = None
        event = threading.Event()

        def callback(res):
            nonlocal result
            print(f"  [Callback] 收到结果: {res}")
            result = res
            event.set()

        cmd = KVCommand(type=CommandType.PUT, key="test", value="hello")
        index = leader.raft.submit_command(cmd, callback)
        print(f"  提交的日志索引: {index}")

        print("  等待回调...")
        waited = 0
        while not event.is_set() and waited < 5:
            time.sleep(0.1)
            waited += 0.1
            print(f"  等待中... commit_index={leader.raft._commit_index}, last_applied={leader.raft._last_applied}")

        if result:
            print(f"  操作成功: {result.success}")
            if hasattr(result, 'error') and result.error:
                print(f"  错误: {result.error}")
        else:
            print("  超时，没有收到回调")
            print(f"  commit_index={leader.raft._commit_index}")
            print(f"  last_applied={leader.raft._last_applied}")
            print(f"  log length={leader.raft._log.length()}")
            print(f"  match_index: {leader.raft._match_index}")
            print(f"  next_index: {leader.raft._next_index}")

        val = leader.state_machine.kv_store.get("test")
        print(f"  直接读取 KV: {val}")

        return True
    finally:
        cluster.stop()


if __name__ == "__main__":
    if test_raft_election():
        test_simple_put()
