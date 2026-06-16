import sys
import os
import time
import traceback

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

print("[1] 开始")

from raft import RaftNode, InMemoryTransport, RaftState
from kvstore import KVStore

print("[2] 导入完成")


def test():
    print("[3] 进入 test 函数")

    transport = InMemoryTransport()
    sm = KVStore()

    print("[4] 创建节点")
    node = RaftNode("n0", [], transport, sm,
                    election_timeout_min=0.2, election_timeout_max=0.4,
                    heartbeat_interval=0.05)

    print("[5] 启动节点")
    node.start()

    print("[6] 等待成为 Leader")
    for i in range(20):
        time.sleep(0.05)
        if node.is_leader():
            print(f"[7] 已成为 Leader，耗时 {i*0.05:.2f}s")
            break

    if not node.is_leader():
        print("[X] 未能成为 Leader")
        return

    print("[8] 准备提交命令")

    from kvstore import KVCommand, CommandType
    print("[9] 导入 KVCommand 成功")

    cmd = KVCommand(type=CommandType.PUT, key="test", value="hello")
    print("[10] 创建 KVCommand 成功")

    import threading
    print("[11] 导入 threading 成功")

    result = None
    event = threading.Event()

    def callback(res):
        nonlocal result
        result = res
        event.set()
        print(f"  [回调] success={res.success}")

    print("[12] 调用 submit_command")
    try:
        idx = node.submit_command(cmd, callback)
        print(f"[13] submit_command 返回: {idx}")
    except Exception as e:
        print(f"[X] submit_command 异常: {e}")
        traceback.print_exc()
        return

    print("[14] 等待回调")
    if event.wait(timeout=2.0):
        print(f"[15] ✓ 命令已提交并应用")
    else:
        print(f"[16] ✗ 命令超时")
        print(f"     commit_index={node._commit_index}")
        print(f"     last_applied={node._last_applied}")
        print(f"     log_len={node._log.length()}")

    val = sm.get("test")
    print(f"[17] KV 中 test = {val.value if val else None}")

    node.stop()
    print("[18] 完成")


if __name__ == "__main__":
    test()
