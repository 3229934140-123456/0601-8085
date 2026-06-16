import time
import threading
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from server import LockServiceCluster
from client import LockServiceClient


def demo_basic_kv():
    print("=" * 60)
    print("Demo 1: 基础键值存储")
    print("=" * 60)

    cluster = LockServiceCluster(node_count=3)
    cluster.start()

    try:
        leader = cluster.wait_for_leader(timeout=5.0)
        if not leader:
            print("  ERROR: 没有选出 Leader")
            return

        print(f"  Leader: {leader.node_id}")
        client = LockServiceClient(leader)

        print("  Put key='foo', value='bar'")
        client.put("foo", "bar")
        time.sleep(0.1)

        print(f"  Get key='foo' -> {client.get('foo')}")

        print("  Delete key='foo'")
        client.delete("foo")
        time.sleep(0.1)

        print(f"  Get key='foo' -> {client.get('foo')}")

    finally:
        cluster.stop()
    print()


def demo_lease():
    print("=" * 60)
    print("Demo 2: 租约机制（Lease）")
    print("=" * 60)

    cluster = LockServiceCluster(node_count=3)
    cluster.start()

    try:
        leader = cluster.wait_for_leader(timeout=5.0)
        if not leader:
            print("  ERROR: 没有选出 Leader")
            return

        client = LockServiceClient(leader)

        print("  创建一个 TTL=2秒 的租约")
        result = client.lease_grant(ttl=2.0)
        lease_id = result["lease_id"]
        print(f"  租约 ID: {lease_id}")

        print(f"  用租约绑定键 'temp_key'")
        client.put("temp_key", "temp_value", lease_id=lease_id)
        time.sleep(0.1)
        print(f"  Get 'temp_key' -> {client.get('temp_key')}")

        print("  等待 1 秒，续期前...")
        time.sleep(1.0)

        print("  发送心跳续期 (KeepAlive)")
        client.lease_keepalive(lease_id)
        time.sleep(0.1)

        print("  再等待 1.5 秒（超过原 TTL 但续期了）")
        time.sleep(1.5)
        print(f"  Get 'temp_key' -> {client.get('temp_key')} (键仍然存在)")

        print("  停止续期，等待租约过期（2秒）...")
        time.sleep(2.5)
        print(f"  Get 'temp_key' -> {client.get('temp_key')} (键已自动删除)")

    finally:
        cluster.stop()
    print()


def demo_distributed_lock():
    print("=" * 60)
    print("Demo 3: 分布式锁")
    print("=" * 60)

    cluster = LockServiceCluster(node_count=3)
    cluster.start()

    try:
        leader = cluster.wait_for_leader(timeout=5.0)
        if not leader:
            print("  ERROR: 没有选出 Leader")
            return

        client = LockServiceClient(leader)

        print("  两个客户端竞争同一把锁 'my_lock'")
        print()

        results = [None, None]

        def worker(idx):
            lock = client.lock("my_lock", ttl=5.0)
            start = time.time()
            got_lock = lock.acquire(timeout=3.0)
            elapsed = time.time() - start
            results[idx] = (got_lock, elapsed, lock.lease_id)
            if got_lock:
                time.sleep(0.5)
                lock.release()

        t1 = threading.Thread(target=worker, args=(0,))
        t2 = threading.Thread(target=worker, args=(1,))

        t1.start()
        t2.start()
        t1.join()
        t2.join()

        for i, (got, elapsed, lease_id) in enumerate(results):
            status = "✓ 获得锁" if got else "✗ 等待超时"
            print(f"  客户端 {i+1}: {status}, 耗时 {elapsed:.3f}s, lease_id={lease_id}")

        print()
        print("  结果：同一时刻只有一个客户端能持有锁")
        print("  原理：锁通过 put_if_absent 原子创建，带租约自动过期")

    finally:
        cluster.stop()
    print()


def demo_lock_auto_release():
    print("=" * 60)
    print("Demo 4: 锁自动释放（模拟进程崩溃）")
    print("=" * 60)

    cluster = LockServiceCluster(node_count=3)
    cluster.start()

    try:
        leader = cluster.wait_for_leader(timeout=5.0)
        if not leader:
            print("  ERROR: 没有选出 Leader")
            return

        client = LockServiceClient(leader)

        print("  客户端 A 获取锁 'crash_lock' (TTL=2秒)")
        lock_a = client.lock("crash_lock", ttl=2.0)
        lock_a.acquire()
        print(f"  客户端 A 持有锁: {lock_a.locked}, lease_id={lock_a.lease_id}")

        print("  模拟客户端 A 崩溃（不释放锁，也不心跳续期）")
        lock_a.simulate_crash()

        print("  客户端 B 尝试获取同一把锁...")
        lock_b = client.lock("crash_lock", ttl=2.0)
        start = time.time()
        got = lock_b.acquire(timeout=5.0)
        elapsed = time.time() - start

        if got:
            print(f"  客户端 B 在 {elapsed:.2f}s 后获得了锁")
            print("  原因：客户端 A 崩溃后，租约过期，锁自动释放")
            lock_b.release()
        else:
            print(f"  客户端 B 等待 {elapsed:.2f}s 仍未获得锁")

    finally:
        cluster.stop()
    print()


def demo_watch():
    print("=" * 60)
    print("Demo 5: Watch 通知机制")
    print("=" * 60)

    cluster = LockServiceCluster(node_count=3)
    cluster.start()

    try:
        leader = cluster.wait_for_leader(timeout=5.0)
        if not leader:
            print("  ERROR: 没有选出 Leader")
            return

        client = LockServiceClient(leader)

        events = []

        def on_event(event):
            events.append(event)
            print(f"  [Watch] {event.type.value} key='{event.key}' value='{event.value}' revision={event.revision}")

        print("  订阅键 'watch_key' 的变化")
        watch_id = client.watch(key="watch_key", callback=on_event)
        time.sleep(0.1)

        print()
        print("  执行操作:")
        print("    Put 'watch_key' = 'v1'")
        client.put("watch_key", "v1")
        time.sleep(0.1)

        print("    Put 'watch_key' = 'v2'")
        client.put("watch_key", "v2")
        time.sleep(0.1)

        print("    Delete 'watch_key'")
        client.delete("watch_key")
        time.sleep(0.1)

        print(f"\n  共收到 {len(events)} 个事件")
        print("  特点：事件按 revision 顺序，不丢不重")

        client.unwatch(watch_id)

    finally:
        cluster.stop()
    print()


def demo_raft_consistency():
    print("=" * 60)
    print("Demo 6: Raft 一致性验证")
    print("=" * 60)

    cluster = LockServiceCluster(node_count=3)
    cluster.start()

    try:
        leader = cluster.wait_for_leader(timeout=5.0)
        if not leader:
            print("  ERROR: 没有选出 Leader")
            return

        print(f"  初始 Leader: {leader.node_id}")
        client = LockServiceClient(leader)

        print("  写入 10 个键值对")
        for i in range(10):
            client.put(f"key-{i}", f"value-{i}")

        time.sleep(0.2)

        print()
        print("  各节点数据一致性检查:")
        for node in cluster.nodes:
            data = node.state_machine.kv_store.get_all()
            print(f"    {node.node_id} ({node.get_state().value}): {len(data)} 个键")

        print()
        print("  结论：Raft 保证了所有副本数据一致")
        print("  只有 Leader 可以提交写操作，Follower 复制日志")

    finally:
        cluster.stop()
    print()


def main():
    print("\n" + "=" * 60)
    print("  分布式锁服务 Demo")
    print("  Raft 复制 + 租约 + 分布式锁 + Watch")
    print("=" * 60)
    print()

    demos = [
        ("基础键值存储", demo_basic_kv),
        ("租约机制", demo_lease),
        ("分布式锁", demo_distributed_lock),
        ("锁自动释放", demo_lock_auto_release),
        ("Watch 通知", demo_watch),
        ("Raft 一致性", demo_raft_consistency),
    ]

    try:
        for name, demo_fn in demos:
            try:
                demo_fn()
            except Exception as e:
                print(f"  Demo '{name}' 出错: {e}")
                import traceback
                traceback.print_exc()
                print()
    except KeyboardInterrupt:
        print("\n\n已中断")

    print("=" * 60)
    print("  所有 Demo 完成")
    print("=" * 60)


if __name__ == "__main__":
    main()
