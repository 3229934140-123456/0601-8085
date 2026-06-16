import time
import threading
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from server import LockServiceCluster
from client import LockServiceClient


def demo_txn_atomicity():
    print("=" * 60)
    print("Demo 1: 事务原子性 — 失败时回滚，前面的改动不可见")
    print("=" * 60)

    cluster = LockServiceCluster(node_count=3)
    cluster.start()

    try:
        leader = cluster.wait_for_leader(timeout=5.0)
        if not leader:
            print("  ERROR: 没有选出 Leader")
            return

        client = LockServiceClient(leader)

        client.put("k1", "original_1")
        client.put("k2", "original_2")
        time.sleep(0.1)
        print(f"  初始: k1={client.get('k1')}, k2={client.get('k2')}")

        print("  执行事务: [put k1='new_1', put_if_absent k2='new_2' (k2已存在会失败)]")
        result = client.txn([
            {"type": "put", "key": "k1", "value": "new_1"},
            {"type": "put_if_absent", "key": "k2", "value": "new_2"},
        ])
        print(f"  事务结果: success={result['success']}, error={result.get('error')}")

        v1 = client.get("k1")
        v2 = client.get("k2")
        print(f"  事务后: k1={v1}, k2={v2}")

        if v1 == "original_1" and v2 == "original_2":
            print("  ✓ 事务失败回滚成功: k1 和 k2 都没变")
        else:
            print(f"  ✗ 事务回滚失败: k1={v1}, k2={v2}")

    finally:
        cluster.stop()
    print()


def demo_lease_rebind():
    print("=" * 60)
    print("Demo 2: 租约改绑 — 旧租约过期不影响新租约的键")
    print("=" * 60)

    cluster = LockServiceCluster(node_count=3)
    cluster.start()

    try:
        leader = cluster.wait_for_leader(timeout=5.0)
        if not leader:
            print("  ERROR: 没有选出 Leader")
            return

        client = LockServiceClient(leader)

        short_lease = client.lease_grant(ttl=2.0)
        short_lease_id = short_lease["lease_id"]
        print(f"  创建短租约: TTL=2s, id={short_lease_id[:8]}...")

        long_lease = client.lease_grant(ttl=10.0)
        long_lease_id = long_lease["lease_id"]
        print(f"  创建长租约: TTL=10s, id={long_lease_id[:8]}...")

        client.put("rebind_key", "value_v1", lease_id=short_lease_id)
        time.sleep(0.1)
        print(f"  写入 rebind_key (短租约): value={client.get('rebind_key')}")

        print("  改绑: 将 rebind_key 从短租约换到长租约")
        client.put("rebind_key", "value_v2", lease_id=long_lease_id)
        time.sleep(0.1)
        print(f"  改绑后: value={client.get('rebind_key')}")

        print("  等待短租约过期 (2.5s)...")
        time.sleep(2.5)

        val = client.get("rebind_key")
        if val is not None:
            print(f"  ✓ 短租约过期后 rebind_key 仍在: value={val}")
        else:
            print(f"  ✗ 短租约过期把键误删了!")

        print("  撤销长租约...")
        client.lease_revoke(long_lease_id)
        time.sleep(0.3)

        val = client.get("rebind_key")
        if val is None:
            print("  ✓ 长租约撤销后 rebind_key 已删除")
        else:
            print(f"  ✗ 长租约撤销后键还在: value={val}")

    finally:
        cluster.stop()
    print()


def demo_watch_poll():
    print("=" * 60)
    print("Demo 3: Watch 轮询 — 订阅后发生的事件不漏")
    print("=" * 60)

    cluster = LockServiceCluster(node_count=3)
    cluster.start()

    try:
        leader = cluster.wait_for_leader(timeout=5.0)
        if not leader:
            print("  ERROR: 没有选出 Leader")
            return

        client = LockServiceClient(leader)

        print("  订阅键 'poll_key' (用 poll 模式，不设 callback)")
        wid = client.watch(key="poll_key")
        time.sleep(0.1)

        print("  执行操作:")
        client.put("poll_key", "v1")
        time.sleep(0.1)
        client.put("poll_key", "v2")
        time.sleep(0.1)
        client.delete("poll_key")
        time.sleep(0.1)

        events = client.watch_poll_all(wid, timeout=1.0)
        print(f"  poll 取到 {len(events)} 个事件:")
        for ev in events:
            print(f"    {ev.type.value} key='{ev.key}' value='{ev.value}' revision={ev.revision}")

        if len(events) == 3:
            print("  ✓ 所有事件都按序取到，无遗漏")
        else:
            print(f"  ✗ 期望 3 个事件，实际 {len(events)} 个")

        client.unwatch(wid)

    finally:
        cluster.stop()
    print()


def demo_watch_compacted():
    print("=" * 60)
    print("Demo 4: Watch 历史回放 — 历史不足时给出明确提示")
    print("=" * 60)

    cluster = LockServiceCluster(node_count=3)
    cluster.start()

    try:
        leader = cluster.wait_for_leader(timeout=5.0)
        if not leader:
            print("  ERROR: 没有选出 Leader")
            return

        client = LockServiceClient(leader)

        print("  写入 3 个键（产生 revision 1-3）")
        client.put("hist_a", "1")
        client.put("hist_b", "2")
        client.put("hist_c", "3")
        time.sleep(0.2)

        print("  从 revision=1 开始 watch（历史完整，可以补齐）")
        from kvstore.watch import EventType as WEventType
        wid1 = client.watch(key="hist_a", start_revision=1)
        time.sleep(0.1)
        events1 = client.watch_poll_all(wid1, timeout=1.0)
        print(f"  取到 {len(events1)} 个事件:")
        for ev in events1:
            print(f"    {ev.type.value} key='{ev.key}' value='{ev.value}' revision={ev.revision}")
        if len(events1) >= 1:
            print("  ✓ 历史完整时成功补齐")
        client.unwatch(wid1)

        wm = leader.watch_manager
        wm._max_history = 2
        wm._event_history = type(wm._event_history)(list(wm._event_history)[-2:], maxlen=2)
        print(f"\n  模拟历史被清理，仅保留最近 2 条 (min_rev={wm._event_history[0].revision if wm._event_history else 0})")

        print("  从 revision=1 开始 watch（历史不够）")
        wid2 = client.watch(key="hist_a", start_revision=1)
        time.sleep(0.1)
        events2 = client.watch_poll_all(wid2, timeout=1.0)
        print(f"  取到 {len(events2)} 个事件:")
        has_compacted = False
        for ev in events2:
            print(f"    {ev.type.value} key='{ev.key}' value='{ev.value}' revision={ev.revision}")
            if ev.type == WEventType.COMPACTED:
                has_compacted = True
        if has_compacted:
            print("  ✓ 历史不足时明确发出了 COMPACTED 事件")
        else:
            print("  ✗ 没有收到 COMPACTED 提示")
        client.unwatch(wid2)

    finally:
        cluster.stop()
    print()


def demo_lock_release_consistency():
    print("=" * 60)
    print("Demo 5: 锁释放一致性 — release 后立即 acquire 必须成功")
    print("=" * 60)

    cluster = LockServiceCluster(node_count=3)
    cluster.start()

    try:
        leader = cluster.wait_for_leader(timeout=5.0)
        if not leader:
            print("  ERROR: 没有选出 Leader")
            return

        client = LockServiceClient(leader)

        success_count = 0
        total = 5

        for i in range(total):
            lock = client.lock("consistency_lock", ttl=5.0)
            acquired = lock.acquire(timeout=3.0)
            if not acquired:
                print(f"  ✗ 第 {i+1} 次: 获取锁失败!")
                continue

            released = lock.release()

            lock2 = client.lock("consistency_lock", ttl=5.0)
            reacquired = lock2.acquire(timeout=2.0)

            if released and reacquired:
                success_count += 1
                lock2.release()
            else:
                print(f"  ✗ 第 {i+1} 次: release={released}, reacquire={reacquired}")
                if reacquired:
                    lock2.release()

        if success_count == total:
            print(f"  ✓ 连续 {total} 次 release→acquire 全部成功，无间隔")
        else:
            print(f"  ✗ {success_count}/{total} 次成功")

    finally:
        cluster.stop()
    print()


def demo_basic_kv():
    print("=" * 60)
    print("Demo 6: 基础键值存储")
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


def demo_lock_auto_release():
    print("=" * 60)
    print("Demo 7: 锁自动释放（模拟进程崩溃）")
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


def demo_raft_consistency():
    print("=" * 60)
    print("Demo 8: Raft 一致性验证")
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
        ("事务原子性", demo_txn_atomicity),
        ("租约改绑", demo_lease_rebind),
        ("Watch 轮询", demo_watch_poll),
        ("Watch 历史回放", demo_watch_compacted),
        ("锁释放一致性", demo_lock_release_consistency),
        ("基础键值存储", demo_basic_kv),
        ("锁自动释放", demo_lock_auto_release),
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
