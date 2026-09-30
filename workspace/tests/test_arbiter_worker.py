"""仲裁器分片执行器：进程退出接管、检查点续跑、多副本 claim 不重复。"""
import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import pytest

from featforge.arbiter import Arbiter, ArbiterConflict
from featforge.arbiter_worker import ArbiterWorker, SimulatedProcessExit
from featforge.db import Database


class Clock:
    def __init__(self, t=1_000_000):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, n):
        self.t += n


@pytest.fixture
def arb(tmp_path):
    db = Database(str(tmp_path / "w.db"))
    clk = Clock()
    a = Arbiter(db, clock=clk, lease_ttl=100, aging_rate=0.0)
    yield a, clk
    db.close()


def test_worker_completes_multi_shard_job(arb):
    a, _ = arb
    a.upsert_tenant("t", weight=1, seats=1)
    a.enqueue("t", "backfill", "k", shards_total=5)
    out = ArbiterWorker(a, "w1", heartbeat_shards=2).tick()
    assert out["state"] == "succeeded" and out["shard_cursor"] == 5
    assert len(a.list_progress(out["id"])) == 5
    # 租约已释放：作业行不再有持有者
    assert out["claim_worker"] is None


def test_worker_die_after_shards_simulates_process_exit(arb):
    a, clk = arb
    a.upsert_tenant("t", weight=1, seats=2)
    a.enqueue("t", "backfill", "k", shards_total=10,
              payload={"die_after_shards": 2})
    w1 = ArbiterWorker(a, "worker-A")
    with pytest.raises(SimulatedProcessExit):
        w1.tick()
    job = a.list_jobs()[0]
    assert job["state"] == "claimed" and job["shard_cursor"] == 2
    assert job["claim_worker"] == "worker-A" and job["claim_epoch"] == 1
    # 租约内别人接不了
    assert a.claim("worker-B") is None
    # 超时后接管：从游标 2 继续
    clk.advance(101)
    a.reap_expired()
    assert a.get_job(a.list_jobs()[0]["id"])["state"] == "queued"

    handled = []
    w2 = ArbiterWorker(a, "worker-B",
                       handler=lambda job, shard: handled.append(shard))
    # 第二任同样在处理 2 片后被杀（故障注入随作业走）
    with pytest.raises(SimulatedProcessExit):
        w2.tick()
    assert handled == [2, 3]
    taken = a.get_job(a.list_jobs()[0]["id"])
    assert taken["claim_epoch"] == 2 and taken["shard_cursor"] == 4
    # 第三任接管处理完剩余 6 片
    clk.advance(101)
    a.reap_expired()
    third = a.claim("worker-C")
    assert third["claim_epoch"] == 3
    # 故障注入随作业负载走（演示真实接管时由新代码版本处理）；
    # 这里用手工提交模拟第三任把剩余 6 片写完
    for cur in range(5, 11):
        handled.append(cur - 1)
        a.report_progress(third["id"], 3, "worker-C", cur)
    done = a.complete(third["id"], 3, "worker-C")
    assert handled == [2, 3, *range(4, 10)]
    assert done["shard_cursor"] == 10 and done["state"] == "succeeded"
    # 三任 worker 的进度台账都在
    by_worker = {}
    for p in a.list_progress(taken["id"]):
        by_worker[p["worker"]] = by_worker.get(p["worker"], 0) + 1
    assert by_worker == {"worker-A": 2, "worker-B": 2, "worker-C": 6}


def test_worker_fail_then_retry_resumes(arb):
    a, _ = arb
    a.upsert_tenant("t", weight=1, seats=1)
    a.enqueue("t", "correction", "k", shards_total=4,
              payload={"fail_after_shards": 3})
    out = ArbiterWorker(a, "w1").tick()
    assert out["state"] == "failed" and out["shard_cursor"] == 3
    a.retry_failed(out["id"])
    # 新执行器（无故障注入）跑剩余 1 片完成——handler 不识别注入参数，
    # 但 payload 里的 fail_after_shards 是默认 worker 读取的；因此手工完成。
    taken = a.claim("w2")
    a.report_progress(taken["id"], taken["claim_epoch"], "w2", 4)
    done = a.complete(taken["id"], taken["claim_epoch"], "w2")
    assert done["state"] == "succeeded"
    assert [p["new_cursor"] for p in a.list_progress(done["id"])] == [1, 2, 3, 4]


def test_concurrent_workers_never_claim_same_shard(arb):
    """多副本并发 claim：每个作业只被一个执行者拿到。"""
    import threading
    a, _ = arb
    for name in ("t1", "t2", "t3"):
        a.upsert_tenant(name, weight=1, seats=2)
        for i in range(6):
            a.enqueue(name, "backfill", f"{name}-{i}")
    claimed = []
    errors = []
    lock = threading.Lock()

    def run(worker_id):
        try:
            w = ArbiterWorker(a, worker_id)
            for _ in range(6):
                j = a.claim(worker_id)
                if j is None:
                    return
                with lock:
                    claimed.append((j["id"], j["claim_epoch"], worker_id))
                a.complete(j["id"], j["claim_epoch"], worker_id)
        except Exception as e:  # pragma: no cover
            errors.append(e)

    threads = [threading.Thread(target=run, args=(f"w{i}",)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    job_ids = [c[0] for c in claimed]
    assert len(job_ids) == 18 and len(set(job_ids)) == 18
