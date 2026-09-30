"""多租户仲裁器单元测试：策略、入队去重、欠额+老化选择、席位、fencing。"""
import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import pytest

from featforge.arbiter import Arbiter, ArbiterConflict, ArbiterError
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
    db = Database(str(tmp_path / "arb.db"))
    clk = Clock()
    a = Arbiter(db, clock=clk, lease_ttl=100, aging_rate=1.0, history_limit=100)
    yield a, clk
    db.close()


def mk(arb, name="t", weight=1.0, seats=1):
    return arb.upsert_tenant(name, weight=weight, seats=seats)


# --------------------------------------------------------------- 策略配置

def test_create_and_upsert_is_idempotent(arb):
    a, _ = arb
    t1 = a.upsert_tenant("t1", weight=2.0, seats=3)
    t2 = a.upsert_tenant("t1", weight=9.0, seats=9)  # 已存在不覆盖
    assert t1["id"] == t2["id"]
    assert t2["weight"] == 2.0 and t2["seats"] == 3


def test_invalid_policy_rejected(arb):
    a, _ = arb
    with pytest.raises(ArbiterError):
        a.upsert_tenant("x", weight=0, seats=1)
    with pytest.raises(ArbiterError):
        a.upsert_tenant("x", weight=1, seats=0)
    mk(a)
    with pytest.raises(ArbiterError):
        a.update_policy("t", weight=-1)
    with pytest.raises(ArbiterError):
        a.update_policy("t", seats=1.5)


def test_policy_change_keeps_claims_and_progress(arb):
    """在线改权重/席位不能让 job 被领取两次，也不能清空进度。"""
    a, _ = arb
    mk(a, "t", weight=1.0, seats=1)
    a.enqueue("t", "backfill", "b1", shards_total=4)
    j = a.claim("w1")
    a.report_progress(j["id"], j["claim_epoch"], "w1", 2)
    a.update_policy("t", weight=5.0, seats=8)
    t = a.get_tenant("t")
    j2 = a.get_job(j["id"])
    assert t["weight"] == 5.0 and t["seats"] == 8
    assert j2["state"] == "claimed" and j2["claim_epoch"] == 1
    assert j2["shard_cursor"] == 2 and j2["claim_worker"] == "w1"
    # 没有第二个作业可领（同一个 job 不会出现第二份 claim）
    assert a.claim("w2") is None


# --------------------------------------------------------------- 入队去重

def test_enqueue_dedup_same_payload(arb):
    a, _ = arb
    mk(a)
    r1 = a.enqueue("t", "snapshot", "k", payload={"a": 1, "b": 2}, shards_total=2)
    r2 = a.enqueue("t", "snapshot", "k", payload={"b": 2, "a": 1}, shards_total=2)
    assert r1["job"]["id"] == r2["job"]["id"] and r2["replayed"] is True


def test_enqueue_conflict_on_different_payload(arb):
    a, _ = arb
    mk(a)
    a.enqueue("t", "snapshot", "k", payload={"a": 1})
    with pytest.raises(ArbiterConflict) as ei:
        a.enqueue("t", "snapshot", "k", payload={"a": 2})
    assert ei.value.code == "business_key_conflict"
    with pytest.raises(ArbiterConflict):
        a.enqueue("t", "snapshot", "k", shards_total=9)
    with pytest.raises(ArbiterError):
        a.enqueue("t", "bogus", "k2")


def test_business_key_scoped_per_tenant(arb):
    a, _ = arb
    mk(a, "t1")
    mk(a, "t2")
    r1 = a.enqueue("t1", "snapshot", "same-key")
    r2 = a.enqueue("t2", "snapshot", "same-key")
    assert r1["job"]["id"] != r2["job"]["id"]


# --------------------------------------------------------------- 选择器

def test_weight_sharing_without_aging(arb):
    a, clk = arb
    # 关闭老化：纯粹 vruntime 欠额调度
    a.aging_rate = 0.0
    a.upsert_tenant("h", weight=4.0, seats=1)
    a.upsert_tenant("l", weight=1.0, seats=1)
    for i in range(40):
        a.enqueue("h", "backfill", f"h{i}")
        a.enqueue("l", "backfill", f"l{i}")
    counts = {"h": 0, "l": 0}
    for _ in range(50):
        j = a.claim("w")
        if j is None:
            break
        counts[j["tenant_name"]] += 1
        a.complete(j["id"], j["claim_epoch"], "w")
    assert counts["h"] == 40 and counts["l"] == 10
    st = {t["name"]: t for t in a.status()["tenants"]}
    assert abs(st["h"]["vruntime"] - st["l"]["vruntime"]) < 1e-9


def test_aging_prevents_starvation_under_flood(arb):
    a, clk = arb
    a.upsert_tenant("big", weight=10.0, seats=1)
    a.upsert_tenant("small", weight=1.0, seats=1)
    a.enqueue("big", "backfill", "b0")
    a.enqueue("small", "snapshot", "only")
    order = []
    picked_round = None
    for n in range(8):
        if n > 0:
            a.enqueue("big", "backfill", f"b{n}")  # 新作业 age=0
        j = a.claim("w")
        order.append(j["tenant_name"])
        a.complete(j["id"], j["claim_epoch"], "w")
        if j["tenant_name"] == "small" and picked_round is None:
            picked_round = n + 1
        clk.advance(10)
    assert picked_round is not None and picked_round <= 3
    assert a.list_jobs("small")[0]["state"] == "succeeded"


def test_claim_order_is_deterministic(arb):
    a, _ = arb
    a.upsert_tenant("x", weight=1, seats=1)
    a.upsert_tenant("y", weight=1, seats=1)
    a.enqueue("x", "backfill", "x1")
    a.enqueue("y", "backfill", "y1")
    first = a.claim("w")
    assert first["tenant_name"] == "x"  # 平手 -> 最老入队 (x 先入队)


def test_sleeping_tenant_aligns_min_vruntime(arb):
    """沉睡很久的租户唤醒时对齐 min_vruntime，不会霸占仲裁器。"""
    a, clk = arb
    a.upsert_tenant("early", weight=1, seats=1)
    a.upsert_tenant("sleeper", weight=1, seats=1)
    for i in range(5):
        a.enqueue("early", "backfill", f"e{i}")
    for _ in range(5):
        j = a.claim("w")
        a.complete(j["id"], j["claim_epoch"], "w")
    assert a.get_tenant("early")["vruntime"] == pytest.approx(5.0)
    a.enqueue("sleeper", "backfill", "s1")  # 唤醒：从 0 对齐到 5
    assert a.get_tenant("sleeper")["vruntime"] == pytest.approx(5.0)
    j = a.claim("w")
    assert j["tenant_name"] == "sleeper"


# --------------------------------------------------------------- 席位/暂停

def test_seats_limit_concurrency(arb):
    a, _ = arb
    a.upsert_tenant("t", weight=1, seats=2)
    for i in range(4):
        a.enqueue("t", "backfill", f"k{i}")
    j1, j2, j3 = a.claim("w1"), a.claim("w1"), a.claim("w1")
    assert j1 and j2 and j3 is None
    assert a.tenant_status("t")["seats_used"] == 2
    a.complete(j1["id"], j1["claim_epoch"], "w1")
    assert a.tenant_status("t")["seats_used"] == 1
    j4 = a.claim("w2")
    assert j4 is not None and j4["id"] != j2["id"]


def test_pause_blocks_new_claims_but_running_shards_finish(arb):
    a, _ = arb
    a.upsert_tenant("t", weight=1, seats=2)
    a.enqueue("t", "backfill", "running", shards_total=3)
    a.enqueue("t", "backfill", "waiting")
    j = a.claim("w1")
    a.pause("t")
    assert a.claim("w1") is None
    # 已开工分片照常心跳/进度/完成
    a.heartbeat(j["id"], j["claim_epoch"], "w1", cursor=1)
    a.report_progress(j["id"], j["claim_epoch"], "w1", 2)
    done = a.complete(j["id"], j["claim_epoch"], "w1")
    assert done["state"] == "succeeded"
    # 完成后也不能领新的（仍暂停）
    assert a.claim("w1") is None
    a.resume("t")
    nxt = a.claim("w1")
    assert nxt["business_key"] == "waiting"


# --------------------------------------------------------------- fencing

def test_lease_expiry_and_epoch_fence(arb):
    a, clk = arb
    a.upsert_tenant("t", weight=1, seats=1)
    a.enqueue("t", "backfill", "k", shards_total=4)
    old = a.claim("worker-A")
    a.report_progress(old["id"], 1, "worker-A", 2)
    # 未超时不能接管
    assert a.claim("worker-B") is None
    clk.advance(101)
    reaped = a.reap_expired()
    assert len(reaped) == 1
    job = a.get_job(old["id"])
    assert job["state"] == "queued" and job["shard_cursor"] == 2
    new = a.claim("worker-B")
    assert new["claim_epoch"] == 2 and new["claim_worker"] == "worker-B"
    # 旧 epoch 全系列拒绝
    for fn in (
        lambda: a.heartbeat(old["id"], 1, "worker-A"),
        lambda: a.report_progress(old["id"], 1, "worker-A", 3),
        lambda: a.complete(old["id"], 1, "worker-A"),
    ):
        with pytest.raises(ArbiterConflict) as ei:
            fn()
        assert ei.value.code == "stale_epoch"
    # 持有者不符
    with pytest.raises(ArbiterConflict) as ei:
        a.heartbeat(old["id"], 2, "worker-A")
    assert ei.value.code == "worker_mismatch"


def test_cursor_monotonic_and_progress_idempotent(arb):
    a, _ = arb
    a.upsert_tenant("t", weight=1, seats=1)
    a.enqueue("t", "correction", "k", shards_total=5)
    j = a.claim("w")
    out = a.report_progress(j["id"], 1, "w", 3)
    assert out["advanced"] is True and out["shard_cursor"] == 3
    again = a.report_progress(j["id"], 1, "w", 3)  # 相等：幂等
    assert again["advanced"] is False
    assert len(a.list_progress(j["id"])) == 1
    with pytest.raises(ArbiterConflict) as ei:
        a.report_progress(j["id"], 1, "w", 2)
    assert ei.value.code == "cursor_regress"
    with pytest.raises(ArbiterError):
        a.report_progress(j["id"], 1, "w", 6)  # 超界 400


def test_heartbeat_extends_lease(arb):
    a, clk = arb
    a.upsert_tenant("t", weight=1, seats=1)
    a.enqueue("t", "backfill", "k")
    j = a.claim("w")
    first_exp = j["lease_expires_at"]
    clk.advance(50)
    renewed = a.heartbeat(j["id"], 1, "w")
    assert renewed["lease_expires_at"] > first_exp
    clk.advance(60)  # 超过原 TTL 但心跳续过期
    assert a.reap_expired() == []


def test_complete_after_completion_rejected(arb):
    a, _ = arb
    a.upsert_tenant("t", weight=1, seats=1)
    a.enqueue("t", "backfill", "k", shards_total=2)
    j = a.claim("w")
    a.complete(j["id"], 1, "w")
    with pytest.raises(ArbiterConflict) as ei:
        a.complete(j["id"], 1, "w")
    assert ei.value.code == "not_claimed"


# --------------------------------------------------------------- 失败/续跑

def test_fail_retry_keeps_cursor(arb):
    a, _ = arb
    a.upsert_tenant("t", weight=1, seats=1)
    a.enqueue("t", "correction", "k", shards_total=6)
    j = a.claim("w1")
    for cur in range(1, 5):
        a.report_progress(j["id"], 1, "w1", cur)
    failed = a.fail(j["id"], 1, "w1", "boom")
    assert failed["state"] == "failed" and failed["shard_cursor"] == 4
    with pytest.raises(ArbiterError):
        a.retry_failed(j["id"] + 999)
    rj = a.retry_failed(j["id"])
    assert rj["id"] == j["id"] and rj["state"] == "queued"
    j2 = a.claim("w2")
    assert j2["id"] == j["id"] and j2["claim_epoch"] == 2
    assert j2["shard_cursor"] == 4
    a.report_progress(j["id"], 2, "w2", 5)
    a.report_progress(j["id"], 2, "w2", 6)
    done = a.complete(j["id"], 2, "w2")
    assert done["state"] == "succeeded"
    # 游标序列单调完整
    curs = [p["new_cursor"] for p in a.list_progress(j["id"])]
    assert curs == [1, 2, 3, 4, 5, 6]


def test_retry_only_for_failed(arb):
    a, _ = arb
    a.upsert_tenant("t")
    a.enqueue("t", "backfill", "k")
    j = a.claim("w")
    with pytest.raises(ArbiterError):
        a.retry_failed(j["id"])  # claimed 不能 retry


# --------------------------------------------------------------- 状态/历史

def test_status_fields_and_history(arb):
    a, clk = arb
    a.upsert_tenant("t", weight=2, seats=2)
    a.enqueue("t", "backfill", "k")
    clk.advance(30)
    j = a.claim("w")
    st = a.tenant_status("t")
    assert st["waiting_seconds"] == 0  # 已被领取
    assert st["seats_used"] == 1 and st["seats_available"] == 1
    hist = a.history(5)
    assert hist[0]["decision"] == "selected"
    assert hist[0]["tenant_name"] == "t"
    assert hist[0]["detail"]["weight"] == 2.0
    full = a.status()
    assert {"tenants", "history", "jobs", "lease_ttl", "time"} <= set(full)


def test_history_records_idle_reason(arb):
    a, _ = arb
    a.upsert_tenant("t", weight=1, seats=1)
    a.enqueue("t", "backfill", "k")
    a.pause("t")
    assert a.claim("w") is None
    h = a.history(1)[0]
    assert h["decision"] == "idle" and h["reason"] == "no_eligible_tenant"
    assert h["detail"]["tenants"] == {"t": "paused"}
