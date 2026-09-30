"""既有回填/订正/重算作业经桥接进入多租户统一 ready set 的集成测试。"""
import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import pytest

from featforge.arbiter import Arbiter
from featforge.arbiter_bridge import LegacyBridge
from featforge.config import settings
from featforge.db import Database
from featforge.store import Store
from conftest import events, make_feature


class Clock:
    def __init__(self, t=1_000_000):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, n):
        self.t += n


@pytest.fixture
def bridged(tmp_path):
    settings.sample_fraction = 1.0
    settings.sample_tick_seconds = 300
    settings.partition_seconds = 3600
    db = Database(str(tmp_path / "b.db"))
    clk = Clock()
    store = Store(db, clock=clk)
    arb = Arbiter(db, clock=clk, lease_ttl=100, aging_rate=1.0)
    bridge = LegacyBridge(store, arb, default_tenant="ops")
    return store, arb, bridge, clk


def test_backfill_mirrored_and_runs_through_arbiter(bridged):
    store, arb, bridge, _ = bridged
    make_feature(store)
    events(store, [("u1", 300, 1.0), ("u1", 3900, 2.0)])
    mirrored = bridge.sync()
    assert mirrored == [1]  # 基线回填作业
    jobs = arb.list_jobs()
    assert len(jobs) == 1 and jobs[0]["kind"] == "backfill"
    assert jobs[0]["tenant_name"] == "amt_1h"  # 按特征名归属租户
    # 经仲裁器 claim 并执行
    out = bridge.claim_and_run("w1")
    assert out["job"]["state"] == "succeeded"
    assert arb.list_jobs()[0]["state"] == "succeeded"
    # 自动切换属于 Worker.tick 职责；桥接只负责执行回填，回填完成后可切换
    store.promote_generation(out["job"]["gen_id"])
    assert store.read_online("amt_1h", "u1")["serving_gen"] == 1


def test_paused_tenant_blocks_legacy_claim(bridged):
    store, arb, bridge, _ = bridged
    make_feature(store)
    events(store, [("u1", 300, 1.0)])
    bridge.sync()
    arb.pause("amt_1h")
    assert bridge.claim_and_run("w1") is None
    arb.resume("amt_1h")
    out = bridge.claim_and_run("w1")
    assert out["job"]["state"] == "succeeded"


def test_bridge_idempotent_sync_and_seats(bridged):
    store, arb, bridge, _ = bridged
    make_feature(store)
    events(store, [("u1", 300, 1.0)])
    assert bridge.sync() == [1]
    assert bridge.sync() == []  # 已镜像不重复
    # 租户席位默认 2；只创建一个作业
    st = arb.tenant_status("amt_1h")
    assert st["queued"] == 1


def test_multi_feature_tenants_share_ready_set(bridged):
    """两个特征（租户）的回填在统一 ready set 里按权重/老化分流。"""
    store, arb, bridge, clk = bridged
    make_feature(store)
    make_feature(store, name="cnt_1h", source="click",
                 value_field="n", transform="count")
    events(store, [("u1", 300, 1.0)])
    for ts in (100, 3700):
        store.ingest_event("click", "u1", ts, raw={"n": 1})
    bridge.sync()
    tenants = {t["name"] for t in arb.list_tenants()}
    assert {"amt_1h", "cnt_1h", "ops"} <= tenants
    # 各跑一个：按入队次序（job1=amt, job2=cnt）
    o1 = bridge.claim_and_run("w1")
    o2 = bridge.claim_and_run("w1")
    assert {o1["legacy_job_id"], o2["legacy_job_id"]} == {1, 2}
    assert o1["job"]["state"] == o2["job"]["state"] == "succeeded"


def test_lease_expiry_reconciles_running_legacy_job(bridged):
    """执行者僵死：租约超时后镜像作业与 jobs 行都复位，可被接管。"""
    store, arb, bridge, clk = bridged
    make_feature(store)
    events(store, [("u1", 300, 1.0)])
    bridge.sync()
    claim = arb.claim("dead-worker")
    legacy = store.get_job(1)
    # 模拟 worker 已把 jobs 置 running 后僵死
    assert legacy["state"] == "queued"
    bridge_out = None
    # claim_and_run 会真正执行；这里手工模拟“领取后、执行前崩溃”：
    c = store.db.conn()
    c.execute("UPDATE jobs SET state='running', arb_claim_epoch=? WHERE id=1",
              (claim["claim_epoch"],))
    c.commit()
    clk.advance(101)
    reset_ids = bridge.reconcile_expired()
    assert reset_ids == [1]
    assert store.get_job(1)["state"] == "queued"
    assert arb.get_job(claim["id"])["state"] == "queued"
    # 新执行者接管并跑完（检查点表为空，从头安全重跑）
    out = bridge.claim_and_run("new-worker")
    assert out["job"]["state"] == "succeeded"
    assert out["claim_epoch"] == claim["claim_epoch"] + 1
