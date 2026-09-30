"""容量凭证与预算结算的单元/并发测试。

核心模块零第三方依赖（SQLite）；HTTP API 测试在缺少 fastapi 时自动 skip。
"""
import sys
import pathlib
import threading

import pytest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from featforge.budget import Budget, BudgetConflict, BudgetError  # noqa: E402
from featforge.budget_worker import BudgetWorker  # noqa: E402
from featforge.db import Database  # noqa: E402

try:
    from fastapi.testclient import TestClient  # noqa: F401
    HAS_FASTAPI = True
except ImportError:  # 最小环境（只有标准库）下只跑核心测试
    HAS_FASTAPI = False


class Clock:
    def __init__(self, t=1_000_000):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, n):
        self.t += n


@pytest.fixture
def budget(tmp_path):
    db = Database(str(tmp_path / "budget.db"))
    clk = Clock()
    b = Budget(db, clock=clk, lease_ttl=100, total_seats=2)
    yield b, clk, db
    db.close()


def make_customer(b, **kw):
    args = dict(refill_amount=10, period_seconds=10, burst_cap=40, initial=40)
    args.update(kw)
    return b.create_customer("acme", **args)


# ------------------------------------------------------------ 策略与预留

def test_policy_validation(budget):
    b, _, _ = budget
    with pytest.raises(BudgetError):
        b.create_customer("x", refill_amount=10, period_seconds=0, burst_cap=20)
    with pytest.raises(BudgetError):
        b.create_customer("x", refill_amount=50, period_seconds=10, burst_cap=20)
    make_customer(b)
    # 重复创建幂等不覆盖
    again = b.create_customer("acme", refill_amount=99, period_seconds=1,
                              burst_cap=99, initial=1)
    assert again["refill_amount"] == 10 and again["available"] == 40
    # 非法参数
    with pytest.raises(BudgetError):
        b.update_policy("acme", burst_cap=0)
    with pytest.raises(BudgetError):
        b.submit_job("acme", 0, "zero")
    with pytest.raises(BudgetError):
        b.submit_job("nope", 1, "k")


def test_reserve_settle_refund(budget):
    b, _, _ = budget
    make_customer(b, initial=30)
    r = b.submit_job("acme", 10, "j1")
    assert r["outcome"] == "reserved"
    assert b.get_customer("acme")["reserved"] == 10
    job = b.claim_next("w1")
    assert job["hold_epoch"] == 1 and job["holder"] == "w1"
    b.report(job["id"], 1, "w1", 4)
    st = b.get_customer("acme")
    assert st["reserved"] == 6 and st["settled"] == 4
    # 幂等重放
    b.report(job["id"], 1, "w1", 4)
    assert b.get_customer("acme")["settled"] == 4
    # 消耗不能后退
    with pytest.raises(BudgetConflict) as ei:
        b.report(job["id"], 1, "w1", 3)
    assert ei.value.code == "used_regress"
    done = b.complete(job["id"], 1, "w1", final_total=7)
    assert done["settled_total"] == 7 and done["refunded_total"] == 3
    st = b.get_customer("acme")
    assert st["reserved"] == 0 and st["available"] == 23 and st["settled"] == 7
    assert b.verify_invariants()["ok"]


def test_submit_replay_conflict(budget):
    b, _, _ = budget
    make_customer(b)
    a1 = b.submit_job("acme", 10, "k")
    a2 = b.submit_job("acme", 10, "k")
    assert a2["replayed"] and a2["job"]["id"] == a1["job"]["id"]
    with pytest.raises(BudgetConflict) as ei:
        b.submit_job("acme", 12, "k")
    assert ei.value.code == "business_key_conflict"


def test_over_burst_cap_rejected(budget):
    b, _, _ = budget
    make_customer(b, burst_cap=10, initial=10)
    with pytest.raises(BudgetConflict) as ei:
        b.submit_job("acme", 11, "too-big")
    assert ei.value.code == "over_burst_cap"


# ------------------------------------------------------------ 延后与唤醒

def test_deferred_earliest_and_wakeup(budget):
    b, clk, _ = budget
    make_customer(b, refill_amount=10, period_seconds=10,
                  burst_cap=30, initial=12)
    r1 = b.submit_job("acme", 10, "first")
    r2 = b.submit_job("acme", 10, "second")
    assert r1["outcome"] == "reserved" and r2["outcome"] == "deferred"
    j2 = r2["job"]
    assert j2["state"] == "waiting"
    assert j2["earliest_ready_at"] == clk.t + 10
    assert [x["id"] for x in b.blocked_jobs()] == [j2["id"]]
    j1 = b.claim_next("w1")
    b.report(j1["id"], 1, "w1", 10)
    b.complete(j1["id"], 1, "w1", final_total=10)
    assert b.get_job(j2["id"])["state"] == "waiting"
    clk.advance(10)
    b.tick()
    j2b = b.get_job(j2["id"])
    assert j2b["state"] == "running" and j2b["held_total"] == 10
    assert b.blocked_jobs() == []


def test_refill_respects_burst_cap(budget):
    b, clk, _ = budget
    make_customer(b, refill_amount=20, period_seconds=10,
                  burst_cap=30, initial=30)
    # 桶满：跨多个周期也不超上限
    clk.advance(100)
    b.tick()
    st = b.get_customer("acme")
    assert st["available"] == 30 and st["outstanding"] <= 30
    # 用掉 10 后桶满窗口被右移到当前；下一笔补充被上限截断为 +10（20 -> 30）
    r = b.submit_job("acme", 10, "j")
    j = b.claim_next("w1")
    b.report(j["id"], 1, "w1", 10)
    b.complete(j["id"], 1, "w1", final_total=10)  # 无退款
    clk.advance(10)
    b.tick()
    assert b.get_customer("acme")["available"] == 30


# ------------------------------------------------------------ 并发防透支

def test_concurrent_reserve_no_overdraft(tmp_path):
    path = str(tmp_path / "conc.db")
    clk = Clock()
    db = Database(path)
    Budget(db, clock=clk, total_seats=200).create_customer(
        "acme", refill_amount=10, period_seconds=10,
        burst_cap=100, initial=50)
    N = 20
    barrier = threading.Barrier(N)
    results, errors = [], []

    def submit(i):
        try:
            bb = Budget(Database(path), clock=clk, total_seats=200)
            barrier.wait()
            results.append(bb.submit_job("acme", 10, f"k-{i:02d}"))
        except Exception as e:
            errors.append(e)

    ts = [threading.Thread(target=submit, args=(i,)) for i in range(N)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    assert not errors
    reserved = [r for r in results if r["outcome"] == "reserved"]
    deferred = [r for r in results if r["outcome"] == "deferred"]
    assert len(reserved) == 5 and len(deferred) == 15
    st = Budget(db, clock=clk, total_seats=200).get_customer("acme")
    assert st["available"] == 0 and st["reserved"] == 50
    assert Budget(db, clock=clk, total_seats=200).verify_invariants()["ok"]


def test_concurrent_terminal_race_settles_once(tmp_path):
    path = str(tmp_path / "race.db")
    clk = Clock()
    db = Database(path)
    b = Budget(db, clock=clk, total_seats=4)
    b.create_customer("acme", refill_amount=10, period_seconds=10,
                      burst_cap=40, initial=40)
    r = b.submit_job("acme", 10, "race")
    jid = r["job"]["id"]
    j = b.claim_next("w1")
    b.report(jid, j["hold_epoch"], "w1", 5)
    barrier = threading.Barrier(2)
    outcomes = []

    def fn_complete():
        bb = Budget(Database(path), clock=clk, total_seats=4)
        barrier.wait()
        outcomes.append(("c", bb.complete(jid, 1, "w1", final_total=5)))

    def fn_terminate():
        bb = Budget(Database(path), clock=clk, total_seats=4)
        barrier.wait()
        outcomes.append(("t", bb.terminate(jid)))

    ts = [threading.Thread(target=fn_complete), threading.Thread(target=fn_terminate)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    final = b.get_job(jid)
    assert final["state"] in ("succeeded", "terminated")
    assert final["used_total"] == 5 and final["refunded_total"] == 5
    nref = db.conn().execute(
        "SELECT COUNT(*) n FROM bg_ledger WHERE job_id=? AND op='refund'",
        (jid,)).fetchone()["n"]
    assert nref == 1
    assert b.verify_invariants()["ok"]


# ------------------------------------------------------------ 超额暂停

def test_overrun_pauses_and_releases_seat(budget):
    b, clk, _ = budget
    make_customer(b, refill_amount=10, period_seconds=10,
                  burst_cap=40, initial=12)
    b.submit_job("acme", 10, "over")
    j = b.claim_next("wA")
    r = b.report(j["id"], 1, "wA", 12)   # available 2 刚好够追加
    assert r["held_total"] == 12 and not r.get("paused_for_budget")
    r = b.report(j["id"], 1, "wA", 13)   # 差 1，没钱
    assert r["state"] == "paused"
    assert r["confirmed_position"] == 12
    assert r["blocked_reason"] == "need_extend"
    # 席位已释放
    assert b.status()["seats_used"] == 0
    with pytest.raises(BudgetConflict) as ei:
        b.report(j["id"], 1, "wA", 13)
    assert ei.value.code == "not_running"
    # 补充后队头恢复，FIFO：新席位 epoch=2，从 12 继续
    clk.advance(10)
    b.tick()
    j2 = b.claim_next("wB")
    assert j2["id"] == j["id"] and j2["hold_epoch"] == 2
    assert j2["used_total"] == 12 and j2["holder"] == "wB"
    r = b.report(j["id"], 2, "wB", 13)
    assert r["state"] == "running" and r["used_total"] == 13
    done = b.complete(j["id"], 2, "wB", final_total=13)
    assert done["state"] == "succeeded" and done["refunded_total"] == 0


def test_complete_short_of_funds_pauses(budget):
    b, clk, _ = budget
    make_customer(b, refill_amount=10, period_seconds=10,
                  burst_cap=40, initial=10)
    b.submit_job("acme", 10, "j")
    j = b.claim_next("w1")
    b.report(j["id"], 1, "w1", 10)
    # 完成时声称实际 12，available 0 -> 暂停而非完成
    r = b.complete(j["id"], 1, "w1", final_total=12)
    assert r["state"] == "paused" and r["pending_total"] == 12
    assert b.get_job(j["id"])["used_total"] == 10


# ------------------------------------------------------------ 失联/接管

def test_takeover_fences_old_holder(budget):
    b, clk, db = budget
    make_customer(b, initial=30)
    b.submit_job("acme", 10, "j")
    j = b.claim_next("wA")
    b.report(j["id"], 1, "wA", 4)
    clk.advance(101)
    lost = b.reap_lost()
    assert len(lost) == 1 and lost[0]["state"] == "lost"
    j2 = b.takeover(j["id"], "wB")
    assert j2["hold_epoch"] == 2 and j2["used_total"] == 4
    # 旧 epoch 上报一律拒绝且不扣费
    for call in (
        lambda: b.report(j["id"], 1, "wA", 5),
        lambda: b.heartbeat(j["id"], 1, "wA"),
        lambda: b.complete(j["id"], 1, "wA"),
    ):
        with pytest.raises(BudgetConflict) as ei:
            call()
        assert ei.value.code == "stale_epoch"
    assert b.get_job(j["id"])["used_total"] == 4
    # 接替者只补差额
    b.report(j["id"], 2, "wB", 10)
    b.complete(j["id"], 2, "wB", final_total=10)
    settled = db.conn().execute(
        "SELECT COALESCE(SUM(d_settled),0) s FROM bg_ledger "
        "WHERE job_id=? AND op='settle'", (j["id"],)).fetchone()["s"]
    assert settled == 10
    assert b.verify_invariants()["ok"]


def test_lease_held_within_ttl_blocks_takeover(budget):
    b, _, _ = budget
    make_customer(b)
    b.submit_job("acme", 10, "j")
    b.claim_next("wA")
    with pytest.raises(BudgetConflict) as ei:
        b.takeover(b.list_jobs()[0]["id"], "wB")
    assert ei.value.code == "not_running"


# ------------------------------------------------------------ 终止幂等

def test_terminate_idempotent_states(budget):
    b, _, db = budget
    make_customer(b, initial=40)
    # running：used 3 / held 8 -> 退 5
    r = b.submit_job("acme", 8, "r")
    jid = r["job"]["id"]
    j = b.claim_next("w1")
    b.report(jid, j["hold_epoch"], "w1", 3)
    t1 = b.terminate(jid)
    t2 = b.terminate(jid)
    assert t1["state"] == "terminated" and t1["refunded_total"] == 5
    assert t2.get("replayed") and t2["refunded_total"] == 5
    assert db.conn().execute(
        "SELECT COUNT(*) n FROM bg_ledger WHERE job_id=? AND op='refund'",
        (jid,)).fetchone()["n"] == 1
    # ready：退全额
    r = b.submit_job("acme", 6, "rd")
    t = b.terminate(r["job"]["id"])
    assert t["refunded_total"] == 6
    # waiting：零退款、零凭证
    for i in range(8):
        b.submit_job("acme", 5, f"f{i}")
    rw = b.submit_job("acme", 7, "wt")
    assert rw["job"]["state"] == "waiting"
    tw = b.terminate(rw["job"]["id"])
    assert tw["refunded_total"] == 0
    assert db.conn().execute(
        "SELECT COUNT(*) n FROM bg_vouchers WHERE job_id=?",
        (rw["job"]["id"],)).fetchone()["n"] == 0
    assert b.verify_invariants()["ok"]


def test_complete_terminate_terminal_once(budget):
    b, _, db = budget
    make_customer(b, initial=40)

    def refunds(jid):
        return db.conn().execute(
            "SELECT COUNT(*) n FROM bg_ledger WHERE job_id=? AND op='refund'",
            (jid,)).fetchone()["n"]

    # complete 赢
    r = b.submit_job("acme", 10, "a"); jid = r["job"]["id"]
    j = b.claim_next("w1")
    b.report(jid, 1, "w1", 8)
    b.complete(jid, 1, "w1", final_total=8)
    late = b.terminate(jid)
    assert late["state"] == "succeeded" and late.get("already_terminal")
    assert refunds(jid) == 1
    # terminate 赢
    r = b.submit_job("acme", 10, "bb"); jid = r["job"]["id"]
    j = b.claim_next("w2")
    b.report(jid, j["hold_epoch"], "w2", 6)
    b.terminate(jid)
    late = b.complete(jid, j["hold_epoch"], "w2", final_total=10)
    assert late["state"] == "terminated" and late.get("already_terminal")
    assert late["used_total"] == 6 and refunds(jid) == 1


# ------------------------------------------------------------ 在线调预算

def test_policy_change_does_not_revoke_vouchers(budget):
    b, _, _ = budget
    make_customer(b, refill_amount=100, period_seconds=10,
                  burst_cap=100, initial=100)
    r = b.submit_job("acme", 10, "j")
    jid = r["job"]["id"]
    before = b.list_vouchers(jid)
    # 在线把预算调到极小：已发出凭证与 reserved 不动
    b.update_policy("acme", refill_amount=1, period_seconds=10, burst_cap=10)
    after = b.list_vouchers(jid)
    assert [dict(v) for v in after] == [dict(v) for v in before]
    st = b.get_customer("acme")
    assert st["reserved"] == 10  # 不撤销
    job = b.claim_next("w1")
    b.report(jid, job["hold_epoch"], "w1", 10)
    done = b.complete(jid, job["hold_epoch"], "w1", final_total=10)
    assert done["state"] == "succeeded"
    # 后续预留按新策略（burst 10、available 退款进来但上限只影响补充）
    r2 = b.submit_job("acme", 10, "j2")
    # 之前 90 可用 + 本次无退款 = 90，仍可预留（调小 cap 不回收存量）
    assert r2["outcome"] == "reserved"


def test_pause_blocks_dispatch_not_refill(budget):
    b, clk, _ = budget
    make_customer(b, initial=0)
    b.pause("acme")
    r = b.submit_job("acme", 10, "j")
    assert r["outcome"] == "deferred"
    clk.advance(10)
    b.tick()
    # 补充到账并预留（工单取得资格 ready），但暂停期间不派发、不退预留
    cust = b.get_customer("acme")
    assert cust["available"] == 0 and cust["reserved"] == 10
    assert b.get_job(r["job"]["id"])["state"] == "ready"
    assert b.claim_next("w1") is None
    b.resume("acme")
    j = b.claim_next("w1")
    assert j is not None and j["id"] == r["job"]["id"]


# ------------------------------------------------------------ worker 集成

def test_worker_full_lifecycle(budget):
    b, _, _ = budget
    make_customer(b, initial=20)
    r = b.submit_job("acme", 5, "w-job", payload={"steps": 5})
    w = BudgetWorker(b, "w1")
    out = w.tick()
    assert out["state"] == "succeeded" and out["used_total"] == 5
    assert b.verify_invariants()["ok"]


def test_worker_die_after_takeover(budget):
    b, clk, _ = budget
    make_customer(b, initial=20)
    b.submit_job("acme", 6, "die", payload={"steps": 6, "die_after": 3})
    w = BudgetWorker(b, "wA")
    with pytest.raises(Exception):
        w.tick()
    j = b.list_jobs()[0]
    assert j["state"] == "running" and j["used_total"] == 3
    clk.advance(101)
    b.reap_lost()
    j2 = b.takeover(j["id"], "wB")
    assert j2["used_total"] == 3
    w2 = BudgetWorker(b, "wB", inject_faults=False)
    out = w2.process(b.get_job(j["id"]))
    assert out["state"] == "succeeded" and out["used_total"] == 6


# ------------------------------------------------------------ HTTP API

def test_seats_shared_across_customers(budget):
    b, _, _ = budget
    b.create_customer("a", refill_amount=100, period_seconds=10,
                      burst_cap=100, initial=100)
    b.create_customer("z", refill_amount=100, period_seconds=10,
                      burst_cap=100, initial=100)
    b.submit_job("a", 1, "a1"); b.submit_job("z", 1, "z1")
    b.submit_job("a", 1, "a2")  # 全局只有 2 席
    ja = b.claim_next("w1"); jz = b.claim_next("w2")
    assert ja is not None and jz is not None
    assert b.claim_next("w3") is None
    assert b.status()["seats_used"] == 2
    # 完成一个释放席位，第三个（无论哪个客户）可入场
    b.complete(ja["id"], ja["hold_epoch"], "w1", final_total=1)
    j3 = b.claim_next("w3")
    assert j3 is not None and b.status()["seats_used"] == 2


def test_heartbeat_keeps_lease(budget):
    b, clk, _ = budget
    make_customer(b)
    b.submit_job("acme", 10, "j")
    j = b.claim_next("w1")
    clk.advance(60)
    b.heartbeat(j["id"], 1, "w1")   # TTL=100 内续期
    clk.advance(60)                  # 累计 120，但上次心跳后只过 60
    assert b.reap_lost() == []
    b.report(j["id"], 1, "w1", 5)   # 未失联，上报正常
    clk.advance(101)
    assert len(b.reap_lost()) == 1


def test_concurrent_takeover_only_one(tmp_path):
    path = str(tmp_path / "to.db")
    clk = Clock()
    db = Database(path)
    b = Budget(db, clock=clk, lease_ttl=100, total_seats=2)
    b.create_customer("acme", 10, 10, 40, initial=40)
    jid = b.submit_job("acme", 10, "j")["job"]["id"]
    j = b.claim_next("wA")
    b.report(jid, 1, "wA", 2)
    clk.advance(101)
    b.reap_lost()
    barrier = threading.Barrier(2)
    winners = []

    def take(who):
        bb = Budget(Database(path), clock=clk, lease_ttl=100, total_seats=2)
        barrier.wait()
        try:
            r = bb.takeover(jid, who)
            winners.append((who, r["hold_epoch"], r["holder"]))
        except BudgetConflict:
            winners.append((who, None, None))

    ts = [threading.Thread(target=take, args=(f"w{i}",)) for i in range(2)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    success = [w for w in winners if w[1] is not None]
    assert len(success) == 1 and success[0][2] in ("w0", "w1")
    final = b.get_job(jid)
    assert final["state"] == "running" and final["hold_epoch"] == 2
    assert b.verify_invariants()["ok"]


# ------------------------------------------------------------ HTTP API

@pytest.mark.skipif(not HAS_FASTAPI, reason="环境无 fastapi")
def test_http_api(tmp_path):
    from fastapi.testclient import TestClient
    from featforge.api import create_app
    from featforge.store import Store

    db = Database(str(tmp_path / "api.db"))
    clk = Clock()
    store = Store(db, clock=clk)
    bud = Budget(db, clock=clk, lease_ttl=100, total_seats=1)
    c = TestClient(create_app(store, budget=bud))

    r = c.post("/api/v1/budget/customers", json={
        "name": "acme", "refill_amount": 10, "period_seconds": 10,
        "burst_cap": 30, "initial": 12})
    assert r.status_code == 201 and r.json()["available"] == 12
    # 重放 200
    assert c.post("/api/v1/budget/jobs", json={
        "customer": "acme", "estimated": 10, "business_key": "j1"}).status_code in (200, 201)
    r2 = c.post("/api/v1/budget/jobs", json={
        "customer": "acme", "estimated": 10, "business_key": "j1"})
    assert r2.status_code == 200 and r2.json()["replayed"] is True
    r3 = c.post("/api/v1/budget/jobs", json={
        "customer": "acme", "estimated": 10, "business_key": "j2"})
    assert r3.status_code == 201 and r3.json()["outcome"] == "deferred"
    j = c.post("/api/v1/budget/claims", json={"holder": "wA"}).json()
    r = c.post(f"/api/v1/budget/jobs/{j['id']}/report",
               json={"hold_epoch": 1, "holder": "wA", "used_total": 13})
    assert r.json()["state"] == "paused"
    clk.advance(10)
    c.post("/api/v1/budget/tick")
    j2 = c.post("/api/v1/budget/claims", json={"holder": "wB"}).json()
    assert j2["id"] == j["id"] and j2["hold_epoch"] == 2
    # 旧 epoch 上报 409
    r = c.post(f"/api/v1/budget/jobs/{j['id']}/report",
               json={"hold_epoch": 1, "holder": "wA", "used_total": 13})
    assert r.status_code == 409 and r.json()["detail"]["error"] in (
        "stale_epoch", "not_running")
    r = c.post(f"/api/v1/budget/jobs/{j['id']}/complete",
               json={"hold_epoch": 2, "holder": "wB", "final_total": 13})
    assert r.json()["state"] == "succeeded"
    assert c.get("/api/v1/budget/verify").json()["ok"]
    assert c.get("/api/v1/budget/status").json()["customers"]
    html = c.get("/").text
    assert "后台计算预算" in html and "被预算阻塞" in html
