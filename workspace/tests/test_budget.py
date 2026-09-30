"""容量凭证与预算结算：预留 / 延后唤醒 / 并发抢额 / 超额暂停 /
失联接替 / 终止幂等 / 完成-终止竞争，以及账本可重放不变量。"""
import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import pytest

from featforge.budget import (
    BudgetConflict, BudgetError, BudgetService,
)
from featforge.db import Database


class Clock:
    def __init__(self, t=1_000_000):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, n):
        self.t += n


@pytest.fixture
def svc(tmp_path):
    db = Database(str(tmp_path / "budget.db"))
    clk = Clock()
    s = BudgetService(db, clock=clk, lease_ttl=100, auto_scheduler=False)
    yield s, clk
    s.stop_scheduler()
    db.close()


def make_customer(svc, **kw):
    args = dict(refill_amount=5, refill_interval=60, burst_cap=10,
                max_seats=2, opening_balance=10)
    args.update(kw)
    return svc.configure_customer("acme", **args)


# ------------------------------------------------------------ 1. 正常预留

def test_normal_reserve_and_settle(svc):
    s, clk = svc
    make_customer(s)
    v = s.submit("acme", "job-1", 4)
    assert v["state"] == "reserved"
    assert v["reserved_total"] == 4
    cust = s.get_customer("acme")
    assert cust["available"] == 6
    assert cust["reserved_open"] == 4
    assert cust["settled_total"] == 0

    a = s.acquire("acme", v["id"], "w1")
    assert a["claim_epoch"] == 1 and a["state"] == "executing"
    r = s.report_consumed("acme", v["id"], 1, "w1", 3)
    assert r["action"] == "settled" and r["confirmed_total"] == 3
    cust = s.get_customer("acme")
    # 已预留未结算少了 3，已结算多了 3；可用余额不变（预留时已扣）
    assert cust["reserved_open"] == 1 and cust["settled_total"] == 3
    assert cust["available"] == 6

    # 实际消耗小于预估：不退款（预留即承诺），只结算到实际
    d = s.complete("acme", v["id"], 1, "w1", 3)
    assert d["state"] == "completed"
    # reserved 4 - settled 3 = 1 未用预留，完成时退回
    assert d["refunded"] == 1
    cust = s.get_customer("acme")
    assert cust["available"] == 7 and cust["settled_total"] == 3
    assert cust["reserved_open"] == 0
    assert s.verify_invariants() == []


def test_submit_idempotent_and_conflict(svc):
    s, _ = svc
    make_customer(s)
    v1 = s.submit("acme", "dup", 3, submitter="u1")
    v2 = s.submit("acme", "dup", 3, submitter="u1")
    assert v1["id"] == v2["id"]
    with pytest.raises(BudgetConflict) as ei:
        s.submit("acme", "dup", 4, submitter="u1")
    assert ei.value.code == "job_key_conflict"


# ----------------------------------------------------- 2. 余额不足自动唤醒

def test_deferred_wakes_on_refill(svc):
    s, clk = svc
    make_customer(s, opening_balance=3, refill_amount=4, refill_interval=60)
    v = s.submit("acme", "big", 6)
    assert v["state"] == "deferred"
    assert v["blocked_reason"] == "insufficient_balance"
    # 3 + 4 = 7 >= 6：下一个补充点即可执行
    assert v["earliest_ready_at"] == 1_000_060

    # 补充点之前仍延后
    clk.advance(59)
    s.tick()
    assert s.get_voucher(v["id"])["state"] == "deferred"

    # 到点：后台 tick 补充 + pump，自动取得资格
    clk.advance(1)
    s.tick()
    v = s.get_voucher(v["id"])
    assert v["state"] == "reserved" and v["reserved_total"] == 6
    cust = s.get_customer("acme")
    assert cust["available"] == 1  # 7 - 6
    assert s.verify_invariants() == []


def test_deferred_fifo_order(svc):
    s, clk = svc
    make_customer(s, opening_balance=5, refill_amount=3, refill_interval=60)
    a = s.submit("acme", "a", 4)  # 直接预留 4
    b = s.submit("acme", "b", 4)  # 余额 1，延后
    c = s.submit("acme", "c", 3)  # 也延后，排在 b 之后
    assert a["state"] == "reserved"
    assert s.get_voucher(b["id"])["state"] == "deferred"
    assert s.get_voucher(c["id"])["state"] == "deferred"
    clk.advance(60)
    s.tick()
    # 1+3=4 只够 b；c 仍延后
    assert s.get_voucher(b["id"])["state"] == "reserved"
    assert s.get_voucher(c["id"])["state"] == "deferred"
    clk.advance(60)
    s.tick()
    assert s.get_voucher(c["id"])["state"] == "reserved"
    assert s.verify_invariants() == []


def test_over_burst_cap_never_runnable(svc):
    s, clk = svc
    make_customer(s, opening_balance=0, refill_amount=5, refill_interval=60,
                  burst_cap=10)
    v = s.submit("acme", "huge", 11)
    assert v["state"] == "deferred"
    assert v["blocked_reason"] == "over_burst_cap"
    assert v["earliest_ready_at"] is None
    clk.advance(10_000)
    s.tick()
    assert s.get_voucher(v["id"])["state"] == "deferred"


# ------------------------------------------------- 3. 并发抢额防透支

def test_concurrent_grab_last_credit(svc):
    import threading
    s, _ = svc
    make_customer(s, opening_balance=10)
    # 10 个提交者同时抢每份 6 的额度：最多 1 个成功，其余延后
    results = []
    errors = []

    def grab(i):
        try:
            results.append(s.submit("acme", f"job-{i}", 6)["state"])
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=grab, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    assert results.count("reserved") == 1
    assert results.count("deferred") == 9
    cust = s.get_customer("acme")
    assert cust["available"] == 4  # 10 - 6，绝不为负
    assert s.verify_invariants() == []
    # 账本中任何一行之后的余额都不允许为负
    for row in s.ledger("acme", limit=1000):
        assert row["balance_after"] >= 0


def test_concurrent_grab_exact_fit(svc):
    import threading
    s, _ = svc
    make_customer(s, opening_balance=10, max_seats=10)
    outcomes = []

    def grab(i):
        outcomes.append(s.submit("acme", f"j{i}", 5)["state"])

    ts = [threading.Thread(target=grab, args=(i,)) for i in range(4)]
    for t in ts:
        t.start()
    for t in ts:
        t.join()
    # 两份恰好 10，第三、四个必须延后（不存在 -5 / -10 的透支窗口）
    assert sorted(outcomes) == ["deferred", "deferred", "reserved", "reserved"]
    assert s.get_customer("acme")["available"] == 0


# ------------------------------------------------- 4. 实际消耗超额暂停

def test_overrun_extra_reserve_then_pause(svc):
    s, clk = svc
    # 余额 6：预估 4，执行到 6 需要追加 2（成功）；再上报 8 需要追加 2
    #（余额 0，失败）→ 停在 6，释放席位
    make_customer(s, opening_balance=6, refill_amount=10, refill_interval=60)
    v = s.submit("acme", "ov", 4)
    a = s.acquire("acme", v["id"], "w1")
    assert a["claim_epoch"] == 1

    r = s.report_consumed("acme", v["id"], 1, "w1", 6)
    assert r["action"] == "settled"
    assert s.get_voucher(v["id"])["reserved_total"] == 6
    assert s.get_customer("acme")["available"] == 0

    r = s.report_consumed("acme", v["id"], 1, "w1", 8)
    assert r["action"] == "paused"
    assert r["confirmed_total"] == 6      # 最近已确认位置
    assert r["requested_total"] == 8
    v = s.get_voucher(v["id"])
    assert v["state"] == "paused" and v["settled_total"] == 6
    assert v["claim_worker"] is None       # 不再占用执行席位
    assert s.get_customer("acme")["seats_used"] == 0

    # 旧持有者仍拿着 epoch=1：凭证已不在 executing，上报直接 409
    with pytest.raises(BudgetConflict) as ei:
        s.report_consumed("acme", v["id"], 1, "w1", 7)
    assert ei.value.code == "not_executing"

    # 补充到账后自动恢复 reserved
    clk.advance(60)
    s.tick()
    v = s.get_voucher(v["id"])
    assert v["state"] == "reserved" and v["reserved_total"] == 8

    # 必须重新占用，epoch 从 1 跳到 2
    a2 = s.acquire("acme", v["id"], "w2")
    assert a2["claim_epoch"] == 2 and a2["state"] == "executing"
    d = s.complete("acme", v["id"], 2, "w2", 8)
    assert d["state"] == "completed"
    assert s.verify_invariants() == []


def test_monotonic_progress_and_idempotent_report(svc):
    s, _ = svc
    make_customer(s)
    v = s.submit("acme", "m", 5)
    a = s.acquire("acme", v["id"], "w1")
    s.report_consumed("acme", v["id"], 1, "w1", 4)
    # 重复/回退上报：幂等，不再扣费
    r = s.report_consumed("acme", v["id"], 1, "w1", 4)
    assert r["action"] == "idempotent"
    r = s.report_consumed("acme", v["id"], 1, "w1", 2)
    assert r["action"] == "idempotent" and r["confirmed_total"] == 4
    assert s.get_voucher(v["id"])["settled_total"] == 4
    assert s.verify_invariants() == []


# --------------------------------------------- 5. 失联接替不重复计费

def test_failover_after_lost_worker(svc):
    s, clk = svc
    # 补充周期远超测试时长，隔离失联接替语义（避免 token bucket 补充/裁剪
    # 干扰退款断言）；burst_cap 留出空间使未用预留能退回余额。
    make_customer(s, opening_balance=10, refill_amount=10,
                  refill_interval=100_000, burst_cap=20)
    v = s.submit("acme", "lost", 8)
    a = s.acquire("acme", v["id"], "w1")  # epoch 1
    s.report_consumed("acme", v["id"], 1, "w1", 5)
    assert s.get_customer("acme")["seats_used"] == 1

    # w1 失联，租约到期
    clk.advance(101)
    reaped = s.reap_expired()
    assert len(reaped) == 1 and reaped[0]["old_epoch"] == 1
    assert reaped[0]["settled_total"] == 5  # 金额沿用
    vv = s.get_voucher(v["id"])
    assert vv["state"] == "executing" and vv["claim_worker"] is None
    assert vv["claim_epoch"] == 1          # 接管前 epoch 不变
    assert s.get_customer("acme")["seats_used"] == 0

    # 旧持有者迟到上报：epoch 1 已不是持有者 → fence 拒绝（租约超时）
    with pytest.raises(BudgetConflict) as ei:
        s.report_consumed("acme", v["id"], 1, "w1", 6)
    assert ei.value.code == "lease_expired"

    # 接替者重新占用：epoch +1，已结算 5 沿用
    a2 = s.acquire("acme", v["id"], "w2")
    assert a2["claim_epoch"] == 2
    assert s.get_voucher(v["id"])["settled_total"] == 5
    s.report_consumed("acme", v["id"], 2, "w2", 7)

    # 旧持有者复活再上报同样的 6 / 7：stale_epoch，绝不二次扣费
    with pytest.raises(BudgetConflict) as ei:
        s.report_consumed("acme", v["id"], 1, "w1", 6)
    assert ei.value.code == "stale_epoch"
    with pytest.raises(BudgetConflict) as ei:
        s.complete("acme", v["id"], 1, "w1", 7)
    assert ei.value.code == "stale_epoch"

    vv = s.get_voucher(v["id"])
    assert vv["settled_total"] == 7        # 不是 5+6/7 重复累加
    d = s.complete("acme", v["id"], 2, "w2", 7)
    assert d["state"] == "completed" and d["refunded"] == 1  # 8-7
    assert s.verify_invariants() == []


def test_takeover_frees_seat_with_same_customer(svc):
    """单席位客户：失联凭证被接替时不得被自己占住席位。"""
    s, clk = svc
    make_customer(s, opening_balance=10, max_seats=1)
    v = s.submit("acme", "x", 3)
    s.acquire("acme", v["id"], "w1")
    clk.advance(101)
    s.reap_expired()
    a2 = s.acquire("acme", v["id"], "w2")
    assert a2["claim_epoch"] == 2 and a2["state"] == "executing"


# ------------------------------------------------- 6. 终止退款幂等

def test_terminate_refunds_only_unused(svc):
    s, _ = svc
    make_customer(s, opening_balance=10)
    v = s.submit("acme", "t", 8)
    a = s.acquire("acme", v["id"], "w1")
    s.report_consumed("acme", v["id"], a["claim_epoch"], "w1", 3)
    cust = s.get_customer("acme")
    assert cust["available"] == 2  # 10 - 8

    r1 = s.terminate("acme", v["id"], reason="cancel")
    assert r1["state"] == "terminated" and not r1["replayed"]
    assert r1["refunded"] == 5      # 8 - 3
    assert s.get_customer("acme")["available"] == 7

    # 重复终止：幂等，不再退款
    r2 = s.terminate("acme", v["id"], reason="cancel-again")
    assert r2["replayed"] and r2["refunded"] == 0
    r3 = s.terminate("acme", v["id"])
    assert r3["refunded"] == 0
    vv = s.get_voucher(v["id"])
    assert vv["refunded_total"] == 5 and vv["settled_total"] == 3
    assert vv["terminate_reason"] == "cancel"  # 第一次的原因保留

    # 终态后旧持有者完成 → 409 terminal
    with pytest.raises(BudgetConflict) as ei:
        s.complete("acme", v["id"], a["claim_epoch"], "w1", 3)
    assert ei.value.code == "terminal"
    assert s.verify_invariants() == []


def test_terminate_reserved_without_acquire(svc):
    s, _ = svc
    make_customer(s)
    v = s.submit("acme", "t2", 6)
    # 未执行直接终止：已结算 0，全额退还
    r = s.terminate("acme", v["id"])
    assert r["refunded"] == 6 and r["state"] == "terminated"
    assert s.get_customer("acme")["available"] == 10
    assert s.verify_invariants() == []


def test_refund_clamped_by_burst_cap(svc):
    s, clk = svc
    make_customer(s, opening_balance=10, refill_amount=10, refill_interval=60,
                  burst_cap=10)
    v = s.submit("acme", "clamp", 8)
    # 余额 2；一个周期后补到 10
    clk.advance(60)
    s.tick()
    assert s.get_customer("acme")["available"] == 10
    # 此时终止：退 8 但只剩 0 空间（余额已 10），全部裁剪为损耗
    r = s.terminate("acme", v["id"])
    assert r["refunded"] == 0 and r["lost"] == 8
    assert s.get_customer("acme")["available"] == 10
    assert s.verify_invariants() == []


# --------------------------------------------- 7. 完成与终止竞争

def test_complete_terminate_race(svc):
    import threading
    s, _ = svc
    make_customer(s, opening_balance=10)
    v = s.submit("acme", "race", 6)
    a = s.acquire("acme", v["id"], "w1")
    s.report_consumed("acme", v["id"], 1, "w1", 4)
    outcome = {}

    def do_complete():
        try:
            outcome["complete"] = s.complete("acme", v["id"], 1, "w1", 4)
        except BudgetConflict as e:
            outcome["complete_err"] = e.code

    def do_terminate():
        try:
            outcome["terminate"] = s.terminate("acme", v["id"])
        except BudgetConflict as e:
            outcome["terminate_err"] = e.code

    t1 = threading.Thread(target=do_complete)
    t2 = threading.Thread(target=do_terminate)
    t1.start(); t2.start(); t1.join(); t2.join()

    vv = s.get_voucher(v["id"])
    assert vv["state"] in ("completed", "terminated")
    # 恰好一种最终结算发生：未用预留 2 只退一次
    assert vv["refunded_total"] == 2
    assert vv["settled_total"] == 4
    if vv["state"] == "completed":
        assert "complete" in outcome
        # 输掉竞争的 terminate 拿到终态幂等结果，不再退款
        assert outcome["terminate"]["replayed"]
        assert outcome["terminate"]["refunded"] == 0
    else:
        assert "terminate" in outcome
        assert outcome["terminate"]["refunded"] == 2
        assert outcome.get("complete_err") == "terminal"
    assert s.verify_invariants() == []


def test_double_complete_only_once(svc):
    s, _ = svc
    make_customer(s)
    v = s.submit("acme", "dc", 5)
    a = s.acquire("acme", v["id"], "w1")
    s.report_consumed("acme", v["id"], 1, "w1", 5)
    d1 = s.complete("acme", v["id"], 1, "w1", 5)
    assert d1["state"] == "completed"
    with pytest.raises(BudgetConflict) as ei:
        s.complete("acme", v["id"], 1, "w1", 5)
    assert ei.value.code == "terminal"
    assert s.verify_invariants() == []


# ----------------------------------------------------- 策略只影响后续

def test_adjust_policy_does_not_revoke(svc):
    s, clk = svc
    make_customer(s, opening_balance=10, refill_amount=5, refill_interval=60,
                  burst_cap=10)
    v = s.submit("acme", "keep", 7)
    a = s.acquire("acme", v["id"], "w1")
    s.report_consumed("acme", v["id"], 1, "w1", 2)
    # 在线收紧：补充归零、突发上限降到 3（余额 3 被裁剪 0）
    st = s.adjust_policy("acme", refill_amount=0, burst_cap=3)
    assert st["available"] == 3
    # 已发出的凭证与已结算金额原封不动
    vv = s.get_voucher(v["id"])
    assert vv["reserved_total"] == 7 and vv["settled_total"] == 2
    assert vv["state"] == "executing" and vv["claim_epoch"] == 1
    # 仍可继续上报到 7（不需要新预留），再终止退 5 中的 3（受新上限裁剪）
    s.report_consumed("acme", v["id"], 1, "w1", 7)
    r = s.terminate("acme", v["id"])
    # balance 3 + unused 0 空间 -> refund 0, lost 0（未用预留 0）
    assert r["refunded"] == 0
    assert s.verify_invariants() == []


def test_raise_cap_unblocks_deferred(svc):
    s, clk = svc
    make_customer(s, opening_balance=10, burst_cap=10,
                  refill_amount=10, refill_interval=60)
    v = s.submit("acme", "big", 12)
    assert v["blocked_reason"] == "over_burst_cap"
    # 调高上限：12 不再不可行；余额 10 还差 2，等一期补充
    s.adjust_policy("acme", burst_cap=12)
    assert s.get_voucher(v["id"])["state"] == "deferred"
    clk.advance(60)
    s.tick()
    assert s.get_voucher(v["id"])["state"] == "reserved"


# ------------------------------------------------------------ 席位

def test_seats_limit_and_pause_releases(svc):
    s, _ = svc
    make_customer(s, opening_balance=10, max_seats=1)
    v1 = s.submit("acme", "s1", 2)
    v2 = s.submit("acme", "s2", 2)
    a1 = s.acquire("acme", v1["id"], "w1")
    assert a1["state"] == "executing"
    # v2 已 reserved，但席位被 v1 占住
    with pytest.raises(BudgetConflict) as ei:
        s.acquire("acme", v2["id"], "w2")
    assert ei.value.code == "seats_full"
    s.complete("acme", v1["id"], a1["claim_epoch"], "w1", 2)
    a2 = s.acquire("acme", v2["id"], "w2")
    assert a2["state"] == "executing"
    assert s.verify_invariants() == []


# ------------------------------------------------------------ 账本/查询

def test_ledger_replay_and_status(svc):
    s, clk = svc
    make_customer(s, opening_balance=4, refill_amount=6, refill_interval=60)
    v = s.submit("acme", "l", 5)
    assert v["state"] == "deferred"
    clk.advance(60)
    s.tick()
    a = s.acquire("acme", v["id"], "w")
    s.report_consumed("acme", v["id"], a["claim_epoch"], "w", 5)
    s.complete("acme", v["id"], a["claim_epoch"], "w", 5)

    led = s.ledger("acme", limit=1000)
    kinds = [r["kind"] for r in led]
    assert "refill" in kinds and "reserve" in kinds
    assert "settle" in kinds
    # 重放 avail_delta == 当前余额
    total = sum(r["avail_delta"] for r in led)
    assert total == s.get_customer("acme")["available"]
    st = s.status()
    assert st["customers"][0]["next_refill_at"] is not None
    assert st["customers"][0]["settled_total"] == 5
    # 完成后无阻塞工单
    assert st["customers"][0]["blocked"] == []
    assert s.verify_invariants() == []
