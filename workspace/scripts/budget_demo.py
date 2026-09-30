#!/usr/bin/env python3
"""容量凭证与预算结算：7 个可复现场景（仅 SQLite + 标准库）。

运行：python3 scripts/budget_demo.py

覆盖：
  1. 正常预留与结算      提交即原子预留，按单调进度结算，完成退未用预留
  2. 余额不足自动唤醒    延后区标最早可执行时刻，补充到账自动取得资格
  3. 并发抢额防透支      多提交者抢最后一份额度，恰好一份成功，余额永不为负
  4. 实际消耗超额暂停    超额先追加预留，追加不上停在已确认位置并释放席位
  5. 失联接替不重复计费  租约超时后接替，旧 epoch 迟到上报 fence 拒绝
  6. 终止退款幂等        只退未用预留；重复终止、完成竞争都只结算一次
  7. 结算与终止竞争      两个线程同时 complete/terminate，终态唯一

每个场景独立临时库与手工时钟，结束时打印账本重放不变量核对结果。
"""
from __future__ import annotations

import os
import sys
import tempfile
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from featforge.budget import BudgetConflict, BudgetService  # noqa: E402
from featforge.db import Database  # noqa: E402

PASS, FAIL = "\033[92mPASS\033[0m", "\033[91mFAIL\033[0m"


class Clock:
    def __init__(self, start: int = 1_000_000):
        self.t = start

    def __call__(self) -> int:
        return self.t

    def advance(self, seconds: int) -> None:
        self.t += seconds


def banner(title: str) -> None:
    print("\n" + "=" * 72)
    print(f"  {title}")
    print("=" * 72)


def check(label: str, ok: bool, detail: str = "") -> bool:
    print(f"  [{PASS if ok else FAIL}] {label}" + (f" — {detail}" if detail else ""))
    return ok


def make_service(clk: Clock, ttl: int = 100, **policy) -> tuple:
    path = os.path.join(tempfile.mkdtemp(prefix="ff-budget-"), "budget.db")
    db = Database(path)
    svc = BudgetService(db, clock=clk, lease_ttl=ttl, auto_scheduler=False)
    args = dict(refill_amount=5, refill_interval=60, burst_cap=10,
                max_seats=4, opening_balance=10)
    args.update(policy)
    svc.configure_customer("acme", **args)
    return db, svc


def expect_conflict(fn, code: str) -> bool:
    try:
        fn()
    except BudgetConflict as e:
        return e.code == code
    return False


def scenario_normal() -> bool:
    banner("场景 1：正常预留、单调结算、完成退未用预留")
    clk = Clock()
    db, svc = make_service(clk)
    ok = True
    v = svc.submit("acme", "train-1", estimate=8, submitter="alice")
    ok &= check("提交 8：余额 10 -> 2，凭证 reserved",
                v["state"] == "reserved"
                and svc.get_customer("acme")["available"] == 2,
                f"state={v['state']}")
    a = svc.acquire("acme", v["id"], "worker-A")
    ok &= check("占用成功 epoch=1 并占 1 席",
                a["claim_epoch"] == 1
                and svc.get_customer("acme")["seats_used"] == 1)
    r = svc.report_consumed("acme", v["id"], 1, "worker-A", 5)
    ok &= check("上报累计消耗 5：已结算 5，已预留未结算 3",
                r["confirmed_total"] == 5
                and svc.get_customer("acme")["settled_total"] == 5
                and svc.get_customer("acme")["reserved_open"] == 3)
    r = svc.report_consumed("acme", v["id"], 1, "worker-A", 5)
    ok &= check("重复上报 5：幂等，不重复扣费",
                r["action"] == "idempotent"
                and svc.get_voucher(v["id"])["settled_total"] == 5)
    d = svc.complete("acme", v["id"], 1, "worker-A", consumed_total=7)
    ok &= check("实际 7 < 预估 8：完成结算 7，退还未用预留 1",
                d["state"] == "completed" and d["refunded"] == 1
                and svc.get_customer("acme")["available"] == 3,
                f"refund={d['refunded']}")
    ok &= check("账本重放不变量", not svc.verify_invariants(),
                str(svc.verify_invariants()))
    db.close()
    return ok


def scenario_deferred_wake() -> bool:
    banner("场景 2：余额不足进延后区，补充到账自动唤醒")
    clk = Clock()
    db, svc = make_service(clk, opening_balance=3, refill_amount=4,
                           refill_interval=60)
    ok = True
    v = svc.submit("acme", "nightly-backfill", estimate=6)
    st = svc.get_customer("acme")
    ok &= check("余额 3 不足 6：deferred，标最早可执行时刻 t+60",
                v["state"] == "deferred"
                and v["earliest_ready_at"] == clk() + 60
                and len(st["blocked"]) == 1,
                f"earliest={v['earliest_ready_at']}")
    clk.advance(59)
    svc.tick()
    ok &= check("t+59 补充未到账，仍延后",
                svc.get_voucher(v["id"])["state"] == "deferred")
    clk.advance(1)
    svc.tick()
    v = svc.get_voucher(v["id"])
    ok &= check("t+60 补充 +4 到账（余额 7），自动取得执行资格",
                v["state"] == "reserved" and v["reserved_total"] == 6
                and svc.get_customer("acme")["available"] == 1)
    a = svc.acquire("acme", v["id"], "worker-B")
    d = svc.complete("acme", v["id"], a["claim_epoch"], "worker-B", 6)
    ok &= check("完成，无未用预留可退",
                d["state"] == "completed" and d["refunded"] == 0)
    ok &= check("账本重放不变量", not svc.verify_invariants(),
                str(svc.verify_invariants()))
    db.close()
    return ok


def scenario_concurrent_grab() -> bool:
    banner("场景 3：多提交者并发抢最后一份额度，绝不透支")
    clk = Clock()
    db, svc = make_service(clk, opening_balance=10, max_seats=20)
    results, errors = [], []

    def grab(i: int) -> None:
        try:
            results.append(
                svc.submit("acme", f"job-{i}", estimate=6)["state"])
        except Exception as e:  # noqa: BLE001
            errors.append(e)

    threads = [threading.Thread(target=grab, args=(i,)) for i in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    ok = True
    ok &= check("12 份 6 额度的工单抢余额 10：恰好 1 份预留成功",
                not errors and results.count("reserved") == 1
                and results.count("deferred") == 11,
                f"reserved={results.count('reserved')} "
                f"deferred={results.count('deferred')} errors={len(errors)}")
    bal = svc.get_customer("acme")["available"]
    min_after = min(r["balance_after"]
                    for r in svc.ledger("acme", limit=1000))
    ok &= check(f"余额恰好 4，账本任何一行都未出现负值（最小 {min_after}）",
                bal == 4 and min_after >= 0, f"balance={bal}")
    ok &= check("账本重放不变量", not svc.verify_invariants(),
                str(svc.verify_invariants()))
    db.close()
    return ok


def scenario_overrun_pause() -> bool:
    banner("场景 4：实际消耗超预估，追加不上预留则暂停并释放席位")
    clk = Clock()
    db, svc = make_service(clk, opening_balance=6, refill_amount=10,
                           refill_interval=60)
    ok = True
    v = svc.submit("acme", "shaky-job", estimate=4)
    a = svc.acquire("acme", v["id"], "worker-C")
    r = svc.report_consumed("acme", v["id"], 1, "worker-C", 6)
    ok &= check("上报 6（超预估 2）：追加预留成功，已预留=6 已结算=6",
                r["action"] == "settled"
                and svc.get_voucher(v["id"])["reserved_total"] == 6
                and svc.get_customer("acme")["available"] == 0)
    r = svc.report_consumed("acme", v["id"], 1, "worker-C", 8)
    vv = svc.get_voucher(v["id"])
    ok &= check("再上报 8 需 +2 但余额 0：paused，确认位置停在 6，释放席位",
                r["action"] == "paused" and r["confirmed_total"] == 6
                and vv["state"] == "paused" and vv["pending_total"] == 8
                and svc.get_customer("acme")["seats_used"] == 0,
                f"confirmed={r['confirmed_total']}")
    ok &= check("旧持有者再上报：凭证非 executing，409 拒绝",
                expect_conflict(
                    lambda: svc.report_consumed(
                        "acme", v["id"], 1, "worker-C", 7),
                    "not_executing"))
    clk.advance(60)
    svc.tick()
    vv = svc.get_voucher(v["id"])
    ok &= check("补充到账：自动恢复 reserved，已预留补到 8",
                vv["state"] == "reserved" and vv["reserved_total"] == 8)
    a2 = svc.acquire("acme", v["id"], "worker-D")
    ok &= check("重新占用 epoch=2", a2["claim_epoch"] == 2)
    d = svc.complete("acme", v["id"], 2, "worker-D", 8)
    ok &= check("接替者完成结算 8", d["state"] == "completed"
                and svc.get_voucher(v["id"])["settled_total"] == 8)
    ok &= check("账本重放不变量", not svc.verify_invariants(),
                str(svc.verify_invariants()))
    db.close()
    return ok


def scenario_failover() -> bool:
    banner("场景 5：进程失联接替，旧持有者迟到上报不重复扣费")
    clk = Clock()
    db, svc = make_service(clk, opening_balance=10, max_seats=1)
    ok = True
    v = svc.submit("acme", "orphan-job", estimate=8)
    svc.acquire("acme", v["id"], "worker-OLD")
    svc.report_consumed("acme", v["id"], 1, "worker-OLD", 5)
    clk.advance(101)
    reaped = svc.reap_expired()
    ok &= check("租约超时回收：1 个失联凭证，已结算 5 沿用，席位释放",
                len(reaped) == 1 and reaped[0]["settled_total"] == 5
                and reaped[0]["old_epoch"] == 1
                and svc.get_customer("acme")["seats_used"] == 0)
    ok &= check("旧持有者迟到上报（epoch 1）：409 lease_expired",
                expect_conflict(
                    lambda: svc.report_consumed(
                        "acme", v["id"], 1, "worker-OLD", 6),
                    "lease_expired"))
    a2 = svc.acquire("acme", v["id"], "worker-NEW")
    ok &= check("接替者占用：epoch 2，已结算仍为 5",
                a2["claim_epoch"] == 2
                and svc.get_voucher(v["id"])["settled_total"] == 5)
    svc.report_consumed("acme", v["id"], 2, "worker-NEW", 7)
    stale_old = expect_conflict(
        lambda: svc.report_consumed(
            "acme", v["id"], 1, "worker-OLD", 7), "stale_epoch")
    ok &= check("旧持有者复活再报 7：409 stale_epoch，金额不二次累加",
                stale_old
                and svc.get_voucher(v["id"])["settled_total"] == 7)
    # 租约未超时的情况下，旧 epoch 上报命中 lease/stale 前先被 epoch fence
    stale_old2 = expect_conflict(
        lambda: svc.heartbeat("acme", v["id"], 1, "worker-OLD"),
        "stale_epoch")
    ok &= check("旧持有者心跳续期同样 409 stale_epoch", stale_old2)
    d = svc.complete("acme", v["id"], 2, "worker-NEW", 7)
    ok &= check("接替者完成：结算 7，退未用预留 1（8-7）",
                d["state"] == "completed" and d["refunded"] == 1)
    ok &= check("账本重放不变量", not svc.verify_invariants(),
                str(svc.verify_invariants()))
    db.close()
    return ok


def scenario_terminate() -> bool:
    banner("场景 6：终止只退未用预留，重复终止幂等")
    clk = Clock()
    db, svc = make_service(clk, opening_balance=10)
    ok = True
    v = svc.submit("acme", "cancelled", estimate=8)
    a = svc.acquire("acme", v["id"], "worker-E")
    svc.report_consumed("acme", v["id"], a["claim_epoch"], "worker-E", 3)
    r1 = svc.terminate("acme", v["id"], reason="user_cancel")
    ok &= check("已结算 3 时终止：退未用预留 5，余额 2 -> 7",
                r1["refunded"] == 5 and not r1["replayed"]
                and svc.get_customer("acme")["available"] == 7)
    r2 = svc.terminate("acme", v["id"], reason="again")
    r3 = svc.terminate("acme", v["id"], reason="again")
    ok &= check("重复终止两次：幂等返回，退款均为 0，总额仍是 5",
                r2["replayed"] and r2["refunded"] == 0
                and r3["refunded"] == 0
                and svc.get_voucher(v["id"])["refunded_total"] == 5)
    ok &= check("终止后旧持有者再完成：409 terminal，无第二笔结算",
                expect_conflict(
                    lambda: svc.complete(
                        "acme", v["id"], a["claim_epoch"], "worker-E", 3),
                    "terminal"))
    vv = svc.get_voucher(v["id"])
    ok &= check("终态金额：已结算 3 / 已退 5 / 裁剪 0",
                vv["settled_total"] == 3 and vv["refunded_total"] == 5
                and vv["lost_total"] == 0)
    ok &= check("账本重放不变量", not svc.verify_invariants(),
                str(svc.verify_invariants()))
    db.close()
    return ok


def scenario_complete_terminate_race() -> bool:
    banner("场景 7：完成与终止并发竞争，终态与退款都只发生一次")
    clk = Clock()
    db, svc = make_service(clk, opening_balance=10)
    v = svc.submit("acme", "race-job", estimate=6)
    a = svc.acquire("acme", v["id"], "worker-F")
    svc.report_consumed("acme", v["id"], a["claim_epoch"], "worker-F", 4)
    outcome = {}

    def do_complete() -> None:
        try:
            outcome["complete"] = svc.complete(
                "acme", v["id"], a["claim_epoch"], "worker-F", 4)
        except BudgetConflict as e:
            outcome["complete_err"] = e.code

    def do_terminate() -> None:
        try:
            outcome["terminate"] = svc.terminate("acme", v["id"], "race")
        except BudgetConflict as e:
            outcome["terminate_err"] = e.code

    t1 = threading.Thread(target=do_complete)
    t2 = threading.Thread(target=do_terminate)
    t1.start(); t2.start(); t1.join(); t2.join()
    vv = svc.get_voucher(v["id"])
    ok = True
    winner = vv["state"]
    ok &= check(f"终态唯一：{winner}，已结算 4 / 退款 2 只发生一次",
                winner in ("completed", "terminated")
                and vv["settled_total"] == 4
                and vv["refunded_total"] == 2
                and vv["lost_total"] == 0,
                f"outcome keys={sorted(outcome)}")
    if winner == "completed":
        ok &= check("输家 terminate 拿到终态幂等结果（refund=0）",
                    outcome.get("terminate", {}).get("replayed")
                    and outcome["terminate"]["refunded"] == 0)
    else:
        ok &= check("输家 complete 收到 409 terminal",
                    outcome.get("complete_err") == "terminal"
                    and outcome["terminate"]["refunded"] == 2)
    ok &= check("账本重放不变量", not svc.verify_invariants(),
                str(svc.verify_invariants()))
    db.close()
    return ok


def main() -> int:
    scenarios = [
        scenario_normal,
        scenario_deferred_wake,
        scenario_concurrent_grab,
        scenario_overrun_pause,
        scenario_failover,
        scenario_terminate,
        scenario_complete_terminate_race,
    ]
    results = [fn() for fn in scenarios]
    print("\n" + "=" * 72)
    print(f"  汇总：{sum(results)}/{len(results)} 个场景通过")
    print("=" * 72)
    return 0 if all(results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
