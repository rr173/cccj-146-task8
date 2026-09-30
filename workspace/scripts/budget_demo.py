#!/usr/bin/env python3
"""后台计算「容量凭证与预算结算」7 个可复现场景（零第三方依赖，SQLite）。

运行：python scripts/budget_demo.py

覆盖：
  1. 正常预留          提交即原子预留、单调进度结算、低于预估部分完成时退款
  2. 余额不足后自动唤醒 余额不足进延后区并标最早可执行时间，补充到账自动取得资格
  3. 并发抢额防透支    多线程抢最后一份额度：恰好一个赢、available 永不为负
  4. 实际消耗超额暂停  超额追加预留失败即停在已确认位置、释放席位，补充后续跑
  5. 失联接替不重复计费 心跳超时被接管，旧持有者迟到上报 409，已结算金额不重复扣
  6. 终止退款幂等      终止只退未用部分；重复终止不二次退款；waiting 终止零退款
  7. 结算与终止竞争    complete 与 terminate 谁先落事务谁定义终局，全局只结一次
"""
from __future__ import annotations

import os
import sys
import tempfile
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from featforge.budget import Budget, BudgetConflict, BudgetError  # noqa: E402
from featforge.budget_worker import (  # noqa: E402
    BudgetWorker, PausedForBudget, SimulatedProcessExit,
)
from featforge.db import Database  # noqa: E402

PASS, FAIL = "\033[92mPASS\033[0m", "\033[91mFAIL\033[0m"


class Clock:
    """手工推进的时钟；advance() 触发补充/心跳超时。"""

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


def make_budget(clock: Clock, seats: int = 4, ttl: int = 1000,
                path: str | None = None) -> tuple[Database, Budget]:
    if path is None:
        path = os.path.join(tempfile.mkdtemp(prefix="ff-bg-"), "bg.db")
    db = Database(path)
    return db, Budget(db, clock=clock, lease_ttl=ttl, total_seats=seats)


def check(label: str, ok: bool, detail: str = "") -> bool:
    print(f"  [{PASS if ok else FAIL}] {label}" + (f" — {detail}" if detail else ""))
    return ok


# ============================================================ 场景 1

def scenario_reserve_and_settle() -> bool:
    banner("场景 1：正常预留（原子预留 + 单调结算 + 完成退未用）")
    clk = Clock()
    db, b = make_budget(clk)
    b.create_customer("acme", refill_amount=10, period_seconds=10,
                      burst_cap=30, initial=30)
    r = b.submit_job("acme", 10, "normal-job")
    ok = check("提交即预留成功，状态 ready",
               r["outcome"] == "reserved" and r["job"]["state"] in
               ("ready", "running"))
    job = b.claim_next("worker-1")
    ok &= check("执行器领取到席位与凭证纪元 epoch=1",
                job is not None and job["hold_epoch"] == 1
                and job["holder"] == "worker-1")
    b.report(job["id"], 1, "worker-1", 4)
    mid = b.get_job(job["id"])
    st = b.get_customer("acme")
    ok &= check("上报累计消耗 4：reserved 10 -> 6，已结算 4",
                mid["used_total"] == 4 and st["reserved"] == 6
                and st["settled"] == 4 and st["available"] == 20,
                f"avail={st['available']} reserved={st['reserved']} "
                f"settled={st['settled']}")
    # 相等重放幂等，不重复扣费
    b.report(job["id"], 1, "worker-1", 4)
    st2 = b.get_customer("acme")
    ok &= check("相同进度重放幂等，不重复扣费", st2["settled"] == 4)
    done = b.complete(job["id"], 1, "worker-1", final_total=7)
    st3 = b.get_customer("acme")
    ok &= check("完成最终结算 7，退还未用 10-7=3",
                done["state"] == "succeeded" and done["settled_total"] == 7
                and done["refunded_total"] == 3
                and st3["reserved"] == 0 and st3["available"] == 23,
                f"avail={st3['available']} settled={st3['settled']}")
    vouchers = b.list_vouchers(job["id"])
    ok &= check("凭证不可变：仅一张 RESERVE-10",
                [(v["kind"], v["amount"]) for v in vouchers]
                == [("reserve", 10)])
    ok &= check("复式台账平衡、无透支", b.verify_invariants()["ok"])
    db.close()
    return ok


# ============================================================ 场景 2

def scenario_deferred_wakeup() -> bool:
    banner("场景 2：余额不足后自动唤醒（延后区 + 最早可执行时间）")
    clk = Clock()
    db, b = make_budget(clk)
    b.create_customer("acme", refill_amount=10, period_seconds=10,
                      burst_cap=30, initial=12)
    r1 = b.submit_job("acme", 10, "first")
    r2 = b.submit_job("acme", 10, "second")
    ok = check("第一个预留成功，第二个余额不足进延后区",
               r1["outcome"] == "reserved" and r2["outcome"] == "deferred"
               and r2["job"]["state"] == "waiting")
    ok &= check("延后工单标出最早可执行时间 = 下个补充点",
                r2["job"]["earliest_ready_at"] == 1_000_010,
                str(r2["job"]["earliest_ready_at"]))
    blocked = b.blocked_jobs()
    ok &= check("状态接口列出被预算阻塞工单",
                len(blocked) == 1 and blocked[0]["business_key"] == "second"
                and blocked[0]["blocked_reason"] == "insufficient_funds")
    # 第一个工单全额花掉，无退款
    j1 = b.claim_next("w1")
    b.report(j1["id"], 1, "w1", 10)
    b.complete(j1["id"], 1, "w1", final_total=10)
    ok &= check("补充未到账前仍在等待", b.get_job(r2["job"]["id"])["state"]
                == "waiting")
    clk.advance(10)
    b.tick()  # 后台时钟推进：补充到账 -> 泵送 -> 派发
    j2 = b.get_job(r2["job"]["id"])
    ok &= check("补充到账后自动取得资格并被派发（running）",
                j2["state"] == "running" and j2["held_total"] == 10
                and j2["used_total"] == 0,
                j2["state"])
    j2c = b.claim_next("w2")
    ok &= check("自动派发的工单可直接被执行器绑定（epoch 已发放）",
                j2c is not None and j2c["id"] == r2["job"]["id"])
    ok &= check("阻塞列表已清空", b.blocked_jobs() == [])
    ok &= check("复式台账平衡、无透支", b.verify_invariants()["ok"])
    db.close()
    return ok


# ============================================================ 场景 3

def scenario_concurrent_grab() -> bool:
    banner("场景 3：并发抢额防透支（多提交者抢最后一份额度）")
    clk = Clock()
    # 独立 db 文件 + 多连接：Budget 内部每次 conn() 是线程局部连接
    path = os.path.join(tempfile.mkdtemp(prefix="ff-bg-conc-"), "bg.db")
    db, b = make_budget(clk, seats=100, path=path)
    b.create_customer("acme", refill_amount=10, period_seconds=10,
                      burst_cap=100, initial=50)
    N = 20
    results: list[dict] = []
    errors: list[Exception] = []
    barrier = threading.Barrier(N)

    def submit(i: int) -> None:
        try:
            # 每个线程一个 Budget 实例（各自线程局部连接，同一 db 文件）
            bb = Budget(Database(path, ), clock=clk, total_seats=100)
            barrier.wait()
            results.append(bb.submit_job("acme", 10, f"grab-{i:02d}"))
        except Exception as e:  # 并发错误必须显性暴露
            errors.append(e)

    threads = [threading.Thread(target=submit, args=(i,)) for i in range(N)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    ok = check("20 个并发提交无数据库错误", not errors,
               "; ".join(map(str, errors[:3])))
    reserved = [r for r in results if r["outcome"] == "reserved"]
    deferred = [r for r in results if r["outcome"] == "deferred"]
    ok &= check("50 额度 / 每张 10：恰好 5 个预留成功、15 个延后",
                len(reserved) == 5 and len(deferred) == 15,
                f"reserved={len(reserved)} deferred={len(deferred)}")
    st = b.get_customer("acme")
    ok &= check("可用额精确为 0（不透支、不多扣）", st["available"] == 0
                and st["reserved"] == 50,
                f"avail={st['available']} reserved={st['reserved']}")
    soon = [r for r in deferred if r["job"]["earliest_ready_at"]]
    never = [r for r in deferred if not r["job"]["earliest_ready_at"]]
    ok &= check("队头延后工单标出补充时间，队尾超出突发上限的标 NULL（永等不到）",
                len(soon) == 5 and len(never) == 10,
                f"有时间={len(soon)} 永等不到={len(never)}")
    ok &= check("最早可执行时间严格 FIFO 单调（含队头阻塞累积）",
                [r["job"]["earliest_ready_at"] for r in soon]
                == sorted(r["job"]["earliest_ready_at"] for r in soon))
    ok &= check("复式台账平衡、无透支", b.verify_invariants()["ok"])
    db.close()
    return ok


# ============================================================ 场景 4

def scenario_overrun_pause() -> bool:
    banner("场景 4：实际消耗超额暂停（停在已确认位置，不能继续占席位）")
    clk = Clock()
    db, b = make_budget(clk, seats=1)
    b.create_customer("acme", refill_amount=10, period_seconds=10,
                      burst_cap=40, initial=12)
    b.submit_job("acme", 10, "over")
    b.submit_job("acme", 10, "behind")   # 排在后面的延后工单
    j = b.claim_next("wA")
    r12 = b.report(j["id"], 1, "wA", 12)  # 超出预估 2，available=2 刚好追加
    ok = check("超预估且额度够：同事务原子追加预留+结算，继续运行",
               not r12.get("paused_for_budget") and r12["state"] == "running"
               and r12["held_total"] == 12 and r12["used_total"] == 12)
    r13 = b.report(j["id"], 1, "wA", 13)  # 再超 1，available=0
    ok &= check("再超且没钱：paused、停在已确认位置 12、reason=need_extend",
                r13.get("paused_for_budget") and r13["state"] == "paused"
                and r13["confirmed_position"] == 12
                and r13["blocked_reason"] == "need_extend")
    ok &= check("已让出执行席位（其他工单不会被它挡住 claim）",
                b.claim_next("wB") is None)  # behind 也还没钱
    try:
        b.report(j["id"], 1, "wA", 13)
        ok &= check("旧持有者继续上报被拒绝", False)
    except BudgetConflict as e:
        ok &= check("旧持有者继续上报被拒绝（not_running，不扣费）",
                    e.code == "not_running")
    clk.advance(10)
    b.tick()  # +10 补充：队头 over 需 1，先满足它；behind 仍等待
    j2 = b.claim_next("wB")
    ok &= check("补充后队头先恢复（FIFO 不插队）：wB 从位置 12 续跑",
                j2 is not None and j2["id"] == j["id"]
                and j2["hold_epoch"] == 2 and j2["used_total"] == 12
                and j2["holder"] == "wB",
                f"id={j2 and j2['id']} epoch={j2 and j2['hold_epoch']}")
    r = b.report(j["id"], 2, "wB", 13)
    ok &= check("接替者在追加凭证上结算到 13，只扣了差额 1",
                r["state"] == "running" and r["used_total"] == 13
                and r["held_total"] == 13)
    done = b.complete(j["id"], 2, "wB", final_total=13)
    ok &= check("完成，无退款（凭证刚好用完）",
                done["state"] == "succeeded" and done["refunded_total"] == 0)
    ok &= check("复式台账平衡、无透支", b.verify_invariants()["ok"])
    db.close()
    return ok


# ============================================================ 场景 5

def scenario_takeover() -> bool:
    banner("场景 5：失联接替不重复计费（fence 旧持有者）")
    clk = Clock()
    db, b = make_budget(clk, seats=2, ttl=100)
    b.create_customer("acme", refill_amount=10, period_seconds=10,
                      burst_cap=40, initial=30)
    r = b.submit_job("acme", 10, "unreliable")
    # worker-A 跑到 4 后“进程被杀”（worker 故障注入）
    wa = BudgetWorker(b, "worker-A")
    job = b.claim_next("worker-A")
    try:
        # 手工模拟：上报 4 后失联
        b.report(job["id"], job["hold_epoch"], "worker-A", 4)
        raise SimulatedProcessExit("worker-A SIGKILL")
    except SimulatedProcessExit:
        pass
    mid = b.get_job(job["id"])
    ok = check("失联前已结算位置 4/10，工单仍 running、席位被占",
                mid["used_total"] == 4 and mid["state"] == "running"
                and mid["hold_epoch"] == 1)
    clk.advance(101)
    lost = b.reap_lost()
    ok &= check("心跳超时：标 lost、释放席位，已结算 4 保留",
                len(lost) == 1 and lost[0]["state"] == "lost"
                and lost[0]["used_total"] == 4)
    j2 = b.takeover(job["id"], "worker-B")
    ok &= check("接替者沿用已结算金额，epoch 1 -> 2，持有者更新",
                j2["used_total"] == 4 and j2["hold_epoch"] == 2
                and j2["holder"] == "worker-B")
    # 旧持有者复活，用旧 epoch 迟到上报：必须 409 且不扣费
    before = b.get_job(job["id"])["used_total"]
    try:
        b.report(job["id"], 1, "worker-A", 5)
        ok &= check("旧持有者迟到上报被拒绝", False)
    except BudgetConflict as e:
        ok &= check("旧持有者迟到上报 409 stale_epoch，不再次扣费",
                    e.code == "stale_epoch")
    after = b.get_job(job["id"])["used_total"]
    ok &= check("迟到上报前后已结算金额不变（4）", before == after == 4)
    # 旧持有者的迟到完成/心跳同样拒绝
    for fn, label in (
        (lambda: b.heartbeat(job["id"], 1, "worker-A"), "心跳"),
        (lambda: b.complete(job["id"], 1, "worker-A"), "完成"),
    ):
        try:
            fn(); ok &= check(f"旧持有者迟到{label}被拒绝", False)
        except BudgetConflict as e:
            ok &= check(f"旧持有者迟到{label} 409 stale_epoch",
                        e.code == "stale_epoch")
    # 新持有者从 4 继续到 10 完成
    b.report(job["id"], 2, "worker-B", 10)
    done = b.complete(job["id"], 2, "worker-B", final_total=10)
    settled_rows = db.conn().execute(
        "SELECT COALESCE(SUM(d_settled),0) s FROM bg_ledger WHERE job_id=? "
        "AND op='settle'", (job["id"],)).fetchone()["s"]
    ok &= check("接替完成：全程只结算 10（没有把 1..4 再扣一遍）",
                done["state"] == "succeeded" and settled_rows == 10,
                f"settle 合计={settled_rows}")
    ok &= check("复式台账平衡、无透支", b.verify_invariants()["ok"])
    db.close()
    return ok


# ============================================================ 场景 6

def scenario_terminate_idempotent() -> bool:
    banner("场景 6：终止退款幂等（只退未用部分，重复终止不重复退款）")
    clk = Clock()
    db, b = make_budget(clk)
    b.create_customer("acme", refill_amount=10, period_seconds=10,
                      burst_cap=40, initial=40)
    # running 工单终止：已用 3，held 8 -> 退 5
    r = b.submit_job("acme", 8, "kill-running")
    jid = r["job"]["id"]
    j = b.claim_next("wx")
    b.report(jid, j["hold_epoch"], "wx", 3)
    t1 = b.terminate(jid, reason="用户取消")
    t2 = b.terminate(jid, reason="重复取消")
    t3 = b.terminate(jid)
    ok = check("终止成功：state=terminated，只退 held-used=5",
                t1["state"] == "terminated" and t1["refunded_total"] == 5
                and t1["used_total"] == 3)
    ok &= check("重复终止返回原结果（replayed/already_terminal），不二次退款",
                t2.get("replayed") and t3.get("replayed")
                and t2["refunded_total"] == 5 and t3["refunded_total"] == 5)
    nref = db.conn().execute(
        "SELECT COUNT(*) n FROM bg_ledger WHERE job_id=? AND op='refund'",
        (jid,)).fetchone()["n"]
    ok &= check("台账中只有一行 refund", nref == 1, f"{nref} 行")
    # 终止后持有者失格：任何迟到上报 409
    try:
        b.report(jid, j["hold_epoch"], "wx", 4)
        ok &= check("终止后迟到上报拒绝", False)
    except BudgetConflict as e:
        ok &= check("终止后旧持有者迟到上报 409 not_running",
                    e.code == "not_running")
    # ready 工单（未领取）终止：退全部凭证
    r = b.submit_job("acme", 6, "kill-ready")
    t = b.terminate(r["job"]["id"])
    ok &= check("未开工 ready 工单终止：退全额 6",
                t["refunded_total"] == 6 and t["used_total"] == 0)
    # waiting 工单终止：从未发出凭证，零退款、零凭证行
    # 先把钱花光：initial 40 -8(running 预留，3 已结算 5 已退) ... 直接造 waiting
    for i in range(8):
        b.submit_job("acme", 5, f"fill-{i}")
    rw = b.submit_job("acme", 9, "kill-waiting")
    # 排在队列尾部大概率 waiting；若拿到资格则换个更小余额场景
    if rw["job"]["state"] != "waiting":
        b.terminate(rw["job"]["id"])
        rw = b.submit_job("acme", 9, "kill-waiting-2")
    ok &= check("构造出一个 waiting 工单", rw["job"]["state"] == "waiting")
    tw = b.terminate(rw["job"]["id"])
    nv = db.conn().execute(
        "SELECT COUNT(*) n FROM bg_vouchers WHERE job_id=?",
        (rw["job"]["id"],)).fetchone()["n"]
    ok &= check("waiting 终止：零退款、零凭证、直接终局",
                tw["state"] == "terminated" and tw["refunded_total"] == 0
                and nv == 0)
    ok &= check("复式台账平衡、无透支", b.verify_invariants()["ok"])
    db.close()
    return ok


# ============================================================ 场景 7

def scenario_complete_terminate_race() -> bool:
    banner("场景 7：结算与终止竞争（终局唯一，只退一次 / 只最终结算一次）")
    clk = Clock()
    db, b = make_budget(clk)
    b.create_customer("acme", refill_amount=10, period_seconds=10,
                      burst_cap=40, initial=40)

    def refund_rows(job_id: int) -> int:
        return db.conn().execute(
            "SELECT COUNT(*) n FROM bg_ledger WHERE job_id=? AND op='refund'",
            (job_id,)).fetchone()["n"]

    # 7a：complete 先到，terminate 后到 -> succeeded 保持，终止不二次退款
    r = b.submit_job("acme", 10, "race-a")
    jid = r["job"]["id"]
    j = b.claim_next("w1")
    b.report(jid, j["hold_epoch"], "w1", 8)
    done = b.complete(jid, j["hold_epoch"], "w1", final_total=8)
    later_t = b.terminate(jid)
    later_c = b.complete(jid, j["hold_epoch"], "w1", final_total=8)
    ok = check("complete 先赢：succeeded，结算 8 退 2",
                done["state"] == "succeeded" and done["refunded_total"] == 2)
    ok &= check("后到的 terminate 拿到 already_terminal，不改变终局、不退款",
                later_t["state"] == "succeeded"
                and later_t.get("already_terminal")
                and later_c.get("already_terminal"))
    ok &= check("只有一行 refund", refund_rows(jid) == 1)

    # 7b：terminate 先到，complete 后到 -> terminated 保持
    r = b.submit_job("acme", 10, "race-b")
    jid = r["job"]["id"]
    j = b.claim_next("w2")
    b.report(jid, j["hold_epoch"], "w2", 6)
    term = b.terminate(jid, reason="抢在完成前")
    later = b.complete(jid, j["hold_epoch"], "w2", final_total=10)
    ok &= check("terminate 先赢：terminated，结算停在 6，退 4",
                term["state"] == "terminated" and term["used_total"] == 6
                and term["refunded_total"] == 4)
    ok &= check("后到的 complete 拿到 already_terminal，不补结算、不退款",
                later["state"] == "terminated"
                and later.get("already_terminal")
                and later["used_total"] == 6
                and later["refunded_total"] == 4)
    ok &= check("只有一行 refund；旧持有者上报被 fence",
                refund_rows(jid) == 1)
    try:
        b.report(jid, j["hold_epoch"], "w2", 7)
        ok &= check("终局后上报拒绝", False)
    except BudgetConflict as e:
        ok &= check("终局后任何上报 409 not_running", e.code == "not_running")

    # 7c：真并发——同一工单上 complete 与 terminate 两个线程对撞
    r = b.submit_job("acme", 10, "race-c")
    jid = r["job"]["id"]
    j = b.claim_next("w3")
    b.report(jid, j["hold_epoch"], "w3", 5)
    path = db.path
    outcomes: list[tuple[str, dict]] = []
    barrier = threading.Barrier(2)

    def do_complete() -> None:
        bb = Budget(Database(path), clock=clk, total_seats=4)
        barrier.wait()
        try:
            outcomes.append(("complete",
                             bb.complete(jid, j["hold_epoch"], "w3",
                                         final_total=5)))
        except Exception as e:
            outcomes.append(("complete-err", {"e": e}))

    def do_terminate() -> None:
        bb = Budget(Database(path), clock=clk, total_seats=4)
        barrier.wait()
        outcomes.append(("terminate", bb.terminate(jid, reason="并发取消")))

    t1 = threading.Thread(target=do_complete)
    t2 = threading.Thread(target=do_terminate)
    t1.start(); t2.start(); t1.join(); t2.join()
    final = b.get_job(jid)
    kinds = {k for k, _ in outcomes}
    ok &= check("两个调用都正常返回（无数据库错误）",
                kinds == {"complete", "terminate"}, str(kinds))
    ok &= check("终局唯一：state 为 succeeded 或 terminated 之一",
                final["state"] in ("succeeded", "terminated"), final["state"])
    # 无论谁赢：used 只可能是 5，退款只可能是 5（held10-used5），且只退一次
    ok &= check("已结算金额唯一（5），退款金额唯一（5），台账只有一行 refund",
                final["used_total"] == 5 and final["refunded_total"] == 5
                and refund_rows(jid) == 1,
                f"state={final['state']} refund_rows={refund_rows(jid)}")
    ok &= check("复式台账平衡、无透支", b.verify_invariants()["ok"])
    db.close()
    return ok


def main() -> int:
    scenarios = [
        scenario_reserve_and_settle,
        scenario_deferred_wakeup,
        scenario_concurrent_grab,
        scenario_overrun_pause,
        scenario_takeover,
        scenario_terminate_idempotent,
        scenario_complete_terminate_race,
    ]
    results = []
    for fn in scenarios:
        try:
            results.append((fn.__name__, fn()))
        except Exception:
            import traceback
            traceback.print_exc()
            results.append((fn.__name__, False))
    banner("汇总")
    for name, ok in results:
        print(f"  [{PASS if ok else FAIL}] {name}")
    print()
    return 0 if all(ok for _, ok in results) else 1


if __name__ == "__main__":
    raise SystemExit(main())
