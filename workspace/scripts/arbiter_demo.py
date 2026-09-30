#!/usr/bin/env python3
"""多租户仲裁器 8 个可复现场景（零第三方依赖，SQLite + 标准库）。

运行：python scripts/arbiter_demo.py

覆盖：
  1. 权重分流        高权重租户获得更多 claim（vruntime 推进 1/weight）
  2. 防饥饿          小租户靠老化积累最终获得席位
  3. 并行上限        seats 限制并发 claimed 分片，完成后席位释放
  4. 暂停后恢复      暂停只阻止新 claim，已开工分片可以写完；恢复后继续
  5. 进程退出后接替  worker 退出 → 租约超时 → 新 worker 接管，旧 epoch 被 fence
  6. 旧 epoch 拒绝   旧持有者复活后的心跳/进度/完成收到 stale_epoch 409
  7. 检查点继续      fail 后 retry 从 shard_cursor 继续，游标不能后退
  8. 业务键重复入队  同键同负载重放原作业，同键异参 409
"""
from __future__ import annotations

import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from featforge.arbiter import (  # noqa: E402
    Arbiter, ArbiterConflict, ArbiterError,
)
from featforge.arbiter_worker import (  # noqa: E402
    ArbiterWorker, SimulatedProcessExit,
)
from featforge.db import Database  # noqa: E402


PASS, FAIL = "\033[92mPASS\033[0m", "\033[91mFAIL\033[0m"


class Clock:
    """手工推进的时钟：claim 用 ttl=1000，advance() 让租约超时。"""

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


def make_arbiter(clock: Clock, ttl: int = 1000,
                 aging_rate: float = 1.0) -> tuple[Database, Arbiter]:
    path = os.path.join(tempfile.mkdtemp(prefix="ff-arb-"), "arb.db")
    db = Database(path)
    return db, Arbiter(db, clock=clock, lease_ttl=ttl, aging_rate=aging_rate,
                       history_limit=200)


def check(label: str, ok: bool, detail: str = "") -> bool:
    print(f"  [{PASS if ok else FAIL}] {label}" + (f" — {detail}" if detail else ""))
    return ok


def scenario_weight_split() -> bool:
    banner("场景 1：权重分流（高权重租户获得更多处理量）")
    clk = Clock()
    db, arb = make_arbiter(clk, aging_rate=0.0)  # 关闭老化，只看欠额
    arb.upsert_tenant("heavy", weight=3.0, seats=1)
    arb.upsert_tenant("light", weight=1.0, seats=1)
    # 各入队 30 个单分片作业
    for i in range(30):
        arb.enqueue("heavy", "backfill", f"h-{i}")
        arb.enqueue("light", "recompute", f"l-{i}")
    counts = {"heavy": 0, "light": 0}
    w = ArbiterWorker(arb, "w1")
    for _ in range(40):
        job = arb.claim("w1")
        if job is None:
            break
        counts[job["tenant_name"]] += 1
        arb.complete(job["id"], job["claim_epoch"], "w1")
    ratio = counts["heavy"] / max(1, counts["light"])
    ok = check(f"claim 次数 heavy={counts['heavy']} light={counts['light']}",
               counts["heavy"] > counts["light"],
               f"比例约 {ratio:.2f}（权重比 3:1）")
    st = {t["name"]: t for t in arb.status()["tenants"]}
    ok &= check("两租户 vruntime 收敛接近（欠额公平）",
                abs(st["heavy"]["vruntime"] - st["light"]["vruntime"]) < 0.5,
                f"{st['heavy']['vruntime']:.3f} vs {st['light']['vruntime']:.3f}")
    db.close()
    return ok


def scenario_anti_starvation() -> bool:
    banner("场景 2：防饥饿（大租户作业洪流中，小租户靠老化获得席位）")
    clk = Clock()
    db, arb = make_arbiter(clk, aging_rate=1.0)
    arb.upsert_tenant("heavy", weight=10.0, seats=1)
    arb.upsert_tenant("tiny", weight=1.0, seats=1)
    # 大租户作业持续到达（每轮再注入一个新的），小租户只有 1 个快照作业
    arb.enqueue("heavy", "backfill", "h-0")
    arb.enqueue("tiny", "snapshot", "tiny-only")
    order = []
    got_tiny_at = None
    for n in range(12):
        if n > 0:  # 洪流：每轮补一个全新的大租户作业（age=0）
            arb.enqueue("heavy", "backfill", f"h-{n}")
        job = arb.claim("w1")
        assert job is not None
        order.append(job["tenant_name"])
        arb.complete(job["id"], job["claim_epoch"], "w1")
        if job["tenant_name"] == "tiny" and got_tiny_at is None:
            got_tiny_at = n + 1
        clk.advance(10)  # 小租户的等待老化持续累积
    heavy_after = order[got_tiny_at:] if got_tiny_at else []
    ok = check("大租户持续有新作业，小租户仍在前 3 次 claim 内被选中",
                got_tiny_at is not None and got_tiny_at <= 3,
                f"第 {got_tiny_at} 次 claim 时选中；顺序 {order}")
    ok &= check("选中小租户后大租户继续获得大部分席位（老化不永久反转份额）",
                heavy_after.count("heavy") >= heavy_after.count("tiny") - 1
                and order.count("heavy") > order.count("tiny"),
                f"全程 heavy={order.count('heavy')} tiny={order.count('tiny')}")
    ok &= check("小租户作业已完成，不会永久饥饿",
                arb.list_jobs("tiny")[0]["state"] == "succeeded")
    # 仲裁历史里能看到选中 tiny 时的老化加分
    picked = next(h for h in arb.history(50)
                  if h["decision"] == "selected" and h["tenant_name"] == "tiny")
    ok &= check("仲裁历史记录了选中小租户时的等待时长与老化加分",
                picked["detail"]["age_seconds"] >= 10
                and picked["detail"]["aging_bonus"] >= 10,
                f"age={picked['detail']['age_seconds']}s "
                f"bonus={picked['detail']['aging_bonus']}")
    db.close()
    return ok


def scenario_parallel_cap() -> bool:
    banner("场景 3：并行席位上限（seats 限制并发 claimed 分片）")
    clk = Clock()
    db, arb = make_arbiter(clk)
    arb.upsert_tenant("a", weight=1.0, seats=2)
    arb.upsert_tenant("b", weight=1.0, seats=1)
    for i in range(5):
        arb.enqueue("a", "backfill", f"a-{i}")
        arb.enqueue("b", "recompute", f"b-{i}")

    def claimed(tenant):
        return arb.tenant_status(tenant)["seats_used"]

    j1 = arb.claim("w1")  # a
    j2 = arb.claim("w1")  # b（b 席位满）
    j3 = arb.claim("w1")  # a 的第 2 席
    j4 = arb.claim("w1")  # a 满(2) b 满(1) -> None
    ok = check("a 占 2 席、b 占 1 席后无可领取项",
               {j["tenant_name"] for j in (j1, j2, j3)} == {"a", "b"}
               and j4 is None and claimed("a") == 2 and claimed("b") == 1,
               f"顺序 {[j['tenant_name'] for j in (j1, j2, j3)]}")
    # 完成一个 a 作业，席位立刻释放，a 可再领（b 仍满，不会越界）
    arb.complete(j1["id"], j1["claim_epoch"], "w1")
    j5 = arb.claim("w2")
    ok &= check("席位释放后 a 可再领，b 仍不超过 1 席",
                j5 is not None and j5["tenant_name"] == "a"
                and claimed("b") == 1 and claimed("a") == 2)
    # 在线把 a 的席位调到 4，不影响已领取作业、不重复领取
    arb.update_policy("a", seats=4)
    rows = arb.list_jobs("a", state="claimed")
    ok &= check("调大席位不影响既有 claim（不会二次领取同一作业）",
                len(rows) == 2 and len({r["id"] for r in rows}) == 2)
    db.close()
    return ok


def scenario_pause_resume() -> bool:
    banner("场景 4：暂停后恢复（暂停只阻止新 claim，已开工分片可以写完）")
    clk = Clock()
    db, arb = make_arbiter(clk)
    arb.upsert_tenant("t1", weight=1.0, seats=2)
    arb.upsert_tenant("t2", weight=1.0, seats=2)
    for i in range(3):
        arb.enqueue("t1", "backfill", f"t1-{i}", shards_total=3)
        arb.enqueue("t2", "backfill", f"t2-{i}")
    running = arb.claim("w1")           # t1 的一个三分片作业开工
    other = arb.claim("w1")             # t2 作业
    arb.pause("t1")
    idle = arb.claim("w1")              # t1 排队作业不可再领；t2 可领
    ok = check("暂停后新 claim 不流向 t1（但可流向未暂停租户）",
               idle is not None and idle["tenant_name"] == "t2",
               f"暂停后领取到 {idle and idle['tenant_name']}")
    # 已开工的 t1 分片照常上报进度并写完
    arb.report_progress(running["id"], running["claim_epoch"], "w1", 1)
    arb.heartbeat(running["id"], running["claim_epoch"], "w1", cursor=2)
    done = arb.complete(running["id"], running["claim_epoch"], "w1")
    ok &= check("暂停期间已开工分片可以写完",
                done["state"] == "succeeded" and done["shard_cursor"] == 3)
    # 全部 t2 完成后，t1 仍暂停 → ready set 虽有作业却 claim 不到
    arb.complete(other["id"], other["claim_epoch"], "w1")
    stalls = arb.tenant_status("t1")
    ok &= check("暂停中 t1 排队作业持续等待（queued>0，占用席位 0）",
                stalls["queued"] == 2 and stalls["seats_used"] == 0
                and stalls["paused"] is True)
    arb.resume("t1")
    nxt = arb.claim("w3")
    ok &= check("恢复后立即可以领取", nxt is not None
                and nxt["tenant_name"] == "t1")
    db.close()
    return ok


def scenario_takeover_after_exit() -> bool:
    banner("场景 5：进程退出后的接替（租约超时 → 新 worker 接管，游标保留）")
    clk = Clock()
    db, arb = make_arbiter(clk, ttl=1000, aging_rate=0.0)
    arb.upsert_tenant("t", weight=1.0, seats=2)
    r = arb.enqueue("t", "backfill", "big", shards_total=8,
                    payload={"die_after_shards": 3})
    job_id = r["job"]["id"]
    w1 = ArbiterWorker(arb, "worker-A")
    try:
        w1.tick()
        ok = False
    except SimulatedProcessExit as e:
        ok = check("worker-A 处理 3 个分片后被杀（无 complete/心跳）", True, str(e))
    mid = arb.get_job(job_id)
    ok &= check("作业仍 claimed，游标停在 3/8，租约未到期",
                mid["state"] == "claimed" and mid["shard_cursor"] == 3
                and mid["claim_epoch"] == 1 and mid["claim_worker"] == "worker-A")
    # 租约未超时：新 worker 不能抢（席位被占）
    blocked = arb.claim("worker-B")
    ok &= check("租约内接管者拿不到该租户席位", blocked is None)
    # 时间推进超过 TTL：reap 回收
    clk.advance(1001)
    reaped = arb.reap_expired()
    ok &= check("心跳超时后作业回到 ready set（游标仍是 3/8）",
                len(reaped) == 1 and reaped[0]["id"] == job_id
                and arb.get_job(job_id)["state"] == "queued"
                and arb.get_job(job_id)["shard_cursor"] == 3)
    # 新 worker claim：epoch 单调变 2，从游标 3 继续
    taken = arb.claim("worker-B")
    ok &= check("接管领取：claim_epoch 单调 1 -> 2，持有者换成 worker-B",
                taken["id"] == job_id and taken["claim_epoch"] == 2
                and taken["claim_worker"] == "worker-B"
                and taken["shard_cursor"] == 3)
    w2 = ArbiterWorker(arb, "worker-B")
    # 去掉故障注入后接管跑完：同键同负载不允许改 payload，因此直接用 process
    # 传入 taken 行（payload 里的 die_after 仍在；这里手动完成剩余分片）
    for shard in range(3, 8):
        arb.report_progress(taken["id"], 2, "worker-B", shard + 1,
                            note=f"takeover shard {shard}")
    final = arb.complete(taken["id"], 2, "worker-B")
    ok &= check("接管后写完剩余分片：8/8 succeeded，只跑了 5 个剩余分片",
                final["state"] == "succeeded" and final["shard_cursor"] == 8)
    progress = arb.list_progress(job_id)
    by_worker = {}
    for p in progress:
        by_worker[p["worker"]] = by_worker.get(p["worker"], 0) + 1
    ok &= check("进度台账同时保留两任持有者的提交（A=3，B=5）",
                by_worker == {"worker-A": 3, "worker-B": 5}, str(by_worker))
    db.close()
    return ok


def scenario_stale_epoch_rejected() -> bool:
    banner("场景 6：旧 epoch 上报拒绝（stale_epoch / 游标后退 / 持有者不符）")
    clk = Clock()
    db, arb = make_arbiter(clk, ttl=1000)
    arb.upsert_tenant("t", weight=1.0, seats=2)
    arb.enqueue("t", "backfill", "j", shards_total=4)
    first = arb.claim("worker-A")
    arb.report_progress(first["id"], 1, "worker-A", 2)
    # worker-A 僵死，租约超时被 worker-B 接管
    clk.advance(1001)
    arb.reap_expired()
    second = arb.claim("worker-B")
    ok = check("接管后 epoch=2", second["claim_epoch"] == 2
               and second["shard_cursor"] == 2)

    def expect_conflict(label, fn, code):
        try:
            fn()
        except ArbiterConflict as e:
            return check(f"{label} → 409 {e.code}", e.code == code, str(e)[:80])
        return check(f"{label} 应返回冲突", False)

    ok &= expect_conflict(
        "旧持有者用 epoch=1 发心跳",
        lambda: arb.heartbeat(first["id"], 1, "worker-A"), "stale_epoch")
    ok &= expect_conflict(
        "旧持有者用 epoch=1 上报进度",
        lambda: arb.report_progress(first["id"], 1, "worker-A", 3),
        "stale_epoch")
    ok &= expect_conflict(
        "旧持有者用 epoch=1 尝试完成",
        lambda: arb.complete(first["id"], 1, "worker-A"), "stale_epoch")
    ok &= expect_conflict(
        "新持有者伪造别人的 worker id",
        lambda: arb.heartbeat(first["id"], 2, "worker-A"), "worker_mismatch")
    ok &= expect_conflict(
        "游标后退 2 -> 1",
        lambda: arb.report_progress(first["id"], 2, "worker-B", 1),
        "cursor_regress")
    # 新持有者正常推进并完成
    arb.report_progress(first["id"], 2, "worker-B", 3)
    arb.report_progress(first["id"], 2, "worker-B", 3)  # 相等：幂等
    done = arb.complete(first["id"], 2, "worker-B")
    ok &= check("当前 epoch 正常完成；重复进度提交幂等",
                done["state"] == "succeeded" and done["shard_cursor"] == 4)
    ok &= expect_conflict(
        "完成后旧 epoch 的迟到完成仍然拒绝",
        lambda: arb.complete(first["id"], 1, "worker-A"), "not_claimed")
    db.close()
    return ok


def scenario_checkpoint_continue() -> bool:
    banner("场景 7：检查点继续（fail 后 retry 从游标续跑，进度不丢）")
    clk = Clock()
    db, arb = make_arbiter(clk)
    arb.upsert_tenant("t", weight=1.0, seats=1)
    # fail_after 由 worker 识别：5 个分片后显式失败
    arb.enqueue("t", "correction", "corr-1", shards_total=12,
                payload={"fail_after_shards": 5})
    w = ArbiterWorker(arb, "w1")
    out = w.tick()
    ok = check("注入失败：作业 failed，游标停在 5/12，进度行已落盘",
               out["state"] == "failed" and out["shard_cursor"] == 5
               and len(arb.list_progress(out["id"])) == 5)
    # retry 是同一个作业行（不产生重复作业）；非 failed 状态 retry 应报错
    try:
        arb.retry_failed(out["id"] + 999)
        ok &= check("不存在的作业 retry 报错", False)
    except ArbiterError:
        ok &= check("不存在的作业 retry 报错", True)
    rj = arb.retry_failed(out["id"])
    ok &= check("retry 是同一个作业行（不产生重复作业）",
                rj["id"] == out["id"] and rj["state"] == "queued"
                and rj["shard_cursor"] == 5)
    # 第二次执行：无故障注入，从游标 5 继续
    claim = arb.claim("w2")
    for shard in range(5, 12):
        arb.report_progress(claim["id"], claim["claim_epoch"], "w2", shard + 1)
    final = arb.complete(claim["id"], claim["claim_epoch"], "w2")
    ok &= check("续跑后 12/12 succeeded",
                final["state"] == "succeeded" and final["shard_cursor"] == 12)
    handled = [p["new_cursor"] for p in arb.list_progress(claim["id"])]
    ok &= check("分片只提交一次：游标序列 1..12 无重复无回退",
                handled == list(range(1, 13)), str(handled))
    db.close()
    return ok


def scenario_business_key_dedup() -> bool:
    banner("场景 8：业务键重复入队（同键同负载重放，同键异参 409）")
    clk = Clock()
    db, arb = make_arbiter(clk)
    arb.upsert_tenant("t", weight=1.0, seats=2)
    a1 = arb.enqueue("t", "snapshot", "snap-2026-09-30",
                     payload={"feature": "gmv", "asof": 1000}, shards_total=4)
    a2 = arb.enqueue("t", "snapshot", "snap-2026-09-30",
                     payload={"asof": 1000, "feature": "gmv"}, shards_total=4)
    ok = check("同键同负载（key 顺序无关）重放原作业，不重复入队",
               a2["replayed"] is True and a2["job"]["id"] == a1["job"]["id"])
    # 同键不同分片数 → 409
    try:
        arb.enqueue("t", "snapshot", "snap-2026-09-30",
                    payload={"feature": "gmv", "asof": 1000}, shards_total=8)
        ok &= check("同键不同分片数返回 business_key_conflict", False)
    except ArbiterConflict as e:
        ok &= check("同键不同分片数返回 business_key_conflict",
                    e.code == "business_key_conflict", str(e)[:80])
    # 同键不同参数 → 409
    try:
        arb.enqueue("t", "snapshot", "snap-2026-09-30",
                    payload={"feature": "gmv", "asof": 2000}, shards_total=4)
        ok &= check("同键不同参数返回 business_key_conflict", False)
    except ArbiterConflict as e:
        ok &= check("同键不同参数返回 business_key_conflict",
                    e.code == "business_key_conflict")
    # 完成后再重放仍返回原作业（幂等覆盖全生命周期）
    j = arb.claim("w1")
    arb.complete(j["id"], j["claim_epoch"], "w1")
    a3 = arb.enqueue("t", "snapshot", "snap-2026-09-30",
                     payload={"feature": "gmv", "asof": 1000}, shards_total=4)
    ok &= check("完成后同键重放仍返回同一个 succeeded 作业",
                a3["replayed"] is True and a3["job"]["state"] == "succeeded")
    # 别的租户可以复用相同业务键（键空间按租户隔离）
    arb.upsert_tenant("other", weight=1.0, seats=1)
    o1 = arb.enqueue("other", "snapshot", "snap-2026-09-30")
    ok &= check("业务键按租户隔离：other 可复用同一键",
                o1["replayed"] is False)
    db.close()
    return ok


def main() -> int:
    scenarios = [
        scenario_weight_split,
        scenario_anti_starvation,
        scenario_parallel_cap,
        scenario_pause_resume,
        scenario_takeover_after_exit,
        scenario_stale_epoch_rejected,
        scenario_checkpoint_continue,
        scenario_business_key_dedup,
    ]
    results = []
    for fn in scenarios:
        try:
            results.append((fn.__name__, fn()))
        except Exception as e:  # 场景脚本必须把失败显式打出来
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
