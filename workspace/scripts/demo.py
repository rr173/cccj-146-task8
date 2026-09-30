#!/usr/bin/env python3
"""端到端演示：在临时库上走一遍 FeatForge 的完整能力。

运行：python scripts/demo.py
"""
from __future__ import annotations

import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from featforge.db import Database  # noqa: E402
from featforge.store import Store  # noqa: E402
from featforge.worker import Worker  # noqa: E402


def banner(title: str) -> None:
    print("\n" + "=" * 72)
    print(f"  {title}")
    print("=" * 72)


def main() -> None:
    db_path = os.path.join(tempfile.mkdtemp(prefix="featforge-demo-"), "demo.db")
    store = Store(Database(db_path), clock=lambda: 0)
    worker = Worker(store)

    banner("1. 定义特征：源 purchase，关联键 user_id，1 小时滚动窗口，sum_v1")
    store.create_feature(
        name="gmv_1h", source="purchase", join_key="user_id",
        value_field="amount", window="1h", transform="sum",
        transform_version=1, backfill_start=0,
        partition_seconds=3600, sample_tick_seconds=300,
    )
    print("特征 gmv_1h 已创建，第 1 代回填作业入队（尚未对读者可见）")

    banner("2. 在线写入源事件（在线当前值持续更新，两代并行接收）")
    events = [("u1", 300, 100.0), ("u1", 600, 50.0), ("u1", 3900, 20.0),
              ("u2", 300, 7.0), ("u2", 3900, 8.0)]
    for i, (e, ts, v) in enumerate(events, start=1):
        r = store.ingest_event("purchase", e, ts, raw={"amount": v},
                               dedup_key=f"ord-{i}")
        print(f"  ingest ord-{i} user={e} ts={ts} amount={v} "
              f"-> watermark={r['watermark']}")

    banner("3. 执行器运行回填：按当时水位截断，PIT 重建，完成后自动校验+切换别名")
    out = worker.tick()
    print(f"  回填作业 #{out['job']['id']}: {out['job']['state']}, "
          f"分区 {out['job']['partitions_done']}/{out['job']['partitions_total']}")
    print(f"  自动切换：{out['promotion']['promoted']}")

    banner("4. 在线读取 + 离线 PIT 历史读取")
    print("  online  u1 @now :", store.read_online("gmv_1h", "u1")["value"])
    for ts in (600, 3000, 3900):
        print(f"  offline u1 @{ts:<5}:", store.read_offline("gmv_1h", "u1", ts)["value"])

    banner("5. 定义升级：sum_v2（等价），创建第 2 代，与旧代并行计算")
    g2 = store.create_generation("gmv_1h", "sum", 2)
    print(f"  第 2 代 #{g2['id']} 创建并回填中……")
    out = worker.tick()
    v = out["promotion"]["validation"]
    print(f"  新/旧覆盖率={v['new_vs_old']['coverage']:.2f} "
          f"p95偏差={v['new_vs_old']['bias_p95']} "
          f"在线/离线匹配率={v['online_vs_offline']['match_rate']:.2f}")
    print(f"  门槛达标，读取别名已切换到第 {out['promotion']['serving_gen']} 代")

    banner("6. 高风险升级被门槛拦截：mean_v99（与 sum 偏差巨大）")
    g3 = store.create_generation("gmv_1h", "mean", 99)
    out = worker.tick()
    promo = out["promotion"]
    print(f"  切换结果: {promo['reason']}")
    for r in promo["validation"]["reasons"]:
        print(f"    - {r}")
    print(f"  读者仍看到完整的第 2 代：serving_gen="
          f"{store.read_online('gmv_1h', 'u1')['serving_gen']}")

    banner("7. 强制切换高风险代，随后一键回滚到先前可验证版本")
    store.promote_generation(g3["id"], force=True)
    print("  force 切换后 serving_gen =", store.read_online("gmv_1h", "u1")["serving_gen"])
    rb = store.rollback_feature("gmv_1h", reason="演示回滚")
    print(f"  已回滚：{rb['from_gen']} -> {rb['to_gen']}，"
          f"当前值 = {store.read_online('gmv_1h', 'u1')['value']}")

    banner("8. 回填失败注入 + 从分区检查点续跑")
    store.create_feature(
        name="cnt_1h", source="click", join_key="user_id",
        value_field="n", window="1h", transform="count",
        transform_version=1, backfill_start=0,
        partition_seconds=3600, sample_tick_seconds=300,
        fail_after_partitions=1,
    )
    for ts in (100, 3700, 7500):
        store.ingest_event("click", "u1", ts, raw={"n": 1})
    out = worker.tick()
    print(f"  首次回填：{out['job']['state']}，"
          f"已完成分区 {out['job']['partitions_done']}/{out['job']['partitions_total']}")
    new_job = store.resume_failed_job(out["job"]["id"])
    out2 = worker.tick()
    print(f"  续跑作业 #{new_job['id']}：{out2['job']['state']}，"
          f"分区 {out2['job']['partitions_done']}/{out2['job']['partitions_total']} "
          "（已完成分区由检查点跳过）")

    banner("9. 迟到事件只重算受影响范围")
    # gmv_1h 当前第 2 代 serving；写入一条落在很早的迟到事件
    late = store.ingest_event("purchase", "u1", 500, raw={"amount": 7.0})
    print(f"  迟到事件 watermark={late['watermark']}，重算作业={late['recompute_jobs']}"
          "（serving + 两个 shadow 影子代，各一个，绝不重复任务）")
    while worker.tick():
        pass  # 跑完全部受影响范围重算
    print("  重算后 offline u1 @600  =", store.read_offline("gmv_1h", "u1", 600)["value"],
          "（含迟到 500：100+50+7）")
    print("  重算后 offline u1 @3900 =", store.read_offline("gmv_1h", "u1", 3900)["value"],
          "（窗口仍含 500：20+7）")
    # 验证未受影响范围：把水位推进后补一个更晚时点（无迟到事件影响）
    print("  在线当前值（anchor=3900）=", store.read_online("gmv_1h", "u1")["value"])

    banner("10. 事件订正台账：replace / 幂等重放 / 409 冲突 / retract / 崩溃恢复")
    from featforge.store import CorrectionConflict

    def run_until_applied(cid: int) -> None:
        while store.get_correction(cid)["state"] != "applied":
            worker.tick()

    print("-- 10.1 普通替换：ord-2 (u1@600, 50) 订正为 500，revision=1")
    r = store.submit_correction("purchase", "ord-2", 1, "replace",
                                raw={"amount": 500.0})
    corr_id = r["correction"]["id"]
    print(f"  台账受理 corr#{corr_id}，落地作业 #{r['job_id']}，"
          f"受影响区间 {[(s['gen_id'], s['tick_start'], s['tick_end']) for s in r['correction']['scopes']]}")
    print("  落地作业未跑，读者已看到完整订正版本（读穿）：offline u1@600 =",
          store.read_offline("gmv_1h", "u1", 600)["value"])
    run_until_applied(corr_id)
    print("  落地完成：state =", store.get_correction(corr_id)["state"],
          "，offline u1@600 =", store.read_offline("gmv_1h", "u1", 600)["value"],
          "（100+500+迟到7）")

    print("-- 10.2 同一 revision 重放：返回原结果，不产生新条目")
    again = store.submit_correction("purchase", "ord-2", 1, "replace",
                                    raw={"amount": 500.0})
    print(f"  replayed={again['replayed']}，仍指向 corr#{again['correction']['id']}")

    print("-- 10.3 同号不同内容：拒绝（HTTP 409 语义）")
    try:
        store.submit_correction("purchase", "ord-2", 1, "replace",
                                raw={"amount": 999.0})
    except CorrectionConflict as e:
        print(f"  CorrectionConflict: {e}")

    print("-- 10.4 撤销 ord-3 (u1@3900)：记录不再参与计算，但仍可审计")
    r = store.submit_correction("purchase", "ord-3", 1, "retract")
    run_until_applied(r["correction"]["id"])
    online = store.read_online("gmv_1h", "u1")
    print(f"  当前值锚点回退到 {online['anchor_ts']}，值 = {online['value']}")
    rec = store.get_record("purchase", "ord-3")
    print(f"  审计视图：versions={len(rec['versions'])}，"
          f"effective={rec['effective']}，current_revision={rec['current_revision']}")

    print("-- 10.5 订正落地中途崩溃：进度已保存，重启后跳过已处理批次")
    r = store.submit_correction("purchase", "ord-1", 1, "replace",
                                raw={"amount": 111.0}, crash_after_batches=1)
    cid = r["correction"]["id"]
    try:
        worker.tick()
    except RuntimeError as e:
        print(f"  模拟崩溃：{e}")
    print(f"  崩溃时已提交 {store.get_correction(cid)['batches_done']} 批；"
          f"作业状态 {store.get_job(r['job_id'])['state']}")
    n = store.recover_interrupted_corrections()
    print(f"  进程重启恢复 {n} 个中断作业，续跑跳过已处理批次……")
    run_until_applied(cid)
    print("  恢复后落地完成：state =", store.get_correction(cid)["state"],
          "，online u1 =", store.read_online("gmv_1h", "u1")["value"])

    banner("11. 特征时间旅行：历史时点快照 + 未来事件排除 + 跨代差异")
    # 当前 serving 是第 2 代（第 7 步回滚后又被订正作业影响？以别名实际指向为准）
    serving = store.read_online("gmv_1h", "u1")["serving_gen"]
    print(f"  当前 serving 代 = {serving}")
    snap = store.feature_snapshot("gmv_1h", "u1", 600, gen_no=1, limit=50)
    print(f"  第1代快照 {snap['snapshot_id']}")
    w = snap["window"]
    print(f"  样本时刻 {snap['asof_ts']}，滚动窗口 "
          f"({w['window_open_exclusive']}, {w['window_close_inclusive']}]，"
          f"快照值 = {snap['value']}")
    for e in snap["events"]:
        tag = "纳入  " if e["included"] else f"排除({e['exclude_reason']})"
        print(f"    [{tag}] event_ts={e['event_ts']} {e['record_key']}"
              f" r{e['record_rev']} {e['raw']}")
    print("  来源版本:",
          [(x["record_key"], x["record_rev"]) for x in snap["source_versions"]])
    # 重复查询同一快照标识
    again = store.feature_snapshot("gmv_1h", "u1", 600, gen_no=1, limit=50)
    print("  重复查询 snapshot_id 一致:",
          again["snapshot_id"] == snap["snapshot_id"])
    # 跨代差异（第 1 代 sum_v1 vs 第 2 代 sum_v2，数值应完全相同）
    d = store.generation_diff("gmv_1h", "u1", 600, a_gen_no=1, b_gen_no=2)
    print(f"  跨代差异 {d['a']['snapshot_id'][:32]}... vs "
          f"{d['b']['snapshot_id'][:32]}...")
    print("  差异统计:", d["diff_stats"])

    banner("审计日志（最后 8 条）")
    for a in store.list_audits(8):
        print(f"  [{a['kind']}] {a['message']}")

    print("\n演示完成。数据库文件：", db_path)
    print("启动服务：FEATFORGE_DB=%s python -m featforge all" % db_path)


if __name__ == "__main__":
    main()
