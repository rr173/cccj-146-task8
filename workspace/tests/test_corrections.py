"""事件订正台账：replace/retract、幂等重放、序号冲突、并发单调推进、
崩溃恢复、失败续跑、读者只看到完整订正版本、全部在算代际同步刷新。"""
import threading

import pytest
from fastapi.testclient import TestClient

from conftest import make_feature
from featforge.api import create_app
from featforge.config import settings
from featforge.db import Database
from featforge.store import CorrectionConflict, FeatError, Store
from featforge.worker import drain


def _baseline(store, worker, name="amt_1h"):
    """gen1 serving：u1 有三条事件 e1@300=10, e2@600=20, e3@3900=5。"""
    make_feature(store, name=name, window="1h")
    store.ingest_event("purchase", "u1", 300, raw={"amount": 10.0}, dedup_key="e1")
    store.ingest_event("purchase", "u1", 600, raw={"amount": 20.0}, dedup_key="e2")
    store.ingest_event("purchase", "u1", 3900, raw={"amount": 5.0}, dedup_key="e3")
    worker.tick()
    assert store.read_online(name, "u1")["serving_gen"] == 1
    assert store.read_online(name, "u1")["value"] == 25.0


def test_replace_refreshes_only_affected_scope(store, worker):
    _baseline(store, worker)
    r = store.submit_correction("purchase", "e2", 1, "replace",
                                raw={"amount": 50.0})
    assert r["replayed"] is False and r["job_id"]
    corr = r["correction"]
    assert corr["state"] == "accepted"
    # 影响范围只覆盖 e2 生效的刻度区间 [600, 4200]
    assert [(s["tick_start"], s["tick_end"]) for s in corr["scopes"]] == [(600, 4200)]

    # 落地作业还没跑，读者已经看到完整订正版本（读穿），而不是旧物化值
    assert store.read_offline("amt_1h", "u1", 600)["value"] == 60.0
    assert store.read_offline("amt_1h", "u1", 600)["correction_pending"] is True
    assert store.read_online("amt_1h", "u1")["value"] == 55.0

    worker.tick()  # 执行订正落地
    corr = store.get_correction(corr["id"])
    assert corr["state"] == "applied" and corr["batches_done"] > 0

    # 物化值已刷新，与读穿结果一致
    assert store.read_offline("amt_1h", "u1", 600)["value"] == 60.0
    assert store.read_offline("amt_1h", "u1", 3900)["value"] == 55.0
    assert store.read_online("amt_1h", "u1")["value"] == 55.0
    # 未受影响刻度保持原值
    assert store.read_offline("amt_1h", "u1", 300)["value"] == 10.0
    # 事件日志：旧行被取代（仍可审计），新行生效
    rows = store.db.conn().execute(
        "SELECT * FROM events WHERE record_key='e2' ORDER BY record_rev").fetchall()
    assert len(rows) == 2
    assert rows[0]["superseded_by"] == corr["id"]
    assert rows[1]["superseded_by"] is None and rows[1]["record_rev"] == 1


def test_replace_can_move_event_time(store, worker):
    _baseline(store, worker)
    store.submit_correction("purchase", "e2", 1, "replace",
                            event_ts=1000, raw={"amount": 50.0})
    worker.tick()
    # 原位置窗口不再包含该事件
    assert store.read_offline("amt_1h", "u1", 600)["value"] == 10.0
    # 新位置生效：tick=1200 窗口 (-2400,1200] 含 300 与 1000
    assert store.read_offline("amt_1h", "u1", 1200)["value"] == 60.0
    assert store.read_online("amt_1h", "u1")["value"] == 55.0


def test_retract_removes_record_but_keeps_audit(store, worker):
    _baseline(store, worker)
    store.submit_correction("purchase", "e3", 1, "retract")
    worker.tick()
    # 最新事件被撤销：当前值锚点回退到 600，窗口 (-3000,600] -> 10+20
    online = store.read_online("amt_1h", "u1")
    assert online["anchor_ts"] == 600 and online["value"] == 30.0
    # 3900 刻度只剩 e2
    assert store.read_offline("amt_1h", "u1", 3900)["value"] == 20.0
    # 已撤销记录仍可审计（行保留、标记撤销），但不再参与计算
    rec = store.get_record("purchase", "e3")
    assert rec["effective"] is False and rec["current_revision"] == 1
    assert len(rec["versions"]) == 1
    assert rec["versions"][0]["retracted_by"] is not None


def test_retract_all_events_clears_online_current(store, worker):
    _baseline(store, worker)
    for rev, key in enumerate(("e1", "e2", "e3"), start=1):
        store.submit_correction("purchase", key, 1, "retract")
    drain(store)
    online = store.read_online("amt_1h", "u1")
    assert online["value"] is None and online["anchor_ts"] is None
    n = store.db.conn().execute(
        "SELECT COUNT(*) c FROM online_current WHERE entity_id='u1'"
    ).fetchone()["c"]
    assert n == 0


def test_same_revision_replay_returns_original(store, worker):
    _baseline(store, worker)
    a = store.submit_correction("purchase", "e2", 1, "replace",
                                raw={"amount": 50.0})
    b = store.submit_correction("purchase", "e2", 1, "replace",
                                raw={"amount": 50.0})
    assert b["replayed"] is True
    assert b["correction"]["id"] == a["correction"]["id"]
    worker.tick()
    # 落地后重放仍返回原结果，不产生新条目、新作业
    c = store.submit_correction("purchase", "e2", 1, "replace",
                                raw={"amount": 50.0})
    assert c["replayed"] is True
    assert c["correction"]["state"] == "applied"
    n = store.db.conn().execute("SELECT COUNT(*) c FROM corrections").fetchone()["c"]
    assert n == 1
    jobs = [j for j in store.list_jobs() if j["kind"] == "correction"]
    assert len(jobs) == 1
    assert store.read_offline("amt_1h", "u1", 600)["value"] == 60.0


def test_revision_conflict_and_stale_rejected(store, worker):
    _baseline(store, worker)
    store.submit_correction("purchase", "e2", 1, "replace", raw={"amount": 50.0})
    # 同号不同内容 -> 409 语义的冲突
    with pytest.raises(CorrectionConflict):
        store.submit_correction("purchase", "e2", 1, "replace",
                                raw={"amount": 51.0})
    store.submit_correction("purchase", "e2", 2, "retract")
    # 序号回退（且该序号从未受理）-> 冲突
    with pytest.raises(CorrectionConflict):
        store.submit_correction("purchase", "e2", 1, "retract")
    # 已受理序号的同内容重放（即使是旧序号）-> 返回原结果
    r = store.submit_correction("purchase", "e2", 1, "replace",
                                raw={"amount": 50.0})
    assert r["replayed"] is True and r["correction"]["revision"] == 1
    # 非法序号与未知记录 -> 400 语义
    with pytest.raises(FeatError):
        store.submit_correction("purchase", "e2", 0, "retract")
    with pytest.raises(FeatError):
        store.submit_correction("purchase", "nope", 1, "retract")
    with pytest.raises(FeatError):
        store.submit_correction("purchase", "e2", 3, "delete")


def test_concurrent_submissions_advance_monotonically(store, worker):
    _baseline(store, worker)
    accepted, replayed, conflicts = [], [], []

    def submit(i):
        try:
            r = store.submit_correction("purchase", "e2", i, "replace",
                                        raw={"amount": float(i * 100)})
            (replayed if r["replayed"] else accepted).append(i)
        except CorrectionConflict:
            conflicts.append(i)

    threads = [threading.Thread(target=submit, args=(i,)) for i in range(2, 8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    # 每个请求要么被接受、要么 409；序号只单调前进，最终停在最大号
    assert len(accepted) + len(conflicts) == 6 and not replayed
    chain = store.list_corrections("purchase", "e2")
    revisions = [c["revision"] for c in chain]
    assert revisions == sorted(set(revisions)) and revisions[-1] == 7
    # 最终生效内容 = 最大序号的提交
    drain(store)
    assert store.read_offline("amt_1h", "u1", 600)["value"] == 710.0
    assert store.read_online("amt_1h", "u1")["value"] == 705.0


def test_concurrent_identical_replay_lands_once(store, worker):
    _baseline(store, worker)
    outcomes = []

    def submit():
        outcomes.append(store.submit_correction(
            "purchase", "e2", 1, "replace", raw={"amount": 50.0})["replayed"])

    threads = [threading.Thread(target=submit) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # 恰好一条被受理，其余全部幂等重放；台账只有一条记录
    assert outcomes.count(False) == 1 and outcomes.count(True) == 4
    n = store.db.conn().execute("SELECT COUNT(*) c FROM corrections").fetchone()["c"]
    assert n == 1


def test_crash_recovery_skips_finished_batches(store, worker):
    settings.correction_batch_ticks = 4  # 13 个刻度 -> 4 批
    _baseline(store, worker)
    r = store.submit_correction("purchase", "e2", 1, "replace",
                                raw={"amount": 50.0}, crash_after_batches=1)
    job_id = r["job_id"]
    with pytest.raises(RuntimeError):
        worker.tick()  # 模拟进程崩溃：作业保持 running
    assert store.get_job(job_id)["state"] == "running"
    assert store.get_correction(r["correction"]["id"])["batches_done"] == 1

    # 进程重启：中断的作业重新排队，已提交批次跳过
    assert store.recover_interrupted_corrections() == 1
    assert store.get_job(job_id)["state"] == "queued"
    worker.tick()
    job = store.get_job(job_id)
    assert job["state"] == "succeeded" and job["attempts"] == 2
    corr = store.get_correction(r["correction"]["id"])
    assert corr["state"] == "applied"
    # 进度行数 == 总批次数：没有批次被重复执行
    assert corr["batches_done"] == 4
    assert store.read_offline("amt_1h", "u1", 600)["value"] == 60.0
    assert store.read_online("amt_1h", "u1")["value"] == 55.0


def test_newer_correction_wins_over_unfinished_old_job(store, worker):
    settings.correction_batch_ticks = 4
    _baseline(store, worker)
    r1 = store.submit_correction("purchase", "e2", 1, "replace",
                                 raw={"amount": 50.0}, crash_after_batches=1)
    with pytest.raises(RuntimeError):
        worker.tick()  # r1 落地 1 批后“崩溃”，物化值里是 50 的内容

    # 更新的订正在旧任务未结束时到达
    r2 = store.submit_correction("purchase", "e2", 2, "replace",
                                 raw={"amount": 77.0})
    # 读者立即看到 r2 的完整版本，不被旧任务的半成品污染
    assert store.read_offline("amt_1h", "u1", 600)["value"] == 87.0

    worker.tick()  # r1 作业仍 running，先执行 r2 的落地作业
    assert store.get_correction(r2["correction"]["id"])["state"] == "applied"

    # 恢复旧任务并跑完：它按最新日志计算，绝不能把 50 写回覆盖 r2 的结果
    store.recover_interrupted_corrections()
    worker.tick()
    assert store.get_correction(r1["correction"]["id"])["state"] == "applied"
    assert store.read_offline("amt_1h", "u1", 600)["value"] == 87.0
    assert store.read_offline("amt_1h", "u1", 3900)["value"] == 82.0
    assert store.read_online("amt_1h", "u1")["value"] == 82.0
    # 物化表本身也是新结果（不只是读穿）
    row = store.db.conn().execute(
        "SELECT num_value FROM offline_values "
        "WHERE gen_id=1 AND entity_id='u1' AND asof_ts=600").fetchone()
    assert row[0] == 87.0


def test_readers_see_only_complete_correction_versions(store, worker):
    settings.correction_batch_ticks = 4
    _baseline(store, worker)
    r = store.submit_correction("purchase", "e2", 1, "replace",
                                raw={"amount": 50.0}, crash_after_batches=2)
    ticks = list(range(600, 3901, 300))
    before = {t: store.read_offline("amt_1h", "u1", t)["value"] for t in ticks}
    online_before = store.read_online("amt_1h", "u1")["value"]
    assert before[600] == 60.0  # 落地前：读穿得到完整新版本

    with pytest.raises(RuntimeError):
        worker.tick()  # 落地 2 批后“崩溃”：一部分物化、一部分未物化
    # 崩溃中：读者看到的仍是完整新版本，绝不出现新旧混杂
    mid = {t: store.read_offline("amt_1h", "u1", t)["value"] for t in ticks}
    assert mid == before
    assert store.read_online("amt_1h", "u1")["value"] == online_before

    store.recover_interrupted_corrections()
    worker.tick()
    after = {t: store.read_offline("amt_1h", "u1", t)["value"] for t in ticks}
    assert after == before  # 落地前后读数一致，无版本抖动
    assert store.get_correction(r["correction"]["id"])["state"] == "applied"
    assert "correction_pending" not in store.read_offline("amt_1h", "u1", 600)


def test_failed_correction_resumes_from_batch_progress(store, worker):
    settings.correction_batch_ticks = 4
    _baseline(store, worker)
    r = store.submit_correction("purchase", "e2", 1, "replace",
                                raw={"amount": 50.0}, fail_after_batches=1)
    worker.tick()
    job = store.get_job(r["job_id"])
    assert job["state"] == "failed" and "注入故障" in job["error"]
    assert store.get_correction(r["correction"]["id"])["state"] == "failed"
    # 失败期间读者仍看到完整订正版本
    assert store.read_offline("amt_1h", "u1", 600)["value"] == 60.0

    new_job = store.resume_failed_job(job["id"])
    worker.tick()
    assert store.get_job(new_job["id"])["state"] == "succeeded"
    corr = store.get_correction(r["correction"]["id"])
    assert corr["state"] == "applied"
    # 已处理批次被跳过：进度行数恰好等于总批次数
    assert corr["batches_done"] == 4
    assert store.read_offline("amt_1h", "u1", 600)["value"] == 60.0


def test_all_live_generations_are_refreshed(store, worker):
    _baseline(store, worker)
    store.create_generation("amt_1h", "sum", 2)  # 等价升级
    worker.tick()
    assert store.read_online("amt_1h", "u1")["serving_gen"] == 2  # gen1 降 shadow
    store.create_generation("amt_1h", "sum", 1)  # gen3 building（尚未回填）

    r = store.submit_correction("purchase", "e2", 1, "replace",
                                raw={"amount": 50.0})
    # 影响范围覆盖全部在算代际：shadow gen1、serving gen2、building gen3
    assert {s["gen_id"] for s in r["correction"]["scopes"]} == {1, 2, 3}
    drain(store, auto_promote=False)
    assert store.get_correction(r["correction"]["id"])["state"] == "applied"
    c = store.db.conn()
    for gen_id in (1, 2, 3):
        v = c.execute(
            "SELECT num_value FROM offline_values "
            "WHERE gen_id=? AND entity_id='u1' AND asof_ts=600",
            (gen_id,)).fetchone()[0]
        assert v == 60.0
    # shadow 代的在线当前值同样被刷新（随时可切回）
    assert c.execute(
        "SELECT num_value FROM online_current WHERE gen_id=1 AND entity_id='u1'"
    ).fetchone()[0] == 55.0
    assert store.read_offline("amt_1h", "u1", 600)["value"] == 60.0


def test_revision_chain_and_record_views(store, worker):
    _baseline(store, worker)
    store.submit_correction("purchase", "e2", 1, "replace", raw={"amount": 50.0})
    store.submit_correction("purchase", "e2", 2, "retract")
    store.submit_correction("purchase", "e2", 3, "replace",
                            event_ts=900, raw={"amount": 70.0})
    chain = store.list_corrections("purchase", "e2")
    assert [c["revision"] for c in chain] == [1, 2, 3]
    assert [c["op"] for c in chain] == ["replace", "retract", "replace"]
    rec = store.get_record("purchase", "e2")
    assert rec["current_revision"] == 3 and rec["effective"] is True
    # 原始行 + r1 替换行 + r3 替换行（r2 撤销不产生新行），全部可审计
    assert [v["record_rev"] for v in rec["versions"]] == [0, 1, 3]
    drain(store)
    assert store.read_offline("amt_1h", "u1", 900)["value"] == 80.0
    assert store.read_online("amt_1h", "u1")["value"] == 75.0


def test_correction_api_end_to_end(tmp_path):
    store = Store(Database(str(tmp_path / "corr.db")), clock=lambda: 0)
    client = TestClient(create_app(store))
    assert client.post("/api/v1/features", json={
        "name": "amt", "source": "purchase", "join_key": "entity_id",
        "value_field": "amount", "window": "1h", "transform": "sum",
        "backfill_start": 0, "partition_seconds": 3600,
        "sample_tick_seconds": 300}).status_code == 201
    for key, ts, v in [("e1", 300, 10.0), ("e2", 600, 20.0), ("e3", 3900, 5.0)]:
        assert client.post("/api/v1/events", json={
            "source": "purchase", "entity_id": "u1", "event_ts": ts,
            "raw": {"amount": v}, "dedup_key": key}).status_code == 202
    client.post("/api/v1/worker/tick")

    # 普通替换 -> 201
    r = client.post("/api/v1/corrections", json={
        "source": "purchase", "record_key": "e2", "revision": 1,
        "op": "replace", "raw": {"amount": 50.0}})
    assert r.status_code == 201 and r.json()["replayed"] is False
    cid = r.json()["correction"]["id"]
    # 同号同内容重放 -> 200，返回原结果
    r = client.post("/api/v1/corrections", json={
        "source": "purchase", "record_key": "e2", "revision": 1,
        "op": "replace", "raw": {"amount": 50.0}})
    assert r.status_code == 200 and r.json()["replayed"] is True
    assert r.json()["correction"]["id"] == cid
    # 同号不同内容 -> 409
    r = client.post("/api/v1/corrections", json={
        "source": "purchase", "record_key": "e2", "revision": 1,
        "op": "replace", "raw": {"amount": 51.0}})
    assert r.status_code == 409
    # 撤销另一条记录 -> 201
    r = client.post("/api/v1/corrections", json={
        "source": "purchase", "record_key": "e3", "revision": 1, "op": "retract"})
    assert r.status_code == 201

    # 修订链与受影响区间、执行进度可查
    chain = client.get("/api/v1/corrections",
                       params={"source": "purchase", "record_key": "e2"}).json()
    assert [c["revision"] for c in chain] == [1]
    detail = client.get(f"/api/v1/corrections/{cid}").json()
    assert detail["scopes"] and detail["job_id"]
    # 记录视图：撤销后仍可通过接口审计
    rec = client.get("/api/v1/records/purchase/e3").json()
    assert rec["effective"] is False and len(rec["versions"]) == 1

    # 跑完两个订正落地作业
    for _ in range(2):
        client.post("/api/v1/worker/tick")
    assert client.get(f"/api/v1/corrections/{cid}").json()["state"] == "applied"
    # 当前值：e3 撤销后锚点回退到 600，e2 已替换为 50 -> 10+50
    assert client.get("/api/v1/features/amt/online/u1").json()["value"] == 60.0
    assert client.get("/api/v1/features/amt/offline/u1",
                      params={"asof_ts": 3900}).json()["value"] == 50.0
    # 状态页展示订正台账
    page = client.get("/")
    assert "事件订正台账" in page.text and "purchase/e2" in page.text
