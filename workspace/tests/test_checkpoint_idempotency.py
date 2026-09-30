"""回填检查点续跑、作业幂等、重复写入。"""
from conftest import drain, events, make_feature


def test_backfill_fails_then_resumes_from_checkpoint(store, worker):
    # 注入故障：物化 1 个分区后失败
    make_feature(store, fail_after_partitions=1)
    # 跨 3 个分区的事件：水位 7500 截断到 7500，分区起点 0/3600/7200
    events(store, [("u1", 100, 1.0), ("u1", 3700, 2.0), ("u1", 7500, 4.0)])
    r1 = worker.tick()
    job = r1["job"]
    assert job["state"] == "failed"
    assert job["partitions_done"] == 1
    assert job["partitions_total"] == 3

    # 检查点确实落盘
    cps = store.db.conn().execute(
        "SELECT partition_start FROM checkpoints WHERE gen_id=1 ORDER BY 1"
    ).fetchall()
    assert [r[0] for r in cps] == [0]

    # 失败作业不能自动切换；继续作业
    new_job = store.resume_failed_job(job["id"])
    results = drain(store)
    final = [j for j in results if j["job"]["id"] == new_job["id"]][0]["job"]
    assert final["state"] == "succeeded"
    # 续跑只处理剩余 2 个分区（第 0 分区从检查点跳过）
    cps_after = store.db.conn().execute(
        "SELECT COUNT(*) c FROM checkpoints WHERE gen_id=1").fetchone()["c"]
    assert cps_after == 3
    assert final["partitions_done"] == 3
    # 数据正确且切换成功
    assert store.read_online("amt_1h", "u1")["serving_gen"] == 1
    assert store.read_offline("amt_1h", "u1", 3900)["value"] == 2.0


def test_cannot_resume_non_failed_job(store, worker):
    make_feature(store)
    events(store, [("u1", 100, 1.0)])
    worker.tick()
    import pytest
    from featforge.store import FeatError
    job_id = store.list_jobs()[0]["id"]
    with pytest.raises(FeatError):
        store.resume_failed_job(job_id)


def test_duplicate_events_are_idempotent(store, worker):
    make_feature(store)
    a = store.ingest_event("purchase", "u1", 300, raw={"amount": 1.0},
                           dedup_key="evt-1")
    b = store.ingest_event("purchase", "u1", 300, raw={"amount": 1.0},
                           dedup_key="evt-1")
    assert a["dedup"] is False and b["dedup"] is True
    worker.tick()
    n = store.db.conn().execute(
        "SELECT COUNT(*) c FROM events WHERE dedup_key='evt-1'").fetchone()["c"]
    assert n == 1
    assert store.read_online("amt_1h", "u1")["value"] == 1.0


def test_repeated_backfill_request_dedupes(store):
    make_feature(store)
    # 排队中的回填作业重复入队返回同一作业
    j1 = store._enqueue_job(
        store.db.conn(), "backfill", gen_id=1, dedup_key="backfill:gen1")
    store.db.conn().commit()
    j2 = store._enqueue_job(
        store.db.conn(), "backfill", gen_id=1, dedup_key="backfill:gen1")
    store.db.conn().commit()
    assert j1 == j2
