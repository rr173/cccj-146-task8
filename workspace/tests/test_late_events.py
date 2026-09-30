"""迟到事件：只重算受影响范围，在线/离线一致，无关分区不动。"""
from conftest import events, make_feature


def test_late_event_recomputes_only_affected_scope(store, worker):
    make_feature(store, window="1h")
    # 事件覆盖三个分区，回填后 gen1 serving（水位 7500，回填末端 7500）
    events(store, [("u1", 300, 10.0), ("u1", 3900, 20.0), ("u1", 7500, 30.0)])
    worker.tick()
    assert store.read_online("amt_1h", "u1")["serving_gen"] == 1

    c = store.db.conn()
    checkpoints_before = {r[0] for r in c.execute(
        "SELECT partition_start FROM checkpoints WHERE gen_id=1").fetchall()}

    # 迟到事件：时间 500（分区 0 内），在回填完成后才到达
    out = store.ingest_event("purchase", "u1", 500, raw={"amount": 7.0})
    assert out["late"] is True
    job_id = out["recompute_jobs"][0]
    worker.tick()  # 执行受影响范围重算
    assert store.get_job(job_id)["state"] == "succeeded"

    # tick=600 的窗口 (-2400,600]：迟到 500 与原 300 两条 -> 17
    assert store.read_offline("amt_1h", "u1", 600)["value"] == 17.0
    # tick=3900 在受影响区间 [600,4100] 内：窗口 (300,3900] 含迟到 500 与 3900 -> 27
    assert store.read_offline("amt_1h", "u1", 3900)["value"] == 27.0
    # tick=4200 超出受影响上界 ceil(500+3600)=4200 之外：500 已滑出窗口 -> 20
    assert store.read_offline("amt_1h", "u1", 4500)["value"] == 20.0

    # 检查点集合不变：迟到重算不是全量回填
    checkpoints_after = {r[0] for r in c.execute(
        "SELECT partition_start FROM checkpoints WHERE gen_id=1").fetchall()}
    assert checkpoints_after == checkpoints_before

    # 在线历史在重算区间内被同步刷新，与离线重新一致：校验通过
    v = store.validate_generation(1)
    assert v["online_vs_offline"]["mismatch_rate"] == 0.0


def test_late_event_before_checkpoint_is_deferred_to_backfill(store, worker):
    # 回填失败只完成分区 0；迟到事件落在尚未完成的分区：不重复入队，
    # 等 resume 回填自然覆盖
    make_feature(store, window="1h", fail_after_partitions=1)
    events(store, [("u1", 100, 1.0), ("u1", 3700, 2.0), ("u1", 7300, 3.0)])
    worker.tick()  # 分区 0 完成后注入故障（水位 7300 截断到 7200，分区 0/3600）

    # 先把水位推到 7600，随后到达的 7400 才是迟到事件，落在尚未完成的分区 2
    store.ingest_event("purchase", "u1", 7600, raw={"amount": 0.0})
    out = store.ingest_event("purchase", "u1", 7400, raw={"amount": 9.0})
    assert out["late"] is True
    assert out["recompute_jobs"] == []  # 该分区无检查点，交给续跑回填
    failed = [j for j in store.list_jobs() if j["state"] == "failed"][0]
    new_job = store.resume_failed_job(failed["id"])
    worker.tick()
    assert store.get_job(new_job["id"])["state"] == "succeeded"
    # 续跑回填按新水位（7600->7500）截断，包含迟到数据：
    # tick=7500 窗口 (3900,7500] 内 7300+7400
    assert store.read_offline("amt_1h", "u1", 7500)["value"] == 12.0
