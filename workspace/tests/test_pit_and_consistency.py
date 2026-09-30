"""点内时间正确性 + 在线/离线自动比对。"""
from conftest import drain, events, make_feature


def test_offline_is_point_in_time_correct(store):
    make_feature(store, window="1h")
    # 事件在 100/2000/4000；baseline 回填在写入 4000 后运行，按当时水位截断到 3600
    events(store, [("u1", 100, 10.0), ("u1", 2000, 5.0), ("u1", 4000, 2.0)])
    results = drain(store)
    assert any(j["job"]["state"] == "succeeded" for j in results)

    # tick=1000 的值只能看到 event_ts<=1000 的事件（100 这条）
    assert store.read_offline("amt_1h", "u1", 1000)["value"] == 10.0
    # tick=3000 窗口 (-600,3000]：100 与 2000 都在内 -> 15
    assert store.read_offline("amt_1h", "u1", 3000)["value"] == 15.0
    # tick=3600（回填末端，含该刻度）：窗口 (0,3600] 仍含 100 与 2000 -> 15
    assert store.read_offline("amt_1h", "u1", 3600)["value"] == 15.0
    # 离线结果严格看不到“未来”事件 4000：末端按水位 4000 向下对齐到 3900
    gen = store.get_generation(store.db.conn(), 1)
    assert gen["backfill_end"] == 3900
    # 末端刻度 3900 的窗口 (300,3900] 只看到 2000，看不到 4000
    assert store.read_offline("amt_1h", "u1", 3900)["value"] == 5.0
    # tick=4200 超出回填覆盖，查不到值（读者读到 None，而不是未来数据）
    assert store.read_offline("amt_1h", "u1", 4200)["value"] is None


def test_online_matches_offline_on_same_ticks(store):
    make_feature(store)
    # 事件时间落在刻度上：300/600/900，在线快照生效刻度为 300/600/900
    events(store, [("u1", 300, 1.0), ("u1", 600, 2.0), ("u1", 900, 3.0)])
    drain(store)
    v = store.validate_generation(1)
    assert v["passed"], v
    ov = v["online_vs_offline"]
    assert ov["expected"] == 3
    assert ov["mismatch_rate"] == 0.0
    assert ov["match_rate"] == 1.0
    # 在线当前值 = 最近锚点 900，窗口内 [0,900] 三条之和
    assert store.read_online("amt_1h", "u1")["value"] == 6.0


def test_online_read_blocks_before_alias_switch(store, worker):
    make_feature(store)
    events(store, [("u1", 100, 1.0)])
    # 尚未跑回填：没有任何完整代际，读者拿不到半成品
    r = store.read_online("amt_1h", "u1")
    assert r["serving_gen"] is None and r["value"] is None
    worker.tick()
    r = store.read_online("amt_1h", "u1")
    assert r["serving_gen"] == 1
