"""代际升级：并行计算、覆盖率/偏差门槛、切换原子性、回滚、切换后回退。"""
import threading

import pytest

from conftest import events, make_feature
from featforge.store import FeatError


def _setup_baseline(store, worker, name):
    make_feature(store, name=name, transform="mean", transform_version=1)
    events(store, [("u1", 300, 10.0), ("u1", 600, 20.0),
                   ("u2", 300, 30.0), ("u2", 600, 40.0)])
    worker.tick()
    assert store.read_online(name, "u1")["serving_gen"] == 1
    return name


def test_equivalent_upgrade_passes_gate_and_promotes(store, worker):
    name = _setup_baseline(store, worker, name="feat_equiv")
    # ewma_v1 数值实现与 mean 完全一致：两代并行回填后偏差应为 0
    g2 = store.create_generation(name, "ewma", 1)
    worker.tick()  # 回填 + 自动校验 + 自动切换
    serving = store.read_online(name, "u1")
    assert serving["serving_gen"] == 2 and serving["generation_id"] == g2["id"]
    # 旧代降为 shadow 但仍保留，可读取验证
    c = store.db.conn()
    old_id = store.get_feature(c, name)["id"]
    assert c.execute(
        "SELECT status FROM generations WHERE feature_id=? AND gen_no=1",
        (old_id,)).fetchone()[0] == "shadow"
    assert c.execute("SELECT status FROM generations WHERE id=?",
                     (g2["id"],)).fetchone()[0] == "serving"


def test_biased_upgrade_is_blocked(store, worker):
    name = _setup_baseline(store, worker, name="feat_blocked")
    g2 = store.create_generation(name, "mean", 99)  # max*10，偏差巨大
    worker.tick()
    v = store.validate_generation(g2["id"])
    assert v["passed"] is False
    assert any("偏差" in r or "覆盖率" in r for r in v["reasons"])
    # 门槛不达标，读取别名仍停留在旧完整代
    assert store.read_online(name, "u1")["serving_gen"] == 1
    promo = store.promote_generation(g2["id"])
    assert promo["promoted"] is False and promo["reason"] == "quality_gate_failed"


def test_force_promote_then_rollback(store, worker):
    name = _setup_baseline(store, worker, name="feat_force")
    g2 = store.create_generation(name, "mean", 99)
    worker.tick()
    assert store.promote_generation(g2["id"], force=True)["promoted"] is True
    assert store.read_online(name, "u1")["serving_gen"] == 2
    # 回滚到上一可验证版本（gen1，回填完整）；mean(10,20)=15
    rb = store.rollback_feature(name)
    assert rb["to_gen"] == 1
    r = store.read_online(name, "u1")
    assert r["serving_gen"] == 1 and r["value"] == 15.0
    # g2 降为 shadow，仍是完整代，可再次切回
    c = store.db.conn()
    assert c.execute("SELECT status FROM generations WHERE id=?",
                     (g2["id"],)).fetchone()[0] == "shadow"


def test_rollback_target_must_be_complete(store, worker):
    # gen2 回填中途失败时不能作为回滚目标
    name = "feat_incomplete"
    make_feature(store, name=name, transform="mean", transform_version=1)
    # 三条事件把水位推到 7500，产生 0/3600/7200 三个分区
    events(store, [("u1", 100, 10.0), ("u1", 3700, 20.0), ("u1", 7500, 30.0)])
    worker.tick()
    assert store.read_online(name, "u1")["serving_gen"] == 1
    g2 = store.create_generation(name, "ewma", 1, fail_after_partitions=1)
    out = worker.tick()
    assert out["job"]["state"] == "failed"  # 3 个分区，完成 1 个后注入故障
    with pytest.raises(FeatError):
        store.rollback_feature(name, to_gen_no=2)
    # serving 始终是完整的 gen1
    assert store.read_online(name, "u1")["serving_gen"] == 1


def test_concurrent_promotion_is_first_wins(store, worker):
    from featforge.store import AliasConflict

    name = _setup_baseline(store, worker, name="feat_concurrent")
    fid = store.get_feature(store.db.conn(), name)["id"]
    g2 = store.create_generation(name, "ewma", 1)
    worker.tick()  # 已自动切换到 gen2
    # 两个并发请求：gen2 已 serving（幂等返回 already_serving），同时人工回滚 gen1。
    # 两者以发起时的服务指针（gen2）为竞争条件串行化：若回滚先提交，promote
    # 必须得到 AliasConflict 而不是再把别名抢回 gen2；无论顺序只有一方达成写操作。
    outcomes = []

    def promote():
        try:
            outcomes.append(("promote", store.promote_generation(g2["id"])))
        except AliasConflict as e:
            outcomes.append(("promote_conflict", str(e)))

    def rollback():
        try:
            outcomes.append(("rollback", store.rollback_feature(name, reason="并发回滚")))
        except AliasConflict as e:
            outcomes.append(("rollback_conflict", str(e)))

    t1, t2 = threading.Thread(target=promote), threading.Thread(target=rollback)
    t1.start(); t2.start(); t1.join(); t2.join()
    by_tag = dict(outcomes)
    # 重叠竞争（双方都钉住发起时的 gen2 指针）时：
    # - 回滚先提交：promote 得到 promote_conflict（不是成功）
    # - promote 先到：它幂等落空（already_serving），回滚随后正常执行
    # 无论是否重叠，promote 绝不会在回滚之后又返回 promoted=True 抢回流量。
    if "promote_conflict" in by_tag:
        assert "promote" not in by_tag  # 输家不返回成功结果
    else:
        promo = by_tag.get("promote", {})
        assert promo.get("promoted") is not True  # gen2 已 serving，最多幂等落空
    # 无论顺序，别名永远恰好指向一个完整代
    serving = store.read_online(name, "u1")["serving_gen"]
    assert serving in (1, 2)
    c = store.db.conn()
    n_serving = c.execute(
        "SELECT COUNT(*) c FROM generations WHERE feature_id=? AND status='serving'",
        (fid,),
    ).fetchone()["c"]
    assert n_serving == 1
    # 成功历史与实际 serving 一致：最后一条历史指向当前 serving 代
    hist = [r["gen_id"] for r in c.execute(
        "SELECT gen_id FROM alias_history WHERE feature_id=? ORDER BY id",
        (fid,)).fetchall()]
    assert hist[-1] == store.serving_gen(c, fid)["id"]
    # 无论顺序，别名永远恰好指向一个完整代
    serving = store.read_online(name, "u1")["serving_gen"]
    assert serving in (1, 2)
    c = store.db.conn()
    n_serving = c.execute(
        "SELECT COUNT(*) c FROM generations WHERE feature_id=? AND status='serving'",
        (fid,),
    ).fetchone()["c"]
    assert n_serving == 1
    # 输掉的一方不写成功历史：alias_history 中最终状态与实际 serving 一致
    hist = [r["gen_id"] for r in c.execute(
        "SELECT gen_id FROM alias_history WHERE feature_id=? ORDER BY id",
        (fid,)).fetchall()]
    assert hist[-1] == store.serving_gen(c, fid)["id"]


def test_post_switch_quality_regression_auto_rolls_back(store, worker):
    # 切换后在线值与离线 PIT 值出现明显偏差，视为质量回退并自动回滚
    name = "feat_quality"
    make_feature(store, name=name, transform="mean", transform_version=1,
                 require_online_match=True)
    # 先建等价的第二代（两代并行），再灌事件：两代都有在线刻度历史
    g2 = store.create_generation(name, "ewma", 1)
    events(store, [("u1", 300, 10.0), ("u1", 600, 20.0)])
    worker.tick()  # gen1 回填并切换
    worker.tick()  # gen2 回填、校验通过并切换
    assert store.read_online(name, "u1")["serving_gen"] == 2
    assert g2["id"] == 2

    # 污染前在线/离线严格一致
    assert store.validate_generation(g2["id"])["online_vs_offline"]["mismatch_rate"] == 0.0

    # 模拟切换后在线数据被污染（运维事故/写入分叉）
    store.db.conn().execute(
        "UPDATE online_current SET num_value=999.0 WHERE gen_id=? AND entity_id='u1'",
        (g2["id"],))
    store.db.conn().execute(
        "UPDATE online_history SET num_value=999.0 WHERE gen_id=?", (g2["id"],))
    store.db.conn().commit()

    result = store.post_switch_quality_check(name)
    assert result["healthy"] is False
    # 已自动回滚到先前可验证版本
    assert store.read_online(name, "u1")["serving_gen"] == 1
