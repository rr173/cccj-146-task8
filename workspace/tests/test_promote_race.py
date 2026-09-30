"""两个已校验版本并发上线：以发起时服务指针为竞争条件，最多一方成功。

修复前两个 promote 都返回成功、后完成者抢走流量并各写一条成功历史；
修复后输家得到 AliasConflict（HTTP 409），不更新别名/状态、不写成功历史。
随后的重复请求、人工回退与在线读流量都保持单一、完整且可验证的版本。
"""
import threading

from conftest import events, make_feature
from featforge.store import AliasConflict, Store


def _three_gens(store, worker):
    """gen1 serving；gen2/gen3 均回填完成、校验通过但尚未切换（candidate）。"""
    make_feature(store, name="amt_race", transform="mean", transform_version=1)
    events(store, [("u1", 300, 10.0), ("u1", 600, 20.0),
                   ("u2", 300, 30.0), ("u2", 600, 40.0)])
    worker.tick()  # gen1 回填（不自动切换）
    assert store.promote_generation(1)["promoted"] is True  # gen1 成为服务指针
    g2 = store.create_generation("amt_race", "ewma", 1)
    g3 = store.create_generation("amt_race", "ewma", 1)
    worker.tick()
    worker.tick()
    c = store.db.conn()
    assert c.execute("SELECT status FROM generations WHERE id=2").fetchone()[0] == "candidate"
    assert c.execute("SELECT status FROM generations WHERE id=3").fetchone()[0] == "candidate"
    assert store.read_online("amt_race", "u1")["serving_gen"] == 1
    return g2, g3


class _RacyStore(Store):
    """在并发 promote 的校验之后、进入写事务之前插入同步屏障，强制双方都在
    服务指针未变（都钉住 gen1）时同时竞争写事务。"""

    barrier = None

    def validate_generation(self, gen_id):
        v = super().validate_generation(gen_id)
        if type(self).barrier is not None:
            type(self).barrier.wait()
        return v


def _make_store(db):
    return _RacyStore(db, clock=lambda: 1_000_000)


def _run_concurrent(db, g2_id, g3_id):
    store_a, store_b = _make_store(db), _make_store(db)
    _RacyStore.barrier = threading.Barrier(2)
    outcomes = {}

    def run(s, gid, tag):
        try:
            outcomes[tag] = ("ok", s.promote_generation(gid))
        except AliasConflict as e:
            outcomes[tag] = ("conflict", str(e))
        except Exception as e:  # pragma: no cover - 测试失败时暴露
            outcomes[tag] = ("error", repr(e))

    t1 = threading.Thread(target=run, args=(store_a, g2_id, "g2"))
    t2 = threading.Thread(target=run, args=(store_b, g3_id, "g3"))
    t1.start(); t2.start(); t1.join(); t2.join()
    _RacyStore.barrier = None
    return outcomes


def _setup(tmp_path):
    from featforge.db import Database
    from featforge.worker import Worker
    db = Database(str(tmp_path / "race.db"))
    base = Store(db, clock=lambda: 1_000_000)
    w = Worker(base, auto_promote=False, poll_seconds=0)
    g2, g3 = _three_gens(base, w)
    fid = base.get_feature(base.db.conn(), "amt_race")["id"]
    return db, base, w, g2, g3, fid


def test_concurrent_promotes_exactly_one_wins(tmp_path):
    db, base, w, g2, g3, fid = _setup(tmp_path)
    outcomes = _run_concurrent(db, g2["id"], g3["id"])

    kinds = {tag: r[0] for tag, r in outcomes.items()}
    assert sorted(kinds.values()) == ["conflict", "ok"], outcomes
    winner_tag = next(t for t, k in kinds.items() if k == "ok")
    loser_tag = next(t for t, k in kinds.items() if k == "conflict")
    assert outcomes[winner_tag][1]["promoted"] is True
    # 输家得到清晰冲突响应：点明发起时指针（第 1 代）与当前指针（赢家）
    msg = outcomes[loser_tag][1]
    assert "已被其他操作改写" in msg and "amt_race" in msg

    winner_id = g2["id"] if winner_tag == "g2" else g3["id"]
    loser_id = g3["id"] if winner_tag == "g2" else g2["id"]
    c = db.conn()

    # 单一服务版本：别名指向赢家，状态表恰好一个 serving
    assert c.execute(
        "SELECT serving_gen_id FROM aliases WHERE feature_id=?",
        (fid,)).fetchone()["serving_gen_id"] == winner_id
    assert c.execute(
        "SELECT COUNT(*) n FROM generations WHERE feature_id=? AND status='serving'",
        (fid,)).fetchone()["n"] == 1
    assert c.execute("SELECT status FROM generations WHERE id=?",
                     (winner_id,)).fetchone()["status"] == "serving"
    # 输家仍是 candidate，没被改成 shadow（事务回滚干净）
    assert c.execute("SELECT status FROM generations WHERE id=?",
                     (loser_id,)).fetchone()["status"] == "candidate"

    # 不写成功历史：只有 baseline(gen1) + 赢家两条 promote 历史
    promoted_gens = [r["gen_id"] for r in c.execute(
        "SELECT gen_id FROM alias_history WHERE feature_id=? AND action='promote' "
        "ORDER BY id", (fid,)).fetchall()]
    assert promoted_gens == [1, winner_id]
    # 没给输家写 promoted 审计
    assert c.execute(
        "SELECT COUNT(*) n FROM audits WHERE kind='promoted' AND ref=?",
        (f"gen:{loser_id}",)).fetchone()["n"] == 0


def test_winner_repeated_request_is_idempotent_and_reads_stable(tmp_path):
    db, base, w, g2, g3, fid = _setup(tmp_path)
    outcomes = _run_concurrent(db, g2["id"], g3["id"])
    winner_id = g2["id"] if outcomes["g2"][0] == "ok" else g3["id"]

    # 赢家的重复请求：幂等落空，不再切换、不再写历史
    r1 = base.promote_generation(winner_id)
    r2 = base.promote_generation(winner_id)
    assert r1["promoted"] is False and r1["reason"] == "already_serving"
    assert r2["promoted"] is False and r2["reason"] == "already_serving"
    c = db.conn()
    assert c.execute(
        "SELECT COUNT(*) n FROM alias_history WHERE feature_id=? AND action='promote'",
        (fid,)).fetchone()["n"] == 2  # baseline + 赢家；重复请求不增加

    # 在线读流量始终读到同一个完整赢家版本（值可验证：mean(10,20)=15）
    online = base.read_online("amt_race", "u1")
    assert online["generation_id"] == winner_id and online["value"] == 15.0
    assert base.read_online("amt_race", "u1")["generation_id"] == winner_id


def test_loser_retry_after_refresh_switches_single_version(tmp_path):
    """冲突后调用方重新读取指针再发起：顺序切换成功，版本仍单一、历史完整。"""
    db, base, w, g2, g3, fid = _setup(tmp_path)
    outcomes = _run_concurrent(db, g2["id"], g3["id"])
    loser_id = g2["id"] if outcomes["g2"][0] == "conflict" else g3["id"]

    # 重新发起是一个新操作（钉住当前赢家指针）：成功且只新增一条 promote 历史
    r = base.promote_generation(loser_id)
    assert r["promoted"] is True
    c = db.conn()
    assert c.execute("SELECT serving_gen_id FROM aliases WHERE feature_id=?",
                     (fid,)).fetchone()["serving_gen_id"] == loser_id
    assert base.read_online("amt_race", "u1")["generation_id"] == loser_id
    assert c.execute(
        "SELECT COUNT(*) n FROM generations WHERE feature_id=? AND status='serving'",
        (fid,)).fetchone()["n"] == 1


def test_rollback_after_race_keeps_single_verifiable_version(tmp_path):
    db, base, w, g2, g3, fid = _setup(tmp_path)
    outcomes = _run_concurrent(db, g2["id"], g3["id"])
    winner_id = g2["id"] if outcomes["g2"][0] == "ok" else g3["id"]

    # 人工回退到上一可验证版本（赢家的前一代）
    rb = base.rollback_feature("amt_race", reason="上线后人工回退")
    prev_gen_no = 2 if winner_id == g3["id"] else 1
    assert rb["rolled_back"] is True and rb["to_gen"] == prev_gen_no
    c = db.conn()
    prev_id = c.execute(
        "SELECT id FROM generations WHERE feature_id=? AND gen_no=?",
        (fid, prev_gen_no)).fetchone()["id"]
    assert c.execute("SELECT serving_gen_id FROM aliases WHERE feature_id=?",
                     (fid,)).fetchone()["serving_gen_id"] == prev_id
    assert c.execute(
        "SELECT COUNT(*) n FROM generations WHERE feature_id=? AND status='serving'",
        (fid,)).fetchone()["n"] == 1
    # 赢家降为 shadow 但仍完整可审计
    assert c.execute("SELECT status FROM generations WHERE id=?",
                     (winner_id,)).fetchone()["status"] == "shadow"
    # 回退后读到的是完整版本（值可验证：mean/ewma 均为 15）
    online = base.read_online("amt_race", "u1")
    assert online["generation_id"] == prev_id and online["value"] == 15.0

    # 重复回滚到当前 serving 代：幂等，不再写历史
    n_before = c.execute(
        "SELECT COUNT(*) n FROM alias_history WHERE feature_id=?",
        (fid,)).fetchone()["n"]
    again = base.rollback_feature("amt_race", to_gen_no=prev_gen_no)
    assert again["rolled_back"] is False and again["reason"] == "already_serving"
    assert c.execute(
        "SELECT COUNT(*) n FROM alias_history WHERE feature_id=?",
        (fid,)).fetchone()["n"] == n_before


def test_concurrent_promote_and_rollback_one_loses(tmp_path):
    """回滚与 promote 都钉住发起时的同一服务指针：后到一方 409，不抢回流量。"""
    db, base, w, g2, g3, fid = _setup(tmp_path)
    # 先让 g2 成为 serving（竞争起点）
    assert base.promote_generation(g2["id"])["promoted"] is True

    store_a, store_b = _make_store(db), _make_store(db)
    # 屏障同时覆盖 promote 的校验阶段与回滚的目标解析阶段
    _RacyStore.barrier = threading.Barrier(2)
    results = {}

    def _wait_if_barrier(self):
        if type(self).barrier is not None:
            type(self).barrier.wait()

    # rollback 没有 validate 阶段：在解析服务指针后通过 monkeypatch 等屏障。
    orig_get_feature = Store.get_feature

    def patched_get_feature(self, c, ref):
        f = orig_get_feature(self, c, ref)
        if isinstance(self, _RacyStore) and ref == "amt_race":
            _wait_if_barrier(self)
        return f

    _RacyStore.get_feature = patched_get_feature

    def promote_g3():
        try:
            results["promote"] = store_a.promote_generation(g3["id"])
        except AliasConflict as e:
            results["promote_conflict"] = str(e)

    def rollback():
        try:
            results["rollback"] = store_b.rollback_feature("amt_race")
        except AliasConflict as e:
            results["rollback_conflict"] = str(e)

    t1, t2 = threading.Thread(target=promote_g3), threading.Thread(target=rollback)
    t1.start(); t2.start(); t1.join(); t2.join()
    _RacyStore.barrier = None
    _RacyStore.get_feature = orig_get_feature

    c = db.conn()
    serving = c.execute("SELECT serving_gen_id FROM aliases WHERE feature_id=?",
                        (fid,)).fetchone()["serving_gen_id"]
    # 恰好一方成功，且输家没有成功结果
    if serving == g3["id"]:
        assert "rollback_conflict" in results
        assert results.get("promote", {}).get("promoted") is True
    else:
        assert serving == 1
        assert "promote_conflict" in results
        assert "promote" not in results
    assert c.execute(
        "SELECT COUNT(*) n FROM generations WHERE feature_id=? AND status='serving'",
        (fid,)).fetchone()["n"] == 1


def test_api_maps_alias_conflict_to_409(tmp_path):
    """HTTP 层：输掉竞争得到明确 409（error=alias_conflict），不写成功历史。"""
    from fastapi.testclient import TestClient
    from featforge.api import create_app

    db, base, w, g2, g3, fid = _setup(tmp_path)
    # g2 先上线，使 g3 的并发 promote 处于“指针已变”的冲突位置
    assert base.promote_generation(g2["id"])["promoted"] is True

    client = TestClient(create_app(base))
    # 直接让 promote 抛 AliasConflict（并发竞争语义已由存储层屏障测试覆盖），
    # 这里验证 API 的异常 -> 409 映射与冲突负载
    def raise_conflict(gen_id, force=False):
        raise AliasConflict("读取别名在切换请求发起后已被其他操作改写：测试冲突")
    base.promote_generation = raise_conflict
    r = client.post(f"/api/v1/generations/{g3['id']}/promote")
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["error"] == "alias_conflict"
    assert "改写" in detail["message"]
    assert detail["generation_id"] == g3["id"]
    assert detail["current_serving_gen"] == 2
    # 冲突不写历史
    c = db.conn()
    assert c.execute(
        "SELECT COUNT(*) n FROM alias_history WHERE feature_id=? AND gen_id=?",
        (fid, g3["id"])).fetchone()["n"] == 0

    # 回滚路径同样映射 409
    def raise_rb_conflict(feature_ref, to_gen_no=None, reason="人工回滚"):
        raise AliasConflict("读取别名在回滚请求发起后已被其他操作改写：测试冲突")
    base.rollback_feature = raise_rb_conflict
    r = client.post("/api/v1/features/amt_race/rollback", json={"reason": "x"})
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "alias_conflict"
