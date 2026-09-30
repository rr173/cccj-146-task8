"""多执行器并发：作业原子领取，不会被两个执行者重复执行。"""
import threading

from conftest import events, make_feature


def test_concurrent_workers_claim_jobs_once(store):
    from featforge.worker import Worker

    make_feature(store)
    events(store, [("u1", 300, 1.0), ("u1", 3900, 2.0)])

    # 两个并发执行器同时空转抢作业；同一作业只能被一个线程领取并执行
    w1, w2 = Worker(store), Worker(store)
    errors = []
    results = []
    lock = threading.Lock()

    def run(worker):
        try:
            for _ in range(3):
                r = worker.tick()
                if r:
                    with lock:
                        results.append(r["job"]["id"])
        except Exception as e:  # pragma: no cover
            errors.append(e)

    t1, t2 = threading.Thread(target=run, args=(w1,)), threading.Thread(target=run, args=(w2,))
    t1.start(); t2.start(); t1.join(); t2.join()

    assert not errors
    # 只有一个回填作业，且只成功执行一次
    succeeded = [j for j in store.list_jobs() if j["state"] == "succeeded"]
    assert len(succeeded) == 1
    assert succeeded[0]["attempts"] == 1
    assert store.read_online("amt_1h", "u1")["serving_gen"] == 1


def test_worker_tick_survives_lost_promote_race(store, worker, monkeypatch):
    """自动切换输掉并发竞争时，tick 不抛异常，记录 alias_conflict 而非成功。"""
    from featforge.store import AliasConflict
    from featforge.worker import Worker

    make_feature(store)
    events(store, [("u1", 300, 1.0)])

    # 回填成功后自动 promote 一定输掉竞争（模拟另一个版本已抢占别名）
    def lose(gen_id, force=False):
        raise AliasConflict("读取别名在切换请求发起后已被其他操作改写：测试")
    monkeypatch.setattr(store, "promote_generation", lose)

    out = Worker(store, auto_promote=True).tick()
    assert out["job"]["state"] == "succeeded"
    promo = out["promotion"]
    assert promo["promoted"] is False
    assert promo["reason"] == "alias_conflict"
    assert "改写" in promo["detail"]

