"""多租户仲裁器 HTTP API：策略、入队、claim/续期/进度/完成、暂停、历史、状态页。"""
import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from fastapi.testclient import TestClient

from featforge.api import create_app
from featforge.arbiter import Arbiter
from featforge.db import Database
from featforge.store import Store


class Clock:
    def __init__(self, t=1_000_000):
        self.t = t

    def __call__(self):
        return self.t

    def advance(self, n):
        self.t += n


def _client(tmp_path):
    db = Database(str(tmp_path / "api-arb.db"))
    clk = Clock()
    store = Store(db, clock=clk)
    arb = Arbiter(db, clock=clk, lease_ttl=100, aging_rate=1.0)
    return TestClient(create_app(store, arbiter=arb)), arb, clk


def test_tenant_lifecycle_and_policy(tmp_path):
    c, arb, _ = _client(tmp_path)
    r = c.post("/api/v1/arbiter/tenants",
               json={"name": "t1", "weight": 2, "seats": 3})
    assert r.status_code == 201 and r.json()["weight"] == 2.0
    # 重复创建幂等（不覆盖）
    r = c.post("/api/v1/arbiter/tenants",
               json={"name": "t1", "weight": 9, "seats": 9})
    assert r.json()["weight"] == 2.0
    r = c.put("/api/v1/arbiter/tenants/t1/policy",
              json={"weight": 5, "seats": 4})
    assert r.status_code == 200 and r.json()["weight"] == 5.0
    # 非法策略 400
    assert c.put("/api/v1/arbiter/tenants/t1/policy",
                 json={"weight": 0}).status_code == 400
    assert c.get("/api/v1/arbiter/tenants/missing").status_code == 400


def test_enqueue_claim_heartbeat_progress_complete(tmp_path):
    c, arb, _ = _client(tmp_path)
    c.post("/api/v1/arbiter/tenants", json={"name": "t", "seats": 2})
    r = c.post("/api/v1/arbiter/jobs", json={
        "tenant": "t", "kind": "snapshot", "business_key": "k1",
        "payload": {"x": 1}, "shards_total": 3})
    assert r.status_code == 201
    jid = r.json()["job"]["id"]
    # 同键同负载重放 200
    r2 = c.post("/api/v1/arbiter/jobs", json={
        "tenant": "t", "kind": "snapshot", "business_key": "k1",
        "payload": {"x": 1}, "shards_total": 3})
    assert r2.status_code == 200 and r2.json()["replayed"] is True
    # 同键异参 409
    r3 = c.post("/api/v1/arbiter/jobs", json={
        "tenant": "t", "kind": "snapshot", "business_key": "k1",
        "shards_total": 9})
    assert r3.status_code == 409
    assert r3.json()["detail"]["error"] == "business_key_conflict"
    # claim
    r = c.post("/api/v1/arbiter/claims", json={"worker_id": "w1"})
    claim = r.json()
    assert claim["id"] == jid and claim["claim_epoch"] == 1
    assert claim["lease_ttl"] == 100
    # 进度
    r = c.post(f"/api/v1/arbiter/jobs/{jid}/progress",
               json={"claim_epoch": 1, "worker_id": "w1", "cursor": 2})
    assert r.status_code == 200 and r.json()["shard_cursor"] == 2
    # 心跳续期
    r = c.post(f"/api/v1/arbiter/jobs/{jid}/heartbeat",
               json={"claim_epoch": 1, "worker_id": "w1"})
    assert r.status_code == 200 and r.json()["last_heartbeat_at"]
    # 完成
    r = c.post(f"/api/v1/arbiter/jobs/{jid}/complete",
               json={"claim_epoch": 1, "worker_id": "w1"})
    assert r.json()["state"] == "succeeded" and r.json()["shard_cursor"] == 3
    # 进度台账
    rows = c.get(f"/api/v1/arbiter/jobs/{jid}/progress").json()
    assert [p["new_cursor"] for p in rows] == [2, 3]


def test_stale_epoch_and_cursor_regress_via_http(tmp_path):
    c, arb, clk = _client(tmp_path)
    c.post("/api/v1/arbiter/tenants", json={"name": "t"})
    c.post("/api/v1/arbiter/jobs", json={
        "tenant": "t", "kind": "backfill", "business_key": "k",
        "shards_total": 4})
    old = c.post("/api/v1/arbiter/claims",
                 json={"worker_id": "wA"}).json()
    c.post(f"/api/v1/arbiter/jobs/{old['id']}/progress",
           json={"claim_epoch": 1, "worker_id": "wA", "cursor": 2})
    # 超时接管
    clk.advance(101)
    c.post("/api/v1/arbiter/reap-expired")
    new = c.post("/api/v1/arbiter/claims",
                 json={"worker_id": "wB"}).json()
    assert new["claim_epoch"] == 2
    # 旧 epoch 上报 409 stale_epoch
    r = c.post(f"/api/v1/arbiter/jobs/{old['id']}/progress",
               json={"claim_epoch": 1, "worker_id": "wA", "cursor": 3})
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "stale_epoch"
    # 游标后退 409 cursor_regress
    r = c.post(f"/api/v1/arbiter/jobs/{old['id']}/progress",
               json={"claim_epoch": 2, "worker_id": "wB", "cursor": 1})
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "cursor_regress"


def test_pause_resume_and_status(tmp_path):
    c, arb, _ = _client(tmp_path)
    c.post("/api/v1/arbiter/tenants", json={"name": "t", "seats": 1})
    c.post("/api/v1/arbiter/jobs", json={
        "tenant": "t", "kind": "backfill", "business_key": "k1",
        "shards_total": 2})
    c.post("/api/v1/arbiter/jobs", json={
        "tenant": "t", "kind": "backfill", "business_key": "k2"})
    running = c.post("/api/v1/arbiter/claims",
                     json={"worker_id": "w1"}).json()
    c.post("/api/v1/arbiter/tenants/t/pause")
    # 暂停后无新 claim
    assert c.post("/api/v1/arbiter/claims",
                  json={"worker_id": "w1"}).json() == {"idle": True}
    # 已开工分片可写完
    done = c.post(f"/api/v1/arbiter/jobs/{running['id']}/complete",
                  json={"claim_epoch": 1, "worker_id": "w1"})
    assert done.json()["state"] == "succeeded"
    # 状态：等待中
    st = c.get("/api/v1/arbiter/tenants/t").json()
    assert st["paused"] is True and st["queued"] == 1
    c.post("/api/v1/arbiter/tenants/t/resume")
    nxt = c.post("/api/v1/arbiter/claims", json={"worker_id": "w1"}).json()
    assert nxt["business_key"] == "k2"


def test_history_and_status_page(tmp_path):
    c, arb, _ = _client(tmp_path)
    c.post("/api/v1/arbiter/tenants", json={"name": "t"})
    c.post("/api/v1/arbiter/jobs", json={
        "tenant": "t", "kind": "recompute", "business_key": "k"})
    c.post("/api/v1/arbiter/claims", json={"worker_id": "w1"})
    hist = c.get("/api/v1/arbiter/history").json()
    assert hist[0]["decision"] == "selected"
    assert hist[0]["tenant_name"] == "t"
    status = c.get("/api/v1/arbiter/status").json()
    assert status["tenants"][0]["name"] == "t"
    # 状态页包含仲裁器区块与租户名
    html = c.get("/").text
    assert "多租户仲裁器" in html and "近期仲裁选择结果" in html
    assert "recompute" in html


def test_retry_endpoint(tmp_path):
    c, arb, _ = _client(tmp_path)
    c.post("/api/v1/arbiter/tenants", json={"name": "t"})
    c.post("/api/v1/arbiter/jobs", json={
        "tenant": "t", "kind": "correction", "business_key": "k",
        "shards_total": 3})
    j = c.post("/api/v1/arbiter/claims",
               json={"worker_id": "w1"}).json()
    c.post(f"/api/v1/arbiter/jobs/{j['id']}/progress",
           json={"claim_epoch": 1, "worker_id": "w1", "cursor": 1})
    r = c.post(f"/api/v1/arbiter/jobs/{j['id']}/fail",
               json={"claim_epoch": 1, "worker_id": "w1", "error": "boom"})
    assert r.json()["state"] == "failed"
    # claimed 之外不能 fail：已 failed 再 fail 409
    r = c.post(f"/api/v1/arbiter/jobs/{j['id']}/retry")
    assert r.json()["state"] == "queued" and r.json()["shard_cursor"] == 1
    j2 = c.post("/api/v1/arbiter/claims",
                json={"worker_id": "w2"}).json()
    assert j2["claim_epoch"] == 2 and j2["shard_cursor"] == 1
