"""容量凭证与预算结算 HTTP API：策略、提交、占用、上报、完成/终止、
账本查询与状态页。"""
import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from fastapi.testclient import TestClient

from featforge.api import create_app
from featforge.budget import BudgetService
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
    db = Database(str(tmp_path / "api-budget.db"))
    clk = Clock()
    store = Store(db, clock=clk)
    budget = BudgetService(db, clock=clk, lease_ttl=100,
                           auto_scheduler=False)
    return TestClient(create_app(store, arbiter=None)), budget, clk
    # arbiter=None → create_app 自建（测试不关注仲裁器）


def _customer(c, name="acme", **kw):
    body = dict(refill_amount=5, refill_interval=60, burst_cap=10,
                max_seats=2, opening_balance=10)
    body.update(kw)
    body["name"] = name
    r = c.post("/api/v1/budget/customers", json=body)
    assert r.status_code == 201, r.text
    return r.json()


def _submit(c, key, estimate, name="acme", submitter="u1"):
    return c.post(f"/api/v1/budget/customers/{name}/vouchers",
                  json={"job_key": key, "estimate": estimate,
                        "submitter": submitter})


def test_full_lifecycle_http(tmp_path):
    c, budget, clk = _client(tmp_path)
    _customer(c)
    # 提交：余额足够 201
    r = _submit(c, "j1", 4)
    assert r.status_code == 201 and r.json()["state"] == "reserved"
    vid = r.json()["id"]

    # 余额不足：202 + deferred + 最早可执行时间
    r = _submit(c, "j2", 7)
    assert r.status_code == 202
    assert r.json()["state"] == "deferred"
    assert r.json()["earliest_ready_at"] == 1_000_060

    # 占用
    r = c.post(f"/api/v1/budget/customers/acme/vouchers/{vid}/acquire",
               json={"worker": "w1"})
    assert r.status_code == 200 and r.json()["claim_epoch"] == 1

    # 上报消耗
    r = c.post(f"/api/v1/budget/customers/acme/vouchers/{vid}/consumed",
               json={"claim_epoch": 1, "worker": "w1", "consumed_total": 3})
    assert r.status_code == 200 and r.json()["action"] == "settled"

    # 完成
    r = c.post(f"/api/v1/budget/customers/acme/vouchers/{vid}/complete",
               json={"claim_epoch": 1, "worker": "w1", "consumed_total": 3})
    assert r.status_code == 200 and r.json()["state"] == "completed"
    assert r.json()["refunded"] == 1

    # 时钟推进，后台 tick 端点唤醒 j2
    clk.advance(61)
    r = c.post("/api/v1/budget/tick")
    assert r.status_code == 200
    # 查 j2：列表里 state=reserved
    rows = c.get("/api/v1/budget/customers/acme/vouchers").json()
    j2 = next(v for v in rows if v["job_key"] == "j2")
    assert j2["state"] == "reserved"


def test_fencing_http(tmp_path):
    c, _, _ = _client(tmp_path)
    _customer(c)
    vid = _submit(c, "f", 5).json()["id"]
    c.post(f"/api/v1/budget/customers/acme/vouchers/{vid}/acquire",
           json={"worker": "w1"})
    # 错误持有者
    r = c.post(f"/api/v1/budget/customers/acme/vouchers/{vid}/consumed",
               json={"claim_epoch": 1, "worker": "impostor",
                     "consumed_total": 2})
    assert r.status_code == 409 and r.json()["detail"]["error"] == \
        "worker_mismatch"
    # 旧 epoch（先用 w2 接替需要先超时；这里直接用错 epoch）
    r = c.post(f"/api/v1/budget/customers/acme/vouchers/{vid}/consumed",
               json={"claim_epoch": 9, "worker": "w1",
                     "consumed_total": 2})
    assert r.status_code == 409 and r.json()["detail"]["error"] == \
        "stale_epoch"
    # 参数非法
    r = _submit(c, "bad", 0)
    assert r.status_code == 400


def test_policy_and_ledger_http(tmp_path):
    c, _, _ = _client(tmp_path)
    _customer(c, opening_balance=3)
    r = c.put("/api/v1/budget/customers/acme/policy",
              json={"burst_cap": 8, "refill_amount": 2})
    assert r.status_code == 200 and r.json()["burst_cap"] == 8

    r = c.get("/api/v1/budget/customers/acme")
    assert r.status_code == 200
    body = r.json()
    assert set(["available", "reserved_open", "settled_total",
                "next_refill_at", "blocked"]).issubset(body)

    r = c.get("/api/v1/budget/ledger?customer=acme")
    assert r.status_code == 200
    assert any(row["kind"] == "refill" for row in r.json())

    r = c.get("/api/v1/budget/status")
    assert r.status_code == 200 and r.json()["customers"]


def test_status_page_renders_budget(tmp_path):
    c, _, _ = _client(tmp_path)
    _customer(c)
    _submit(c, "page-job", 10)
    r = c.get("/")
    assert r.status_code == 200
    assert "容量凭证与预算结算".encode() in r.content
    assert "page-job".encode() in r.content


def test_terminate_idempotent_http(tmp_path):
    c, _, _ = _client(tmp_path)
    _customer(c)
    vid = _submit(c, "term", 6).json()["id"]
    r1 = c.post(f"/api/v1/budget/customers/acme/vouchers/{vid}/terminate",
                json={"reason": "x"})
    assert r1.status_code == 200 and r1.json()["refunded"] == 6
    r2 = c.post(f"/api/v1/budget/customers/acme/vouchers/{vid}/terminate",
                json={"reason": "x"})
    assert r2.status_code == 200 and r2.json()["refunded"] == 0
    assert r2.json()["state"] == "terminated"
