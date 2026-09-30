"""HTTP API 端到端冒烟：定义 -> 写入 -> 回填 -> 升级 -> 读取 -> 状态页。"""
from fastapi.testclient import TestClient

from featforge.api import create_app
from featforge.db import Database
from featforge.store import Store
from featforge.worker import Worker


def _client(tmp_path):
    store = Store(Database(str(tmp_path / "api.db")), clock=lambda: 0)
    return TestClient(create_app(store)), store


def test_full_lifecycle(tmp_path):
    client, store = _client(tmp_path)

    # 1) 创建定义
    r = client.post("/api/v1/features", json={
        "name": "pay_1h", "source": "purchase", "join_key": "entity_id",
        "value_field": "amount", "window": "1h", "transform": "sum",
        "backfill_start": 0, "partition_seconds": 3600,
        "sample_tick_seconds": 300,
    })
    assert r.status_code == 201 and r.json()["name"] == "pay_1h"

    # 2) 写事件（特征 value_field=amount，事件携带 amount 字段）
    for ts, v in [(300, 1.0), (600, 2.0), (3900, 5.0)]:
        assert client.post("/api/v1/events", json={
            "source": "purchase", "entity_id": "u1",
            "event_ts": ts, "raw": {"amount": v}}).status_code == 202

    # 3) 执行器跑一轮（回填 + 校验 + 自动切换）
    r = client.post("/api/v1/worker/tick")
    assert r.json()["job"]["state"] == "succeeded"

    # 4) 在线/离线读取。在线锚点 3900，窗口 (300,3900] 内 600+3900 两事件
    r = client.get("/api/v1/features/pay_1h/online/u1")
    assert r.json()["serving_gen"] == 1 and r.json()["value"] == 7.0
    r = client.get("/api/v1/features/pay_1h/offline/u1", params={"asof_ts": 600})
    assert r.json()["value"] == 3.0

    # 5) 定义升级：等价变换 sum_v2，门槛达标自动切换
    r = client.post("/api/v1/features/pay_1h/generations", json={
        "transform": "sum", "transform_version": 2})
    assert r.status_code == 201
    client.post("/api/v1/worker/tick")
    assert client.get("/api/v1/features/pay_1h/online/u1").json()["serving_gen"] == 2

    # 6) 作业列表 / 状态页可访问
    assert len(client.get("/api/v1/jobs").json()) >= 2
    page = client.get("/")
    assert page.status_code == 200 and "FeatForge" in page.text
    assert "pay_1h" in page.text

    # 7) 回滚
    r = client.post("/api/v1/features/pay_1h/rollback", json={"reason": "测试回滚"})
    assert r.json()["rolled_back"] is True
    assert client.get("/api/v1/features/pay_1h/online/u1").json()["serving_gen"] == 1


def test_bad_window_rejected(tmp_path):
    client, _ = _client(tmp_path)
    r = client.post("/api/v1/features", json={
        "name": "bad", "source": "s", "window": "abc"})
    assert r.status_code == 400
