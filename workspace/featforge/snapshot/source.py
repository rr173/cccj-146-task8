"""快照导出端的“物化来源”抽象。

ExportService 只依赖 :class:`FeatureSource` 协议，不依赖具体存储：

* :class:`DemoFeatureStore`：本模块自带的零依赖 SQLite 特征库，演示/测试用，
  同时演示“同特征多版本、schema 不同”的场景；
* :class:`FeatForgeOfflineSource`：把现有 FeatForge ``Store`` 的离线 PIT 物化
  结果适配成本协议——导出的正是某个特征版本在截止时刻已物化好的值。

cutoff 语义（与 FeatForge 点内时间一致）：``asof_ts <= cutoff_ts`` 的行才导出，
截止时刻之后才物化的值绝不出现在 bundle 中。
"""
from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Protocol


@dataclass(frozen=True)
class Materialized:
    feature: str
    version: int
    columns: list[dict]
    cutoff_ts: int
    rows: list[dict]  # [{"entity_id", "value", "asof_ts"}, ...]

    def row_count(self) -> int:
        return len(self.rows)


class FeatureSource(Protocol):
    def materialize(self, feature: str, version: int,
                    cutoff_ts: int) -> Materialized: ...


# ---------------------------------------------------------------- DemoFeatureStore

_SCHEMA = """
CREATE TABLE IF NOT EXISTS demo_feature_rows (
    feature TEXT NOT NULL,
    version INTEGER NOT NULL,
    entity_id TEXT NOT NULL,
    asof_ts INTEGER NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (feature, version, entity_id, asof_ts)
);
"""


class DemoFeatureStore:
    """演示用特征库：(特征, 版本, 实体, 时刻) -> JSON 值。

    每个 (特征, 版本) 的列结构在首次写入时注册；版本间列结构可以不同，
    以便演示 schema 标识不兼容时导入端拒绝激活。
    """

    def __init__(self, path: str):
        self.path = path
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._local = threading.local()

    def conn(self) -> sqlite3.Connection:
        c = getattr(self._local, "conn", None)
        if c is None:
            c = sqlite3.connect(self.path, timeout=30, check_same_thread=False,
                                isolation_level=None)
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA busy_timeout=30000")
            c.executescript(_SCHEMA)
            self._local.conn = c
        return c

    def seed(self, feature: str, version: int, rows: list[tuple],
             columns: list[dict]) -> None:
        """批量写入物化行。rows: [(entity_id, asof_ts, json可序列化值), ...]"""
        import json
        c = self.conn()
        with self._lock:
            c.execute("BEGIN IMMEDIATE")
            try:
                c.executemany(
                    "INSERT OR REPLACE INTO demo_feature_rows"
                    "(feature, version, entity_id, asof_ts, value) "
                    "VALUES (?,?,?,?,?)",
                    [(feature, version, e, ts, json.dumps(v, sort_keys=True))
                     for e, ts, v in rows],
                )
                c.execute("COMMIT")
            except BaseException:
                c.execute("ROLLBACK")
                raise

    def materialize(self, feature: str, version: int,
                    cutoff_ts: int) -> Materialized:
        import json
        c = self.conn()
        rows = c.execute(
            "SELECT entity_id, asof_ts, value FROM demo_feature_rows "
            "WHERE feature=? AND version=? AND asof_ts<=? "
            "ORDER BY asof_ts, entity_id",
            (feature, version, cutoff_ts),
        ).fetchall()
        data = [
            {"entity_id": r["entity_id"], "asof_ts": r["asof_ts"],
             "value": json.loads(r["value"])}
            for r in rows
        ]
        return Materialized(
            feature=feature, version=version,
            columns=demo_columns(feature, version),
            cutoff_ts=cutoff_ts, rows=data,
        )


# 演示 schema 注册表（与数据解耦，便于演示 v2 加列导致不兼容）。
_DEMO_COLUMNS = {
    ("__default__", 0): [
        {"name": "entity_id", "type": "string"},
        {"name": "value", "type": "json"},
        {"name": "asof_ts", "type": "int64"},
    ],
}


def register_demo_columns(feature: str, version: int,
                          columns: list[dict]) -> None:
    _DEMO_COLUMNS[(feature, version)] = columns


def demo_columns(feature: str, version: Optional[int]) -> list[dict]:
    return list(_DEMO_COLUMNS.get(
        (feature, version), _DEMO_COLUMNS[("__default__", 0)]))


# ---------------------------------------------------------------- FeatForge 适配

class FeatForgeOfflineSource:
    """把 FeatForge Store 的离线 PIT 物化结果适配为快照来源。

    导出某个特征指定代际（``gen_no``）在截止时刻的值；列结构为
    实体 id + 数值/类别值 + 样本时刻。``gen_no`` 作为快照版本号写入 bundle，
    导入端据此区分代际。
    """

    def __init__(self, store):
        self.store = store

    def materialize(self, feature: str, version: int,
                    cutoff_ts: int) -> Materialized:
        c = self.store.db.conn()
        f = self.store.get_feature(c, feature)
        gen = c.execute(
            "SELECT * FROM generations WHERE feature_id=? AND gen_no=?",
            (f["id"], version),
        ).fetchone()
        if gen is None:
            raise KeyError(f"特征 {feature} 不存在第 {version} 代")
        rows_raw = c.execute(
            "SELECT entity_id, asof_ts, num_value, cat_value FROM offline_values "
            "WHERE gen_id=? AND asof_ts<=? ORDER BY asof_ts, entity_id",
            (gen["id"], cutoff_ts),
        ).fetchall()
        rows = [
            {"entity_id": r["entity_id"], "asof_ts": r["asof_ts"],
             "value": (r["cat_value"] if gen["kind"] == "categorical"
                       else r["num_value"])}
            for r in rows_raw
        ]
        columns = [
            {"name": "entity_id", "type": "string"},
            {"name": "value",
             "type": "category" if gen["kind"] == "categorical" else "float64"},
            {"name": "asof_ts", "type": "int64"},
        ]
        return Materialized(feature=feature, version=version,
                            columns=columns, cutoff_ts=cutoff_ts, rows=rows)
