"""快照搬运模块的 SQLite 元数据库（导出端 + 导入端共用建表脚本）。

每个实例（导出端/导入端）一个独立库文件，WAL + busy_timeout；写事务统一走
``BEGIN IMMEDIATE`` 串行化，块租约的领取/接管与激活切换都依赖它。
"""
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

SCHEMA = """
-- 导出端：幂等键台账。building 带 fencing token，只有持当前 fence 的人能发布。
CREATE TABLE IF NOT EXISTS ss_export_keys (
    idempotency_key TEXT PRIMARY KEY,
    params_hash TEXT NOT NULL,
    state TEXT NOT NULL,                 -- building|published
    bundle_id TEXT,
    fence_token TEXT,
    fence_expires_at REAL,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);

-- 导出端：已发布的不可变 bundle（manifest 全文）。块字节存对象存储目录。
CREATE TABLE IF NOT EXISTS ss_bundles (
    bundle_id TEXT PRIMARY KEY,
    feature TEXT NOT NULL,
    version INTEGER NOT NULL,
    schema_id TEXT NOT NULL,
    cutoff_ts INTEGER NOT NULL,
    manifest_json TEXT NOT NULL,
    created_at REAL NOT NULL
);

-- 导入端：一次搬运会话。状态机：receiving -> activating -> active|failed
CREATE TABLE IF NOT EXISTS ss_transfers (
    transfer_id TEXT PRIMARY KEY,
    source_bundle_id TEXT NOT NULL,
    feature TEXT NOT NULL,
    version INTEGER NOT NULL,
    schema_id TEXT NOT NULL,
    expected_schema_id TEXT,
    source_base_url TEXT NOT NULL,
    state TEXT NOT NULL,                  -- receiving|activating|active|failed
    chunks_total INTEGER NOT NULL,
    chunks_committed INTEGER NOT NULL DEFAULT 0,
    auto_activate INTEGER NOT NULL DEFAULT 1,
    manifest_json TEXT,
    error_code TEXT,
    error TEXT,
    created_at REAL NOT NULL,
    updated_at REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_ss_transfers_state
    ON ss_transfers(state);

-- 导入端：分块检查点 + 块租约（fencing token 单调递增，旧持有者迟到提交被拒）
CREATE TABLE IF NOT EXISTS ss_chunks (
    transfer_id TEXT NOT NULL,
    chunk_index INTEGER NOT NULL,
    state TEXT NOT NULL,                  -- pending|leased|committed
    fence_seq INTEGER NOT NULL DEFAULT 0, -- fencing token 单调递增（接管 +1）
    lease_holder TEXT,
    lease_expires_at REAL,
    updated_at REAL NOT NULL,
    PRIMARY KEY (transfer_id, chunk_index)
);

-- 导入端：当前对读者服务的 active bundle。每特征整行原子替换。
CREATE TABLE IF NOT EXISTS ss_active (
    feature TEXT PRIMARY KEY,
    bundle_id TEXT NOT NULL,
    schema_id TEXT NOT NULL,
    version INTEGER NOT NULL,
    manifest_json TEXT NOT NULL,
    activated_at REAL NOT NULL
);
"""


class SnapshotDB:
    def __init__(self, path: str):
        self.path = path
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self.init_schema()

    def conn(self) -> sqlite3.Connection:
        c = getattr(self._local, "conn", None)
        if c is None:
            c = sqlite3.connect(self.path, timeout=30, check_same_thread=False,
                                isolation_level=None)
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA busy_timeout=30000")
            c.execute("PRAGMA foreign_keys=ON")
            self._local.conn = c
        return c

    def init_schema(self) -> None:
        self.conn().executescript(SCHEMA)

    def close(self) -> None:
        c = getattr(self._local, "conn", None)
        if c is not None:
            c.close()
            self._local.conn = None
