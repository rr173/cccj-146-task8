"""SQLite 元数据与物化存储。

一个库文件同时承担：
- 元数据：特征定义、代际、别名、作业、审计
- 在线存储：当前值 + 按样本刻度的历史值（供在线/离线比对）
- 离线存储：按 (代际, 实体, 样本时点) 的点内时间正确历史值
- 原始事件日志：回填重算的唯一事实来源
"""
from __future__ import annotations

import sqlite3
import threading
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS features (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL,            -- 源事件类型
    join_key TEXT NOT NULL,          -- 关联键（事件 payload 字段名）
    value_field TEXT NOT NULL,       -- 取值字段
    window_seconds INTEGER NOT NULL,
    transform_name TEXT NOT NULL,
    transform_version INTEGER NOT NULL,
    kind TEXT NOT NULL DEFAULT 'numeric',
    require_online_match INTEGER NOT NULL DEFAULT 0,
    backfill_start INTEGER,          -- 回填区间起点（含）
    partition_seconds INTEGER NOT NULL,
    sample_tick_seconds INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS generations (
    id INTEGER PRIMARY KEY,
    feature_id INTEGER NOT NULL REFERENCES features(id),
    gen_no INTEGER NOT NULL,
    transform_name TEXT NOT NULL,
    transform_version INTEGER NOT NULL,
    kind TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    status TEXT NOT NULL,            -- building|candidate|serving|shadow|retired
    backfill_status TEXT NOT NULL DEFAULT 'pending', -- pending|running|done|failed
    backfill_end INTEGER,
    fail_after_partitions INTEGER,   -- 故障注入：处理 N 个分区后失败
    UNIQUE(feature_id, gen_no)
);

CREATE TABLE IF NOT EXISTS aliases (
    feature_id INTEGER PRIMARY KEY REFERENCES features(id),
    serving_gen_id INTEGER NOT NULL REFERENCES generations(id),
    switched_at INTEGER NOT NULL
);

-- 只读别名指向的代际；切换时整行原子替换，读者永远只看到完整代际。
CREATE TABLE IF NOT EXISTS alias_history (
    id INTEGER PRIMARY KEY,
    feature_id INTEGER NOT NULL,
    gen_id INTEGER NOT NULL,
    action TEXT NOT NULL,           -- promote|rollback
    reason TEXT NOT NULL,
    at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS jobs (
    id INTEGER PRIMARY KEY,
    kind TEXT NOT NULL,             -- backfill|recompute|correction
    gen_id INTEGER REFERENCES generations(id),
    feature_id INTEGER REFERENCES features(id),
    entity_id TEXT,
    range_start INTEGER,
    range_end INTEGER,
    correction_id INTEGER,          -- kind=correction 时指向订正台账条目
    dedup_key TEXT NOT NULL UNIQUE,
    state TEXT NOT NULL,            -- queued|running|succeeded|failed
    attempts INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    enqueued_at INTEGER NOT NULL,
    started_at INTEGER,
    finished_at INTEGER,
    partitions_total INTEGER DEFAULT 0,
    partitions_done INTEGER DEFAULT 0
);

-- 回填检查点：每个分区完成后提交一行，重跑作业时据此跳过。
CREATE TABLE IF NOT EXISTS checkpoints (
    gen_id INTEGER NOT NULL,
    partition_start INTEGER NOT NULL,
    finished_at INTEGER NOT NULL,
    rows_written INTEGER NOT NULL,
    PRIMARY KEY (gen_id, partition_start)
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    entity_id TEXT NOT NULL,
    event_ts INTEGER NOT NULL,
    ingested_at INTEGER NOT NULL,
    seq INTEGER NOT NULL,
    value TEXT,
    raw_json TEXT NOT NULL,
    dedup_key TEXT UNIQUE,          -- 幂等：重复写入直接忽略
    is_late INTEGER NOT NULL DEFAULT 0,
    record_key TEXT,                -- 记录标识（原始事件 = dedup_key），订正链共用
    record_rev INTEGER NOT NULL DEFAULT 0,  -- 记录内容版本：0=原始，N=第 N 次订正
    superseded_by INTEGER,          -- 被哪条订正取代（replace），NULL=仍有效
    retracted_by INTEGER            -- 被哪条订正撤销（retract），NULL=仍有效
);
CREATE INDEX IF NOT EXISTS idx_events_src_ent_ts
    ON events(source, entity_id, event_ts);
CREATE INDEX IF NOT EXISTS idx_events_src_ts ON events(source, event_ts);

-- 事件订正台账：每次 replace/retract 操作形成一条不可变审计条目。
-- 条目内容（revision/op/负载/内容哈希）写入后不再修改，仅执行状态推进。
CREATE TABLE IF NOT EXISTS corrections (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    record_key TEXT NOT NULL,       -- 被订正记录（原始事件的 dedup_key）
    revision INTEGER NOT NULL,      -- 每条记录单调递增的订正序号
    op TEXT NOT NULL,               -- replace|retract
    entity_id TEXT NOT NULL,        -- 订正前记录所属实体
    new_event_ts INTEGER,           -- replace：生效后的事件时间
    new_entity_id TEXT,             -- replace：生效后的关联实体
    new_raw_json TEXT,              -- replace：生效后的负载
    content_hash TEXT NOT NULL,     -- 提交内容哈希：同号重放校验
    supersedes_event_id INTEGER,    -- 被取代/撤销的事件行
    new_event_id INTEGER,           -- replace 产生的新事件行
    state TEXT NOT NULL,            -- accepted|applying|applied|failed
    error TEXT,
    job_id INTEGER,                 -- 最新一次落地作业
    fail_after_batches INTEGER,     -- 故障注入：N 批后失败（演示续跑）
    crash_after_batches INTEGER,    -- 故障注入：N 批后模拟进程崩溃
    created_at INTEGER NOT NULL,
    applied_at INTEGER,
    UNIQUE(source, record_key, revision)
);

-- 订正影响范围：受理时按 (代际, 实体) 一次性算出的刻度区间。
-- 落地作业按此分批刷新；读者据此判断哪些点需要读穿。
CREATE TABLE IF NOT EXISTS correction_scopes (
    id INTEGER PRIMARY KEY,
    correction_id INTEGER NOT NULL REFERENCES corrections(id),
    gen_id INTEGER NOT NULL,
    entity_id TEXT NOT NULL,
    tick_start INTEGER NOT NULL,
    tick_end INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_correction_scopes_gen_ent
    ON correction_scopes(gen_id, entity_id);

-- 订正落地进度：每批完成后与数据同事务提交一行，崩溃/续跑据此跳过已处理批次。
CREATE TABLE IF NOT EXISTS correction_progress (
    correction_id INTEGER NOT NULL,
    gen_id INTEGER NOT NULL,
    batch_no INTEGER NOT NULL,
    entity_id TEXT NOT NULL,
    tick_start INTEGER NOT NULL,
    tick_end INTEGER NOT NULL,
    rows_written INTEGER NOT NULL,
    finished_at INTEGER NOT NULL,
    PRIMARY KEY (correction_id, gen_id, batch_no)
);

-- 离线物化值：点内时间正确，asof_ts 处只能看到 event_ts <= asof_ts 的事件。
CREATE TABLE IF NOT EXISTS offline_values (
    gen_id INTEGER NOT NULL,
    entity_id TEXT NOT NULL,
    asof_ts INTEGER NOT NULL,
    num_value REAL,
    cat_value TEXT,
    PRIMARY KEY (gen_id, entity_id, asof_ts)
);

CREATE TABLE IF NOT EXISTS online_current (
    gen_id INTEGER NOT NULL,
    entity_id TEXT NOT NULL,
    anchor_ts INTEGER NOT NULL,     -- 当前值对应的时点（窗口右端）
    num_value REAL,
    cat_value TEXT,
    PRIMARY KEY (gen_id, entity_id)
);

-- 在线值的样本刻度历史，用于与离线值做同点抽样比对。
CREATE TABLE IF NOT EXISTS online_history (
    gen_id INTEGER NOT NULL,
    entity_id TEXT NOT NULL,
    asof_ts INTEGER NOT NULL,       -- 对齐到样本刻度
    num_value REAL,
    cat_value TEXT,
    PRIMARY KEY (gen_id, entity_id, asof_ts)
);

CREATE TABLE IF NOT EXISTS watermarks (
    source TEXT PRIMARY KEY,
    watermark_ts INTEGER NOT NULL,  -- 曾观察到的最大事件时间
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS validations (
    id INTEGER PRIMARY KEY,
    gen_id INTEGER NOT NULL,
    baseline_gen_id INTEGER,
    mode TEXT NOT NULL,             -- new_vs_old|online_vs_offline
    coverage REAL,
    bias_p50 REAL,
    bias_p95 REAL,
    bias_max REAL,
    mismatch_rate REAL,
    sample_pairs INTEGER NOT NULL,
    compared INTEGER NOT NULL,
    passed INTEGER NOT NULL,
    detail TEXT NOT NULL,
    at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS audits (
    id INTEGER PRIMARY KEY,
    at INTEGER NOT NULL,
    kind TEXT NOT NULL,
    ref TEXT,
    message TEXT NOT NULL
);

-- ============================================================
-- 多租户仲裁器（unified ready set）
-- ============================================================

-- 租户：权重/并行席位/暂停状态/虚拟运行时间。在线改权重/席位只更新本
-- 表，绝不触碰 jobs 行与分片进度；vruntime 持久化使选择跨重启连续。
CREATE TABLE IF NOT EXISTS arb_tenants (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    weight REAL NOT NULL,            -- 相对份额（claim 速率 ∝ weight）
    seats INTEGER NOT NULL,          -- 并行席位上限（运行中分片数）
    paused INTEGER NOT NULL DEFAULT 0,  -- 暂停只阻止新 claim，已开工分片可写完
    vruntime REAL NOT NULL DEFAULT 0,   -- 虚拟运行时间：每次 claim += 1/weight
    total_claims INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

-- 统一 ready set：回填/订正/重算/快照四类分片作业同表排队。
-- business_key 是入队幂等键：同键同负载重放返回原作业；同键异参 409。
-- claim_epoch 每次 claim 单调 +1（含租约超时后的接管），旧 epoch 的
-- 心跳/进度/完成一律 fence 拒绝；shard_cursor 是只能前进的分片游标。
CREATE TABLE IF NOT EXISTS arb_jobs (
    id INTEGER PRIMARY KEY,
    tenant_id INTEGER NOT NULL REFERENCES arb_tenants(id),
    kind TEXT NOT NULL,             -- backfill|correction|recompute|snapshot
    business_key TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    payload_hash TEXT NOT NULL,
    shards_total INTEGER NOT NULL DEFAULT 1,
    shard_cursor INTEGER NOT NULL DEFAULT 0,  -- 已提交分片数（只能前进）
    state TEXT NOT NULL,            -- queued|claimed|succeeded|failed
    claim_epoch INTEGER NOT NULL DEFAULT 0,
    claim_worker TEXT,
    claimed_at INTEGER,
    lease_expires_at INTEGER,       -- 心跳续期；超时后回到 queued 可被接管
    last_heartbeat_at INTEGER,
    attempts INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    enqueued_at INTEGER NOT NULL,
    finished_at INTEGER,
    UNIQUE(tenant_id, business_key)
);
CREATE INDEX IF NOT EXISTS idx_arb_jobs_state_tenant
    ON arb_jobs(state, tenant_id);

-- 进度台账：每次进度提交追加一行（含 epoch 与当时的游标），供审计；
-- 游标后退会在冲突前被拒绝，已提交行永不回滚。
CREATE TABLE IF NOT EXISTS arb_progress (
    id INTEGER PRIMARY KEY,
    job_id INTEGER NOT NULL REFERENCES arb_jobs(id),
    claim_epoch INTEGER NOT NULL,
    worker TEXT,
    old_cursor INTEGER NOT NULL,
    new_cursor INTEGER NOT NULL,
    note TEXT,
    at INTEGER NOT NULL
);

-- 仲裁历史：每次 claim 决策一行（含选/跳过/超时原因、当时的
-- vruntime/欠额/老化），状态页展示“近期选择结果”，自动截断保留。
CREATE TABLE IF NOT EXISTS arb_history (
    id INTEGER PRIMARY KEY,
    at INTEGER NOT NULL,
    decision TEXT NOT NULL,         -- selected|idle|lease_expired|skipped
    tenant_id INTEGER,
    tenant_name TEXT,
    job_id INTEGER,
    claim_epoch INTEGER,
    reason TEXT,
    detail TEXT NOT NULL DEFAULT '{}'
);
"""


class Database:
    """线程局部连接 + WAL，API 线程与工作线程可共享同一数据库文件。"""

    def __init__(self, path: str):
        self.path = path
        if path != ":memory:":
            Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self.init_schema()

    def conn(self) -> sqlite3.Connection:
        c = getattr(self._local, "conn", None)
        if c is None:
            c = sqlite3.connect(
                self.path, timeout=30, check_same_thread=False, isolation_level=None
            )
            c.row_factory = sqlite3.Row
            c.execute("PRAGMA journal_mode=WAL")
            c.execute("PRAGMA foreign_keys=ON")
            c.execute("PRAGMA busy_timeout=30000")
            self._local.conn = c
        return c

    def init_schema(self) -> None:
        c = self.conn()
        c.executescript(SCHEMA)
        self._migrate(c)
        c.commit()

    @staticmethod
    def _ensure_column(c: sqlite3.Connection, table: str, name: str, ddl: str) -> None:
        cols = [r[1] for r in c.execute(f"PRAGMA table_info({table})").fetchall()]
        if name not in cols:
            c.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")

    def _migrate(self, c: sqlite3.Connection) -> None:
        """老库文件的就地迁移：补订正台账相关列并回填记录标识。"""
        self._ensure_column(c, "events", "record_key", "TEXT")
        self._ensure_column(c, "events", "record_rev",
                            "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column(c, "events", "superseded_by", "INTEGER")
        self._ensure_column(c, "events", "retracted_by", "INTEGER")
        self._ensure_column(c, "jobs", "correction_id", "INTEGER")
        # 多租户仲裁器桥接：镜像分片当前的 claim_epoch（fencing 用）
        self._ensure_column(c, "jobs", "arb_claim_epoch", "INTEGER NOT NULL DEFAULT 0")
        self._ensure_column(c, "jobs", "arb_job_id", "INTEGER")
        c.execute("UPDATE events SET record_key=dedup_key WHERE record_key IS NULL")
        c.execute("CREATE INDEX IF NOT EXISTS idx_events_record "
                  "ON events(source, record_key)")

    def close(self) -> None:
        c = getattr(self._local, "conn", None)
        if c is not None:
            c.close()
            self._local.conn = None
