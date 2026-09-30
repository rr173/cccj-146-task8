"""独立特征快照搬运模块（FeatForge Snapshot Transport）。

把某个特征版本在给定截止时刻的物化结果导出为**不可变 bundle**（内容寻址
manifest + 固定块序 + schema 标识），由另一实例**分块拉取、校验、原子启用**。

导出端：幂等键台账（同键同参返回原 bundle；同键异参 409；fencing 防中断
半成品发布）；导入端：持久检查点 + 带 fencing token 的块租约（多执行器续传、
过期接管、迟到提交拒绝），全部块哈希/总数/schema 通过后一次性切换 active。

零第三方依赖（SQLite + 标准库 HTTP），既可作为库嵌入，也可以两个独立
服务进程跨实例运行::

    python -m featforge.snapshot exporter --root /data/exporter --port 8101 --seed
    python -m featforge.snapshot importer --root /data/importer --port 8102
    python -m featforge.snapshot worker   --exporter http://localhost:8101 \\
        --importer http://localhost:8102 --transfer imp_xxx
    python scripts/snapshot_demo.py
"""
from __future__ import annotations

from .blobstore import BlobStore
from .client import SnapshotClient, SnapshotHTTPError
from .codec import canonical_json, params_hash, schema_id, sha256_hex
from .db import SnapshotDB
from .errors import (
    BuildInterrupted,
    ChunkCorrupt,
    FenceConflict,
    IdempotencyConflict,
    MissingChunks,
    NotFound,
    SchemaMismatch,
)
from .executor import ImportExecutor
from .exporter import ExportService
from .importer import ImportService
from .manifest import build_manifest, bundle_id, verify_manifest
from .source import DemoFeatureStore, FeatForgeOfflineSource, Materialized

__all__ = [
    "BlobStore", "SnapshotDB",
    "ExportService", "ImportService", "ImportExecutor",
    "SnapshotClient", "SnapshotHTTPError",
    "DemoFeatureStore", "FeatForgeOfflineSource", "Materialized",
    "build_manifest", "bundle_id", "verify_manifest",
    "canonical_json", "params_hash", "schema_id", "sha256_hex",
    "BuildInterrupted", "ChunkCorrupt", "FenceConflict",
    "IdempotencyConflict", "MissingChunks", "NotFound", "SchemaMismatch",
]
