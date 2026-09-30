"""特征快照搬运模块的错误类型与 HTTP 状态码映射。

所有错误都带稳定的 ``code``（供调用方程序分支使用），``http_status`` 给出
对应 HTTP 状态；内存级服务（不经过 HTTP 直接调用）时同样使用这些状态约定。
"""
from __future__ import annotations


class SnapshotError(Exception):
    code = "snapshot_error"
    http_status = 400


class NotFound(SnapshotError):
    code = "not_found"
    http_status = 404


class IdempotencyConflict(SnapshotError):
    """同幂等键已存在，但参数（或 schema 标识）不同。"""

    code = "idempotency_conflict"
    http_status = 409

    def __init__(self, message: str, *, existing: dict | None = None):
        super().__init__(message)
        self.existing = existing or {}


class ExportInProgress(SnapshotError):
    """同幂等键正在由另一个持有者（fence 仍有效）生成。"""

    code = "export_in_progress"
    http_status = 409


class FenceConflict(SnapshotError):
    """提交使用的 fencing token 已失效：租约被别人接管，迟到提交必须拒绝。"""

    code = "fence_conflict"
    http_status = 409


class ChunkCorrupt(SnapshotError):
    """块内容哈希与 manifest 固定块序中的期望哈希不一致。"""

    code = "chunk_corrupt"
    http_status = 422


class MissingChunks(SnapshotError):
    code = "missing_chunks"
    http_status = 422


class ChunkCountMismatch(SnapshotError):
    code = "chunk_count_mismatch"
    http_status = 422


class SchemaMismatch(SnapshotError):
    code = "schema_mismatch"
    http_status = 422


class TransferFailed(SnapshotError):
    code = "transfer_failed"
    http_status = 409


class TransferCompleted(SnapshotError):
    code = "transfer_completed"
    http_status = 409


class BuildInterrupted(SnapshotError):
    """生成阶段被故障注入中断（演示“中断后不能发布半成品”）。"""

    code = "build_interrupted"
    http_status = 500
