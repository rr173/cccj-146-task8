"""导出端服务：物化结果 -> 不可变 bundle。

幂等与并发规则（与订正台账同款语义）：

* 同幂等键 + 同参数（params_hash 相同）且已发布 -> 返回原 bundle；
* 同幂等键 + 同参数但仍在生成、且持有者 fence 未过期 -> 409 export_in_progress
  （另一个执行者正在做同一件事，调用方应稍后用同键重查，而不是开工第二份）；
* 同幂等键 + 不同参数 -> 409 idempotency_conflict（无论原 bundle 是否已发布）；
* 仍在生成但 fence 已过期（持有者崩溃/卡死）-> 新执行者凭**新 fencing token**
  接管旧记录，旧持有者醒后发布时 CAS 失败，半成品永不上线；
* 生成过程中任何异常（含故障注入中断）-> 幂等记录回到可重试状态、
  不发布 bundle（“中断后不能发布半成品”）。
"""
from __future__ import annotations

import json
import time
import uuid
from typing import Callable, Optional

from .blobstore import BlobStore
from .chunking import build_chunks
from .codec import params_hash
from .db import SnapshotDB
from .errors import (
    BuildInterrupted,
    ExportInProgress,
    IdempotencyConflict,
    NotFound,
)
from .manifest import build_manifest, verify_manifest


class ExportService:
    def __init__(
        self,
        db: SnapshotDB,
        blobs: BlobStore,
        source,
        clock: Callable[[], float] = time.time,
        fence_ttl_seconds: float = 30.0,
    ):
        self.db = db
        self.blobs = blobs
        self.source = source
        self.clock = clock
        self.fence_ttl = fence_ttl_seconds

    # ------------------------------------------------- 幂等键台账

    def _begin_export(self, key: str, phash: str) -> tuple[str, bool]:
        """返回 (fence_token, reusing_building)。

        整个判定在单条 ``BEGIN IMMEDIATE`` 事务内完成，并发提交被串行化。
        """
        c = self.db.conn()
        now = self.clock()
        c.execute("BEGIN IMMEDIATE")
        try:
            row = c.execute(
                "SELECT * FROM ss_export_keys WHERE idempotency_key=?", (key,)
            ).fetchone()
            if row is None:
                token = f"fence_{uuid.uuid4().hex}"
                c.execute(
                    "INSERT INTO ss_export_keys(idempotency_key, params_hash, "
                    "state, fence_token, fence_expires_at, created_at, updated_at) "
                    "VALUES (?,?, 'building', ?, ?, ?, ?)",
                    (key, phash, token, now + self.fence_ttl, now, now),
                )
                c.execute("COMMIT")
                return token, False

            if row["params_hash"] != phash:
                existing = self._published_bundle(row)
                c.execute("ROLLBACK")
                raise IdempotencyConflict(
                    f"幂等键 {key!r} 已用于参数指纹 {row['params_hash']}，"
                    f"本次为 {phash}，拒绝复用",
                    existing=existing,
                )

            if row["state"] == "published":
                c.execute("ROLLBACK")
                # 已发布：bundle 一定存在，直接复用原结果（幂等重放）。
                return "", False

            # building：同参数
            if row["fence_expires_at"] and row["fence_expires_at"] > now:
                c.execute("ROLLBACK")
                raise ExportInProgress(
                    f"幂等键 {key!r} 正在由另一个执行者生成 "
                    f"(fence 过期于 {row['fence_expires_at']:.0f})"
                )

            # fence 已过期：接管，换发单调的新 fencing token
            token = f"fence_{uuid.uuid4().hex}"
            c.execute(
                "UPDATE ss_export_keys SET fence_token=?, fence_expires_at=?, "
                "updated_at=? WHERE idempotency_key=? AND state='building'",
                (token, now + self.fence_ttl, now, key),
            )
            c.execute("COMMIT")
            return token, True
        except BaseException:
            if c.in_transaction:
                c.execute("ROLLBACK")
            raise

    def _published_bundle(self, key_row) -> dict:
        if key_row["bundle_id"]:
            row = self.db.conn().execute(
                "SELECT manifest_json FROM ss_bundles WHERE bundle_id=?",
                (key_row["bundle_id"],),
            ).fetchone()
            if row:
                return json.loads(row["manifest_json"])
        return {}

    def _release_building(self, key: str, fence: str) -> None:
        """生成失败：清掉 building 记录，同键可以立即重试（不会留下半成品）。"""
        c = self.db.conn()
        c.execute("BEGIN IMMEDIATE")
        c.execute(
            "DELETE FROM ss_export_keys WHERE idempotency_key=? "
            "AND state='building' AND fence_token=?",
            (key, fence),
        )
        c.execute("COMMIT")

    # ------------------------------------------------- 导出

    def export(
        self,
        *,
        idempotency_key: str,
        feature: str,
        version: int,
        cutoff_ts: int,
        rows_per_chunk: int = 1000,
        params: Optional[dict] = None,
        fail_after_chunks: Optional[int] = None,
    ) -> dict:
        params = dict(params or {})
        params.update({"feature": feature, "version": version,
                       "cutoff_ts": cutoff_ts})
        phash = params_hash(params)
        fence, _reused = self._begin_export(idempotency_key, phash)

        if not fence:  # 已发布：幂等重放
            row = self.db.conn().execute(
                "SELECT b.manifest_json FROM ss_export_keys k "
                "JOIN ss_bundles b ON b.bundle_id=k.bundle_id "
                "WHERE k.idempotency_key=?",
                (idempotency_key,),
            ).fetchone()
            if row is None:
                # 理论不可达（published 必有 bundle），防御性处理
                raise NotFound("幂等记录指向的 bundle 不存在")
            return {"replayed": True, "manifest": json.loads(row["manifest_json"])}

        try:
            mat = self.source.materialize(feature, version, cutoff_ts)
            chunks = build_chunks(mat.rows, rows_per_chunk)

            # 块先内容寻址落盘（已是不可变对象，但此时 bundle 尚未发布，
            # 对任何读取 API 都不可见）。
            public_chunks = []
            for ch in chunks:
                if fail_after_chunks is not None and \
                        ch["index"] >= fail_after_chunks:
                    raise BuildInterrupted(
                        f"故障注入：已生成 {ch['index']} 块后中断"
                    )
                self.blobs.put_object(ch.pop("_data"), ch["sha256"])
                public_chunks.append(ch)

            manifest = build_manifest(
                feature=mat.feature, version=mat.version,
                columns=mat.columns, cutoff_ts=mat.cutoff_ts,
                params=params, chunks=public_chunks,
                row_count=mat.row_count(), clock=lambda: int(self.clock()),
            )
            verify_manifest(manifest)
            bid = manifest["manifest_hash"]
            self.blobs.write_bundle_manifest(bid, manifest)

            return self._publish(idempotency_key, fence, bid, manifest)
        except BaseException:
            # 生成中断：绝不发布；记录回到可重试状态。
            self._release_building(idempotency_key, fence)
            raise

    def _publish(self, key: str, fence: str, bid: str,
                 manifest: dict) -> dict:
        """持当前 fence 的 CAS 发布；接管发生后旧持有者在这里输掉。"""
        c = self.db.conn()
        now = self.clock()
        c.execute("BEGIN IMMEDIATE")
        try:
            # 先占 bundle 行（不同幂等键导出相同内容：内容相同直接复用）
            exists = c.execute(
                "SELECT 1 FROM ss_bundles WHERE bundle_id=?", (bid,)
            ).fetchone()
            if not exists:
                c.execute(
                    "INSERT INTO ss_bundles(bundle_id, feature, version, "
                    "schema_id, cutoff_ts, manifest_json, created_at) "
                    "VALUES (?,?,?,?,?,?,?)",
                    (bid, manifest["feature"], manifest["version"],
                     manifest["schema_id"], manifest["cutoff_ts"],
                     json.dumps(manifest, ensure_ascii=False, sort_keys=True),
                     now),
                )
            # CAS：只有仍持有当前 fence 的 building 记录能翻成 published
            cur = c.execute(
                "UPDATE ss_export_keys SET state='published', bundle_id=?, "
                "fence_token=NULL, fence_expires_at=NULL, updated_at=? "
                "WHERE idempotency_key=? AND state='building' AND fence_token=?",
                (bid, now, key, fence),
            )
            if cur.rowcount != 1:
                c.execute("ROLLBACK")
                # 被别人接管或已被失败清理；本次生成的字节只是未引用对象，不构成 bundle
                raise IdempotencyConflict(
                    f"fencing token 已失效，bundle {bid} 未发布（可能已被接管）"
                )
            c.execute("COMMIT")
        except BaseException:
            if c.in_transaction:
                c.execute("ROLLBACK")
            raise
        return {"replayed": False, "manifest": manifest}

    # ------------------------------------------------- 读取

    def get_bundle(self, bundle_id: str) -> dict:
        row = self.db.conn().execute(
            "SELECT manifest_json FROM ss_bundles WHERE bundle_id=?",
            (bundle_id,),
        ).fetchone()
        if row is None:
            raise NotFound(f"bundle {bundle_id} 不存在或尚未发布")
        return json.loads(row["manifest_json"])

    def list_bundles(self) -> list[dict]:
        rows = self.db.conn().execute(
            "SELECT bundle_id, feature, version, schema_id, cutoff_ts, "
            "created_at FROM ss_bundles ORDER BY created_at"
        ).fetchall()
        return [dict(r) for r in rows]

    def get_export_status(self, key: str) -> dict:
        row = self.db.conn().execute(
            "SELECT * FROM ss_export_keys WHERE idempotency_key=?", (key,)
        ).fetchone()
        if row is None:
            raise NotFound(f"幂等键 {key!r} 无记录")
        d = dict(row)
        d.pop("fence_token", None)
        return d

    def get_chunk(self, bundle_id: str, index: int) -> tuple[bytes, str]:
        manifest = self.get_bundle(bundle_id)
        if not 0 <= index < len(manifest["chunks"]):
            raise NotFound(f"bundle {bundle_id} 没有块 {index}")
        ch = manifest["chunks"][index]
        return self.blobs.get_object(ch["sha256"]), ch["sha256"]
