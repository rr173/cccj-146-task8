"""导入端服务：分块拉取、租约/检查点续传、整体校验、原子启用。

块租约（支持多执行器续传）
--------------------------
* 每块同一时刻最多一个有效持有者：``acquire`` 在 ``BEGIN IMMEDIATE``
  事务里挑选 pending 或过期 leased 块，写回持有者与过期时刻；
* 租约 TTL 到期后任意执行器可接管，fencing token（``fence_seq``）单调
  **+1**——“同一块只能有一个有效持有者”，且新旧持有者可被严格排序；
* 旧持有者醒来迟到提交时携带旧 token，条件 UPDATE 命不中 -> 409 fence_conflict，
  迟到块字节被丢弃。

检查点
------
每块状态与 fence 都在 SQLite 持久化（pending/leased/committed + 计数）。
执行器甚至导入服务进程崩溃重启后，已 committed 的块不会重传，未完成/租约
过期的块继续被领取。

激活
----
所有块到齐后（或显式调用激活）：重验 manifest、逐块重算哈希、核对块总数/行数、
核对 schema 标识；全部通过才在一个 ``BEGIN IMMEDIATE`` 事务里把 active 整行
替换为新 bundle。任一项失败 -> transfer=failed，active 指针不动，原 bundle
继续服务（缺块/损坏/不兼容都不会被读者看到）。
"""
from __future__ import annotations

import json
import time
from typing import Callable, Optional

from .atomicfs import rm_quiet
from .blobstore import BlobStore
from .codec import sha256_hex
from .db import SnapshotDB
from .errors import (
    ChunkCorrupt,
    FenceConflict,
    MissingChunks,
    NotFound,
    TransferCompleted,
    TransferFailed,
)
from .manifest import check_schema_compatible, verify_manifest


class HttpFetchError(Exception):
    def __init__(self, status: int, body: bytes):
        self.status = status
        self.body = body
        super().__init__(f"源端返回 HTTP {status}")


def _rollback(c) -> None:
    if c.in_transaction:
        c.execute("ROLLBACK")


def import_id(source_bundle_id: str) -> str:
    """搬运会话幂等：同一 bundle 的重复创建返回同一会话。"""
    return f"imp_{source_bundle_id[:24]}"


class ImportService:
    def __init__(
        self,
        db: SnapshotDB,
        blobs: BlobStore,
        fetcher,
        clock: Callable[[], float] = time.time,
        lease_ttl_seconds: float = 15.0,
        external_base_url: str = "",
    ):
        self.db = db
        self.blobs = blobs
        self.fetcher = fetcher          # (base_url, bundle_id) -> manifest dict
        self.clock = clock
        self.lease_ttl = lease_ttl_seconds
        self.external_base_url = external_base_url.rstrip("/")
        # 仅演示/测试用：激活前对暂存区动手脚，验证三道闸门（缺块/损坏/篡改）
        self._sabotage: dict[str, str] = {}

    # ------------------------------------------------- 创建搬运会话

    def create_transfer(
        self,
        *,
        bundle_url: str,
        expected_schema_id: Optional[str] = None,
        lease_ttl_seconds: Optional[float] = None,
        auto_activate: bool = True,
        idempotency_key: Optional[str] = None,
    ) -> dict:
        # bundle_url 形如 http://host/api/snapshot/export/bundles/<id>
        base_url, bid = bundle_url.rsplit("/bundles/", 1)
        base_url = base_url
        tid = idempotency_key or import_id(bid)
        ttl = lease_ttl_seconds if lease_ttl_seconds is not None else self.lease_ttl

        c = self.db.conn()
        row = c.execute(
            "SELECT * FROM ss_transfers WHERE transfer_id=?", (tid,)
        ).fetchone()
        if row is not None:
            # 幂等：同一会话重复创建直接返回当前状态
            return self.get_transfer(tid, replayed=True)

        manifest = self.fetcher(base_url, bid)
        verify_manifest(manifest)
        if manifest["manifest_hash"] != bid:
            raise ChunkCorrupt(
                f"URL 中的 bundle id {bid} 与 manifest 自哈希 "
                f"{manifest['manifest_hash']} 不一致"
            )

        now = self.clock()
        c.execute("BEGIN IMMEDIATE")
        try:
            c.execute(
                "INSERT INTO ss_transfers(transfer_id, source_bundle_id, feature, "
                "version, schema_id, expected_schema_id, source_base_url, state, "
                "chunks_total, chunks_committed, auto_activate, manifest_json, "
                "created_at, updated_at) "
                "VALUES (?,?,?,?,?,?,?, 'receiving', ?, 0, ?, ?, ?, ?)",
                (tid, bid, manifest["feature"], manifest["version"],
                 manifest["schema_id"], expected_schema_id, base_url,
                 len(manifest["chunks"]),
                 1 if auto_activate else 0,
                 json.dumps(manifest, ensure_ascii=False, sort_keys=True),
                 now, now),
            )
            c.executemany(
                "INSERT INTO ss_chunks(transfer_id, chunk_index, state, "
                "fence_seq, updated_at) VALUES (?,?,'pending',0,?)",
                [(tid, i, now) for i in range(len(manifest["chunks"]))],
            )
            c.execute("COMMIT")
        except BaseException:
            _rollback(c)
            raise
        return self.get_transfer(tid)

    # ------------------------------------------------- 会话状态

    def get_manifest(self, tid: str) -> dict:
        t = self._transfer_row(tid)
        return json.loads(t["manifest_json"])

    def _transfer_row(self, tid: str):
        row = self.db.conn().execute(
            "SELECT * FROM ss_transfers WHERE transfer_id=?", (tid,)
        ).fetchone()
        if row is None:
            raise NotFound(f"搬运会话 {tid} 不存在")
        return row

    def get_transfer(self, tid: str, replayed: bool = False) -> dict:
        row = self._transfer_row(tid)
        d = dict(row)
        d["replayed"] = replayed
        d["manifest"] = json.loads(d.pop("manifest_json"))
        d.pop("source_base_url", None)
        return d

    def list_transfers(self) -> list[dict]:
        rows = self.db.conn().execute(
            "SELECT transfer_id, source_bundle_id, feature, version, schema_id, "
            "state, chunks_total, chunks_committed, error_code, created_at "
            "FROM ss_transfers ORDER BY created_at"
        ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------- 块租约

    def acquire_chunk(self, tid: str, executor_id: str) -> Optional[dict]:
        """领取下一个可做的块；无块可做（全部 committed）时返回 None。"""
        now = self.clock()
        c = self.db.conn()
        c.execute("BEGIN IMMEDIATE")
        try:
            t = c.execute(
                "SELECT state FROM ss_transfers WHERE transfer_id=?", (tid,)
            ).fetchone()
            if t is None:
                c.execute("ROLLBACK")
                raise NotFound(f"搬运会话 {tid} 不存在")
            if t["state"] in ("active", "failed"):
                c.execute("ROLLBACK")
                return None
            # 先接管最早过期的失联租约（同一块只有一个有效持有者，过期即失效），
            # 再领新的 pending 块；fence_seq 越大代表持有者越新。
            row = c.execute(
                "SELECT * FROM ss_chunks WHERE transfer_id=? AND ("
                "state='pending' OR (state='leased' AND lease_expires_at<=?)) "
                "ORDER BY CASE state WHEN 'leased' THEN 0 ELSE 1 END, "
                "lease_expires_at, chunk_index LIMIT 1",
                (tid, now),
            ).fetchone()
            if row is None:
                c.execute("ROLLBACK")
                return None
            new_seq = row["fence_seq"] + 1   # 接管时 fencing token 单调 +1
            c.execute(
                "UPDATE ss_chunks SET state='leased', fence_seq=?, "
                "lease_holder=?, lease_expires_at=?, updated_at=? "
                "WHERE transfer_id=? AND chunk_index=?",
                (new_seq, executor_id, now + self.lease_ttl, now,
                 tid, row["chunk_index"]),
            )
            c.execute("COMMIT")
        except BaseException:
            _rollback(c)
            raise

        manifest = self.get_manifest(tid)
        ch = manifest["chunks"][row["chunk_index"]]
        base = self.external_base_url
        return {
            "transfer_id": tid,
            "chunk_index": row["chunk_index"],
            "fence_token": str(new_seq),
            "lease_expires_at": now + self.lease_ttl,
            "expected_sha256": ch["sha256"],
            "size_bytes": ch["size_bytes"],
            "chunk_url": (f"{base}/imports/{tid}/chunks/{row['chunk_index']}"
                          if base else None),
            "source_chunk_url": f"{self._transfer_row(tid)['source_base_url']}"
                                f"/bundles/{manifest['manifest_hash']}"
                                f"/chunks/{row['chunk_index']}",
        }

    def renew_lease(self, tid: str, index: int, fence_token: str,
                    executor_id: str) -> dict:
        now = self.clock()
        c = self.db.conn()
        c.execute("BEGIN IMMEDIATE")
        cur = c.execute(
            "UPDATE ss_chunks SET lease_expires_at=?, updated_at=? "
            "WHERE transfer_id=? AND chunk_index=? AND state='leased' "
            "AND fence_seq=? AND lease_holder=?",
            (now + self.lease_ttl, now, tid, index, int(fence_token),
             executor_id),
        )
        c.execute("COMMIT")
        if cur.rowcount != 1:
            raise FenceConflict("续租失败：fencing token 已失效或持有者已变更")
        return {"lease_expires_at": now + self.lease_ttl}

    # ------------------------------------------------- 块提交

    def put_chunk(self, tid: str, index: int, fence_token: str,
                  data: bytes) -> dict:
        """提交块字节：先验 fence（迟到提交拒绝），再验哈希（坏块拒绝）。"""
        t = self._transfer_row(tid)
        if t["state"] == "active":
            raise TransferCompleted("该 bundle 已激活，无需重复提交")
        if t["state"] == "failed":
            raise TransferFailed("搬运会话已失败")

        manifest = self.get_manifest(tid)
        if not 0 <= index < len(manifest["chunks"]):
            raise NotFound(f"块序号 {index} 越界")
        ch = manifest["chunks"][index]
        digest = sha256_hex(data)

        c = self.db.conn()
        c.execute("BEGIN IMMEDIATE")
        try:
            row = c.execute(
                "SELECT * FROM ss_chunks WHERE transfer_id=? AND chunk_index=?",
                (tid, index),
            ).fetchone()
            if row["state"] == "committed":
                # 已提交块只有**最后一个有效持有者**（当前 fence_seq）的重传
                # 才算幂等；旧世代持有者的迟到提交 token 更小，照样拒绝。
                if str(row["fence_seq"]) != fence_token:
                    c.execute("ROLLBACK")
                    raise FenceConflict(
                        f"块 {index} 已由更新的持有者提交（当前 "
                        f"seq={row['fence_seq']}），token {fence_token} "
                        "的迟到提交被拒绝"
                    )
                c.execute("ROLLBACK")
                accepted = digest == ch["sha256"]
                return {"committed": True, "already": True,
                        "fence_conflict": not accepted}
            # 未提交：必须持有当前租约
            if row["state"] != "leased" \
                    or str(row["fence_seq"]) != fence_token:
                c.execute("ROLLBACK")
                raise FenceConflict(
                    f"块 {index} 的 fencing token {fence_token} 无效"
                    f"（state={row['state']}，当前 seq={row['fence_seq']}，"
                    f"持有者={row['lease_holder']}）；提交被拒绝"
                )
            if row["lease_expires_at"] is not None \
                    and row["lease_expires_at"] <= self.clock():
                c.execute("ROLLBACK")
                # 租约已过期：块随时会被（或已被）新执行者接管，
                # 旧持有者即使 token 数字没变也不能提交——fence 含有效期。
                raise FenceConflict(
                    f"块 {index} 的租约已于 {row['lease_expires_at']:.0f} "
                    f"过期；旧持有者 {row['lease_holder']} 的迟到提交被拒绝"
                )
            if digest != ch["sha256"]:
                c.execute("ROLLBACK")
                raise ChunkCorrupt(
                    f"块 {index} 哈希不匹配：期望 {ch['sha256'][:16]}，"
                    f"实际 {digest[:16]}"
                )
            # 内容寻址暂存：同名即同内容，迟到/重复写不会互相污染
            self.blobs.put_import_chunk(tid, data, digest)
            c.execute(
                "UPDATE ss_chunks SET state='committed', lease_holder=NULL, "
                "lease_expires_at=NULL, updated_at=? "
                "WHERE transfer_id=? AND chunk_index=? AND fence_seq=?",
                (self.clock(), tid, index, int(fence_token)),
            )
            c.execute(
                "UPDATE ss_transfers SET chunks_committed=("
                "SELECT COUNT(*) FROM ss_chunks WHERE transfer_id=? "
                "AND state='committed'), updated_at=? WHERE transfer_id=?",
                (tid, self.clock(), tid),
            )
            c.execute("COMMIT")
        except BaseException:
            _rollback(c)
            raise

        t = self._transfer_row(tid)
        out = {"committed": True, "already": False,
               "chunks_committed": t["chunks_committed"],
               "chunks_total": t["chunks_total"]}
        if (t["auto_activate"] and t["state"] == "receiving"
                and t["chunks_committed"] == t["chunks_total"]):
            # 最后一块的提交者同步触发整体校验 + 原子启用
            act = self.activate(tid)
            out["activated"] = True
            out["active_bundle_id"] = act["active_bundle_id"]
        return out

    # ------------------------------------------------- 整体校验 + 启用

    def _apply_sabotage(self, tid: str, manifest: dict) -> Optional[str]:
        mode = self._sabotage.pop(tid, None)
        if mode is None:
            return None
        if mode == "delete_chunk":
            victim = manifest["chunks"][-1]["sha256"]
            self.blobs.remove_import_chunk(tid, victim)
            return "暂存区最后一块被外部删除（模拟缺块）"
        if mode == "flip_chunk":
            victim = manifest["chunks"][0]
            p = self.blobs.import_chunk_path(tid, victim["sha256"])
            data = bytearray(p.read_bytes())
            data[0] ^= 0xFF
            p.write_bytes(bytes(data))
            return "暂存区首块字节被翻转（激活时重哈希必失败）"
        if mode == "fake_schema":
            # 只改内存中 manifest 的 schema_id（模拟磁盘 manifest 被篡改/不兼容）
            manifest["schema_id"] = "sch_tampered"
            return "manifest schema_id 被篡改（与列定义/期望不符）"
        return None

    def activate(self, tid: str) -> dict:
        """整体校验通过后一次性切换 active；任一失败：failed + 旧 bundle 继续服务。"""
        c = self.db.conn()
        c.execute("BEGIN IMMEDIATE")
        t = c.execute(
            "SELECT * FROM ss_transfers WHERE transfer_id=?", (tid,)
        ).fetchone()
        if t is None:
            c.execute("ROLLBACK")
            raise NotFound(f"搬运会话 {tid} 不存在")
        if t["state"] == "active":
            c.execute("ROLLBACK")
            return {"already_active": True,
                    "active_bundle_id": t["source_bundle_id"]}
        if t["state"] == "failed":
            c.execute("ROLLBACK")
            raise TransferFailed("搬运会话已失败，不能激活")
        # receiving 或 activating（上次在激活窗口内崩溃）都可以进入/重跑：
        # 所有校验与物化均幂等，重跑只会重新过一遍闸门。
        c.execute(
            "UPDATE ss_transfers SET state='activating', updated_at=? "
            "WHERE transfer_id=? AND state IN ('receiving','activating')",
            (self.clock(), tid),
        )
        c.execute("COMMIT")

        manifest = self.get_manifest(tid)
        try:
            sabotage_msg = self._apply_sabotage(tid, manifest)

            # 闸门 1：块总数 / 固定块序 / 行数合计 / manifest 自哈希 / schema 自洽
            verify_manifest(manifest)

            committed = c.execute(
                "SELECT chunk_index FROM ss_chunks WHERE transfer_id=? "
                "AND state='committed' ORDER BY chunk_index", (tid,)
            ).fetchall()
            have = {r["chunk_index"] for r in committed}
            want = set(range(len(manifest["chunks"])))
            if have != want:
                missing = sorted(want - have)
                raise MissingChunks(f"缺少 {len(missing)} 个块: {missing[:5]}")

            # 闸门 2：逐块重算字节哈希（不只信提交时的检查；文件可能事后损坏）
            digest_to_object = {}
            for ch in manifest["chunks"]:
                p = self.blobs.import_chunk_path(tid, ch["sha256"])
                if not p.exists():
                    raise MissingChunks(
                        f"块 {ch['index']} 暂存文件缺失: {ch['sha256'][:16]}"
                    )
                data = p.read_bytes()
                if sha256_hex(data) != ch["sha256"]:
                    raise ChunkCorrupt(
                        f"块 {ch['index']} 激活时重哈希失败"
                    )
                if len(data) != ch["size_bytes"]:
                    raise ChunkCorrupt(
                        f"块 {ch['index']} 大小 {len(data)} 与 manifest "
                        f"{ch['size_bytes']} 不一致"
                    )
                digest_to_object[ch["sha256"]] = p

            # 闸门 3：schema 标识（与导入端预期不兼容则拒绝）
            check_schema_compatible(manifest, t["expected_schema_id"])

            # 全部通过：把不可变块组装成自包含 bundle 目录（激活前对读者不可见）
            self.blobs.materialize_bundle(t["source_bundle_id"], manifest,
                                          digest_to_object)

            # 一次性切换：active 整行 upsert 是读者唯一引用点
            now = self.clock()
            c.execute("BEGIN IMMEDIATE")
            c.execute(
                "INSERT INTO ss_active(feature, bundle_id, schema_id, version, "
                "manifest_json, activated_at) VALUES (?,?,?,?,?,?) "
                "ON CONFLICT(feature) DO UPDATE SET bundle_id=excluded.bundle_id, "
                "schema_id=excluded.schema_id, version=excluded.version, "
                "manifest_json=excluded.manifest_json, "
                "activated_at=excluded.activated_at",
                (manifest["feature"], t["source_bundle_id"],
                 manifest["schema_id"], manifest["version"],
                 json.dumps(manifest, ensure_ascii=False, sort_keys=True), now),
            )
            c.execute(
                "UPDATE ss_transfers SET state='active', error_code=NULL, "
                "error=NULL, updated_at=? WHERE transfer_id=?", (now, tid),
            )
            c.execute("COMMIT")
        except Exception as e:
            # 激活失败：指针不动，旧 bundle 继续服务
            from .errors import SnapshotError
            code = e.code if isinstance(e, SnapshotError) else "activation_error"
            c.execute("BEGIN IMMEDIATE")
            c.execute(
                "UPDATE ss_transfers SET state='failed', error_code=?, "
                "error=?, updated_at=? WHERE transfer_id=?",
                (code, str(e), self.clock(), tid),
            )
            c.execute("COMMIT")
            rm_quiet(self.blobs.materialized_path(t["source_bundle_id"]))
            raise

        return {"active_bundle_id": t["source_bundle_id"],
                "feature": manifest["feature"]}

    # ------------------------------------------------- 调试/演示注入

    def prime_sabotage(self, tid: str, mode: str) -> None:
        self._transfer_row(tid)  # 存在性检查
        if mode not in ("delete_chunk", "flip_chunk", "fake_schema"):
            raise ValueError("未知破坏模式")
        self._sabotage[tid] = mode

    # ------------------------------------------------- 读取

    def active_manifest(self, feature: str) -> dict:
        row = self.db.conn().execute(
            "SELECT manifest_json FROM ss_active WHERE feature=?", (feature,)
        ).fetchone()
        if row is None:
            raise NotFound(f"特征 {feature} 尚无 active bundle")
        return json.loads(row["manifest_json"])

    def active_overview(self) -> list[dict]:
        rows = self.db.conn().execute(
            "SELECT feature, bundle_id, schema_id, version, activated_at "
            "FROM ss_active ORDER BY feature"
        ).fetchall()
        return [dict(r) for r in rows]

    def read_rows(self, feature: str, entity_id: Optional[str] = None,
                  bundle_id: Optional[str] = None,
                  limit: int = 1000) -> dict:
        """从 active（或指定历史 bundle）的物化块中按 JSONL 顺序读出。

        读取直接落在不可变块文件上；激活切换对读者是一次整行指针替换——
        切换前打开的请求读旧 bundle，切换后的请求读新 bundle，互不混杂。
        """
        if bundle_id is not None:
            # 已激活过的历史 bundle 的物化目录永久保留（旧 bundle 可继续读）
            path = self.blobs.materialized_path(bundle_id) / "manifest.json"
            if not path.exists():
                raise NotFound(f"本地未物化 bundle {bundle_id}")
            manifest = json.loads(path.read_bytes())
        else:
            manifest = self.active_manifest(feature)
        out = []
        total = 0
        for ch in manifest["chunks"]:
            p = (self.blobs.materialized_path(manifest["manifest_hash"])
                 / "chunks" / str(ch["index"]))
            for line in p.read_bytes().splitlines():
                total += 1
                row = json.loads(line)
                if entity_id is None or row["entity_id"] == entity_id:
                    if len(out) < limit:
                        out.append(row)
        return {"bundle_id": manifest["manifest_hash"],
                "schema_id": manifest["schema_id"],
                "cutoff_ts": manifest["cutoff_ts"],
                "total_rows": total,
                "rows": out}
