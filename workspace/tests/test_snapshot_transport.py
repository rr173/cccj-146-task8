"""特征快照搬运模块测试：内容寻址/幂等/租约/激活/HTTP/读者隔离。

同时兼容 pytest 与 ``python -m unittest``（本机无第三方依赖时也能跑）：
    python -m unittest tests.test_snapshot_transport -v
    python -m pytest tests/test_snapshot_transport.py
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from featforge.snapshot import (  # noqa: E402
    BlobStore,
    BuildInterrupted,
    ChunkCorrupt,
    DemoFeatureStore,
    ExportService,
    FenceConflict,
    IdempotencyConflict,
    ImportExecutor,
    ImportService,
    MissingChunks,
    NotFound,
    SnapshotClient,
    SnapshotDB,
    SnapshotHTTPError,
    SchemaMismatch,
)
from featforge.snapshot.demodata import seed_demo  # noqa: E402
from featforge.snapshot.httpapi import run_server  # noqa: E402
from featforge.snapshot.manifest import verify_manifest  # noqa: E402


class FakeClock:
    def __init__(self, start: float = 1000.0):
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


class Services:
    """导出端 + 导入端（同进程，可控时钟）。"""

    def __init__(self, tmpdir: str, lease_ttl: float = 10.0,
                 fence_ttl: float = 30.0, entities: int = 8,
                 rows_per_chunk: int = 5):
        self.rows_per_chunk = rows_per_chunk
        self.fstore = DemoFeatureStore(f"{tmpdir}/features.db")
        seed_demo(self.fstore, entities=entities)
        self.clk = FakeClock()
        self.exporter = ExportService(
            SnapshotDB(f"{tmpdir}/exporter.db"),
            BlobStore(f"{tmpdir}/eblobs"), self.fstore,
            clock=self.clk, fence_ttl_seconds=fence_ttl)
        self.importer = ImportService(
            SnapshotDB(f"{tmpdir}/importer.db"),
            BlobStore(f"{tmpdir}/iblobs"),
            self._fetch, clock=self.clk, lease_ttl_seconds=lease_ttl)
        self.bids = {}

    def _fetch(self, base_url: str, bid: str) -> dict:
        return self.exporter.get_bundle(bid)

    def export(self, key="k1", version=1, cutoff=1_000_000, params=None, **kw):
        kw.setdefault("rows_per_chunk", self.rows_per_chunk)
        r = self.exporter.export(
            idempotency_key=key, feature="gmv_1h", version=version,
            cutoff_ts=cutoff, params={"job": key} if params is None else params,
            **kw)
        self.bids[key] = r["manifest"]["manifest_hash"]
        return r

    def start_transfer(self, key="k1", **kw):
        bid = self.bids[key]
        kw.setdefault("idempotency_key", f"tr-{key}")
        return self.importer.create_transfer(
            bundle_url=f"http://x/bundles/{bid}", **kw)

    def complete(self, tid: str, bid: str, executor="w"):
        n = len(self.exporter.get_bundle(bid)["chunks"])
        last = {}
        for i in range(n):
            lease = self.importer.acquire_chunk(tid, executor)
            assert lease["chunk_index"] == i
            data, _ = self.exporter.get_chunk(bid, i)
            last = self.importer.put_chunk(tid, i, lease["fence_token"], data)
        return last


class SnapshotTransportTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.mkdtemp(prefix="ss-test-")
        self.svc = Services(self.tmp)

    # ------------------------------------------------- manifest / 编码

    def test_manifest_is_content_addressed_and_deterministic(self):
        r1 = self.svc.export("a", params={"job": "same"})
        r2 = self.svc.export("b", params={"job": "same"})
        self.assertEqual(r1["manifest"]["manifest_hash"],
                         r2["manifest"]["manifest_hash"])
        verify_manifest(r1["manifest"])
        # 篡改 manifest 任何一处，自哈希校验必失败
        m = json.loads(json.dumps(r1["manifest"]))
        m["row_count"] += 1
        with self.assertRaises(Exception):
            verify_manifest(m)
        m2 = json.loads(json.dumps(r1["manifest"]))
        m2["chunks"][0]["sha256"] = "0" * 64
        # 块序/行数未变，但 manifest_hash 仍须检出篡改
        with self.assertRaises(Exception):
            verify_manifest(m2)

    def test_cutoff_is_honored_and_chunk_order_fixed(self):
        # 截止 100150：只应包含 asof_ts <= cutoff 的行（100000..100700 共 8 行）
        self.svc.fstore  # seeded entities=8, v1 rows at 100000+i*100
        mat = self.svc.fstore.materialize("gmv_1h", 1, 100_700)
        self.assertEqual(len(mat.rows), 8)
        r = self.svc.exporter.export(
            idempotency_key="cut", feature="gmv_1h", version=1,
            cutoff_ts=100_700, rows_per_chunk=3, params={"job": "cut"})
        idxs = [c["index"] for c in r["manifest"]["chunks"]]
        self.assertEqual(idxs, list(range(len(idxs))))
        self.assertEqual(sum(c["row_count"] for c in r["manifest"]["chunks"]),
                         r["manifest"]["row_count"])

    def test_schema_id_differs_when_columns_change(self):
        r1 = self.svc.export("s1", version=1)
        r2 = self.svc.export("s2", version=2, cutoff=2_000_000)
        self.assertNotEqual(r1["manifest"]["schema_id"],
                            r2["manifest"]["schema_id"])

    # ------------------------------------------------- 导出幂等

    def test_duplicate_export_returns_original_bundle(self):
        r1 = self.svc.export("dup")
        r2 = self.svc.export("dup")
        self.assertTrue(r2["replayed"])
        self.assertEqual(r1["manifest"], r2["manifest"])

    def test_same_key_different_params_conflicts_409(self):
        self.svc.export("k")
        with self.assertRaises(IdempotencyConflict) as cm:
            self.svc.exporter.export(
                idempotency_key="k", feature="gmv_1h", version=1,
                cutoff_ts=1, rows_per_chunk=5, params={"job": "different"})
        self.assertEqual(cm.exception.http_status, 409)

    def test_interrupted_export_publishes_nothing_and_is_retryable(self):
        before = {b["bundle_id"] for b in self.svc.exporter.list_bundles()}
        with self.assertRaises(BuildInterrupted):
            self.svc.export("boom", fail_after_chunks=2)
        # 没有半成品发布；幂等台账不留 building 残迹
        after = {b["bundle_id"] for b in self.svc.exporter.list_bundles()}
        self.assertEqual(before, after)
        with self.assertRaises(NotFound):
            self.svc.exporter.get_export_status("boom")
        # 立即重试成功，且因为数据/参数一致，内容寻址得到同一 bundle
        r1 = self.svc.export("good1", params={"job": "boom-retry"})
        r_retry = self.svc.export("boom", params={"job": "boom-retry"})
        self.assertEqual(r_retry["manifest"]["manifest_hash"],
                         r1["manifest"]["manifest_hash"])

    def test_concurrent_building_with_valid_fence_conflicts(self):
        from featforge.snapshot.codec import params_hash
        from featforge.snapshot.errors import ExportInProgress
        phash = params_hash({"job": "busy", "feature": "gmv_1h",
                             "version": 1, "cutoff_ts": 1_000_000})
        c = self.svc.exporter.db.conn()
        c.execute("BEGIN IMMEDIATE")
        c.execute(
            "INSERT INTO ss_export_keys(idempotency_key, params_hash, state, "
            "fence_token, fence_expires_at, created_at, updated_at) "
            "VALUES (?,?, 'building', ?, ?, ?, ?)",
            ("busy", phash, "fence_old", self.svc.clk() + 100,
             self.svc.clk(), self.svc.clk()))
        c.execute("COMMIT")
        with self.assertRaises(ExportInProgress):
            self.svc.exporter.export(
                idempotency_key="busy", feature="gmv_1h", version=1,
                cutoff_ts=1_000_000, rows_per_chunk=5,
                params={"job": "busy"})

    def test_expired_build_fence_can_be_taken_over(self):
        from featforge.snapshot.errors import ExportInProgress
        # fence 已过期：新执行者可以开工（接管），不抛 in-progress
        c = self.svc.exporter.db.conn()
        from featforge.snapshot.codec import params_hash
        phash = params_hash({"job": "stale", "feature": "gmv_1h",
                             "version": 1, "cutoff_ts": 1_000_000})
        c.execute("BEGIN IMMEDIATE")
        c.execute(
            "INSERT INTO ss_export_keys(idempotency_key, params_hash, state, "
            "fence_token, fence_expires_at, created_at, updated_at) "
            "VALUES (?,?, 'building', ?, ?, ?, ?)",
            ("stale", phash, "fence_old", self.svc.clk() - 1,
             self.svc.clk(), self.svc.clk()))
        c.execute("COMMIT")
        r = self.svc.exporter.export(
            idempotency_key="stale", feature="gmv_1h", version=1,
            cutoff_ts=1_000_000, rows_per_chunk=5,
            params={"job": "stale"})
        self.assertFalse(r["replayed"])
        st = self.svc.exporter.get_export_status("stale")
        self.assertEqual(st["state"], "published")

    # ------------------------------------------------- 导入租约 / 续传

    def test_same_chunk_single_holder_and_takeover_bumps_fence(self):
        r = self.svc.export("lease")
        bid = r["manifest"]["manifest_hash"]
        tr = self.svc.start_transfer("lease")
        tid = tr["transfer_id"]
        a = self.svc.importer.acquire_chunk(tid, "A")
        b = self.svc.importer.acquire_chunk(tid, "B")
        self.assertNotEqual(a["chunk_index"], b["chunk_index"])
        # TTL 内别人不能接管 A 的块（领取只会跳到下一个 pending）
        again = self.svc.importer.acquire_chunk(tid, "C")
        self.assertNotEqual(again["chunk_index"], a["chunk_index"])
        # 过期后接管同一块，fence 单调 +1
        self.svc.clk.advance(11)
        taken = self.svc.importer.acquire_chunk(tid, "C")
        self.assertEqual(taken["chunk_index"], a["chunk_index"])
        self.assertEqual(int(taken["fence_token"]),
                         int(a["fence_token"]) + 1)

    def test_late_commit_by_old_holder_rejected(self):
        r = self.svc.export("late")
        bid = r["manifest"]["manifest_hash"]
        tid = self.svc.start_transfer("late")["transfer_id"]
        lease = self.svc.importer.acquire_chunk(tid, "old")
        self.svc.clk.advance(11)
        takeover = self.svc.importer.acquire_chunk(tid, "new")
        self.assertEqual(takeover["chunk_index"], lease["chunk_index"])
        data, _ = self.svc.exporter.get_chunk(bid, lease["chunk_index"])
        # 旧 fence + 过期租约：双重失效，提交拒绝
        with self.assertRaises(FenceConflict):
            self.svc.importer.put_chunk(
                tid, lease["chunk_index"], lease["fence_token"], data)
        # 新 fence 正常提交
        out = self.svc.importer.put_chunk(
            tid, takeover["chunk_index"], takeover["fence_token"], data)
        self.assertTrue(out["committed"])
        # 旧 fence 在块已提交后再来一次：依然拒绝（不能借幂等名义）
        with self.assertRaises(FenceConflict):
            self.svc.importer.put_chunk(
                tid, lease["chunk_index"], lease["fence_token"], data)

    def test_corrupt_chunk_rejected_lease_preserved(self):
        r = self.svc.export("corrupt")
        bid = r["manifest"]["manifest_hash"]
        tid = self.svc.start_transfer("corrupt")["transfer_id"]
        lease = self.svc.importer.acquire_chunk(tid, "w")
        with self.assertRaises(ChunkCorrupt):
            self.svc.importer.put_chunk(tid, lease["chunk_index"],
                                        lease["fence_token"], b"garbage\n")
        # 坏提交没有把块置为 committed
        row = self.svc.importer.db.conn().execute(
            "SELECT state FROM ss_chunks WHERE transfer_id=? AND chunk_index=?",
            (tid, lease["chunk_index"])).fetchone()
        self.assertEqual(row["state"], "leased")

    def test_checkpoint_resume_after_executor_disappears(self):
        r = self.svc.export("resume")
        bid = r["manifest"]["manifest_hash"]
        tid = self.svc.start_transfer("resume", auto_activate=False)[
            "transfer_id"]
        # 执行器 A 完成前 3 块
        for i in range(3):
            lease = self.svc.importer.acquire_chunk(tid, "A")
            data, _ = self.svc.exporter.get_chunk(bid, i)
            self.svc.importer.put_chunk(tid, i, lease["fence_token"], data)
        # 全新导入服务实例（同库）：已提交块不重传，过期租约可接管
        svc2 = ImportService(
            SnapshotDB(f"{self.tmp}/importer.db"),
            BlobStore(f"{self.tmp}/iblobs"), self.svc._fetch,
            clock=self.svc.clk, lease_ttl_seconds=10.0)
        tr = svc2.get_transfer(tid)
        self.assertEqual(tr["chunks_committed"], 3)
        n = len(r["manifest"]["chunks"])
        for i in range(3, n):
            lease = svc2.acquire_chunk(tid, "B")
            self.assertEqual(lease["chunk_index"], i)
            data, _ = self.svc.exporter.get_chunk(bid, i)
            svc2.put_chunk(tid, i, lease["fence_token"], data)
        self.assertEqual(svc2.get_transfer(tid)["chunks_committed"], n)
        svc2.activate(tid)
        self.assertEqual(svc2.get_transfer(tid)["state"], "active")

    # ------------------------------------------------- 激活闸门 / 读者隔离

    def test_happy_path_atomic_activation(self):
        r = self.svc.export("ok")
        bid = r["manifest"]["manifest_hash"]
        tid = self.svc.start_transfer("ok")["transfer_id"]
        last = self.svc.complete(tid, bid)
        self.assertIn("activated", last)
        self.assertEqual(self.svc.importer.active_manifest("gmv_1h")[
                             "manifest_hash"], bid)

    def test_missing_chunk_blocks_activation_old_bundle_stays(self):
        r1 = self.svc.export("base")
        bid1 = r1["manifest"]["manifest_hash"]
        t1 = self.svc.start_transfer("base")["transfer_id"]
        self.svc.complete(t1, bid1)
        r2 = self.svc.export("next", version=2, cutoff=2_000_000)
        bid2 = r2["manifest"]["manifest_hash"]
        t2 = self.svc.start_transfer(
            "next", idempotency_key="tr-next")
        tid2 = t2["transfer_id"]
        self.svc.importer.prime_sabotage(tid2, "delete_chunk")
        with self.assertRaises(MissingChunks):
            self.svc.complete(tid2, bid2)
        self.assertEqual(self.svc.importer.get_transfer(tid2)["state"],
                         "failed")
        # active 指针纹丝不动
        self.assertEqual(self.svc.importer.active_manifest("gmv_1h")[
                             "manifest_hash"], bid1)

    def test_corrupt_at_activation_blocks_switch(self):
        r1 = self.svc.export("base2")
        bid1 = r1["manifest"]["manifest_hash"]
        self.svc.complete(self.svc.start_transfer("base2")["transfer_id"],
                          bid1)
        r2 = self.svc.export("next2", version=2, cutoff=2_000_000)
        tid2 = self.svc.start_transfer(
            "next2", idempotency_key="tr-next2")["transfer_id"]
        self.svc.importer.prime_sabotage(tid2, "flip_chunk")
        with self.assertRaises(ChunkCorrupt):
            self.svc.complete(tid2, r2["manifest"]["manifest_hash"])
        self.assertEqual(self.svc.importer.active_manifest("gmv_1h")[
                             "manifest_hash"], bid1)

    def test_schema_incompatible_blocks_activation(self):
        r1 = self.svc.export("base3")
        bid1 = r1["manifest"]["manifest_hash"]
        self.svc.complete(self.svc.start_transfer("base3")["transfer_id"],
                          bid1)
        r2 = self.svc.export("next3", version=2, cutoff=2_000_000)
        tid2 = self.svc.start_transfer(
            "next3", expected_schema_id=r1["manifest"]["schema_id"],
            idempotency_key="tr-next3")["transfer_id"]
        with self.assertRaises(SchemaMismatch):
            self.svc.complete(tid2, r2["manifest"]["manifest_hash"])
        self.assertEqual(self.svc.importer.active_manifest("gmv_1h")[
                             "manifest_hash"], bid1)

    def test_reader_isolation_before_and_after_switch_and_old_readable(self):
        r1 = self.svc.export("iso1")
        bid1 = r1["manifest"]["manifest_hash"]
        tid1 = self.svc.start_transfer("iso1")["transfer_id"]
        # 激活前：尚无 active
        with self.assertRaises(NotFound):
            self.svc.importer.read_rows("gmv_1h")
        self.svc.complete(tid1, bid1)
        before = self.svc.importer.read_rows("gmv_1h")
        self.assertEqual(before["bundle_id"], bid1)
        r2 = self.svc.export("iso2", version=2, cutoff=2_000_000)
        bid2 = r2["manifest"]["manifest_hash"]
        tid2 = self.svc.start_transfer(
            "iso2", auto_activate=False,
            idempotency_key="tr-iso2")["transfer_id"]
        # 收块但未激活：读者仍只看到旧 bundle
        n = len(r2["manifest"]["chunks"])
        for i in range(n):
            lease = self.svc.importer.acquire_chunk(tid2, "w")
            data, _ = self.svc.exporter.get_chunk(bid2, i)
            self.svc.importer.put_chunk(tid2, i, lease["fence_token"], data)
        during = self.svc.importer.read_rows("gmv_1h")
        self.assertEqual(during["bundle_id"], bid1)
        self.svc.importer.activate(tid2)
        after = self.svc.importer.read_rows("gmv_1h")
        self.assertEqual(after["bundle_id"], bid2)
        # 旧 bundle 物化目录保留，可继续独立读取
        old = self.svc.importer.read_rows("gmv_1h", bundle_id=bid1)
        self.assertEqual(old["bundle_id"], bid1)
        self.assertNotEqual(old["total_rows"], after["total_rows"])

    def test_transfer_creation_is_idempotent(self):
        r = self.svc.export("idem")
        t1 = self.svc.start_transfer("idem")
        t2 = self.svc.start_transfer("idem")
        self.assertTrue(t2["replayed"])
        self.assertEqual(t1["transfer_id"], t2["transfer_id"])

    def test_activating_state_is_resumable_after_crash(self):
        r = self.svc.export("crashact")
        bid = r["manifest"]["manifest_hash"]
        tid = self.svc.start_transfer(
            "crashact", auto_activate=False)["transfer_id"]
        n = len(r["manifest"]["chunks"])
        for i in range(n):
            lease = self.svc.importer.acquire_chunk(tid, "w")
            data, _ = self.svc.exporter.get_chunk(bid, i)
            self.svc.importer.put_chunk(tid, i, lease["fence_token"], data)
        # 模拟进程在“激活窗口”内崩溃：状态已置 activating，但指针尚未切换
        c = self.svc.importer.db.conn()
        c.execute(
            "UPDATE ss_transfers SET state='activating' WHERE transfer_id=?",
            (tid,))
        self.svc.importer.activate(tid)   # 重启后重跑闸门 -> 成功
        self.assertEqual(self.svc.importer.get_transfer(tid)["state"],
                         "active")
        self.assertEqual(self.svc.importer.active_manifest("gmv_1h")[
                             "manifest_hash"], bid)

    # ------------------------------------------------- HTTP 层

    def test_http_endpoints_all_scenarios(self):
        httpd_ex = run_server("127.0.0.1", 0, exporter=self.svc.exporter)
        httpd_im = run_server("127.0.0.1", 0, importer=self.svc.importer)
        threading.Thread(target=httpd_ex.serve_forever, daemon=True).start()
        threading.Thread(target=httpd_im.serve_forever, daemon=True).start()
        try:
            ep = httpd_ex.server_address[1]
            ip = httpd_im.server_address[1]
            ec = SnapshotClient(f"http://127.0.0.1:{ep}")
            ic = SnapshotClient(f"http://127.0.0.1:{ip}")
            body = {"idempotency_key": "H1", "feature": "gmv_1h",
                    "version": 1, "cutoff_ts": 1_000_000,
                    "rows_per_chunk": 4, "params": {"job": "H1"}}
            r = ec.create_export(body)
            bid = r["manifest"]["manifest_hash"]
            self.assertEqual(len(r["manifest"]["chunks"]), 4)
            # 块字节经 HTTP 拉取并核对 x-chunk-sha256（在 _request 中为 bytes）
            data = ec.get_chunk(bid, 0)
            self.assertEqual(
                data,
                self.svc.exporter.get_chunk(bid, 0)[0])
            # 重复 / 冲突
            self.assertTrue(ec.create_export(body)["replayed"])
            bad = dict(body, cutoff_ts=1)
            with self.assertRaises(SnapshotHTTPError) as cm:
                ec.create_export(bad)
            self.assertEqual(cm.exception.status, 409)
            # 导入 + 执行器
            tr = ic.create_import({
                "bundle_url": f"http://127.0.0.1:{ep}/bundles/{bid}",
                "idempotency_key": "H1"})
            w = ImportExecutor(f"http://127.0.0.1:{ep}",
                               f"http://127.0.0.1:{ip}", "w1", verbose=False)
            outs = w.run_once(tr["transfer_id"])
            statuses = {o.status for o in outs}
            self.assertIn("activated", statuses)
            self.assertEqual(ic.get_transfer(tr["transfer_id"])["state"],
                             "active")
            rows = ic.read_rows("gmv_1h")
            self.assertEqual(rows["bundle_id"], bid)
        finally:
            httpd_ex.shutdown(); httpd_im.shutdown()
            httpd_ex.server_close(); httpd_im.server_close()

    def test_http_corrupt_put_returns_422(self):
        httpd_ex = run_server("127.0.0.1", 0, exporter=self.svc.exporter)
        httpd_im = run_server("127.0.0.1", 0, importer=self.svc.importer)
        threading.Thread(target=httpd_ex.serve_forever, daemon=True).start()
        threading.Thread(target=httpd_im.serve_forever, daemon=True).start()
        try:
            ep = httpd_ex.server_address[1]
            ip = httpd_im.server_address[1]
            ec = SnapshotClient(f"http://127.0.0.1:{ep}")
            ic = SnapshotClient(f"http://127.0.0.1:{ip}")
            r = ec.create_export({
                "idempotency_key": "H2", "feature": "gmv_1h", "version": 1,
                "cutoff_ts": 1_000_000, "rows_per_chunk": 4,
                "params": {"job": "H2"}})
            bid = r["manifest"]["manifest_hash"]
            tr = ic.create_import({
                "bundle_url": f"http://127.0.0.1:{ep}/bundles/{bid}",
                "idempotency_key": "H2"})
            lease = ic.acquire_chunk(tr["transfer_id"], "wbad")
            data = ec.get_chunk(bid, lease["chunk_index"])
            with self.assertRaises(SnapshotHTTPError) as cm:
                ic.put_chunk(tr["transfer_id"], lease["chunk_index"],
                             lease["fence_token"], data, corrupt=True)
            self.assertEqual(cm.exception.status, 422)
            self.assertEqual(cm.exception.code, "chunk_corrupt")
        finally:
            httpd_ex.shutdown(); httpd_im.shutdown()
            httpd_ex.server_close(); httpd_im.server_close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
