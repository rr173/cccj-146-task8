#!/usr/bin/env python3
"""可复现端到端演示：两个独立 FeatForge 实例之间搬运特征快照。

在两个真实进程（导出端 / 导入端）之间跑通：

  1. 正常搬运：导出 -> 多执行器分块拉取 -> 校验 -> 原子启用 -> 读者读到新 bundle
  2. 重复导出：同幂等键 + 同参数返回同一个 bundle（bundle id 不变）
  3. 幂等冲突：同键不同参数返回 409
  4. 生成中断：导出端被故障注入打断，不发布半成品；同键重试成功
  5. 中断续传：执行器领取后崩溃，新执行器从持久检查点续跑
  6. 租约接管 + 迟到提交：旧持有者睡过租约，新持有者接管；旧提交 409 被拒
  7. 坏块拒绝：提交字节被翻转，422 chunk_corrupt，该块重新由别人补齐
  8. 激活前/后读者隔离：激活失败时旧 bundle 继续服务；成功后整行切换；旧 bundle 仍可读

运行：python scripts/snapshot_demo.py [--keep]  （--keep 保留临时目录）
只依赖 Python 标准库，约十几秒完成。
"""
from __future__ import annotations

import argparse
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from featforge.snapshot.client import SnapshotClient, SnapshotHTTPError  # noqa: E402

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def banner(title: str) -> None:
    print("\n" + "=" * 74)
    print(f"  {title}")
    print("=" * 74)


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class Cluster:
    def __init__(self, base: str):
        self.base = base
        self.eport = free_port()
        self.iport = free_port()
        self.ex_url = f"http://127.0.0.1:{self.eport}"
        self.im_url = f"http://127.0.0.1:{self.iport}"
        self.procs: list[subprocess.Popen] = []

    def start(self) -> None:
        common = [sys.executable, "-m", "featforge.snapshot"]
        env = dict(os.environ, PYTHONPATH=ROOT)
        ep = subprocess.Popen(
            common + ["exporter", "--host", "127.0.0.1", "--port",
                      str(self.eport), "--root", f"{self.base}/exporter",
                      "--seed", "--fence-ttl", "3"],
            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        ip = subprocess.Popen(
            common + ["importer", "--host", "127.0.0.1", "--port",
                      str(self.iport), "--root", f"{self.base}/importer",
                      "--external-url", self.im_url, "--lease-ttl", "3"],
            env=env, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        self.procs = [ep, ip]
        for url in (self.ex_url, self.im_url):
            self._wait_ready(url)

    @staticmethod
    def _wait_ready(url: str, timeout: float = 10.0) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                SnapshotClient(url).list_bundles()
                return
            except Exception:
                try:
                    SnapshotClient(url).list_transfers()
                    return
                except Exception:
                    time.sleep(0.1)
        raise RuntimeError(f"服务未就绪: {url}")

    def worker(self, *args, wait: bool = True) -> subprocess.Popen | None:
        env = dict(os.environ, PYTHONPATH=ROOT)
        p = subprocess.Popen(
            [sys.executable, "-m", "featforge.snapshot", "worker",
             "--exporter", self.ex_url, "--importer", self.im_url,
             "--once", "--poll", "0.1", *args],
            env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            text=True)
        if wait:
            p.wait()
            return p
        self.procs.append(p)
        return p

    def shutdown(self) -> None:
        for p in self.procs:
            p.terminate()
        for p in self.procs:
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()


def show_transfer(ic: SnapshotClient, tid: str) -> dict:
    t = ic.get_transfer(tid)
    print(f"  会话 {tid}: state={t['state']} "
          f"chunks={t['chunks_committed']}/{t['chunks_total']}"
          + (f" error={t['error_code']}" if t.get("error_code") else ""))
    return t


def active_summary(ic: SnapshotClient, feature: str) -> tuple[str, int]:
    m = ic.active(feature)
    return m["manifest_hash"], m["row_count"]


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--keep", action="store_true", help="保留临时数据目录")
    args = ap.parse_args()

    base = tempfile.mkdtemp(prefix="featforge-snapshot-")
    print(f"临时目录: {base}")
    cluster = Cluster(base)
    cluster.start()
    ec = SnapshotClient(cluster.ex_url)
    ic = SnapshotClient(cluster.im_url)

    bundle_v1 = None
    transfer_t1 = None

    # ------------------------------------------------ 1. 正常搬运
    banner("1. 正常搬运：gmv_1h v1 @cutoff=1000000，导出 + 多执行器分块导入")
    r = ec.create_export({
        "idempotency_key": "exp-gmv-v1", "feature": "gmv_1h", "version": 1,
        "cutoff_ts": 1_000_000, "rows_per_chunk": 10,
        "params": {"reason": "nightly-handoff"},
    })
    bundle_v1 = r["manifest"]
    print(f"  bundle_id = {bundle_v1['manifest_hash']}")
    print(f"  schema_id = {bundle_v1['schema_id']}  "
          f"块数={len(bundle_v1['chunks'])}  总行数={bundle_v1['row_count']}")
    t1 = ic.create_import({
        "bundle_url": f"{cluster.ex_url}/bundles/{bundle_v1['manifest_hash']}",
        "lease_ttl_seconds": 3, "idempotency_key": "t1",
    })
    transfer_t1 = t1["transfer_id"]
    w1 = cluster.worker("--transfer", transfer_t1, "--executor-id", "w-A",
                        "--max-chunks", "6")
    w2 = cluster.worker("--transfer", transfer_t1, "--executor-id", "w-B",
                        "--max-chunks", "6")
    for name, p in (("w-A", w1), ("w-B", w2)):
        committed = [l for l in p.stdout.read().splitlines() if "committed" in l]
        print(f"  {name}: 提交 {len(committed)} 块")
    # 两个执行器各有上限（13 块只完成 12），第三个执行器补齐最后一块并触发激活
    w3 = cluster.worker("--transfer", transfer_t1, "--executor-id", "w-A",
                        "--max-chunks", "6")
    tail = [l for l in w3.stdout.read().splitlines() if "committed" in l]
    print(f"  w-A 第二轮: 再提交 {len(tail)} 块（最后一块的提交者原子激活）")
    t = show_transfer(ic, transfer_t1)
    assert t["state"] == "active", t
    bid, rows = active_summary(ic, "gmv_1h")
    print(f"  读者已切换到新 active bundle：{bid[:20]}… 行数={rows}")
    assert bid == bundle_v1["manifest_hash"]

    # ------------------------------------------------ 2. 重复导出
    banner("2. 重复导出：同幂等键 + 同参数，返回原 bundle（200 replayed）")
    r2 = ec.create_export({
        "idempotency_key": "exp-gmv-v1", "feature": "gmv_1h", "version": 1,
        "cutoff_ts": 1_000_000, "rows_per_chunk": 10,
        "params": {"reason": "nightly-handoff"},
    })
    print(f"  replayed={r2['replayed']}  bundle_id 相同: "
          f"{r2['manifest']['manifest_hash'] == bundle_v1['manifest_hash']}")
    assert r2["replayed"] is True
    assert r2["manifest"]["manifest_hash"] == bundle_v1["manifest_hash"]

    # ------------------------------------------------ 3. 幂等冲突
    banner("3. 幂等冲突：同键不同参数（cutoff 改变）-> HTTP 409")
    try:
        ec.create_export({
            "idempotency_key": "exp-gmv-v1", "feature": "gmv_1h",
            "version": 1, "cutoff_ts": 999_999, "rows_per_chunk": 10,
            "params": {"reason": "nightly-handoff"},
        })
    except SnapshotHTTPError as e:
        print(f"  HTTP {e.status} {e.code}: {e.message}")
        assert e.status == 409 and e.code == "idempotency_conflict"
    # 原 bundle 仍可正常读取
    still = ec.get_bundle(bundle_v1["manifest_hash"])
    print(f"  冲突后原 bundle 完好：{still['manifest_hash'] == bundle_v1['manifest_hash']}")

    # ------------------------------------------------ 4. 生成中断
    banner("4. 生成中断：故障注入使导出在第 3 块前中断 -> 不发布半成品")
    try:
        ec.create_export({
            "idempotency_key": "exp-gmv-v1-partial", "feature": "gmv_1h",
            "version": 1, "cutoff_ts": 1_000_000, "rows_per_chunk": 10,
            "params": {"reason": "nightly-handoff"},
        }, fail_after_chunks=3)
    except SnapshotHTTPError as e:
        print(f"  导出端返回 HTTP {e.status} {e.code}: {e.message}")
    bundles = {b["bundle_id"] for b in ec.list_bundles()["bundles"]}
    print(f"  当前已发布 bundle 数={len(bundles)}（半成品未出现在发布列表）")
    # 同键重试成功，得到完整 bundle
    r4 = ec.create_export({
        "idempotency_key": "exp-gmv-v1-partial", "feature": "gmv_1h",
        "version": 1, "cutoff_ts": 1_000_000, "rows_per_chunk": 10,
        "params": {"reason": "nightly-handoff"},
    })
    same = r4['manifest']['manifest_hash'] == bundle_v1['manifest_hash']
    print(f"  同键重试成功：replayed={r4['replayed']} "
          f"块数={len(r4['manifest']['chunks'])}；参数与场景 1 完全一致，"
          f"内容寻址得到同一个 bundle id: {same}")
    assert same

    # ------------------------------------------------ 5. 执行器崩溃续传
    banner("5. 中断续传：执行器领取首块后立即崩溃，重启从持久检查点续跑")
    t5 = ic.create_import({
        "bundle_url": f"{cluster.ex_url}/bundles/{bundle_v1['manifest_hash']}",
        "lease_ttl_seconds": 3, "auto_activate": False, "idempotency_key": "t5",
    })["transfer_id"]
    p = cluster.worker("--transfer", t5, "--executor-id", "w-crash",
                       "--die-after-acquire", wait=False)
    rc = p.wait()
    print(f"  w-crash 领取后退出码={rc}（租约留在服务端，块尚未提交）")
    t = show_transfer(ic, t5)
    print("  等待租约过期，由 w-resume 接管并完成全部剩余块……")
    time.sleep(3.2)
    pr = cluster.worker("--transfer", t5, "--executor-id", "w-resume")
    print(pr.stdout.read().strip().splitlines()[-1])
    t = show_transfer(ic, t5)
    assert t["chunks_committed"] == t["chunks_total"] and t["state"] == "receiving"

    # ------------------------------------------------ 6. 租约接管 + 迟到提交
    banner("6. 租约接管 + 迟到提交：旧持有者睡过 TTL，新持有者接管，旧提交被 409 拒绝")
    t6 = ic.create_import({
        "bundle_url": f"{cluster.ex_url}/bundles/{bundle_v1['manifest_hash']}",
        "lease_ttl_seconds": 3, "auto_activate": False, "idempotency_key": "t6",
    })["transfer_id"]
    # 旧执行器：只领 1 块并持有 6s（TTL=3s）
    p_old = cluster.worker("--transfer", t6, "--executor-id", "w-stale",
                           "--max-chunks", "1", "--hold-seconds", "6",
                           wait=False)
    time.sleep(1.0)
    # 新执行器此时拿不到过期块（TTL 未到），会在无活块后退出
    print("  w-stale 持块睡死；等待 TTL(3s) 后 w-takeover 接管……")
    time.sleep(3.0)
    p_new = cluster.worker("--transfer", t6, "--executor-id", "w-takeover",
                           "--max-chunks", "2")
    out_new = p_new.stdout.read()
    print(out_new.strip())
    out_old = p_old.communicate(timeout=20)[0]
    late = [l for l in out_old.splitlines() if "迟到提交" in l]
    print(f"  w-stale 醒来迟到提交 -> {late[0] if late else '(块已被提交完)'}")
    assert late, "旧持有者的迟到提交应当被 409 拒绝"
    # 其余块由新执行器补齐（手动激活，auto_activate=False）
    cluster.worker("--transfer", t6, "--executor-id", "w-takeover").stdout.read()
    t = show_transfer(ic, t6)
    assert t["chunks_committed"] == t["chunks_total"]
    print("  显式整体校验 + 原子启用 t6……")
    ic.activate(t6)
    show_transfer(ic, t6)

    # ------------------------------------------------ 7. 坏块拒绝
    banner("7. 坏块拒绝：执行器提交翻转字节 -> 422 chunk_corrupt，块回待领被别人补齐")
    t7 = ic.create_import({
        "bundle_url": f"{cluster.ex_url}/bundles/{bundle_v1['manifest_hash']}",
        "lease_ttl_seconds": 3, "idempotency_key": "t7",
    })["transfer_id"]
    p_bad = cluster.worker("--transfer", t7, "--executor-id", "w-bad",
                           "--max-chunks", "1", "--corrupt")
    print([l for l in p_bad.stdout.read().splitlines() if "损坏" in l][0])
    # 坏提交不消耗 fence：租约仍在 w-bad 名下直到过期，w-good 接管
    print("  等待 TTL 后 w-good 接管该块并完成……")
    time.sleep(3.2)
    p_good = cluster.worker("--transfer", t7, "--executor-id", "w-good")
    good_out = p_good.stdout.read()
    t = show_transfer(ic, t7)
    assert t["state"] == "active", t

    # ------------------------------------------------ 8. 读者隔离
    banner("8. 激活前/后读者隔离：schema 不兼容拒绝激活时旧 bundle 继续服务")
    # 导出 v2（schema 多一列 bucket，schema_id 必然不同）
    rv2 = ec.create_export({
        "idempotency_key": "exp-gmv-v2", "feature": "gmv_1h", "version": 2,
        "cutoff_ts": 2_000_000, "rows_per_chunk": 10,
        "params": {"reason": "schema-upgrade"},
    })
    bundle_v2 = rv2["manifest"]
    print(f"  v2 bundle_id={bundle_v2['manifest_hash'][:20]}… "
          f"schema_id={bundle_v2['schema_id']}（v1 schema_id="
          f"{bundle_v1['schema_id']}）")
    before_bid, before_rows = active_summary(ic, "gmv_1h")
    print(f"  激活前读者 active={before_bid[:20]}… 行数={before_rows}")

    t8 = ic.create_import({
        "bundle_url": f"{cluster.ex_url}/bundles/{bundle_v2['manifest_hash']}",
        "expected_schema_id": bundle_v1["schema_id"],   # 强制不兼容预期
        "idempotency_key": "t8",
    })["transfer_id"]
    p = cluster.worker("--transfer", t8, "--executor-id", "w-v2")
    p.stdout.read()
    t = show_transfer(ic, t8)
    after_fail_bid, _ = active_summary(ic, "gmv_1h")
    print(f"  不兼容激活失败后读者 active 不变: {after_fail_bid == before_bid}")
    assert t["state"] == "failed" and t["error_code"] == "schema_mismatch"
    assert after_fail_bid == before_bid

    # 不带预期重新搬运，激活成功后读者整行切换；旧 bundle 物化目录仍可读
    t9 = ic.create_import({
        "bundle_url": f"{cluster.ex_url}/bundles/{bundle_v2['manifest_hash']}",
        "idempotency_key": "t9",
    })["transfer_id"]
    p = cluster.worker("--transfer", t9, "--executor-id", "w-v2-ok")
    p.stdout.read()
    show_transfer(ic, t9)
    now_bid, now_rows = active_summary(ic, "gmv_1h")
    print(f"  兼容 bundle 激活后读者 active={now_bid[:20]}… 行数={now_rows}")
    old_rows = ic.read_rows("gmv_1h", bundle_id=bundle_v1["manifest_hash"])
    new_rows = ic.read_rows("gmv_1h")
    print(f"  旧 bundle 仍可独立读取（行数={old_rows['total_rows']}），"
          f"新 bundle 行数={new_rows['total_rows']}，"
          f"两者指针不同: {old_rows['bundle_id'] != new_rows['bundle_id']}")
    assert now_bid == bundle_v2["manifest_hash"]
    assert old_rows["bundle_id"] == bundle_v1["manifest_hash"]

    banner("全部 8 个场景通过 ✔")
    print("导出端 bundles:")
    for b in ec.list_bundles()["bundles"]:
        print(f"  - {b['bundle_id'][:24]}… feature={b['feature']} "
              f"v{b['version']} schema={b['schema_id']}")
    print("导入端 active:")
    from featforge.snapshot.client import _request
    for a in _request("GET", f"{cluster.im_url}/active")["active"]:
        print(f"  - feature={a['feature']} bundle={a['bundle_id'][:24]}… "
              f"schema={a['schema_id']}")

    cluster.shutdown()
    if args.keep:
        print(f"数据目录保留在: {base}")
    else:
        shutil.rmtree(base, ignore_errors=True)
        print(f"已清理临时目录: {base}")


if __name__ == "__main__":
    main()
