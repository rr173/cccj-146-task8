"""导入端执行器（worker）：从导入服务领租约 -> 从导出端拉块 -> 校验提交。

执行器是**无状态**的：它只是“租约领取者 + 搬运工”，所有进度（块状态、fence、
计数）都持久化在导入服务端。因此：

* 可以同时跑任意多个执行器，同一块只有一个有效持有者（靠租约 + fencing token）；
* 执行器随时被 kill -9，重启后从检查点续传：committed 的块不重拉，
  租约过期的块被自己或别人接管；
* 旧持有者迟到提交会被服务端以 409 fence_conflict 拒绝，执行器把它作为
  “已被接管”正常退出当前块循环，而不是覆盖新持有者的结果。
"""
from __future__ import annotations

import sys
import time
from dataclasses import dataclass

from .client import SnapshotClient, SnapshotHTTPError


@dataclass
class ChunkOutcome:
    index: int
    status: str          # committed|already|fence_lost|retry|error
    detail: str = ""


class ImportExecutor:
    def __init__(self, exporter_url: str, importer_url: str,
                 executor_id: str, poll_interval: float = 0.2,
                 verbose: bool = True):
        self.exporter = SnapshotClient(exporter_url)
        self.importer = SnapshotClient(importer_url)
        self.executor_id = executor_id
        self.poll_interval = poll_interval
        self.verbose = verbose

    def log(self, msg: str) -> None:
        if self.verbose:
            print(f"[worker {self.executor_id}] {msg}", flush=True)

    def run_once(
        self,
        transfer_id: str,
        *,
        max_chunks: int | None = None,
        hold_seconds: float = 0.0,
        corrupt_put: bool = False,
        die_after_acquire: bool = False,
        sleep_fn=time.sleep,
    ) -> list[ChunkOutcome]:
        """领取并完成至多 max_chunks 个块；无块可领时结束本轮。"""
        outcomes: list[ChunkOutcome] = []
        done = 0
        while max_chunks is None or done < max_chunks:
            lease = None
            try:
                lease = self.importer.acquire_chunk(transfer_id,
                                                    self.executor_id)
            except SnapshotHTTPError as e:
                self.log(f"acquire 失败: {e}")
                outcomes.append(ChunkOutcome(-1, "error", str(e)))
                return outcomes
            if lease is None:
                return outcomes

            idx = lease["chunk_index"]
            token = lease["fence_token"]
            ttl = lease["lease_expires_at"] - time.time()
            self.log(f"领取块 {idx} fence={token} ttl={ttl:.1f}s")

            if die_after_acquire:
                # 模拟执行器崩溃：租约留在服务端，重启后靠过期接管续传
                self.log(f"领取块 {idx} 后立即崩溃（die_after_acquire）")
                sys.exit(7)

            if hold_seconds:
                # 演示“旧持有者卡住”：睡过租约期，醒来后迟到提交必须被拒
                self.log(f"持有块 {idx} 不提交，等待 {hold_seconds}s "
                         "（租约将过期）……")
                sleep_fn(hold_seconds)

            try:
                data = self._fetch_source_chunk(lease)
                r = self.importer.put_chunk(
                    transfer_id, idx, token, data, corrupt=corrupt_put)
            except SnapshotHTTPError as e:
                if e.status == 409 and e.code == "fence_conflict":
                    self.log(f"块 {idx} 迟到提交被拒绝（{e.code}）："
                             "租约已被新持有者接管，放弃本块")
                    outcomes.append(ChunkOutcome(idx, "fence_lost", e.message))
                    return outcomes
                if e.status == 422 and e.code == "chunk_corrupt":
                    self.log(f"块 {idx} 被判定损坏（{e.message}）")
                    outcomes.append(ChunkOutcome(idx, "error", e.message))
                    return outcomes
                self.log(f"块 {idx} 提交出错: {e}")
                outcomes.append(ChunkOutcome(idx, "error", str(e)))
                return outcomes

            if r.get("already"):
                self.log(f"块 {idx} 已提交（幂等）")
                outcomes.append(ChunkOutcome(idx, "already"))
            else:
                self.log(
                    f"提交块 {idx} 成功 "
                    f"({r.get('chunks_committed')}/{r.get('chunks_total')})")
                outcomes.append(ChunkOutcome(idx, "committed"))
            if r.get("activated"):
                self.log(f"全部到齐并原子激活：active={r['active_bundle_id']}")
                outcomes.append(
                    ChunkOutcome(idx, "activated", r["active_bundle_id"]))
                return outcomes
            done += 1
        return outcomes

    def _fetch_source_chunk(self, lease: dict) -> bytes:
        # source_chunk_url 是导出端绝对路径，直接用通用 GET
        from .client import _request
        return _request("GET", lease["source_chunk_url"])

    def run_forever(self, transfer_id: str) -> None:
        while True:
            t = self.importer.get_transfer(transfer_id)
            if t["state"] in ("active", "failed"):
                self.log(f"搬运结束 state={t['state']}")
                return
            outcomes = self.run_once(transfer_id)
            if not outcomes:
                # 可能所有块都在别人手里且租约未过期
                time.sleep(self.poll_interval)
            if all(o.status in ("fence_lost", "error") for o in outcomes):
                return
