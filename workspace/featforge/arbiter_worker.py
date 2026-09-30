"""仲裁器分片执行器：claim -> 逐分片处理 -> 进度提交 -> 心跳续期 -> complete。

执行者无状态、可多副本：
- 每次只处理当前 claim_epoch 租约内的分片，所有上报都带 epoch 做 fence；
- 从 ``shard_cursor`` 继续，已提交分片绝不重做（检查点续跑）；
- 进程退出（模拟 kill）只是不再心跳，租约超时后分片被别的执行者接管，
  旧执行者复活后的上报会因 stale epoch 收到 409。

故障注入（放在作业 payload 里，仅用于演示/测试）：
- ``die_after_shards``：处理并提交 N 个分片后模拟进程被杀——不再有任何调用；
- ``fail_after_shards``：处理 N 个分片后显式 fail（游标保留，可 retry）。
"""
from __future__ import annotations

import json
import os
from typing import Callable, Optional

from .config import settings
from .arbiter import Arbiter, ArbiterError


class SimulatedProcessExit(Exception):
    """模拟进程退出：执行器立即停止，不再发心跳/完成（等租约超时被接管）。"""


ShardHandler = Callable[[dict, int], object]


def default_shard_handler(job: dict, shard_index: int) -> dict:
    """默认分片处理器。

    backfill/correction/recompute 只产出确定性的分片记录；snapshot 作业把
    每个分片的检查点物化成内容寻址文件，重跑时同分片同内容，天然可校验。
    """
    payload = job.get("payload") or {}
    record = {
        "job_id": job["id"], "kind": job["kind"],
        "tenant": job["tenant_name"], "business_key": job["business_key"],
        "shard": shard_index, "shards_total": job["shards_total"],
        "payload": payload,
    }
    if job["kind"] == "snapshot":
        root = payload.get("snapshot_dir") or settings.arbiter_snapshot_dir
        tdir = os.path.join(root, f"job-{job['id']}")
        os.makedirs(tdir, exist_ok=True)
        path = os.path.join(tdir, f"shard-{shard_index:06d}.json")
        with open(path, "w", encoding="utf-8") as f:
            json.dump(record, f, ensure_ascii=False, sort_keys=True)
        record["artifact"] = path
    return record


class ArbiterWorker:
    def __init__(self, arbiter: Arbiter, worker_id: str,
                 handler: Optional[ShardHandler] = None,
                 heartbeat_shards: int = 4):
        self.arbiter = arbiter
        self.worker_id = worker_id
        self.handler = handler or default_shard_handler
        # 每处理 N 个分片发一次心跳（长分片作业的租约续期）
        self.heartbeat_shards = max(1, heartbeat_shards)

    def tick(self) -> Optional[dict]:
        """领取并处理一个作业到完成（或故障注入点），无可领取项返回 None。"""
        job = self.arbiter.claim(self.worker_id)
        if job is None:
            return None
        return self.process(job, job["claim_epoch"])

    def process(self, job: dict, claim_epoch: int) -> dict:
        """在给定 epoch 租约内处理该作业的剩余分片。

        接管场景下可由外部传入旧 job 行与新 epoch；本方法始终以参数中的
        epoch 上报，若进程其实已失去租约，所有上报都会收到 409 stale_epoch。
        """
        payload = job.get("payload") or {}
        die_after = payload.get("die_after_shards")
        fail_after = payload.get("fail_after_shards")
        processed_here = 0
        cursor = job["shard_cursor"]
        total = job["shards_total"]
        for shard in range(cursor, total):
            # 先处理再提交：处理结果随进度一起 fence，崩溃在提交前只会
            # 重算这一个分片（无副作用的确定性处理器，重算安全）。
            self.handler(job, shard)
            cursor = shard + 1
            self.arbiter.report_progress(
                job["id"], claim_epoch, self.worker_id, cursor,
                note=f"shard {shard} done")
            processed_here += 1
            if cursor % self.heartbeat_shards == 0 and cursor < total:
                self.arbiter.heartbeat(job["id"], claim_epoch, self.worker_id)
            if die_after is not None and processed_here >= int(die_after):
                # 模拟 SIGKILL：不 complete、不 fail、不再心跳，租约将超时
                raise SimulatedProcessExit(
                    f"worker {self.worker_id} 在分片 {cursor}/{total} 后退出，"
                    "租约超时后可被接管，已提交游标保留")
            if fail_after is not None and processed_here >= int(fail_after):
                return self.arbiter.fail(
                    job["id"], claim_epoch, self.worker_id,
                    error=f"注入失败：已完成 {cursor}/{total} 分片，可 retry 续跑")
        return self.arbiter.complete(job["id"], claim_epoch, self.worker_id)


def drain(arbiter: Arbiter, worker_id: str = "drainer",
          handler: Optional[ShardHandler] = None) -> list[dict]:
    """跑到 ready set 为空（不等待租约超时），返回完成的作业列表。"""
    w = ArbiterWorker(arbiter, worker_id, handler=handler)
    done = []
    while True:
        try:
            out = w.tick()
        except SimulatedProcessExit:
            break
        if out is None:
            return done
        done.append(out)
