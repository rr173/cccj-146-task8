"""容器化执行器：轮询领取作业并执行。

- 原子 claim 保证同一作业不会被两个执行者重复执行
- 回填作业完成后自动做质量校验，达标才自动切换读取别名；不达标停在 candidate
- 失败作业保留在 failed，等待 resume 从检查点继续
"""
from __future__ import annotations

import logging
import time

from .config import settings
from .store import AliasConflict, Store

log = logging.getLogger("featforge.worker")


class Worker:
    def __init__(self, store: Store, auto_promote: bool = True,
                 poll_seconds: float | None = None):
        self.store = store
        self.auto_promote = auto_promote
        self.poll_seconds = (poll_seconds if poll_seconds is not None
                             else settings.worker_poll_seconds)

    def tick(self) -> dict | None:
        """执行一轮：领作业 -> 跑 -> 回填成功后校验/切换。"""
        job = self.store.claim_next_job()
        if job is None:
            return None
        log.info("领取作业 #%s kind=%s gen=%s", job["id"], job["kind"], job["gen_id"])
        result = self.store.run_job(job)
        outcome: dict = {"job": result}
        if (result["state"] == "succeeded" and job["kind"] == "backfill"
                and self.auto_promote):
            try:
                promo = self.store.promote_generation(job["gen_id"])
            except AliasConflict as e:
                # 并发切换输掉竞争：另一个版本已成为唯一 serving，本方不写历史。
                # 作业本身已成功，这里只记录未切换原因，绝不当作执行异常。
                promo = {"promoted": False, "reason": "alias_conflict",
                         "detail": str(e)}
                log.info("第 %s 代自动切换输掉竞争：%s", job["gen_id"], e)
            outcome["promotion"] = promo
            if promo.get("promoted"):
                log.info("第 %s 代已自动切换为 serving", job["gen_id"])
            else:
                log.info("第 %s 代未切换：%s", job["gen_id"],
                         promo.get("reason"))
        return outcome

    def run_forever(self) -> None:
        log.info("执行器启动（auto_promote=%s）", self.auto_promote)
        # 进程重启：上次中断的订正作业重新排队，已提交批次按进度跳过
        recovered = self.store.recover_interrupted_corrections()
        if recovered:
            log.info("恢复 %s 个中断的订正落地作业", recovered)
        while True:
            try:
                self.tick()
            except Exception:  # 单作业异常不能杀死执行器进程
                log.exception("作业执行异常")
            time.sleep(self.poll_seconds)


def drain(store: Store, auto_promote: bool = True) -> list[dict]:
    """测试/脚本用：同步跑到队列清空，返回每轮结果。"""
    worker = Worker(store, auto_promote=auto_promote, poll_seconds=0)
    results = []
    while True:
        r = worker.tick()
        if r is None:
            return results
        results.append(r)
