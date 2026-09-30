"""预算工单执行器：claim -> 按单调进度上报消耗 -> 超额追加预留 -> complete。

执行者无状态、可多副本，席位由 Budget 全局计数（同一工单同一时刻只有一个
持有者）。所有上报都带 ``hold_epoch`` 凭证纪元做 fence：

- 从工单的 ``used_total``（最近已确认位置）继续，接管时不重做已结算部分；
- 每步上报**累计**实际消耗；实际消耗超预估时由 Budget 原子追加预留，
  钱不够则工单 paused、本执行器让出 claim（``PausedForBudget``），补充
  到账后由任意执行者从断点继续；
- ``die_after``：处理若干步后模拟进程被 SIGKILL（不再心跳/上报），租约
  超时后被接管，旧持有者复活后的上报一律 409 stale_epoch、不重复扣费。

故障注入字段（作业 payload，仅用于演示/测试）：
- ``steps``：执行步数；``cost_per_step``：每步实际消耗（默认 1，>1 即超预估）；
- ``die_after``：执行 N 步后模拟进程退出；
- ``final_total``：完成时上报的最终累计消耗（演示“低于预估退款”）。
"""
from __future__ import annotations

from typing import Callable, Optional

from .budget import Budget, BudgetConflict


class PausedForBudget(Exception):
    """工单因超额且追加预留失败而暂停：执行器必须让出席位，停止上报。"""


class SimulatedProcessExit(Exception):
    """模拟进程退出：立即停止，等心跳超时后被接管。"""


StepHandler = Callable[[dict, int, int], None]


def default_step_handler(job: dict, step: int, used: int) -> None:
    """默认处理器为空操作：消耗数字本身就是演示对象，无外部副作用。"""


class BudgetWorker:
    def __init__(self, budget: Budget, worker_id: str,
                 handler: Optional[StepHandler] = None,
                 inject_faults: bool = True):
        self.budget = budget
        self.worker_id = worker_id
        self.handler = handler or default_step_handler
        # 接管场景设 False：接替者沿用故障注入前的作业但不应再次被杀
        self.inject_faults = inject_faults

    def tick(self, customer_ref=None) -> Optional[dict]:
        """领取并执行一个工单直到终局/暂停/失联模拟；无工单返回 None。"""
        job = self.budget.claim_next(self.worker_id, customer_ref=customer_ref)
        if job is None:
            return None
        return self.process(job)

    def process(self, job: dict) -> dict:
        """在当前凭证纪元内从已确认位置跑到完成（或故障注入点）。"""
        payload = job.get("payload") or {}
        steps = int(payload.get("steps", job["estimated"]))
        cost = int(payload.get("cost_per_step", 1))
        die_after = payload.get("die_after") if self.inject_faults else None
        epoch = job["hold_epoch"]
        jid = job["id"]
        # 已确认位置（可能是前一任持有者结算的）：接替者从这里继续。
        # cost 固定时已完成步数 = used // cost。
        position = job["used_total"]
        done_steps = position // cost
        for step in range(done_steps, steps):
            self.handler(job, step, position + cost)
            position += cost
            out = self.budget.report(jid, epoch, self.worker_id, position)
            if out.get("paused_for_budget"):
                raise PausedForBudget(
                    f"工单 #{jid} 停在已确认位置 {out['confirmed_position']}，"
                    f"目标 {position} 暂无额度；已释放执行席位，补充后从断点继续")
            if die_after is not None and (step - done_steps + 1) >= int(die_after):
                raise SimulatedProcessExit(
                    f"worker {self.worker_id} 在 {position}/{job['estimated']} "
                    "后退出，心跳超时后可被接管，已结算金额保留")
        final = payload.get("final_total", position)
        return self.budget.complete(jid, epoch, self.worker_id,
                                   final_total=int(final))


def drain(budget: Budget, worker_id: str = "drainer",
          handler: Optional[StepHandler] = None) -> list[dict]:
    """跑到 claim 不到工单为止（遇到预算暂停即停，等外部 tick 补充）。"""
    w = BudgetWorker(budget, worker_id, handler=handler)
    done = []
    while True:
        try:
            out = w.tick()
        except (PausedForBudget, SimulatedProcessExit):
            break
        if out is None:
            return done
        done.append(out)
