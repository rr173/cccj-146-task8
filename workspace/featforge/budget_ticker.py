"""预算后台时钟：周期性推进补充到账、唤醒延后区、派发席位、回收失联工单。

与业务执行器解耦：这个线程不跑工单，只做 ``Budget.tick()`` 的记账维护；
执行器（BudgetWorker / HTTP claim）自己 claim、上报、完成。API 进程与
worker 进程都可以启动它（tick 全部走 IMMEDIATE 事务，重复触发无害）。
"""
from __future__ import annotations

import logging
import threading
import time

from .budget import Budget

log = logging.getLogger("featforge.budget_ticker")


class BudgetTicker:
    def __init__(self, budget: Budget, interval_seconds: float = 1.0,
                 run_immediately: bool = False):
        self.budget = budget
        self.interval_seconds = interval_seconds
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        if run_immediately:
            self.tick_once()

    def tick_once(self) -> None:
        try:
            self.budget.tick()
        except Exception:
            log.exception("预算 tick 异常")

    def _run(self) -> None:
        log.info("预算后台时钟启动（每 %ss tick）", self.interval_seconds)
        while not self._stop.wait(self.interval_seconds):
            self.tick_once()

    def start(self) -> "BudgetTicker":
        if self._thread is None:
            self._thread = threading.Thread(
                target=self._run, name="budget-ticker", daemon=True)
            self._thread.start()
        return self

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2)
            self._thread = None
