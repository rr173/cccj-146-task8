"""后台计算的容量凭证与预算结算。

模型
----
每个客户配置一个**令牌桶预算**：

- ``refill_amount`` / ``period_seconds``：按时间补充（每 period 到账一笔）；
- ``burst_cap``：突发上限，``available + reserved`` 不得超过它；
- ``available``：当前可用额（尚未预留）；
- ``reserved_total``：所有未完成工单手里凭证的面额合计。

工单提交时声明预计消耗 ``estimated``，系统在**一个 IMMEDIATE 事务**里完成
“检查余额 → 扣款 → 发凭证 → 置 ready”：余额够则原子预留成功，多个提交者
争抢最后一份额度时只有一个能赢，绝不透支；余额不足的工单进入**延后区**
（state=waiting），并按 FIFO 与补充速率标出 ``earliest_ready_at``。每次补充
到账（懒计算，``tick`` 时也会统一推进）后重新泵送延后队列，满足额度的工单
自动取得执行资格（state=ready）。

凭证与结算
----------
工单拿到资格即持有不可变凭证（``bg_vouchers``，初始 RESERVE，超额时追加
EXTEND，在线调整预算**只影响后续预留，不撤销已发出的凭证**）。执行过程按
**单调进度**上报累计实际消耗 ``used_total``：已确认部分立即从 reserved 结
转到 settled。超过预估时必须**追加预留**：

- 额度够：在同一事务内原子“追加预留 + 结算到新值”，工单继续；
- 额度不够：只结算到当前凭证面额，工单**停在最近已确认位置**（state=
  paused，``blocked_reason=need_extend``），**立即释放执行席位**，等待补充；
  补充到账后自动重新取得资格、回到就绪队列，由新席位从 ``used_total`` 继续。

fencing / 接管
--------------
每次取得执行席位（初次派发、失联接管、暂停后恢复）``hold_epoch`` 单调 +1。
心跳、进度、完成都必须携带当前 epoch 与持有者，旧持有者的**迟到上报一律
409 stale_epoch，不会再次扣费**。接替者沿用 ``used_total``（已结算金额）。
心跳超过 ``lease_ttl`` 的 running 工单被 ``reap_lost`` 标记为 lost、让出席
位并可被直接接管（epoch +1）。

终止与完成（终局唯一）
----------------------
- terminate：只退**尚未使用**的部分（held - used），重复终止返回原结果
  （``replayed=True``），不重复退款；
- complete：若最终实际消耗低于凭证面额，差额一次性结算+退款；
- complete 与 terminate 竞争：先落事务者定义终局，后到者拿到对方终局结果
  （``already_terminal=True``），全局恰好发生一次退款或最终结算。
"""
from __future__ import annotations

import json
import time
from typing import Callable, Optional

from .config import settings
from .db import Database

# waiting：延后区（余额不足）；ready：已取得资格，等执行席位
# running：持有席位；paused：超额且追加预留失败，已让出席位
# succeeded / terminated：终局；lost：失联，无席位，可被接管
JOB_STATES = ("waiting", "ready", "running", "paused",
              "succeeded", "terminated", "lost")
ACTIVE_STATES = ("waiting", "ready", "running", "paused", "lost")
QUEUE_STATES = ("waiting", "paused")        # 受预算约束、需要泵的状态
LEDGER_OPS = ("reserve", "release_refused", "extend", "settle",
              "refund", "refill")


class BudgetError(Exception):
    """请求本身不合法（HTTP 400 语义）。"""


class BudgetConflict(Exception):
    """fencing / 状态竞争冲突（HTTP 409 语义）。

    ``code`` 机器可读：stale_epoch / lease_expired / not_running /
    worker_mismatch / used_regress / business_key_conflict /
    over_burst_cap
    """

    def __init__(self, message: str, code: str = "conflict"):
        super().__init__(message)
        self.code = code


def canonical(payload: object) -> str:
    return json.dumps(payload, sort_keys=True, default=str,
                      ensure_ascii=False, separators=(",", ":"))


class Budget:
    def __init__(
        self,
        db: Database,
        clock: Optional[Callable[[], int]] = None,
        lease_ttl: Optional[int] = None,
        total_seats: Optional[int] = None,
        ledger_limit: Optional[int] = None,
    ):
        self.db = db
        self.now = clock or (lambda: int(time.time()))
        self.lease_ttl = (lease_ttl if lease_ttl is not None
                          else settings.budget_lease_ttl)
        self.total_seats = (total_seats if total_seats is not None
                            else settings.budget_seats)
        self.ledger_limit = (ledger_limit if ledger_limit is not None
                             else settings.budget_ledger_limit)

    # ============================================================== 内部工具

    def _audit(self, c, kind: str, ref: str, message: str) -> None:
        c.execute(
            "INSERT INTO audits(at,kind,ref,message) VALUES(?,?,?,?)",
            (self.now(), kind, ref, message))

    def _post_ledger(self, c, customer_id, job_id, op: str,
                     d_available: int, d_reserved: int, d_settled: int,
                     claim_epoch: Optional[int] = None,
                     note: Optional[str] = None) -> None:
        """追加一条复式台账行（三栏增量之和恒为 0）。"""
        assert d_available + d_reserved + d_settled == 0, (
            f"台账不平：{d_available},{d_reserved},{d_settled}")
        c.execute(
            "INSERT INTO bg_ledger(at,customer_id,job_id,op,claim_epoch,"
            "d_available,d_reserved,d_settled,note) VALUES(?,?,?,?,?,?,?,?,?)",
            (self.now(), customer_id, job_id, op, claim_epoch,
             d_available, d_reserved, d_settled, note))

    def _customer_row(self, c, ref) -> dict:
        row = c.execute(
            "SELECT * FROM bg_customers WHERE id=? OR name=?", (ref, ref)
        ).fetchone()
        if row is None:
            raise BudgetError(f"客户不存在: {ref}")
        return dict(row)

    def _job_row(self, c, job_id: int) -> dict:
        row = c.execute("SELECT * FROM bg_jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise BudgetError(f"预算工单不存在: {job_id}")
        return dict(row)

    def _seats_used(self, c) -> int:
        return c.execute(
            "SELECT COUNT(*) AS n FROM bg_jobs WHERE state='running'"
        ).fetchone()["n"]

    @staticmethod
    def _rollback(c) -> None:
        """安全回滚：早退分支可能已手动结束事务，忽略“无活动事务”。"""
        try:
            c.execute("ROLLBACK")
        except Exception:
            pass

    def _catchup_locked(self, c, cust: dict, at: Optional[int] = None) -> int:
        """把补充推进到当前时刻（离散补充、突发上限封顶）。

        返回期间新到账的额度合计。纯内存式循环 + 封顶，因此每个桶每客户
        至多循环一次（refill=0 或已封顶时立即退出）。**循环结束统一落库**，
        保证只补充、不预留时到账金额也不丢。
        """
        now = at if at is not None else self.now()
        amount, period = cust["refill_amount"], cust["period_seconds"]
        added_total = 0
        if amount > 0 and period > 0:
            while True:
                outstanding = cust["available"] + cust["reserved_total"]
                room = cust["burst_cap"] - outstanding
                if room <= 0:
                    # 已顶到突发上限：桶满不补充，计费窗口整体右移
                    cust["last_refill_at"] = now
                    break
                next_at = cust["last_refill_at"] + period
                if next_at > now:
                    break
                granted = min(amount, room)
                cust["available"] += granted
                added_total += granted
                self._post_ledger(c, cust["id"], None, "refill",
                                  granted, 0, -granted,
                                  note=f"补充到账 +{granted}（计划 {amount}，"
                                       f"突发上限 {cust['burst_cap']}）")
                cust["last_refill_at"] = next_at
                if granted < amount:
                    break  # 被 burst_cap 截断，下一笔等下一个窗口
        c.execute(
            "UPDATE bg_customers SET available=?, last_refill_at=? WHERE id=?",
            (cust["available"], cust["last_refill_at"], cust["id"]))
        return added_total

    def _earliest_ready_locked(self, cust: dict, need: int,
                               ahead_need: int = 0) -> Optional[int]:
        """模拟离散补充，返回 need+ahead_need 额度最早齐备的时刻。

        ahead_need 是 FIFO 上排在本工单之前、当前也没拿到钱的工单的需求
        之和（队头阻塞：不能越过前面的工单先拿资格）。超过突发上限或停止
        补充时返回 None（永远等不到）。
        """
        amount, period = cust["refill_amount"], cust["period_seconds"]
        if amount <= 0 or period <= 0:
            return None
        sim_avail = cust["available"]
        sim_outstanding = cust["available"] + cust["reserved_total"]
        sim_last = cust["last_refill_at"]
        target = need + ahead_need
        guard = 0
        while sim_avail < target:
            next_at = sim_last + period
            granted = min(amount, cust["burst_cap"] - sim_outstanding)
            if granted <= 0:
                return None
            sim_avail += granted
            sim_outstanding += granted
            sim_last = next_at
            guard += 1
            if guard > 1_000_000:
                return None
        return sim_last

    def _annotate_locked(self, c, cust: dict) -> None:
        """重算延后区/暂停队列的最早可执行时间（FIFO 队头阻塞）。"""
        rows = c.execute(
            "SELECT * FROM bg_jobs WHERE customer_id=? AND state IN ('waiting',"
            "'paused') ORDER BY enq_seq, id", (cust["id"],)).fetchall()
        ahead = 0
        for r in rows:
            r = dict(r)
            need = (r["estimated"] if r["state"] == "waiting"
                    else r["pending_total"] - r["held_total"])
            earliest = None if need <= 0 else self._earliest_ready_locked(
                cust, need, ahead)
            c.execute(
                "UPDATE bg_jobs SET earliest_ready_at=? WHERE id=?",
                (earliest, r["id"]))
            if need > 0:
                ahead += need

    def _pump_locked(self, c, cust: dict) -> None:
        """补充后/余额可能变化时：推进补充 → 泵送队列 → 派发席位。

        必须在已持有该客户事务（BEGIN IMMEDIATE）时调用。
        """
        self._catchup_locked(c, cust)
        self._fund_queue_locked(c, cust)
        self._dispatch_locked(c)

    def _fund_queue_locked(self, c, cust: dict) -> int:
        """按 FIFO 给 waiting/paused 工单发钱（初始 RESERVE 或恢复 EXTEND）。

        遇到第一个钱不够的队头即停（队头阻塞，后面不得插队），随后重算
        各工单 earliest_ready_at。返回取得资格的工单数。
        """
        rows = c.execute(
            "SELECT * FROM bg_jobs WHERE customer_id=? AND state IN ('waiting',"
            "'paused') ORDER BY enq_seq, id", (cust["id"],)).fetchall()
        funded = 0
        for r in rows:
            r = dict(r)
            need = (r["estimated"] if r["state"] == "waiting"
                    else r["pending_total"] - r["held_total"])
            if need <= 0:
                continue
            if cust["available"] < need:
                break
            cust["available"] -= need
            cust["reserved_total"] += need
            now = self.now()
            if r["state"] == "waiting":
                self._write_voucher(c, r["id"], "reserve", need,
                                    r["estimated"], r["hold_epoch"])
                self._post_ledger(c, cust["id"], r["id"], "reserve",
                                  -need, need, 0, r["hold_epoch"],
                                  note="延后工单取得执行资格")
                c.execute("UPDATE bg_jobs SET held_total=? WHERE id=?",
                          (r["estimated"], r["id"]))
                self._audit(c, "bg_resume", f"bgjob:{r['id']}",
                            f"工单 #{r['id']} 延后区唤醒：预留 {need}")
            else:
                new_held = r["held_total"] + need
                self._write_voucher(c, r["id"], "extend", need, new_held,
                                    r["hold_epoch"])
                self._post_ledger(c, cust["id"], r["id"], "extend",
                                  -need, need, 0, r["hold_epoch"],
                                  note=f"暂停工单额度补齐：追加 {need}")
                c.execute("UPDATE bg_jobs SET held_total=? WHERE id=?",
                          (new_held, r["id"]))
                self._audit(c, "bg_resume", f"bgjob:{r['id']}",
                            f"工单 #{r['id']} 暂停恢复：追加预留 {need}")
            c.execute(
                "UPDATE bg_customers SET available=?, reserved_total=? WHERE id=?",
                (cust["available"], cust["reserved_total"], cust["id"]))
            c.execute(
                "UPDATE bg_jobs SET state='ready', blocked_reason=NULL, "
                "earliest_ready_at=NULL, pending_total=0, updated_at=? WHERE id=?",
                (now, r["id"]))
            funded += 1
        self._annotate_locked(c, cust)
        return funded

    def _write_voucher(self, c, job_id: int, kind: str, amount: int,
                       cumulative: int, claim_epoch: int) -> None:
        seq = c.execute(
            "SELECT COALESCE(MAX(seq),0)+1 AS s FROM bg_vouchers WHERE job_id=?",
            (job_id,)).fetchone()["s"]
        c.execute(
            "INSERT INTO bg_vouchers(job_id,seq,kind,amount,cumulative,"
            "claim_epoch,at) VALUES(?,?,?,?,?,?,?)",
            (job_id, seq, kind, amount, cumulative, claim_epoch, self.now()))

    def _dispatch_locked(self, c) -> None:
        """全局席位派发：ready 工单按 enq_seq 最早者入场（hold_epoch +1）。

        入场后 holder 仍为 NULL，等待执行器 claim_next 绑定；last_heartbeat
        从入场时刻起算。暂停客户的 ready 工单不派发（补充与预留照常）。
        SQLite 的 BEGIN IMMEDIATE 是全库写锁，席位计数在同一事务内读改写，
        因此跨客户的派发同样原子、不会超卖席位。
        """
        while True:
            used = self._seats_used(c)
            if used >= self.total_seats:
                return
            row = c.execute(
                "SELECT j.* FROM bg_jobs j JOIN bg_customers cu ON cu.id=j.customer_id "
                "WHERE j.state='ready' AND cu.paused=0 "
                "ORDER BY j.enq_seq, j.id LIMIT 1").fetchone()
            if row is None:
                return
            j = dict(row)
            new_epoch = j["hold_epoch"] + 1
            now = self.now()
            c.execute(
                "UPDATE bg_jobs SET state='running', hold_epoch=?, holder=NULL, "
                "last_heartbeat_at=?, updated_at=? WHERE id=?",
                (new_epoch, now, now, j["id"]))
            self._audit(
                c, "bg_dispatch", f"bgjob:{j['id']}",
                f"工单 #{j['id']}（客户 #{j['customer_id']}）取得执行席位，"
                f"凭证纪元 epoch={new_epoch}，凭证面额 {j['held_total']}")

    def claim_next(self, holder: str, customer_ref=None) -> Optional[dict]:
        """执行器领取下一个可跑工单：推进补充/唤醒后，原子“派发+绑定”。

        优先绑定已经入场但尚无持有者的 running 工单（系统 tick 提前派发的
        情形）；否则在同一事务内把最早的 ready 工单派发给该 holder。
        无坑位/无就绪工单时返回 None。接管 lost 工单请用 takeover。
        """
        if not holder:
            raise BudgetError("holder 不能为空")
        if customer_ref is not None:
            self._pump_customer(customer_ref)
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            now = self.now()
            row = c.execute(
                "SELECT j.* FROM bg_jobs j JOIN bg_customers cu ON cu.id=j.customer_id "
                "WHERE j.state='running' AND j.holder IS NULL AND cu.paused=0 "
                "ORDER BY j.enq_seq, j.id LIMIT 1").fetchone()
            if row is None and self._seats_used(c) < self.total_seats:
                row = c.execute(
                    "SELECT j.* FROM bg_jobs j JOIN bg_customers cu "
                    "ON cu.id=j.customer_id WHERE j.state='ready' AND cu.paused=0 "
                    "ORDER BY j.enq_seq, j.id LIMIT 1").fetchone()
                if row is not None:
                    j0 = dict(row)
                    c.execute(
                        "UPDATE bg_jobs SET state='running', hold_epoch=?, "
                        "holder=?, last_heartbeat_at=?, updated_at=? WHERE id=?",
                        (j0["hold_epoch"] + 1, holder, now, now, j0["id"]))
                    jid = j0["id"]
                    epoch = j0["hold_epoch"] + 1
                else:
                    jid = None
            elif row is not None:
                j0 = dict(row)
                c.execute(
                    "UPDATE bg_jobs SET holder=?, last_heartbeat_at=?, "
                    "updated_at=? WHERE id=?", (holder, now, now, j0["id"]))
                jid, epoch = j0["id"], j0["hold_epoch"]
            else:
                jid = None
            if jid is not None:
                job = self._job_row(c, jid)
                self._audit(
                    c, "bg_claim", f"bgjob:{jid}",
                    f"执行器 {holder} 领取工单 #{jid}（epoch={epoch}），"
                    f"从已确认位置 {job['used_total']}/{job['estimated']} 执行")
            c.execute("COMMIT")
        except Exception:
            self._rollback(c)
            raise
        return self.get_job(jid) if jid is not None else None

    def _pump_customer(self, ref) -> None:
        """公开操作前的统一入口：推进某客户补充/泵送/派发。"""
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            cust = self._customer_row(c, ref)
            self._pump_locked(c, cust)
            c.execute("COMMIT")
        except Exception:
            self._rollback(c)
            raise

    # ============================================================== 预算策略

    def _validate_policy(self, refill_amount: int, period_seconds: int,
                         burst_cap: int, initial: Optional[int] = None) -> None:
        for name, v in (("refill_amount", refill_amount),
                        ("period_seconds", period_seconds),
                        ("burst_cap", burst_cap)):
            if not isinstance(v, int) or isinstance(v, bool) or v < 0:
                raise BudgetError(f"{name} 必须是 >= 0 的整数")
        if period_seconds <= 0:
            raise BudgetError("period_seconds 必须 > 0")
        if burst_cap <= 0:
            raise BudgetError("burst_cap 必须 > 0")
        if refill_amount > burst_cap:
            raise BudgetError(
                f"refill_amount({refill_amount}) 不能大于 burst_cap({burst_cap})")
        if initial is not None:
            if (not isinstance(initial, int) or isinstance(initial, bool)
                    or initial < 0):
                raise BudgetError("initial 必须是 >= 0 的整数")
            if initial > burst_cap:
                raise BudgetError(
                    f"initial({initial}) 不能大于 burst_cap({burst_cap})")

    def create_customer(self, name: str, refill_amount: int,
                        period_seconds: int, burst_cap: int,
                        initial: Optional[int] = None) -> dict:
        """创建客户预算。已存在则原样返回（绝不覆盖运维中的策略）。"""
        self._validate_policy(refill_amount, period_seconds, burst_cap, initial)
        start = min(initial if initial is not None else burst_cap, burst_cap)
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            row = c.execute("SELECT id FROM bg_customers WHERE name=?",
                            (name,)).fetchone()
            if row is not None:
                c.execute("COMMIT")
                return self.get_customer(name)
            now = self.now()
            c.execute(
                "INSERT INTO bg_customers(name,refill_amount,period_seconds,"
                "burst_cap,available,reserved_total,last_refill_at,paused,"
                "created_at,updated_at) VALUES(?,?,?,?,?,0,?,0,?,?)",
                (name, refill_amount, period_seconds, burst_cap, start,
                 now, now, now))
            cid = c.execute("SELECT id FROM bg_customers WHERE name=?",
                            (name,)).fetchone()[0]
            # 期初余额记一笔 refill（d_settled 为负：期初授予未结算总额）
            if start:
                self._post_ledger(c, cid, None, "refill", start, 0, -start,
                                  note=f"期初余额 {start}（突发上限 {burst_cap}）")
            self._audit(c, "bg_customer_created", f"customer:{cid}",
                        f"客户 {name} 创建：每 {period_seconds}s 补充 "
                        f"{refill_amount}，突发上限 {burst_cap}，期初 {start}")
            c.execute("COMMIT")
        except Exception:
            self._rollback(c)
            raise
        return self.get_customer(name)

    def update_policy(self, name: str,
                      refill_amount: Optional[int] = None,
                      period_seconds: Optional[int] = None,
                      burst_cap: Optional[int] = None,
                      paused: Optional[bool] = None) -> dict:
        """在线调整预算策略。

        **只影响后续补充与后续预留**：不修改 available/reserved，不撤销任何
        已发出凭证（bg_vouchers 行永不变），已 running/paused 的工单照旧。
        调小 burst_cap 不回收存量；补充时自然按新上限封顶。
        """
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            t = self._customer_row(c, name)
            new_refill = t["refill_amount"] if refill_amount is None else refill_amount
            new_period = t["period_seconds"] if period_seconds is None else period_seconds
            new_cap = t["burst_cap"] if burst_cap is None else burst_cap
            new_paused = t["paused"] if paused is None else int(bool(paused))
            self._validate_policy(new_refill, new_period, new_cap)
            now = self.now()
            c.execute(
                "UPDATE bg_customers SET refill_amount=?, period_seconds=?, "
                "burst_cap=?, paused=?, updated_at=? WHERE id=?",
                (new_refill, new_period, new_cap, new_paused, now, t["id"]))
            changes = []
            if refill_amount is not None and refill_amount != t["refill_amount"]:
                changes.append(f"补充 {t['refill_amount']} -> {new_refill}")
            if period_seconds is not None and period_seconds != t["period_seconds"]:
                changes.append(f"周期 {t['period_seconds']} -> {new_period}s")
            if burst_cap is not None and burst_cap != t["burst_cap"]:
                changes.append(f"突发上限 {t['burst_cap']} -> {new_cap}")
            if paused is not None and int(bool(paused)) != t["paused"]:
                changes.append(f"暂停 {'否->是' if new_paused else '是->否'}")
            if changes:
                self._audit(c, "bg_policy", f"customer:{t['id']}",
                            f"客户 {t['name']} 预算策略变更：{'; '.join(changes)}"
                            "（只影响后续预留，不撤销已发出凭证）")
            cust = self._customer_row(c, t["id"])
            # 新周期可能立刻有补充到账；暂停只阻止派发，不阻止补充入账
            self._catchup_locked(c, cust)
            self._annotate_locked(c, cust)
            if not new_paused:
                self._dispatch_locked(c)
            c.execute("COMMIT")
        except Exception:
            self._rollback(c)
            raise
        return self.get_customer(name)

    def pause(self, name: str) -> dict:
        """暂停：补充照常到账，但不自动派发（演示/运维闸门）。"""
        return self.update_policy(name, paused=True)

    def resume(self, name: str) -> dict:
        return self.update_policy(name, paused=False)

    def list_customers(self) -> list[dict]:
        return [self._customer_snapshot(dict(r)) for r in self.db.conn()
                .execute("SELECT * FROM bg_customers ORDER BY id").fetchall()]

    def get_customer(self, ref) -> dict:
        return self._customer_snapshot(
            self._customer_row(self.db.conn(), ref))

    def _next_refill_at(self, cust: dict) -> Optional[int]:
        if cust["refill_amount"] <= 0:
            return None
        return cust["last_refill_at"] + cust["period_seconds"]

    def _customer_snapshot(self, t: dict) -> dict:
        # 视图口径：
        # - settled：已结算的实际消耗合计（settle 行的 d_settled 之和）；
        # - granted：累计授予（期初+补充）；可用+预留+已结算=累计授予。
        c = self.db.conn()
        agg = c.execute(
            "SELECT COALESCE(SUM(CASE WHEN op='settle' THEN d_settled ELSE 0 "
            "END),0) AS consumed, COALESCE(SUM(CASE WHEN op='refill' THEN "
            "d_available ELSE 0 END),0) AS granted FROM bg_ledger "
            "WHERE customer_id=?", (t["id"],)).fetchone()
        return {
            "id": t["id"], "name": t["name"],
            "refill_amount": t["refill_amount"],
            "period_seconds": t["period_seconds"],
            "burst_cap": t["burst_cap"],
            "available": t["available"],
            "reserved": t["reserved_total"],
            "settled": agg["consumed"],
            "granted_total": agg["granted"],
            "outstanding": t["available"] + t["reserved_total"],
            "paused": bool(t["paused"]),
            "last_refill_at": t["last_refill_at"],
            "next_refill_at": self._next_refill_at(t),
            "updated_at": t["updated_at"],
        }

    # ============================================================== 工单提交

    def submit_job(self, customer_ref, estimated: int, business_key: str,
                   payload: Optional[dict] = None) -> dict:
        """提交工单并原子预留预计消耗。

        余额充足：available -= estimated，发 RESERVE 凭证，state=ready
        （随后尝试派发席位）。余额不足：**不扣一分钱**，进延后区 state=
        waiting，标 blocked_reason=insufficient_funds 与 earliest_ready_at。
        """
        if not isinstance(estimated, int) or isinstance(estimated, bool) \
                or estimated <= 0:
            raise BudgetError("estimated 必须是 > 0 的整数")
        if not business_key:
            raise BudgetError("business_key 不能为空")
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            cust = self._customer_row(c, customer_ref)
            existing = c.execute(
                "SELECT * FROM bg_jobs WHERE customer_id=? AND business_key=?",
                (cust["id"], business_key)).fetchone()
            if existing is not None:
                ex = dict(existing)
                if ex["estimated"] != estimated:
                    self._rollback(c)
                    raise BudgetConflict(
                        f"业务键 {business_key} 已用于客户 {cust['name']} 的另一个"
                        f"工单（现有预估 {ex['estimated']}）：同键不允许不同预估",
                        code="business_key_conflict")
                c.execute("COMMIT")
                return {"replayed": True, "job": self.get_job(ex["id"])}
            if estimated > cust["burst_cap"]:
                self._rollback(c)
                raise BudgetConflict(
                    f"工单预估 {estimated} 超过客户 {cust['name']} 的突发上限 "
                    f"{cust['burst_cap']}：任何时刻都无法预留，请先调预算",
                    code="over_burst_cap")
            self._catchup_locked(c, cust)
            now = self.now()
            seq = c.execute(
                "SELECT COALESCE(MAX(enq_seq),0)+1 AS s FROM bg_jobs "
                "WHERE customer_id=?", (cust["id"],)).fetchone()["s"]
            cur = c.execute(
                "INSERT INTO bg_jobs(customer_id,business_key,payload_json,"
                "estimated,held_total,used_total,settled_total,refunded_total,"
                "state,hold_epoch,enq_seq,created_at,updated_at) "
                "VALUES(?,?,?,?,?,0,0,0,'waiting',0,?,?,?)",
                (cust["id"], business_key, canonical(payload), estimated, 0,
                 seq, now, now))
            jid = cur.lastrowid
            if cust["available"] >= estimated:
                # 原子预留：检查与扣款在同一个 IMMEDIATE 事务内，
                # 并发提交抢最后一份额度时只有一个事务能走到这里。
                cust["available"] -= estimated
                cust["reserved_total"] += estimated
                c.execute(
                    "UPDATE bg_customers SET available=?, reserved_total=? "
                    "WHERE id=?",
                    (cust["available"], cust["reserved_total"], cust["id"]))
                self._write_voucher(c, jid, "reserve", estimated, estimated, 0)
                self._post_ledger(c, cust["id"], jid, "reserve",
                                  -estimated, estimated, 0, 0,
                                  note=f"提交即预留预计消耗 {estimated}")
                c.execute(
                    "UPDATE bg_jobs SET state='ready', held_total=?, "
                    "pending_total=0, updated_at=? WHERE id=?",
                    (estimated, now, jid))
                self._audit(c, "bg_reserve", f"bgjob:{jid}",
                            f"工单 #{jid} 预留 {estimated}，取得执行资格")
                self._annotate_locked(c, cust)
                if not cust["paused"]:
                    self._dispatch_locked(c)
                outcome = "reserved"
            else:
                c.execute(
                    "UPDATE bg_jobs SET blocked_reason='insufficient_funds' "
                    "WHERE id=?", (jid,))
                self._annotate_locked(c, cust)
                jr = self._job_row(c, jid)
                self._audit(
                    c, "bg_deferred", f"bgjob:{jid}",
                    f"工单 #{jid} 余额不足进入延后区：需 {estimated}，"
                    f"可用 {cust['available']}，最早可执行 "
                    f"{jr['earliest_ready_at']}")
                outcome = "deferred"
            c.execute("COMMIT")
        except Exception:
            self._rollback(c)
            raise
        return {"replayed": False, "outcome": outcome,
                "job": self.get_job(jid)}

    def list_jobs(self, customer_ref=None, state: Optional[str] = None,
                  limit: int = 200) -> list[dict]:
        c = self.db.conn()
        sql = ("SELECT j.*, cu.name AS customer_name FROM bg_jobs j "
               "JOIN bg_customers cu ON cu.id=j.customer_id")
        args: list = []
        conds = []
        if customer_ref is not None:
            t = self._customer_row(c, customer_ref)
            conds.append("j.customer_id=?")
            args.append(t["id"])
        if state:
            if state not in JOB_STATES:
                raise BudgetError(f"state 必须是 {JOB_STATES} 之一")
            conds.append("j.state=?")
            args.append(state)
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        sql += " ORDER BY j.id DESC LIMIT ?"
        args.append(limit)
        return [self._job_snapshot(dict(r))
                for r in c.execute(sql, args).fetchall()]

    def get_job(self, job_id: int) -> dict:
        row = self.db.conn().execute(
            "SELECT j.*, cu.name AS customer_name FROM bg_jobs j "
            "JOIN bg_customers cu ON cu.id=j.customer_id WHERE j.id=?",
            (job_id,)).fetchone()
        if row is None:
            raise BudgetError(f"预算工单不存在: {job_id}")
        return self._job_snapshot(dict(row))

    def _job_snapshot(self, j: dict) -> dict:
        j["customer"] = j.pop("customer_name")
        j["payload"] = json.loads(j.pop("payload_json") or "{}")
        return j

    def list_vouchers(self, job_id: int) -> list[dict]:
        return [dict(r) for r in self.db.conn().execute(
            "SELECT * FROM bg_vouchers WHERE job_id=? ORDER BY seq",
            (job_id,)).fetchall()]

    # ============================================================== fence

    def _fenced(self, c, job_id: int, hold_epoch: int,
                holder: str) -> dict:
        if not isinstance(hold_epoch, int) or isinstance(hold_epoch, bool) \
                or hold_epoch < 1:
            raise BudgetError("hold_epoch 必须是 >= 1 的整数")
        if not holder:
            raise BudgetError("holder 不能为空")
        job = self._job_row(c, job_id)
        if job["state"] != "running":
            raise BudgetConflict(
                f"工单 #{job_id} 当前状态为 {job['state']}，不接受执行期上报"
                f"（epoch={hold_epoch}）；工单可能已终止、完成或被接管",
                code="not_running")
        if job["hold_epoch"] != hold_epoch:
            raise BudgetConflict(
                f"工单 #{job_id} 的凭证纪元 epoch={hold_epoch} 已过期：当前为 "
                f"{job['hold_epoch']}（席位已被接替），旧持有者的上报拒绝、不扣费",
                code="stale_epoch")
        if job["holder"] != holder:
            raise BudgetConflict(
                f"工单 #{job_id} 当前由 {job['holder']} 持有，{holder} 的上报拒绝",
                code="worker_mismatch")
        if self.now() >= job["last_heartbeat_at"] + self.lease_ttl:
            raise BudgetConflict(
                f"工单 #{job_id} 的心跳已于 {job['last_heartbeat_at']}+"
                f"{self.lease_ttl}s 超时，请先被接管再上报",
                code="lease_expired")
        return job

    def attach(self, job_id: int, holder: str) -> dict:
        """执行器把自己绑定到刚取得席位的工单（ready 队列由系统派发为
        running 时尚无持有者）。幂等：同一 holder 重复绑定返回原行。"""
        if not holder:
            raise BudgetError("holder 不能为空")
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            job = self._job_row(c, job_id)
            if job["state"] != "running":
                raise BudgetConflict(
                    f"工单 #{job_id} 状态 {job['state']}，只有 running 工单可绑定",
                    code="not_running")
            if job["holder"] is not None and job["holder"] != holder:
                raise BudgetConflict(
                    f"工单 #{job_id} 已由 {job['holder']} 绑定",
                    code="worker_mismatch")
            if job["holder"] is None:
                c.execute(
                    "UPDATE bg_jobs SET holder=?, last_heartbeat_at=?, "
                    "updated_at=? WHERE id=?",
                    (holder, self.now(), self.now(), job_id))
                self._audit(c, "bg_attach", f"bgjob:{job_id}",
                            f"工单 #{job_id} 执行器 {holder} 绑定席位 "
                            f"epoch={job['hold_epoch']}，已结算位置 "
                            f"{job['used_total']}/{job['estimated']}")
            c.execute("COMMIT")
        except Exception:
            self._rollback(c)
            raise
        return self.get_job(job_id)

    def heartbeat(self, job_id: int, hold_epoch: int, holder: str) -> dict:
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            job = self._fenced(c, job_id, hold_epoch, holder)
            c.execute(
                "UPDATE bg_jobs SET last_heartbeat_at=?, updated_at=? WHERE id=?",
                (self.now(), self.now(), job_id))
            c.execute("COMMIT")
        except Exception:
            self._rollback(c)
            raise
        return self.get_job(job_id)

    def extend_reservation(self, job_id: int, hold_epoch: int, holder: str,
                           extra: int) -> dict:
        """执行期显式追加预留（知道自己即将超预估时主动申请）。

        钱不够返回 blocked（state 不变），调用方可随后自行决定是否退出
        席位（mark_paused）；正常超额暂停由 report 的原子路径处理。
        """
        if not isinstance(extra, int) or isinstance(extra, bool) or extra <= 0:
            raise BudgetError("extra 必须是 > 0 的整数")
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            job = self._fenced(c, job_id, hold_epoch, holder)
            cust = self._customer_row(c, job["customer_id"])
            if cust["available"] < extra:
                self._rollback(c)
                return {"granted": False, "reason": "insufficient_funds",
                        "job": self.get_job(job_id)}
            cust["available"] -= extra
            cust["reserved_total"] += extra
            new_held = job["held_total"] + extra
            c.execute(
                "UPDATE bg_customers SET available=?, reserved_total=? WHERE id=?",
                (cust["available"], cust["reserved_total"], cust["id"]))
            self._write_voucher(c, job_id, "extend", extra, new_held,
                                hold_epoch)
            self._post_ledger(c, cust["id"], job_id, "extend",
                              -extra, extra, 0, hold_epoch,
                              note=f"执行期显式追加预留 {extra}")
            c.execute(
                "UPDATE bg_jobs SET held_total=?, updated_at=? WHERE id=?",
                (new_held, self.now(), job_id))
            self._audit(c, "bg_extend", f"bgjob:{job_id}",
                        f"工单 #{job_id} 追加预留 {extra}，凭证面额 "
                        f"{job['held_total']} -> {new_held}")
            self._annotate_locked(c, cust)
            c.execute("COMMIT")
        except Exception:
            self._rollback(c)
            raise
        return {"granted": True, "job": self.get_job(job_id)}

    def report(self, job_id: int, hold_epoch: int, holder: str,
               used_total: int) -> dict:
        """按单调进度上报**累计**实际消耗并结算。

        - used 后退：409 used_regress；used == 当前已确认：幂等重放；
        - used <= held：把 held 中 (used-old) 从 reserved 结转为 settled；
        - used > held：尝试原子“追加预留(used-held) + 结算”；余额不足则
          只结算到 held（held 全部转 settled），工单 paused、释放席位。
        """
        if not isinstance(used_total, int) or isinstance(used_total, bool) \
                or used_total < 0:
            raise BudgetError("used_total 必须是 >= 0 的整数")
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            job = self._fenced(c, job_id, hold_epoch, holder)
            if used_total < job["used_total"]:
                raise BudgetConflict(
                    f"实际消耗只能单调增长：已确认 {job['used_total']}，"
                    f"拒绝回退到 {used_total}",
                    code="used_regress")
            cust = self._customer_row(c, job["customer_id"])
            now = self.now()
            paused = False
            if used_total > job["used_total"]:
                target = min(used_total, job["held_total"])
                delta = target - job["used_total"]
                if delta:
                    # 已确认消耗：reserved -> settled
                    cust["reserved_total"] -= delta
                    c.execute(
                        "UPDATE bg_customers SET reserved_total=? WHERE id=?",
                        (cust["reserved_total"], cust["id"]))
                    self._post_ledger(c, cust["id"], job_id, "settle",
                                      0, -delta, delta, hold_epoch,
                                      note=f"进度结算到 {target}")
                    c.execute(
                        "UPDATE bg_jobs SET used_total=?, settled_total=?, "
                        "updated_at=? WHERE id=?",
                        (target, target, now, job_id))
                    job["used_total"] = target
                    job["settled_total"] = target
                if used_total > job["held_total"]:
                    extra = used_total - job["held_total"]
                    self._catchup_locked(c, cust)
                    if cust["available"] >= extra:
                        # 原子追加预留 + 结算到新位置（同一事务，不可分片）
                        cust["available"] -= extra
                        cust["reserved_total"] += extra
                        new_held = job["held_total"] + extra
                        c.execute(
                            "UPDATE bg_customers SET available=?, "
                            "reserved_total=? WHERE id=?",
                            (cust["available"], cust["reserved_total"],
                             cust["id"]))
                        self._write_voucher(c, job_id, "extend", extra,
                                            new_held, hold_epoch)
                        self._post_ledger(c, cust["id"], job_id, "extend",
                                          -extra, extra, 0, hold_epoch,
                                          note=f"超额自动追加预留 {extra}")
                        cust["reserved_total"] -= extra
                        c.execute(
                            "UPDATE bg_customers SET reserved_total=? WHERE id=?",
                            (cust["reserved_total"], cust["id"]))
                        self._post_ledger(c, cust["id"], job_id, "settle",
                                          0, -extra, extra, hold_epoch,
                                          note=f"追加部分即时结算到 {used_total}")
                        c.execute(
                            "UPDATE bg_jobs SET held_total=?, used_total=?, "
                            "settled_total=?, updated_at=? WHERE id=?",
                            (new_held, used_total, used_total, now, job_id))
                        self._audit(
                            c, "bg_extend", f"bgjob:{job_id}",
                            f"工单 #{job_id} 实际消耗超预估，原子追加预留 "
                            f"{extra} 并结算到 {used_total}，继续执行")
                    else:
                        # 没钱追加：停在最近已确认位置（=held_total），
                        # 立即释放执行席位，等补充后从该位置恢复。
                        c.execute(
                            "UPDATE bg_jobs SET state='paused', holder=NULL, "
                            "blocked_reason='need_extend', pending_total=?, "
                            "last_heartbeat_at=NULL, updated_at=? WHERE id=?",
                            (used_total, now, job_id))
                        self._audit(
                            c, "bg_paused", f"bgjob:{job_id}",
                            f"工单 #{job_id} 实际消耗 {used_total} 超出凭证 "
                            f"{job['held_total']} 且追加预留失败：结算停在 "
                            f"{job['held_total']}，释放执行席位等待额度")
                        paused = True
                        self._annotate_locked(c, cust)
                if not paused:
                    c.execute(
                        "UPDATE bg_jobs SET last_heartbeat_at=? WHERE id=?",
                        (now, job_id))
            # paused 释放了席位：尝试把席位给别的 ready 工单
            self._dispatch_locked(c)
            c.execute("COMMIT")
        except Exception:
            self._rollback(c)
            raise
        out = self.get_job(job_id)
        out["paused_for_budget"] = paused
        if paused:
            out["confirmed_position"] = out["held_total"]
        return out

    def complete(self, job_id: int, hold_epoch: int, holder: str,
                 final_total: Optional[int] = None) -> dict:
        """完成工单：最终结算 + 退还未用部分；终局唯一。

        final_total 给定时按其最终结算（同样走“超额原子追加或暂停”规则：
        钱不够则不能完成，工单 paused）。与 terminate 竞争时后到者拿到
        already_terminal 结果，绝不二次结算/退款。
        """
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            job = self._job_row(c, job_id)
            if job["state"] in ("succeeded", "terminated"):
                c.execute("COMMIT")
                out = self.get_job(job_id)
                out["already_terminal"] = True
                return out
            self._fenced(c, job_id, hold_epoch, holder)  # 非 running/旧 epoch → 409
            cust = self._customer_row(c, job["customer_id"])
            total = (job["used_total"] if final_total is None
                     else final_total)
            if not isinstance(total, int) or isinstance(total, bool) \
                    or total < job["used_total"]:
                raise BudgetConflict(
                    f"最终消耗 {total} 不能低于已确认位置 {job['used_total']}",
                    code="used_regress")
            now = self.now()
            if total > job["held_total"]:
                extra = total - job["held_total"]
                self._catchup_locked(c, cust)
                if cust["available"] < extra:
                    # 最终结算也没钱：暂停，让出席位，等额度到账后再 complete
                    c.execute(
                        "UPDATE bg_jobs SET state='paused', holder=NULL, "
                        "blocked_reason='need_extend', pending_total=?, "
                        "last_heartbeat_at=NULL, updated_at=? WHERE id=?",
                        (total, now, job_id))
                    self._annotate_locked(c, cust)
                    self._dispatch_locked(c)
                    c.execute("COMMIT")
                    out = self.get_job(job_id)
                    out["paused_for_budget"] = True
                    out["confirmed_position"] = job["held_total"]
                    return out
                cust["available"] -= extra
                cust["reserved_total"] += extra
                new_held = job["held_total"] + extra
                c.execute(
                    "UPDATE bg_customers SET available=?, reserved_total=? "
                    "WHERE id=?",
                    (cust["available"], cust["reserved_total"], cust["id"]))
                self._write_voucher(c, job_id, "extend", extra, new_held,
                                    hold_epoch)
                self._post_ledger(c, cust["id"], job_id, "extend",
                                  -extra, extra, 0, hold_epoch,
                                  note=f"完成前追加预留 {extra}")
                job["held_total"] = new_held
            # 结算到最终值
            settle_delta = total - job["used_total"]
            if settle_delta:
                cust["reserved_total"] -= settle_delta
                self._post_ledger(c, cust["id"], job_id, "settle",
                                  0, -settle_delta, settle_delta, hold_epoch,
                                  note=f"完成最终结算到 {total}")
            # 退未用部分（held - total）
            refund = job["held_total"] - total
            if refund:
                cust["available"] += refund
                cust["reserved_total"] -= refund
                self._post_ledger(c, cust["id"], job_id, "refund",
                                  refund, -refund, 0, hold_epoch,
                                  note=f"完成退还未用凭证 {refund}")
            c.execute(
                "UPDATE bg_customers SET available=?, reserved_total=? WHERE id=?",
                (cust["available"], cust["reserved_total"], cust["id"]))
            c.execute(
                "UPDATE bg_jobs SET state='succeeded', used_total=?, "
                "settled_total=?, refunded_total=refunded_total+?, holder=NULL, "
                "blocked_reason=NULL, earliest_ready_at=NULL, "
                "last_heartbeat_at=NULL, finished_at=?, updated_at=? WHERE id=?",
                (total, total, refund, now, now, job_id))
            self._audit(
                c, "bg_complete", f"bgjob:{job_id}",
                f"工单 #{job_id} 完成：结算 {total}，退还未用 {refund}，"
                f"凭证 {job['held_total']} 结清")
            # 退款增加了 available，同客户延后队列可能因此凑够资格
            self._fund_queue_locked(c, cust)
            self._dispatch_locked(c)
            c.execute("COMMIT")
        except Exception:
            self._rollback(c)
            raise
        return self.get_job(job_id)

    def _pump_after_money(self, c, cust: dict) -> None:
        """终局退款后尝试唤醒同客户延后队列并派发（FIFO，不越队头）。

        暂停客户同样做补充/预留（fund），但不派发（ready 工单等 resume）。
        """
        self._fund_queue_locked(c, cust)
        if not cust["paused"]:
            self._dispatch_locked(c)

    # ============================================================== 失联/接管

    def reap_lost(self) -> list[dict]:
        """心跳超时的 running 工单：标 lost、释放席位，可被直接接管。

        已结算金额（used_total）原样保留；hold_epoch 不变（接管派发时 +1，
        保证接替者纪元更新）。
        """
        c = self.db.conn()
        reaped: list[dict] = []
        try:
            c.execute("BEGIN IMMEDIATE")
            now = self.now()
            rows = c.execute(
                "SELECT * FROM bg_jobs WHERE state='running' AND "
                "last_heartbeat_at + ? < ?", (self.lease_ttl, now)).fetchall()
            for r in rows:
                r = dict(r)
                c.execute(
                    "UPDATE bg_jobs SET state='lost', holder=NULL, "
                    "last_heartbeat_at=NULL, updated_at=? WHERE id=?",
                    (now, r["id"]))
                self._audit(
                    c, "bg_lost", f"bgjob:{r['id']}",
                    f"工单 #{r['id']} 心跳超时失联（旧持有者 {r['holder']} "
                    f"epoch={r['hold_epoch']}）：释放席位，已结算 "
                    f"{r['used_total']}/{r['estimated']} 保留，等待接管")
                reaped.append(r)
            self._dispatch_locked(c)
            c.execute("COMMIT")
        except Exception:
            self._rollback(c)
            raise
        return [self.get_job(r["id"]) for r in reaped]

    def takeover(self, job_id: int, holder: str) -> dict:
        """接替失联（lost）工单：沿用已结算金额，取得新席位与新 epoch。

        ready/paused 的等待由系统自动派发；本接口用于显式接管 lost 工单。
        """
        if not holder:
            raise BudgetError("holder 不能为空")
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            job = self._job_row(c, job_id)
            if job["state"] != "lost":
                raise BudgetConflict(
                    f"工单 #{job_id} 状态为 {job['state']}，只有 lost 工单可接管",
                    code="not_running")
            if self._seats_used(c) >= self.total_seats:
                self._rollback(c)
                raise BudgetConflict("执行席位已满，无法接管",
                                     code="seats_full")
            new_epoch = job["hold_epoch"] + 1
            now = self.now()
            c.execute(
                "UPDATE bg_jobs SET state='running', hold_epoch=?, holder=?, "
                "last_heartbeat_at=?, updated_at=? WHERE id=?",
                (new_epoch, holder, now, now, job_id))
            self._audit(
                c, "bg_takeover", f"bgjob:{job_id}",
                f"工单 #{job_id} 被 {holder} 接管：epoch "
                f"{job['hold_epoch']} -> {new_epoch}，沿用已结算位置 "
                f"{job['used_total']}/{job['estimated']}，凭证 {job['held_total']}")
            c.execute("COMMIT")
        except Exception:
            self._rollback(c)
            raise
        return self.get_job(job_id)

    # ============================================================== 终止

    def terminate(self, job_id: int, reason: Optional[str] = None) -> dict:
        """终止工单：只退尚未使用的部分（held - used）。幂等。

        - waiting：未发出凭证，零退款直接终止；
        - ready / paused / lost：退全部 held-used（paused/lost 时 held 已
          全部结算，退款通常为 0；paused 若估算>held 则等待中的需求作废）；
        - running：持有者失格（epoch 不增加，旧持有者任何后续上报 409）；
        - 终局重复终止：replayed=True，不二次退款；
        - 与 complete 竞争：先落事务者赢，后到者 already_terminal。
        """
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            job = self._job_row(c, job_id)
            if job["state"] in ("succeeded", "terminated"):
                c.execute("COMMIT")
                out = self.get_job(job_id)
                out["replayed"] = True
                out["already_terminal"] = True
                return out
            cust = self._customer_row(c, job["customer_id"])
            now = self.now()
            refund = max(0, job["held_total"] - job["used_total"])
            if refund:
                cust["available"] += refund
                cust["reserved_total"] -= refund
                self._post_ledger(c, cust["id"], job_id, "refund",
                                  refund, -refund, 0, job["hold_epoch"],
                                  note=f"终止退还未用部分 {refund}")
            c.execute(
                "UPDATE bg_customers SET available=?, reserved_total=? WHERE id=?",
                (cust["available"], cust["reserved_total"], cust["id"]))
            c.execute(
                "UPDATE bg_jobs SET state='terminated', holder=NULL, "
                "blocked_reason=NULL, earliest_ready_at=NULL, "
                "last_heartbeat_at=NULL, finished_at=?, updated_at=?, "
                "refunded_total=refunded_total+? WHERE id=?",
                (now, now, refund, job_id))
            self._audit(
                c, "bg_terminate", f"bgjob:{job_id}",
                f"工单 #{job_id} 终止（{job['state']}）：已结算 "
                f"{job['used_total']}，退还未用 {refund}"
                + (f"，原因：{reason}" if reason else ""))
            self._annotate_locked(c, cust)
            self._pump_after_money(c, cust)
            c.execute("COMMIT")
        except Exception:
            self._rollback(c)
            raise
        return self.get_job(job_id)

    # ============================================================== 时钟/查询

    def tick(self) -> dict:
        """推进所有客户的补充到账、唤醒延后区、派发席位、回收失联工单。

        服务模式由后台线程周期调用；演示/测试用手工时钟时显式调用即可。
        """
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            customers = [dict(r) for r in c.execute(
                "SELECT * FROM bg_customers ORDER BY id").fetchall()]
            for cust in customers:
                self._catchup_locked(c, cust)
                # 暂停只阻止派发：补充与预留（延后区取得资格）照常，
                # 工单停在 ready，等 resume 后入场。
                self._fund_queue_locked(c, cust)
                if not cust["paused"]:
                    self._dispatch_locked(c)
            # 失联回收（之后立即派发空出席位）
            now = self.now()
            lost = c.execute(
                "SELECT id FROM bg_jobs WHERE state='running' AND "
                "last_heartbeat_at + ? < ?", (self.lease_ttl, now)).fetchall()
            for r in lost:
                c.execute(
                    "UPDATE bg_jobs SET state='lost', holder=NULL, "
                    "last_heartbeat_at=NULL, updated_at=? WHERE id=?",
                    (now, r["id"]))
            self._dispatch_locked(c)
            c.execute("COMMIT")
        except Exception:
            self._rollback(c)
            raise
        return {"time": self.now(), "lost": [r["id"] for r in lost]}

    def ledger(self, customer_ref=None, job_id: Optional[int] = None,
               limit: Optional[int] = None) -> list[dict]:
        c = self.db.conn()
        if limit is None:
            limit = self.ledger_limit
        sql = ("SELECT l.*, cu.name AS customer_name FROM bg_ledger l "
               "JOIN bg_customers cu ON cu.id=l.customer_id")
        args: list = []
        conds = []
        if customer_ref is not None:
            t = self._customer_row(c, customer_ref)
            conds.append("l.customer_id=?")
            args.append(t["id"])
        if job_id is not None:
            conds.append("l.job_id=?")
            args.append(job_id)
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        sql += " ORDER BY l.id DESC LIMIT ?"
        args.append(limit)
        return [dict(r) for r in c.execute(sql, args).fetchall()]

    def blocked_jobs(self) -> list[dict]:
        """被预算阻塞的工单：waiting（余额不足）与 paused（超额待追加）。"""
        rows = self.db.conn().execute(
            "SELECT j.*, cu.name AS customer_name FROM bg_jobs j "
            "JOIN bg_customers cu ON cu.id=j.customer_id "
            "WHERE j.state IN ('waiting','paused') "
            "ORDER BY j.customer_id, j.enq_seq, j.id").fetchall()
        return [self._job_snapshot(dict(r)) for r in rows]

    def status(self) -> dict:
        """状态页数据：可用额/已预留/已结算/下次补充时间 + 被阻塞工单。"""
        c = self.db.conn()
        customers = [dict(r) for r in c.execute(
            "SELECT * FROM bg_customers ORDER BY id").fetchall()]
        out_customers = []
        totals = {"available": 0, "reserved": 0, "settled": 0}
        for t in customers:
            snap = self._customer_snapshot(t)
            out_customers.append(snap)
            totals["available"] += snap["available"]
            totals["reserved"] += snap["reserved"]
            totals["settled"] += snap["settled"]
        jobs = [self._job_snapshot(dict(r)) for r in c.execute(
            "SELECT j.*, cu.name AS customer_name FROM bg_jobs j "
            "JOIN bg_customers cu ON cu.id=j.customer_id "
            "ORDER BY j.id DESC LIMIT 100").fetchall()]
        return {
            "customers": out_customers,
            "jobs": jobs,
            "blocked": self.blocked_jobs(),
            "totals": totals,
            "seats_used": self._seats_used(c),
            "total_seats": self.total_seats,
            "lease_ttl": self.lease_ttl,
            "time": self.now(),
        }

    # ============================================================== 对账

    def verify_invariants(self) -> dict:
        """对账：复式台账平衡、三栏与客户行一致、available 永不为负。

        供演示/测试在每轮后调用；任何不成立都抛 AssertionError。
        """
        c = self.db.conn()
        row = c.execute(
            "SELECT COALESCE(SUM(d_available),0) a, "
            "COALESCE(SUM(d_reserved),0) r, COALESCE(SUM(d_settled),0) s, "
            "COALESCE(SUM(d_available+d_reserved+d_settled),0) flat "
            "FROM bg_ledger").fetchone()
        assert row["flat"] == 0, f"复式台账不平：{dict(row)}"
        for t in c.execute("SELECT * FROM bg_customers ORDER BY id").fetchall():
            t = dict(t)
            agg = c.execute(
                "SELECT COALESCE(SUM(d_available),0) a, "
                "COALESCE(SUM(d_reserved),0) r, "
                "COALESCE(SUM(d_settled),0) s FROM bg_ledger "
                "WHERE customer_id=?", (t["id"],)).fetchone()
            assert agg["a"] == t["available"], (
                f"客户 {t['name']} available 台账 {agg['a']} != 行 {t['available']}")
            assert agg["r"] == t["reserved_total"], (
                f"客户 {t['name']} reserved 台账 {agg['r']} != 行 "
                f"{t['reserved_total']}")
            assert t["available"] >= 0, f"客户 {t['name']} available 透支"
            assert t["available"] + t["reserved_total"] <= t["burst_cap"] + 10**-9
            # reserved 与未终局工单的 (held - used - refunded) 合计一致
            jobsum = c.execute(
                "SELECT COALESCE(SUM(held_total-used_total-refunded_total),0) s "
                "FROM bg_jobs WHERE customer_id=? AND state IN "
                "('ready','running','waiting','lost')", (t["id"],)).fetchone()
            # paused 工单 held 已全部结算，不占 reserved
            assert jobsum["s"] == t["reserved_total"], (
                f"客户 {t['name']} 工单占用预留 {jobsum['s']} != "
                f"reserved_total {t['reserved_total']}")
        # running 工单数不超过席位
        assert self._seats_used(c) <= self.total_seats, "执行席位超卖"
        return {"ok": True, "customers": len(c.execute(
            "SELECT id FROM bg_customers").fetchall()),
            "ledger_balance": {"available": row["a"], "reserved": row["r"],
                               "settled": row["s"]}}
