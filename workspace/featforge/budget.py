"""容量凭证与预算结算。

语义总览
========
每个客户一份预算策略：按固定周期补充的计算额度（refill_amount /
refill_interval）、余额突发上限 burst_cap、执行席位 max_seats。

工单提交时必须声明预计消耗 estimate，系统在一个 ``BEGIN IMMEDIATE`` 事务内
**原子预留**：余额足够立即开出凭证（reserved）；余额不足进入延后区
（deferred）并标出最早可执行时刻 earliest_ready_at；estimate 本身超过
突发上限的工单永远不可行（over_burst_cap）。补充采用惰性计算：任何触碰
余额的操作先把 last_refill_at 按周期整倍推进，补充到账后同一事务内 pump
延后区/暂停凭证，够额即自动取得执行资格（FIFO，按最早可执行时刻排序）。

执行期间凭证被“占用”（executing）并占据客户的执行席位，每次占用/接管让
claim_epoch 单调 +1（fencing）。执行者按单调进度上报累计实际消耗：
  - 实际消耗 ≤ 已预留：直接结算，不退款；
  - 超出已预留且能追加预留：原子追加后结算；
  - 追加不出来：只结算到已预留位置，凭证停在 paused、释放席位，等额度到账
    后自动恢复 reserved，旧持有者的 epoch 已失效，必须重新占用。
租约超时后进程失联：席位释放、凭证回到可占用状态但 **claim_epoch 不变**，
接替者重新占用时 epoch +1；旧持有者的迟到上报命中 stale_epoch，已结算金额
沿用，绝不二次扣费。

终态（completed / terminated）全表唯一：重复完成、重复终止、完成与终止
竞争都只产生一次退款/最终结算；终止只退还尚未使用的预留
（reserved_total − settled_total），退款同样受突发上限裁剪，裁掉部分记
lost_total 与 cap_clamp 账本。账本只追加，任意时刻都能由账本重放出
余额、已预留未结算、已结算三个总额。

在线调整预算策略只修改客户行，绝不撤销已经发出的凭证：缩小突发上限会把
当前余额向下裁剪（cap_clamp 留痕），只影响后续预留。
"""
from __future__ import annotations

import threading
import time
from typing import Callable, Optional

from .config import settings
from .db import Database

# 凭证状态
ST_DEFERRED = "deferred"
ST_RESERVED = "reserved"
ST_EXECUTING = "executing"
ST_PAUSED = "paused"
ST_COMPLETED = "completed"
ST_TERMINATED = "terminated"
TERMINAL_STATES = (ST_COMPLETED, ST_TERMINATED)
NON_TERMINAL_STATES = (ST_DEFERRED, ST_RESERVED, ST_EXECUTING, ST_PAUSED)


class BudgetError(Exception):
    """请求本身不合法（HTTP 400 语义）。"""


class BudgetConflict(Exception):
    """fencing/幂等/席位冲突（HTTP 409 语义）。

    ``code``：stale_epoch / lease_expired / not_executing /
    worker_mismatch / seats_full / progress_regress / job_key_conflict /
    terminal
    """

    def __init__(self, message: str, code: str = "conflict"):
        super().__init__(message)
        self.code = code


class BudgetService:
    def __init__(
        self,
        db: Database,
        clock: Optional[Callable[[], int]] = None,
        lease_ttl: Optional[int] = None,
        auto_scheduler: bool = True,
    ):
        self.db = db
        self.now = clock or (lambda: int(time.time()))
        self.lease_ttl = (lease_ttl if lease_ttl is not None
                          else settings.budget_lease_ttl)
        # 每个客户一把条件变量：等待凭证可执行/可恢复时阻塞，补充到账或
        # 时钟推进唤醒。账本正确性只依赖 SQLite 事务，CV 仅用于及时唤醒。
        self._cond_lock = threading.RLock()
        self._conds: dict[int, threading.Condition] = {}
        self._stop_scheduler = False
        self._scheduler: Optional[threading.Thread] = None
        if auto_scheduler:
            self.start_scheduler()

    # ------------------------------------------------------------ 生命周期

    def close(self) -> None:
        self.stop_scheduler()

    def start_scheduler(self) -> None:
        """后台线程：到点后惰性补充 + pump + 回收失联租约。

        所有金额逻辑都是惰性的，没有这个线程系统也正确（下一次 API 调用
        会补算）；线程只负责让延后工单“到点自动醒来”。
        """
        if self._scheduler is not None:
            return
        self._stop_scheduler = False
        t = threading.Thread(target=self._scheduler_loop, daemon=True,
                             name="budget-scheduler")
        self._scheduler = t
        t.start()

    def stop_scheduler(self) -> None:
        self._stop_scheduler = True
        with self._cond_lock:
            for cv in self._conds.values():
                cv.notify_all()
        t = self._scheduler
        if t is not None and t.is_alive():
            t.join(timeout=2)
        self._scheduler = None

    def _scheduler_loop(self) -> None:
        while not self._stop_scheduler:
            try:
                self.tick()
            except Exception:
                pass
            time.sleep(min(1.0, max(0.05, settings.budget_tick_seconds)))

    def _cond(self, customer_id: int) -> threading.Condition:
        with self._cond_lock:
            cv = self._conds.get(customer_id)
            if cv is None:
                cv = threading.Condition(self._cond_lock)
                self._conds[customer_id] = cv
            return cv

    def _notify(self, customer_id: int) -> None:
        with self._cond_lock:
            cv = self._conds.get(customer_id)
            if cv is not None:
                cv.notify_all()

    # ============================================================== 策略

    @staticmethod
    def _validate_policy(refill_amount: int, refill_interval: int,
                         burst_cap: int, max_seats: int) -> None:
        for name, v in (("refill_amount", refill_amount),
                        ("refill_interval", refill_interval),
                        ("burst_cap", burst_cap),
                        ("max_seats", max_seats)):
            if not isinstance(v, int) or isinstance(v, bool) or v < 0:
                raise BudgetError(f"{name} 必须是 >= 0 的整数")
        if refill_amount > burst_cap:
            raise BudgetError("refill_amount 不能大于 burst_cap（单期补充"
                              "无法在突发上限内容纳）")
        if refill_interval < 0:
            raise BudgetError("refill_interval 必须 >= 0；0 表示不自动补充")
        if max_seats < 1:
            raise BudgetError("max_seats 必须 >= 1")

    def configure_customer(self, name: str, refill_amount: int,
                           refill_interval: int, burst_cap: int,
                           max_seats: int = 1,
                           opening_balance: Optional[int] = None) -> dict:
        """创建客户预算。已存在时原样返回，绝不覆盖运行中的策略。"""
        self._validate_policy(refill_amount, refill_interval,
                              burst_cap, max_seats)
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            row = c.execute("SELECT id FROM budget_customers WHERE name=?",
                            (name,)).fetchone()
            if row is not None:
                c.execute("COMMIT")
                return self.get_customer(name)
            now = self.now()
            bal = burst_cap if opening_balance is None else opening_balance
            if bal < 0 or bal > burst_cap:
                raise BudgetError(
                    f"opening_balance 必须在 0..{burst_cap} 之间，收到 {bal}")
            cur = c.execute(
                "INSERT INTO budget_customers(name,refill_amount,"
                "refill_interval,burst_cap,max_seats,balance,last_refill_at,"
                "created_at,updated_at) VALUES(?,?,?,?,?,0,?,?,?)",
                (name, refill_amount, refill_interval, burst_cap, max_seats,
                 now, now, now))
            cid = cur.lastrowid
            if bal:
                # 余额只由账本产生：INSERT 0，期初余额作为第一笔 refill，
                # 保证 balance 永远等于 ledger 重放值。
                self._append(c, now, cid, None, "refill", avail=bal,
                             note="opening")
            self._audit(c, "budget_customer_created", f"bcustomer:{cid}",
                        f"预算客户 {name} 创建：每期 +{refill_amount}/"
                        f"{refill_interval}s，突发上限 {burst_cap}，"
                        f"席位 {max_seats}，期初余额 {bal}")
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        return self.get_customer(name)

    def adjust_policy(self, customer_ref,
                      refill_amount: Optional[int] = None,
                      refill_interval: Optional[int] = None,
                      burst_cap: Optional[int] = None,
                      max_seats: Optional[int] = None) -> dict:
        """在线调整预算策略：只影响后续预留，不撤销任何已发出的凭证。

        缩小 burst_cap 时把当前余额向下裁剪，差额写 cap_clamp 账本；
        调大上限不自动补钱，下次补充自然到账。
        """
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            cust = self._customer_row(c, customer_ref)
            now = self.now()
            self._refill_locked(c, cust, now)
            new_amount = (cust["refill_amount"] if refill_amount is None
                          else refill_amount)
            new_interval = (cust["refill_interval"] if refill_interval is None
                            else refill_interval)
            new_cap = cust["burst_cap"] if burst_cap is None else burst_cap
            new_seats = cust["max_seats"] if max_seats is None else max_seats
            self._validate_policy(new_amount, new_interval, new_cap, new_seats)
            lost = 0
            if cust["balance"] > new_cap:
                lost = cust["balance"] - new_cap
                self._append(c, now, cust["id"], None, "cap_clamp",
                             avail=-lost, note=f"burst_cap {cust['burst_cap']}"
                                              f" -> {new_cap}")
            changes = []
            if refill_amount is not None and refill_amount != cust["refill_amount"]:
                changes.append(f"每期补充 {cust['refill_amount']} -> {new_amount}")
            if refill_interval is not None and refill_interval != cust["refill_interval"]:
                changes.append(f"周期 {cust['refill_interval']}s -> {new_interval}s")
                # 周期变了，锚点重置为当前时刻，避免立刻补出一大笔或欠补
                cust["last_refill_at"] = now
            if burst_cap is not None and burst_cap != cust["burst_cap"]:
                changes.append(f"突发上限 {cust['burst_cap']} -> {new_cap}")
            if max_seats is not None and max_seats != cust["max_seats"]:
                changes.append(f"席位 {cust['max_seats']} -> {new_seats}")
            c.execute(
                "UPDATE budget_customers SET refill_amount=?, refill_interval=?,"
                "burst_cap=?, max_seats=?, last_refill_at=?, updated_at=? "
                "WHERE id=?",
                (new_amount, new_interval, new_cap, new_seats,
                 cust["last_refill_at"], now, cust["id"]))
            if changes:
                self._audit(c, "budget_policy", f"bcustomer:{cust['id']}",
                            f"客户 {cust['name']} 预算策略调整："
                            f"{'; '.join(changes)}（不撤销已发出凭证，"
                            "只影响后续预留）")
            # 重新读取最新行（cap_clamp 后余额已变），立刻尝试解锁延后/暂停
            # 凭证：例如调高突发上限/补充额后，有些工单可能当场可执行。
            cust = self._customer_row(c, cust["id"])
            self._pump_locked(c, cust, now)
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        cid = self._as_id(customer_ref)
        self._notify(cid)
        return self.get_customer(cid)

    def get_customer(self, ref) -> dict:
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            cust = self._customer_row(c, ref)
            self._refill_locked(c, cust, self.now())
            out = self._customer_view(c, cust)
            c.execute("COMMIT")
            return out
        except Exception:
            c.execute("ROLLBACK")
            raise

    def list_customers(self) -> list[dict]:
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            now = self.now()
            out = []
            rows = c.execute(
                "SELECT * FROM budget_customers ORDER BY id").fetchall()
            for r in rows:
                cust = dict(r)
                self._refill_locked(c, cust, now)
                out.append(self._customer_view(c, cust))
            c.execute("COMMIT")
            return out
        except Exception:
            c.execute("ROLLBACK")
            raise

    # ============================================================== 提交/预留

    def submit(self, customer_ref, job_key: str, estimate: int,
               submitter: str = "submitter") -> dict:
        """工单提交：原子预留 estimate。

        余额足 → reserved（凭证已开出）；不足 → deferred 并标最早可执行
        时刻；estimate > burst_cap → deferred 且 over_burst_cap（永不可行）。
        同客户同 job_key 重放：同参数返回原凭证，异参 409。
        """
        if not job_key:
            raise BudgetError("job_key 不能为空")
        if not isinstance(estimate, int) or isinstance(estimate, bool) \
                or estimate <= 0:
            raise BudgetError("estimate 必须是 > 0 的整数")
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            cust = self._customer_row(c, customer_ref)
            now = self.now()
            existing = c.execute(
                "SELECT * FROM budget_vouchers WHERE customer_id=? AND job_key=?",
                (cust["id"], job_key)).fetchone()
            if existing is not None:
                ex = dict(existing)
                if ex["estimate"] != estimate or ex["submitter"] != submitter:
                    raise BudgetConflict(
                        f"job_key {job_key} 已存在（estimate={ex['estimate']}），"
                        "同键不允许提交不同参数", code="job_key_conflict")
                c.execute("COMMIT")
                return self.get_voucher(ex["id"])
            self._refill_locked(c, cust, now)
            cur = c.execute(
                "INSERT INTO budget_vouchers(customer_id,job_key,submitter,"
                "estimate,reserved_total,settled_total,state,submitted_at) "
                "VALUES(?,?,?,?,0,0,'deferred',?)",
                (cust["id"], job_key, submitter, estimate, now))
            vid = cur.lastrowid
            # 统一走 pump：新单与既有延后单按最早可执行时刻 FIFO 决胜，
            # 不能插队抢走更早排队工单的额度。
            self._pump_locked(c, cust, now)
            v = self._voucher_row(c, vid)
            self._audit(c, "voucher_submitted", f"bvoucher:{vid}",
                        f"凭证 #{vid}（客户 {cust['name']} job={job_key}）"
                        f"提交预估 {estimate}：{v['state']}"
                        + (f"，最早可执行 {v['earliest_ready_at']}"
                           if v["state"] == ST_DEFERRED else ""))
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        self._notify(self._as_id(customer_ref))
        return self.get_voucher(vid)

    def acquire(self, customer_ref, voucher_id: int, worker: str) -> dict:
        """占用一个可执行凭证（reserved），席位 +1，返回新的 claim_epoch。

        若凭证仍 executing 但租约已超时（持有者失联），本调用即“接替”：
        epoch +1 后旧持有者 fence 失效，已结算金额原样沿用。
        executing 且租约仍有效 → 409 seats_full/stale。
        deferred/paused → 409（未取得执行资格）。
        """
        if not worker:
            raise BudgetError("worker 不能为空")
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            cust, v = self._cust_voucher(c, customer_ref, voucher_id)
            now = self.now()
            self._refill_locked(c, cust, now)
            self._pump_locked(c, cust, now)
            v = self._voucher_row(c, voucher_id)
            if v["state"] in TERMINAL_STATES:
                raise BudgetConflict(
                    f"凭证 #{voucher_id} 已终态（{v['state']}），不可占用",
                    code="terminal")
            if v["state"] in (ST_DEFERRED, ST_PAUSED):
                raise BudgetConflict(
                    f"凭证 #{voucher_id} 状态 {v['state']}：尚未取得执行"
                    "资格，不能占用", code="not_ready")
            if v["state"] == ST_EXECUTING:
                if (v["lease_expires_at"] is not None
                        and now < v["lease_expires_at"]):
                    raise BudgetConflict(
                        f"凭证 #{voucher_id} 正由 {v['claim_worker']} 执行中"
                        f"（epoch={v['claim_epoch']}，租约 {v['lease_expires_at']}"
                        " 才到期）", code="seats_full")
                # 租约超时（或已被 reap 清空）：旧持有者失联，接替
                # （epoch 在下面统一 +1；超时凭证不占席位，见 _seats_used）
                self._audit(c, "voucher_lease_expired",
                            f"bvoucher:{voucher_id}",
                            f"凭证 #{voucher_id} 持有者 {v['claim_worker']} "
                            f"失联（epoch={v['claim_epoch']} 到期 "
                            f"{v['lease_expires_at']}），席位释放等待接替，"
                            f"已结算 {v['settled_total']} 沿用")
            seats_used = self._seats_used(c, cust["id"], now)
            if seats_used >= cust["max_seats"]:
                raise BudgetConflict(
                    f"客户 {cust['name']} 执行席位已满（{seats_used}/"
                    f"{cust['max_seats']}）", code="seats_full")
            new_epoch = v["claim_epoch"] + 1
            expires = now + self.lease_ttl
            c.execute(
                "UPDATE budget_vouchers SET state='executing', claim_epoch=?, "
                "claim_worker=?, claimed_at=?, lease_expires_at=?, "
                "last_heartbeat_at=?, attempts=attempts+1 WHERE id=?",
                (new_epoch, worker, now, expires, now, voucher_id))
            self._audit(c, "voucher_acquired", f"bvoucher:{voucher_id}",
                        f"凭证 #{voucher_id} 被 {worker} 占用，epoch="
                        f"{new_epoch}"
                        + ("（接替失联持有者）" if v["state"] == ST_EXECUTING
                           else ""))
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        self._notify(cust["id"])
        return self.get_voucher(voucher_id)

    def heartbeat(self, customer_ref, voucher_id: int, epoch: int,
                  worker: str) -> dict:
        """执行心跳：fence 校验后续租，证明持有者仍然存活。"""
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            cust, v = self._cust_voucher(c, customer_ref, voucher_id)
            now = self.now()
            self._fence(v, epoch, worker, now)
            c.execute(
                "UPDATE budget_vouchers SET lease_expires_at=?, "
                "last_heartbeat_at=? WHERE id=?",
                (now + self.lease_ttl, now, voucher_id))
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        return self.get_voucher(voucher_id)

    def report_consumed(self, customer_ref, voucher_id: int, epoch: int,
                        worker: str, consumed_total: int) -> dict:
        """按单调进度上报累计实际消耗并结算。

        返回字段 action：
          - settled：正常结算（含追加预留成功）；
          - paused：超出预估且追加预留不足，停在已预留位置、释放席位；
            response.confirmed_total 是最近已确认位置，requested_total
            是执行者想要的位置；
          - idempotent：重复/回退上报（consumed_total <= 已结算）原样确认。
        旧 epoch / 错误持有者 / 租约超时 → 409，绝不扣费。
        """
        if not isinstance(consumed_total, int) or isinstance(consumed_total, bool) \
                or consumed_total < 0:
            raise BudgetError("consumed_total 必须是 >= 0 的整数")
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            cust, v = self._cust_voucher(c, customer_ref, voucher_id)
            now = self.now()
            self._refill_locked(c, cust, now)
            self._pump_locked(c, cust, now)
            v = self._voucher_row(c, voucher_id)
            self._fence(v, epoch, worker, now)
            if consumed_total <= v["settled_total"]:
                # 幂等重放或迟到重复上报：确认到既有位置，不再扣费
                c.execute("COMMIT")
                out = self.get_voucher(voucher_id)
                out["action"] = "idempotent"
                out["confirmed_total"] = v["settled_total"]
                out["requested_total"] = consumed_total
                return out
            action = "settled"
            confirmed = consumed_total
            if consumed_total > v["reserved_total"]:
                need = consumed_total - v["reserved_total"]
                # 先 pump 过后仍可能不够（别的凭证先拿到额度 / 未到补充点）
                if cust["balance"] >= need:
                    self._reserve_locked(c, cust, v["id"], need, now,
                                         kind="reserve_extra",
                                         note="overrun")
                    v = self._voucher_row(c, voucher_id)
                else:
                    # 追加预留失败：只结算到已预留位置，停在 paused，
                    # 释放执行席位（claim_worker/租约清空，epoch 保留）
                    confirmed = v["reserved_total"]
                    self._settle_locked(c, cust, v,
                                        confirmed - v["settled_total"], now)
                    c.execute(
                        "UPDATE budget_vouchers SET state='paused', "
                        "pending_total=?, claim_worker=NULL, claimed_at=NULL, "
                        "lease_expires_at=NULL, last_heartbeat_at=NULL "
                        "WHERE id=?",
                        (consumed_total, voucher_id))
                    self._audit(c, "voucher_paused", f"bvoucher:{voucher_id}",
                                f"凭证 #{voucher_id} 实际消耗请求到 "
                                f"{consumed_total}，追加预留 {need} 不足，"
                                f"停在已确认位置 {confirmed} 并释放席位，"
                                f"等待额度到账（持有者 {worker} epoch="
                                f"{epoch} fence 保留）")
                    action = "paused"
                    c.execute("COMMIT")
                    self._notify(cust["id"])
                    out = self.get_voucher(voucher_id)
                    out["action"] = action
                    out["confirmed_total"] = confirmed
                    out["requested_total"] = consumed_total
                    return out
            self._settle_locked(c, cust, v,
                                consumed_total - v["settled_total"], now)
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        self._notify(cust["id"])
        out = self.get_voucher(voucher_id)
        out["action"] = action
        out["confirmed_total"] = confirmed
        out["requested_total"] = consumed_total
        return out

    def complete(self, customer_ref, voucher_id: int, epoch: int,
                 worker: str, consumed_total: Optional[int] = None) -> dict:
        """完成工单：最终结算（必要时追加预留），退还剩余预留，释放席位。

        终态唯一：已经 completed/terminated 时返回 409 terminal（不产生任何
        第二笔结算/退款）。完成与终止并发时由数据库事务决出一个胜者。
        """
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            cust, v = self._cust_voucher(c, customer_ref, voucher_id)
            now = self.now()
            self._refill_locked(c, cust, now)
            self._pump_locked(c, cust, now)
            v = self._voucher_row(c, voucher_id)
            if v["state"] in TERMINAL_STATES:
                raise BudgetConflict(
                    f"凭证 #{voucher_id} 已是终态 {v['state']}（结局 "
                    f"{v['outcome']}），最终结算只允许一次",
                    code="terminal")
            if consumed_total is None:
                consumed_total = v["settled_total"]
            if (not isinstance(consumed_total, int)
                    or isinstance(consumed_total, bool)
                    or consumed_total < v["settled_total"]):
                raise BudgetConflict(
                    f"完成时消耗 {consumed_total} 不能小于已结算 "
                    f"{v['settled_total']}（进度只能单调前进）",
                    code="progress_regress")
            self._fence(v, epoch, worker, now, allow_states=ST_EXECUTING)
            if consumed_total > v["reserved_total"]:
                need = consumed_total - v["reserved_total"]
                if cust["balance"] < need:
                    # 完成时超额也必须先追加预留；额度不足则与上报路径一致：
                    # 停在最近已确认位置（已预留）、释放席位、进入 paused，
                    # 不允许在没额度的情况下“完成”。
                    confirmed = v["reserved_total"]
                    self._settle_locked(c, cust, v,
                                        confirmed - v["settled_total"], now)
                    c.execute(
                        "UPDATE budget_vouchers SET state='paused', "
                        "pending_total=?, claim_worker=NULL, claimed_at=NULL, "
                        "lease_expires_at=NULL, last_heartbeat_at=NULL "
                        "WHERE id=?",
                        (consumed_total, voucher_id))
                    self._audit(c, "voucher_paused", f"bvoucher:{voucher_id}",
                                f"凭证 #{voucher_id} 完成时需追加 {need} 但"
                                f"余额不足，停在 {confirmed} 暂停等额度")
                    c.execute("COMMIT")
                    self._notify(cust["id"])
                    out = self.get_voucher(voucher_id)
                    out["action"] = "paused"
                    out["confirmed_total"] = confirmed
                    out["requested_total"] = consumed_total
                    return out
                self._reserve_locked(c, cust, v["id"], need, now,
                                     kind="reserve_extra",
                                     note="complete_overrun")
                v = self._voucher_row(c, voucher_id)
            final_settle = consumed_total - v["settled_total"]
            if final_settle:
                self._settle_locked(c, cust, v, final_settle, now)
                v = self._voucher_row(c, voucher_id)
            refund, lost = self._refund_unused(c, cust, v, now,
                                               reason="complete")
            c.execute(
                "UPDATE budget_vouchers SET state='completed', outcome="
                "'completed', finalized_at=?, claim_worker=NULL, claimed_at="
                "NULL, lease_expires_at=NULL, last_heartbeat_at=NULL, "
                "pending_total=NULL WHERE id=?",
                (now, voucher_id))
            self._audit(c, "voucher_completed", f"bvoucher:{voucher_id}",
                        f"凭证 #{voucher_id} 完成：最终结算 {consumed_total}，"
                        f"退还未用预留 {refund}"
                        + (f"（{lost} 超突发上限裁剪）" if lost else "")
                        + f"（worker={worker} epoch={epoch}）")
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        self._notify(cust["id"])
        out = self.get_voucher(voucher_id)
        out["settled_final"] = consumed_total
        out["refunded"] = refund
        out["lost"] = lost
        return out

    def terminate(self, customer_ref, voucher_id: int,
                  reason: str = "manual") -> dict:
        """终止工单：只退还尚未使用的预留（reserved_total − settled_total）。

        幂等：重复终止返回同一个终态结果（refunded 为 0，不再退第二次）。
        与 complete 竞争时先到事务者定终态；后到的 complete 收 409。
        失联持有者无需 fence：终止是提交者/运维侧动作。
        """
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            cust, v = self._cust_voucher(c, customer_ref, voucher_id)
            now = self.now()
            if v["state"] in TERMINAL_STATES:
                # 终态唯一：重复终止 / 完成后终止都走这里，幂等返回
                c.execute("COMMIT")
                out = self.get_voucher(voucher_id)
                out["replayed"] = True
                out["refunded"] = 0
                out["lost"] = 0
                return out
            self._refill_locked(c, cust, now)
            v = self._voucher_row(c, voucher_id)
            refund, lost = self._refund_unused(c, cust, v, now,
                                               reason=f"terminate:{reason}")
            c.execute(
                "UPDATE budget_vouchers SET state='terminated', outcome="
                "'terminated', terminate_reason=?, finalized_at=?, "
                "claim_worker=NULL, claimed_at=NULL, lease_expires_at=NULL, "
                "last_heartbeat_at=NULL, pending_total=NULL WHERE id=?",
                (reason, now, voucher_id))
            self._audit(c, "voucher_terminated", f"bvoucher:{voucher_id}",
                        f"凭证 #{voucher_id} 终止（{reason}）：已结算 "
                        f"{v['settled_total']}，退还未用预留 {refund}"
                        + (f"（{lost} 超突发上限裁剪）" if lost else ""))
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        self._notify(cust["id"])
        out = self.get_voucher(voucher_id)
        out["replayed"] = False
        out["refunded"] = refund
        out["lost"] = lost
        return out

    # ------------------------------------------------------------ 失联回收

    def _reap_locked(self, c, now: int) -> list[dict]:
        """在已持有的事务内回收失联凭证（tick 与 reap_expired 共用）。"""
        reaped = []
        rows = c.execute(
            "SELECT * FROM budget_vouchers WHERE state='executing' "
            "AND lease_expires_at IS NOT NULL AND lease_expires_at<=?",
            (now,)).fetchall()
        for r in rows:
            r = dict(r)
            c.execute(
                "UPDATE budget_vouchers SET claim_worker=NULL, "
                "claimed_at=NULL, lease_expires_at=NULL, "
                "last_heartbeat_at=NULL WHERE id=?",
                (r["id"],))
            self._audit(c, "voucher_lease_expired",
                        f"bvoucher:{r['id']}",
                        f"凭证 #{r['id']} 租约超时回收：旧持有者 "
                        f"{r['claim_worker']} epoch={r['claim_epoch']} "
                        f"失效，已结算 {r['settled_total']} 沿用")
            reaped.append({"id": r["id"], "customer_id": r["customer_id"],
                           "old_epoch": r["claim_epoch"],
                           "old_worker": r["claim_worker"],
                           "settled_total": r["settled_total"]})
        return reaped

    def reap_expired(self) -> list[dict]:
        """回收失联持有者：executing 但租约超时的凭证释放席位，可被接替。

        状态保持 executing、claim_epoch 不变，接替时 epoch +1；不碰金额：
        已结算金额沿用，旧持有者之后的上报由 fence 拒绝。
        """
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            reaped = self._reap_locked(c, self.now())
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        for r in reaped:
            self._notify(r["customer_id"])
        return reaped

    def tick(self) -> dict:
        """一轮后台维护：补充 + pump + 失联回收（同一事务）。"""
        c = self.db.conn()
        touched: set[int] = set()
        n_reaped = 0
        try:
            c.execute("BEGIN IMMEDIATE")
            now = self.now()
            for r in c.execute(
                    "SELECT * FROM budget_customers").fetchall():
                cust = dict(r)
                before = cust["balance"]
                self._refill_locked(c, cust, now)
                promoted = self._pump_locked(c, cust, now)
                if promoted or cust["balance"] != before:
                    touched.add(cust["id"])
            for r in self._reap_locked(c, now):
                touched.add(r["customer_id"])
                n_reaped += 1
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        for cid in touched:
            self._notify(cid)
        return {"touched": sorted(touched), "reaped": n_reaped}

    # ------------------------------------------------------------ 等待

    def wait_runnable(self, customer_ref, voucher_id: int,
                      timeout: Optional[float] = None) -> dict:
        """阻塞到凭证取得执行资格（reserved）或进入终态。

        供演示/执行者使用；每次唤醒后走一次 acquire 语义的检查。
        """
        cid = self._as_id(customer_ref)
        deadline = None if timeout is None else time.time() + timeout
        cv = self._cond(cid)
        while True:
            v = self.get_voucher(voucher_id)
            if v["state"] in (ST_RESERVED, ST_EXECUTING) \
                    or v["state"] in TERMINAL_STATES:
                return v
            if deadline is not None and time.time() >= deadline:
                raise TimeoutError(f"凭证 #{voucher_id} 等待额度超时")
            with cv:
                remaining = None if deadline is None else max(0.0, deadline - time.time())
                cv.wait(timeout=min(1.0, remaining) if remaining is not None else 1.0)

    # ============================================================== 查询

    def get_voucher(self, voucher_id: int) -> dict:
        c = self.db.conn()
        r = c.execute(
            "SELECT v.*, cu.name AS customer_name FROM budget_vouchers v "
            "JOIN budget_customers cu ON cu.id=v.customer_id WHERE v.id=?",
            (voucher_id,)).fetchone()
        if r is None:
            raise BudgetError(f"凭证不存在: {voucher_id}")
        return self._voucher_view(dict(r))

    def list_vouchers(self, customer_ref=None, state: Optional[str] = None,
                      limit: int = 200) -> list[dict]:
        c = self.db.conn()
        sql = ("SELECT v.*, cu.name AS customer_name FROM budget_vouchers v "
               "JOIN budget_customers cu ON cu.id=v.customer_id")
        args: list = []
        conds = []
        if customer_ref is not None:
            cust = self._customer_row(c, customer_ref)
            conds.append("v.customer_id=?")
            args.append(cust["id"])
        if state:
            conds.append("v.state=?")
            args.append(state)
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        sql += " ORDER BY v.id DESC LIMIT ?"
        args.append(limit)
        return [self._voucher_view(dict(r))
                for r in c.execute(sql, args).fetchall()]

    def ledger(self, customer_ref=None, voucher_id: Optional[int] = None,
               limit: int = 200) -> list[dict]:
        c = self.db.conn()
        sql = ("SELECT l.*, cu.name AS customer_name FROM budget_ledger l "
               "JOIN budget_customers cu ON cu.id=l.customer_id")
        args: list = []
        conds = []
        if customer_ref is not None:
            cust = self._customer_row(c, customer_ref)
            conds.append("l.customer_id=?")
            args.append(cust["id"])
        if voucher_id is not None:
            conds.append("l.voucher_id=?")
            args.append(voucher_id)
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        sql += " ORDER BY l.id DESC LIMIT ?"
        args.append(limit)
        return [dict(r) for r in c.execute(sql, args).fetchall()]

    def status(self) -> dict:
        """状态页数据：每个客户的可用/已预留/已结算/下次补充时间 + 阻塞工单。"""
        return {"time": self.now(), "lease_ttl": self.lease_ttl,
                "customers": self.list_customers(),
                "vouchers": self.list_vouchers(limit=100)}

    # ============================================================== 内部

    def _as_id(self, ref) -> int:
        if isinstance(ref, int):
            return ref
        c = self.db.conn()
        r = c.execute("SELECT id FROM budget_customers WHERE name=?",
                      (ref,)).fetchone()
        if r is None:
            raise BudgetError(f"预算客户不存在: {ref}")
        return r[0]

    def _customer_row(self, c, ref) -> dict:
        if isinstance(ref, int):
            row = c.execute("SELECT * FROM budget_customers WHERE id=?",
                            (ref,)).fetchone()
        else:
            row = c.execute("SELECT * FROM budget_customers WHERE name=?",
                            (ref,)).fetchone()
        if row is None:
            raise BudgetError(f"预算客户不存在: {ref}")
        return dict(row)

    def _cust_voucher(self, c, customer_ref, voucher_id: int):
        cust = self._customer_row(c, customer_ref)
        r = c.execute("SELECT * FROM budget_vouchers WHERE id=?",
                      (voucher_id,)).fetchone()
        if r is None:
            raise BudgetError(f"凭证不存在: {voucher_id}")
        v = dict(r)
        if v["customer_id"] != cust["id"]:
            raise BudgetError(
                f"凭证 #{voucher_id} 不属于客户 {cust['name']}")
        return cust, v

    def _voucher_row(self, c, voucher_id: int) -> dict:
        r = c.execute("SELECT * FROM budget_vouchers WHERE id=?",
                      (voucher_id,)).fetchone()
        return dict(r)

    def _append(self, c, at: int, customer_id: int, voucher_id, kind: str,
                avail: int = 0, reserve: int = 0, settle: int = 0,
                note: str = "") -> None:
        """追加账本行并维护客户余额（balance_after == 重放余额）。"""
        row = c.execute(
            "SELECT balance FROM budget_customers WHERE id=?",
            (customer_id,)).fetchone()
        new_balance = row["balance"] + avail
        c.execute(
            "INSERT INTO budget_ledger(at,customer_id,voucher_id,kind,"
            "avail_delta,reserve_delta,settle_delta,balance_after,note) "
            "VALUES(?,?,?,?,?,?,?,?,?)",
            (at, customer_id, voucher_id, kind, avail, reserve, settle,
             new_balance, note))
        c.execute("UPDATE budget_customers SET balance=? WHERE id=?",
                  (new_balance, customer_id))

    def _refill_locked(self, c, cust: dict, now: int) -> None:
        """惰性补充：把锚点按周期整倍推进，补 min(amount, cap-balance)。"""
        interval = cust["refill_interval"]
        if not interval or now < cust["last_refill_at"]:
            return
        elapsed_periods = (now - cust["last_refill_at"]) // interval
        if elapsed_periods <= 0:
            return
        # 离散补充：每期到账一笔 refill_amount，受突发上限裁剪。跨多期时
        # 锚点整倍推进（余额仍只记一笔到账后的净额）。
        grant = min(cust["refill_amount"],
                    cust["burst_cap"] - cust["balance"])
        new_anchor = cust["last_refill_at"] + elapsed_periods * interval
        c.execute(
            "UPDATE budget_customers SET last_refill_at=? WHERE id=?",
            (new_anchor, cust["id"]))
        cust["last_refill_at"] = new_anchor
        if grant > 0:
            self._append(c, now, cust["id"], None, "refill", avail=grant,
                         note=f"+{cust['refill_amount']}/{interval}s")
            cust["balance"] += grant

    def _next_refill_at(self, cust: dict, now: int):
        interval = cust["refill_interval"]
        if not interval:
            return None
        anchor = cust["last_refill_at"]
        if now < anchor:
            return anchor
        return anchor + ((now - anchor) // interval + 1) * interval

    def _earliest_ready(self, cust: dict, amount: int, now: int):
        """延后凭证最早可执行时刻；不可行（超突发上限/永不补充）返回 None。

        与 _refill_locked 使用同一套锚点推进规则：先把锚点整倍推到最近
        的已过补充点（本期补充视为已到账，更早各期受突发上限限制蒸发），
        余额仍不足再逐期向后模拟。
        """
        if amount > cust["burst_cap"]:
            return None
        interval = cust["refill_interval"]
        if not interval or cust["refill_amount"] <= 0:
            return None if cust["balance"] < amount else now
        bal = cust["balance"]
        t = cust["last_refill_at"]
        if now >= t:
            periods = (now - t) // interval
            t += periods * interval
            if periods > 0:
                bal = min(cust["burst_cap"], bal + cust["refill_amount"])
        if bal >= amount:
            return now
        while bal < amount:
            t += interval
            bal = min(cust["burst_cap"], bal + cust["refill_amount"])
        return t

    def _reserve_locked(self, c, cust: dict, voucher_id: int, amount: int,
                        now: int, kind: str = "reserve",
                        note: str = "") -> None:
        """从余额原子预留 amount（调用方已确保 balance >= amount）。"""
        self._append(c, now, cust["id"], voucher_id, kind,
                     avail=-amount, reserve=amount, note=note)
        c.execute(
            "UPDATE budget_vouchers SET reserved_total=reserved_total+? "
            "WHERE id=?", (amount, voucher_id))
        cust["balance"] -= amount

    def _settle_locked(self, c, cust: dict, v: dict, amount: int,
                       now: int) -> None:
        """把已预留中的 amount 确认为已结算（余额不再变动）。"""
        if amount <= 0:
            return
        if amount > v["reserved_total"] - v["settled_total"]:
            raise AssertionError("结算金额超过未结算预留（程序不变量被破坏）")
        self._append(c, now, cust["id"], v["id"], "settle",
                     avail=0, reserve=-amount, settle=amount)
        c.execute(
            "UPDATE budget_vouchers SET settled_total=settled_total+? "
            "WHERE id=?", (amount, v["id"]))

    def _refund_unused(self, c, cust: dict, v: dict, now: int,
                       reason: str) -> tuple[int, int]:
        """退还未使用预留，返回 (实际退回, 被突发上限裁剪的损耗)。

        reserved − settled 才是“尚未使用”；退款记 reserve_delta 为负，
        avail_delta 为正。余额因此超过 burst_cap 的部分裁剪为 lost。
        """
        unused = v["reserved_total"] - v["settled_total"]
        if unused <= 0:
            return 0, 0
        room = cust["burst_cap"] - cust["balance"]
        refund = max(0, min(unused, room))
        lost = unused - refund
        # 账本守恒：reserved_open=Σreserve_delta，终态必须归零，所以本行
        # 释放全部 unused 的预留；余额只加真正退回的 refund。被突发上限
        # 裁掉的 lost 在预留时本就来自余额，这里只留痕、不再二次扣余额。
        self._append(c, now, cust["id"], v["id"], "refund",
                     avail=refund, reserve=-unused, note=reason)
        if lost:
            self._append(c, now, cust["id"], v["id"], "cap_clamp",
                         avail=0, note=f"refund_over_cap:{reason} lost={lost}")
        c.execute(
            "UPDATE budget_vouchers SET refunded_total=refunded_total+?, "
            "lost_total=lost_total+? WHERE id=?",
            (refund, lost, v["id"]))
        cust["balance"] += refund
        return refund, lost

    def _seats_used(self, c, customer_id: int, now: Optional[int] = None) -> int:
        """占用席位数：只数租约仍有效的 executing 凭证。

        失联（租约超时）的凭证已不占席位，接替者才能用同席位重新占用。
        """
        if now is None:
            now = self.now()
        return c.execute(
            "SELECT COUNT(*) AS n FROM budget_vouchers WHERE customer_id=? "
            "AND state='executing' AND lease_expires_at IS NOT NULL "
            "AND lease_expires_at>?", (customer_id, now)).fetchone()["n"]

    def _try_promote(self, c, cust: dict, voucher_id: int, now: int) -> bool:
        """尝试让单个延后/暂停凭证取得执行资格。成功 True。"""
        v = self._voucher_row(c, voucher_id)
        if v["state"] == ST_DEFERRED:
            need = v["estimate"] - v["reserved_total"]
            if need > cust["burst_cap"]:
                if v["blocked_reason"] != "over_burst_cap":
                    c.execute(
                        "UPDATE budget_vouchers SET blocked_reason="
                        "'over_burst_cap', earliest_ready_at=NULL WHERE id=?",
                        (voucher_id,))
                return False
            if cust["balance"] < need:
                ready = self._earliest_ready(cust, need, now)
                c.execute(
                    "UPDATE budget_vouchers SET blocked_reason="
                    "'insufficient_balance', earliest_ready_at=? WHERE id=?",
                    (ready, voucher_id))
                return False
            self._reserve_locked(c, cust, voucher_id, need, now, note="submit")
            c.execute(
                "UPDATE budget_vouchers SET state='reserved', "
                "blocked_reason=NULL, earliest_ready_at=NULL WHERE id=?",
                (voucher_id,))
            return True
        if v["state"] == ST_PAUSED:
            requested = v["pending_total"]
            need = requested - v["reserved_total"]
            if need <= 0:
                need = 0
            if need > 0 and cust["balance"] < need:
                # 暂停期间不重算 earliest（继续等待补充到账）
                return False
            if need > 0:
                self._reserve_locked(c, cust, voucher_id, need, now,
                                     kind="reserve_extra",
                                     note="resume_after_pause")
            c.execute(
                "UPDATE budget_vouchers SET state='reserved', pending_total="
                "NULL, blocked_reason=NULL, earliest_ready_at=NULL WHERE id=?",
                (voucher_id,))
            self._audit(c, "voucher_resumed", f"bvoucher:{voucher_id}",
                        f"凭证 #{voucher_id} 额度到账，追加预留 {need}，"
                        "重新取得执行资格（需重新占用，旧 epoch 持有者"
                        " fence 失效）")
            return True
        return False

    def _pump_locked(self, c, cust: dict, now: int) -> int:
        """补充到账后按 FIFO 推进所有被阻塞凭证，返回成功个数。

        排序：deferred 在前、paused 在后；deferred 按最早可执行时刻
        （不可行的 over_burst_cap 为 NULL 放最后）、再按提交顺序。
        一次循环里额度用尽的自然跳过；每个可恢复的都会被尝试。
        """
        rows = c.execute(
            "SELECT * FROM budget_vouchers WHERE customer_id=? AND state IN "
            "('deferred','paused') ORDER BY "
            "CASE WHEN state='paused' THEN 1 ELSE 0 END, "
            "earliest_ready_at IS NULL, earliest_ready_at, submitted_at, id",
            (cust["id"],)).fetchall()
        promoted = 0
        for r in rows:
            vid = r["id"]
            if self._try_promote(c, cust, vid, now):
                promoted += 1
        return promoted

    def _fence(self, v: dict, epoch: int, worker: str, now: int,
               allow_states=ST_EXECUTING) -> None:
        if not isinstance(epoch, int) or isinstance(epoch, bool) or epoch < 1:
            raise BudgetError("claim_epoch 必须是 >= 1 的整数")
        if not worker:
            raise BudgetError("worker 不能为空")
        states = (allow_states,) if isinstance(allow_states, str) \
            else tuple(allow_states)
        if v["state"] not in states:
            if v["state"] in TERMINAL_STATES:
                raise BudgetConflict(
                    f"凭证 #{v['id']} 已终态（{v['state']}），不接受上报",
                    code="terminal")
            raise BudgetConflict(
                f"凭证 #{v['id']} 当前状态 {v['state']}，未处于执行占用，"
                f"拒绝 epoch={epoch} 的上报", code="not_executing")
        if v["claim_worker"] is None:
            # state=executing 但持有者已被 reap 清空：等待接替，旧持有者的
            # 迟到上报按租约超时拒绝（不扣费）；接替后同 epoch 才是 stale。
            raise BudgetConflict(
                f"凭证 #{v['id']} 没有当前持有者（租约已超时，等待接替），"
                f"worker {worker} 的上报拒绝", code="lease_expired")
        if v["claim_epoch"] != epoch:
            raise BudgetConflict(
                f"凭证 #{v['id']} 的 claim_epoch {epoch} 已过期：当前有效 "
                f"epoch 为 {v['claim_epoch']}（持有者已失联并被接替），"
                "旧持有者的迟到上报拒绝，不重复扣费",
                code="stale_epoch")
        if v["claim_worker"] != worker:
            raise BudgetConflict(
                f"凭证 #{v['id']} 当前由 {v['claim_worker']} 持有，"
                f"{worker} 的上报拒绝", code="worker_mismatch")
        if now >= (v["lease_expires_at"] or 0):
            raise BudgetConflict(
                f"凭证 #{v['id']} 租约已于 {v['lease_expires_at']} 超时，"
                "请重新占用（接替）后再上报", code="lease_expired")

    def _audit(self, c, kind: str, ref: str, message: str) -> None:
        c.execute(
            "INSERT INTO audits(at,kind,ref,message) VALUES(?,?,?,?)",
            (self.now(), kind, ref, message))

    # ------------------------------------------------------------ 视图

    @staticmethod
    def _voucher_view(r: dict) -> dict:
        reserved_open = r["reserved_total"] - r["settled_total"] - \
            r["refunded_total"] - r["lost_total"]
        r["reserved_open"] = max(0, reserved_open)
        return r

    def _customer_view(self, c, cust: dict) -> dict:
        agg = c.execute(
            "SELECT COALESCE(SUM(reserve_delta),0) AS reserved_open, "
            "COALESCE(SUM(settle_delta),0) AS settled, "
            "COALESCE(SUM(CASE WHEN avail_delta<0 AND kind IN "
            "('reserve','reserve_extra') THEN -avail_delta ELSE 0 END),0) "
            "AS reserved_total FROM budget_ledger WHERE customer_id=?",
            (cust["id"],)).fetchone()
        blocked = [self._voucher_view(dict(r)) for r in c.execute(
            "SELECT v.*, cu.name AS customer_name FROM budget_vouchers v "
            "JOIN budget_customers cu ON cu.id=v.customer_id "
            "WHERE v.customer_id=? AND v.state IN ('deferred','paused') "
            "ORDER BY CASE WHEN v.state='paused' THEN 1 ELSE 0 END, "
            "v.earliest_ready_at IS NULL, v.earliest_ready_at, "
            "v.submitted_at, v.id",
            (cust["id"],)).fetchall()]
        now = self.now()
        return {
            "id": cust["id"],
            "name": cust["name"],
            "refill_amount": cust["refill_amount"],
            "refill_interval": cust["refill_interval"],
            "burst_cap": cust["burst_cap"],
            "max_seats": cust["max_seats"],
            "seats_used": self._seats_used(c, cust["id"], now),
            "available": cust["balance"],
            "reserved_open": agg["reserved_open"],
            "reserved_total": agg["reserved_total"],
            "settled_total": agg["settled"],
            "refunded_total": c.execute(
                "SELECT COALESCE(SUM(refunded_total),0) n FROM budget_vouchers "
                "WHERE customer_id=?", (cust["id"],)).fetchone()["n"],
            "lost_total": c.execute(
                "SELECT COALESCE(SUM(lost_total),0) n FROM budget_vouchers "
                "WHERE customer_id=?", (cust["id"],)).fetchone()["n"],
            "next_refill_at": self._next_refill_at(cust, now),
            "last_refill_at": cust["last_refill_at"],
            "blocked": blocked,
            "updated_at": cust["updated_at"],
        }

    def verify_invariants(self, customer_ref=None) -> list[str]:
        """核对“账本可重放”等不变量；返回违例描述列表（空列表=全部通过）。"""
        problems: list[str] = []
        c = self.db.conn()
        customers = ([self._customer_row(c, customer_ref)]
                     if customer_ref is not None
                     else [dict(r) for r in c.execute(
                         "SELECT * FROM budget_customers").fetchall()])
        for cust in customers:
            s = c.execute(
                "SELECT COALESCE(SUM(avail_delta),0) AS bal, "
                "MIN(balance_after) AS mn FROM budget_ledger "
                "WHERE customer_id=?", (cust["id"],)).fetchone()
            if s["bal"] != cust["balance"]:
                problems.append(
                    f"客户 {cust['name']}: 余额 {cust['balance']} 与账本重放 "
                    f"{s['bal']} 不一致")
            if cust["balance"] < 0:
                problems.append(
                    f"客户 {cust['name']}: 余额透支 {cust['balance']}")
            if cust["balance"] > cust["burst_cap"]:
                problems.append(
                    f"客户 {cust['name']}: 余额 {cust['balance']} 超过突发上限 "
                    f"{cust['burst_cap']}")
            if s["mn"] is not None and s["mn"] < 0:
                problems.append(
                    f"客户 {cust['name']}: 账本中出现负余额行 {s['mn']}（抢额"
                    "透支）")
            for r in c.execute(
                    "SELECT * FROM budget_vouchers WHERE customer_id=?",
                    (cust["id"],)).fetchall():
                v = dict(r)
                if v["settled_total"] > v["reserved_total"]:
                    problems.append(
                        f"凭证 #{v['id']}: 已结算 {v['settled_total']} > 已预留 "
                        f"{v['reserved_total']}")
                # 终态时未用预留必须恰好退完（退回 + 突发裁剪）
                if v["state"] in TERMINAL_STATES and \
                        v["refunded_total"] + v["lost_total"] != \
                        v["reserved_total"] - v["settled_total"]:
                    problems.append(
                        f"凭证 #{v['id']}: 终态退款 {v['refunded_total']}+"
                        f"{v['lost_total']} 不等于未用预留 "
                        f"{v['reserved_total'] - v['settled_total']}")
                # 非终态不应发生退款
                if v["state"] not in TERMINAL_STATES and \
                        (v["refunded_total"] or v["lost_total"]):
                    problems.append(
                        f"凭证 #{v['id']}: 非终态却已退款/裁剪")
        return problems
