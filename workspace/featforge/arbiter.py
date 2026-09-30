"""多租户仲裁器：回填 / 订正 / 重算 / 快照四类作业的统一 ready set。

调度语义
--------
每个租户维护：
- ``weight``：相对份额，稳态下单位时间的 claim 速率 ∝ weight；
- ``seats``：并行席位上限，该租户同时处于 claimed 的分片数达到上限即不可再选；
- ``vruntime``：虚拟运行时间，每次被选中推进 ``1/weight``；
- 老化（aging）：排队越久加分越多，``score = vruntime - aging_rate * 等待秒数``。

选择器在一个 ``BEGIN IMMEDIATE`` 事务内：先回收心跳超时的分片（回到 queued），
再在**有排队作业、未暂停、席位未满**的租户中取 score 最小者（平手按最老排队
作业、租户 id 决胜）。因此高权重租户拿到更多处理量，而长期排队的小租户靠
老化积累最终必然被选中（防饥饿）。

fencing
-------
每次 claim 让该作业的 ``claim_epoch`` 单调 +1（接管也 +1）；心跳/进度/完成
都必须携带当前 epoch 与持有者 worker id，旧 epoch 的上报一律 409。分片游标
``shard_cursor`` 只能前进：后退 409，相等视为幂等重放。暂停只阻止新 claim，
已经开工的分片允许写完；在线修改权重/席位只改租户行，绝不重复领取作业，也
不清空游标与进度。
"""
from __future__ import annotations

import hashlib
import json
import time
from typing import Callable, Optional

from .config import settings
from .db import Database

JOB_KINDS = ("backfill", "correction", "recompute", "snapshot")
ACTIVE_STATES = ("queued", "claimed")


class ArbiterError(Exception):
    """请求本身不合法（HTTP 400 语义）。"""


class ArbiterConflict(Exception):
    """fencing/幂等冲突：旧 epoch 上报、游标后退、同键异参（HTTP 409 语义）。

    ``code`` 是机器可读的冲突码：
    stale_epoch / lease_expired / not_claimed / worker_mismatch /
    cursor_regress / business_key_conflict
    """

    def __init__(self, message: str, code: str = "conflict"):
        super().__init__(message)
        self.code = code


def canonical(payload: object) -> str:
    return json.dumps(payload, sort_keys=True, default=str,
                      ensure_ascii=False, separators=(",", ":"))


def _payload_hash(kind: str, payload: object, shards_total: int) -> str:
    return hashlib.sha256(
        canonical({"kind": kind, "payload": payload, "shards": shards_total}).encode()
    ).hexdigest()


class Arbiter:
    def __init__(
        self,
        db: Database,
        clock: Optional[Callable[[], int]] = None,
        lease_ttl: Optional[int] = None,
        aging_rate: Optional[float] = None,
        history_limit: Optional[int] = None,
    ):
        self.db = db
        self.now = clock or (lambda: int(time.time()))
        self.lease_ttl = (lease_ttl if lease_ttl is not None
                          else settings.arbiter_lease_ttl)
        self.aging_rate = (aging_rate if aging_rate is not None
                           else settings.arbiter_aging_rate)
        self.history_limit = (history_limit if history_limit is not None
                              else settings.arbiter_history_limit)

    # ============================================================== 租户策略

    def _validate_policy(self, weight: float, seats: int) -> None:
        if not isinstance(weight, (int, float)) or isinstance(weight, bool) \
                or weight <= 0:
            raise ArbiterError("weight 必须是 > 0 的数值")
        if not isinstance(seats, int) or isinstance(seats, bool) or seats < 1:
            raise ArbiterError("seats 必须是 >= 1 的整数")

    def upsert_tenant(self, name: str, weight: float = 1.0,
                      seats: int = 1) -> dict:
        """创建租户（已存在则原样返回，绝不覆盖运维中的策略）。"""
        self._validate_policy(weight, seats)
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            row = c.execute("SELECT * FROM arb_tenants WHERE name=?",
                            (name,)).fetchone()
            if row is not None:
                c.execute("COMMIT")
                return self.get_tenant(name)
            now = self.now()
            c.execute(
                "INSERT INTO arb_tenants(name,weight,seats,paused,vruntime,"
                "total_claims,created_at,updated_at) VALUES(?,?,?,0,0,0,?,?)",
                (name, float(weight), seats, now, now))
            tid = c.execute("SELECT id FROM arb_tenants WHERE name=?",
                            (name,)).fetchone()[0]
            self._audit(c, "tenant_created", f"tenant:{tid}",
                        f"租户 {name} 创建：weight={weight} seats={seats}")
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        return self.get_tenant(name)

    def _tenant_row(self, c, ref) -> dict:
        row = c.execute(
            "SELECT * FROM arb_tenants WHERE id=? OR name=?", (ref, ref)
        ).fetchone()
        if row is None:
            raise ArbiterError(f"租户不存在: {ref}")
        return dict(row)

    def get_tenant(self, ref) -> dict:
        return self._tenant_row(self.db.conn(), ref)

    def update_policy(self, name: str, weight: Optional[float] = None,
                      seats: Optional[int] = None,
                      paused: Optional[bool] = None) -> dict:
        """在线修改权重/席位/暂停。

        只更新 arb_tenants 的策略列与 vruntime（改权重不回溯历史），
        **绝不触碰** arb_jobs 与 arb_progress：一个已被领取的分片不会因此
        被领取第二次，游标与已提交进度原样保留。
        """
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            t = self._tenant_row(c, name)
            new_weight = t["weight"] if weight is None else float(weight)
            new_seats = t["seats"] if seats is None else seats
            new_paused = t["paused"] if paused is None else int(bool(paused))
            self._validate_policy(new_weight, new_seats)
            c.execute(
                "UPDATE arb_tenants SET weight=?, seats=?, paused=?, updated_at=? "
                "WHERE id=?",
                (new_weight, new_seats, new_paused, self.now(), t["id"]))
            changes = []
            if weight is not None and float(weight) != t["weight"]:
                changes.append(f"weight {t['weight']} -> {new_weight}")
            if seats is not None and seats != t["seats"]:
                changes.append(f"seats {t['seats']} -> {new_seats}")
            if paused is not None and int(bool(paused)) != t["paused"]:
                changes.append(f"paused {bool(t['paused'])} -> {bool(new_paused)}")
            if changes:
                self._audit(c, "tenant_policy", f"tenant:{t['id']}",
                            f"租户 {t['name']} 策略变更：{'; '.join(changes)}"
                            "（不影响已领取分片与既有进度）")
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        return self.get_tenant(name)

    def pause(self, name: str) -> dict:
        """暂停只阻止新的 claim；已 claimed 的分片继续跑直到写完或租约超时。"""
        c = self.db.conn()
        t = self._tenant_row(c, name)
        if not t["paused"]:
            self.update_policy(name, paused=True)
        return self.get_tenant(name)

    def resume(self, name: str) -> dict:
        return self.update_policy(name, paused=False)

    def list_tenants(self) -> list[dict]:
        return [dict(r) for r in self.db.conn().execute(
            "SELECT * FROM arb_tenants ORDER BY id").fetchall()]

    # ============================================================== 入队去重

    def enqueue(self, tenant_ref, kind: str, business_key: str,
                payload: Optional[dict] = None,
                shards_total: int = 1) -> dict:
        """幂等入队到统一 ready set。

        - 同租户 + 同 business_key + 同负载指纹：重放返回原作业（replayed=True）；
        - 同键不同负载（kind/参数/分片数）：409 business_key_conflict；
        - 不允许跨租户复用键（UNIQUE 限定在租户内）。
        新加入队列的租户把 vruntime 抬到当前活跃租户最小值，避免沉睡租户
        以远古 vruntime 一次性霸占仲裁器。
        """
        if kind not in JOB_KINDS:
            raise ArbiterError(f"kind 必须是 {JOB_KINDS} 之一")
        if not business_key:
            raise ArbiterError("business_key 不能为空")
        if not isinstance(shards_total, int) or isinstance(shards_total, bool) \
                or shards_total < 1:
            raise ArbiterError("shards_total 必须是 >= 1 的整数")
        phash = _payload_hash(kind, payload, shards_total)
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            t = self._tenant_row(c, tenant_ref)
            existing = c.execute(
                "SELECT * FROM arb_jobs WHERE tenant_id=? AND business_key=?",
                (t["id"], business_key)).fetchone()
            if existing is not None:
                if existing["payload_hash"] != phash:
                    c.execute("ROLLBACK")
                    raise ArbiterConflict(
                        f"业务键 {business_key} 已用于租户 {t['name']} 的另一个作业"
                        f"（现有 kind={existing['kind']} "
                        f"state={existing['state']}）：同一业务键不允许提交不同参数",
                        code="business_key_conflict")
                c.execute("COMMIT")
                return {"replayed": True, "job": self.get_job(existing["id"])}
            # 唤醒时对齐全局 min_vruntime，只增不减（标准 CFS 语义）：
            # 沉睡很久、vruntime 停在远古值的租户唤醒后不能一次性霸占仲裁器。
            # 只在该租户自己没有活跃分片时对齐；其新作业此刻尚未插入。
            own_active = c.execute(
                "SELECT COUNT(*) AS n FROM arb_jobs WHERE tenant_id=? "
                "AND state IN ('queued','claimed')", (t["id"],)).fetchone()["n"]
            if own_active == 0:
                row = c.execute(
                    "SELECT MIN(vruntime) AS m FROM arb_tenants WHERE id!=?",
                    (t["id"],)).fetchone()
                if row["m"] is not None and row["m"] > t["vruntime"]:
                    c.execute("UPDATE arb_tenants SET vruntime=? WHERE id=?",
                              (row["m"], t["id"]))
            cur = c.execute(
                "INSERT INTO arb_jobs(tenant_id,kind,business_key,payload_json,"
                "payload_hash,shards_total,state,enqueued_at) "
                "VALUES(?,?,?,?,?,?,'queued',?)",
                (t["id"], kind, business_key, canonical(payload), phash,
                 shards_total, self.now()))
            jid = cur.lastrowid
            self._audit(c, "job_enqueued", f"arbjob:{jid}",
                        f"租户 {t['name']} 入队 {kind} 作业 #{jid} "
                        f"key={business_key} 分片={shards_total}")
            c.execute("COMMIT")
        except ArbiterConflict:
            raise
        except Exception:
            c.execute("ROLLBACK")
            raise
        return {"replayed": False, "job": self.get_job(jid)}

    def list_jobs(self, tenant_ref=None, state: Optional[str] = None,
                  limit: int = 200) -> list[dict]:
        c = self.db.conn()
        sql = ("SELECT j.*, t.name AS tenant_name FROM arb_jobs j "
               "JOIN arb_tenants t ON t.id=j.tenant_id")
        args: list = []
        conds = []
        if tenant_ref is not None:
            t = self._tenant_row(c, tenant_ref)
            conds.append("j.tenant_id=?")
            args.append(t["id"])
        if state:
            conds.append("j.state=?")
            args.append(state)
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        sql += " ORDER BY j.id DESC LIMIT ?"
        args.append(limit)
        return [dict(r) for r in c.execute(sql, args).fetchall()]

    def get_job(self, job_id: int) -> dict:
        row = self.db.conn().execute(
            "SELECT j.*, t.name AS tenant_name FROM arb_jobs j "
            "JOIN arb_tenants t ON t.id=j.tenant_id WHERE j.id=?",
            (job_id,)).fetchone()
        if row is None:
            raise ArbiterError(f"仲裁作业不存在: {job_id}")
        d = dict(row)
        d["payload"] = json.loads(d.pop("payload_json") or "{}")
        return d

    def list_progress(self, job_id: int) -> list[dict]:
        return [dict(r) for r in self.db.conn().execute(
            "SELECT * FROM arb_progress WHERE job_id=? ORDER BY id",
            (job_id,)).fetchall()]

    # ============================================================== claim

    def _write_history(self, c, decision: str, tenant_id, tenant_name,
                       job_id, claim_epoch, reason: Optional[str],
                       detail: Optional[dict]) -> None:
        c.execute(
            "INSERT INTO arb_history(at,decision,tenant_id,tenant_name,job_id,"
            "claim_epoch,reason,detail) VALUES(?,?,?,?,?,?,?,?)",
            (self.now(), decision, tenant_id, tenant_name, job_id, claim_epoch,
             reason, canonical(detail or {})))
        c.execute(
            "DELETE FROM arb_history WHERE id NOT IN ("
            "SELECT id FROM arb_history ORDER BY id DESC LIMIT ?)",
            (self.history_limit,))

    def _audit(self, c, kind: str, ref: str, message: str) -> None:
        c.execute(
            "INSERT INTO audits(at,kind,ref,message) VALUES(?,?,?,?)",
            (self.now(), kind, ref, message))

    def reap_expired(self) -> list[dict]:
        """心跳超时的 claimed 分片回到 ready set（state=queued）。

        不改变 claim_epoch：epoch 在下次 claim 时才 +1，保证接管方拿到的
        一定是比旧持有者更新的 epoch。游标与进度原样保留。
        """
        c = self.db.conn()
        reaped: list[dict] = []
        try:
            c.execute("BEGIN IMMEDIATE")
            now = self.now()
            rows = c.execute(
                "SELECT j.*, t.name AS tenant_name FROM arb_jobs j "
                "JOIN arb_tenants t ON t.id=j.tenant_id "
                "WHERE j.state='claimed' AND j.lease_expires_at<?",
                (now,)).fetchall()
            for r in rows:
                r = dict(r)
                c.execute(
                    "UPDATE arb_jobs SET state='queued', claim_worker=NULL, "
                    "claimed_at=NULL, lease_expires_at=NULL, "
                    "last_heartbeat_at=NULL WHERE id=?",
                    (r["id"],))
                self._write_history(
                    c, "lease_expired", r["tenant_id"], r["tenant_name"],
                    r["id"], r["claim_epoch"], "heartbeat_timeout",
                    {"old_epoch": r["claim_epoch"], "worker": r["claim_worker"],
                     "cursor": r["shard_cursor"],
                     "leased_at": r["claimed_at"],
                     "expired_at": r["lease_expires_at"]})
                self._audit(c, "lease_expired", f"arbjob:{r['id']}",
                            f"作业 #{r['id']} 租约超时（worker={r['claim_worker']} "
                            f"epoch={r['claim_epoch']}），回到 ready set，"
                            f"游标保留在 {r['shard_cursor']}")
                reaped.append(r)
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        return reaped

    def claim(self, worker_id: str) -> Optional[dict]:
        """按 欠额 + 老化 从 ready set 领取下一个分片。

        返回含新 claim_epoch 与租约到期时间的作业；没有可领取项时返回 None。
        """
        if not worker_id:
            raise ArbiterError("worker_id 不能为空")
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            now = self.now()
            # 1) 先回收超时租约（旧持有者之后的任何上报都会被 fence）
            expired = c.execute(
                "SELECT j.id, t.name AS tenant_name, j.claim_epoch, "
                "j.claim_worker, j.shard_cursor, j.claimed_at, j.lease_expires_at "
                "FROM arb_jobs j JOIN arb_tenants t ON t.id=j.tenant_id "
                "WHERE j.state='claimed' AND j.lease_expires_at<?",
                (now,)).fetchall()
            for r in expired:
                c.execute(
                    "UPDATE arb_jobs SET state='queued', claim_worker=NULL, "
                    "claimed_at=NULL, lease_expires_at=NULL, "
                    "last_heartbeat_at=NULL WHERE id=?",
                    (r["id"],))
                self._write_history(
                    c, "lease_expired", None, r["tenant_name"], r["id"],
                    r["claim_epoch"], "heartbeat_timeout",
                    {"old_epoch": r["claim_epoch"], "worker": r["claim_worker"],
                     "cursor": r["shard_cursor"], "leased_at": r["claimed_at"],
                     "expired_at": r["lease_expires_at"]})
            # 2) 候选租户：至少有一个 queued 作业
            candidates = [dict(r) for r in c.execute(
                "SELECT t.*, MIN(j.enqueued_at) AS oldest_queued, "
                "(SELECT COUNT(*) FROM arb_jobs a WHERE a.tenant_id=t.id "
                "AND a.state='claimed') AS seats_used "
                "FROM arb_tenants t JOIN arb_jobs j ON j.tenant_id=t.id "
                "AND j.state='queued' GROUP BY t.id").fetchall()]
            eligible = [t for t in candidates
                        if not t["paused"] and t["seats_used"] < t["seats"]]
            if not eligible:
                if candidates:
                    reasons = {}
                    for t in candidates:
                        reasons[t["name"]] = (
                            "paused" if t["paused"]
                            else f"seats_full({t['seats_used']}/{t['seats']})")
                    self._write_history(
                        c, "idle", None, None, None, None,
                        "no_eligible_tenant", {"tenants": reasons})
                c.execute("COMMIT")
                return None
            min_vruntime = min(t["vruntime"] for t in eligible)
            scored = []
            for t in eligible:
                age = max(0, now - t["oldest_queued"])
                aging_bonus = self.aging_rate * age
                scored.append((t["vruntime"] - aging_bonus,
                               t["oldest_queued"], t["id"], t,
                               age, aging_bonus, min_vruntime))
            # score 最小 → 最老排队作业 → 最小租户 id（确定性决胜）
            scored.sort(key=lambda x: (x[0], x[1], x[2]))
            score, oldest, _, winner, age, bonus, minv = scored[0]
            job = dict(c.execute(
                "SELECT * FROM arb_jobs WHERE tenant_id=? AND state='queued' "
                "ORDER BY enqueued_at, id LIMIT 1",
                (winner["id"],)).fetchone())
            new_epoch = job["claim_epoch"] + 1
            expires = now + self.lease_ttl
            c.execute(
                "UPDATE arb_jobs SET state='claimed', claim_epoch=?, "
                "claim_worker=?, claimed_at=?, lease_expires_at=?, "
                "last_heartbeat_at=?, attempts=attempts+1, error=NULL "
                "WHERE id=?",
                (new_epoch, worker_id, now, expires, now, job["id"]))
            advance = 1.0 / float(winner["weight"])
            new_vruntime = winner["vruntime"] + advance
            c.execute(
                "UPDATE arb_tenants SET vruntime=?, total_claims=total_claims+1 "
                "WHERE id=?",
                (new_vruntime, winner["id"]))
            self._write_history(
                c, "selected", winner["id"], winner["name"], job["id"],
                new_epoch, None,
                {"score": round(score, 6), "age_seconds": age,
                 "aging_bonus": round(bonus, 6),
                 "vruntime_before": round(winner["vruntime"], 6),
                 "vruntime_after": round(new_vruntime, 6),
                 "min_vruntime": round(minv, 6),
                 "lag_before": round(minv - winner["vruntime"], 6),
                 "weight": winner["weight"],
                 "seats_used": winner["seats_used"], "seats": winner["seats"]})
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        claim = self.get_job(job["id"])
        claim["lease_ttl"] = self.lease_ttl
        return claim

    # ============================================================== fence 校验

    def _fenced(self, c, job_id: int, claim_epoch: int,
                worker_id: str) -> dict:
        if not isinstance(claim_epoch, int) or isinstance(claim_epoch, bool) \
                or claim_epoch < 1:
            raise ArbiterError("claim_epoch 必须是 >= 1 的整数")
        if not worker_id:
            raise ArbiterError("worker_id 不能为空")
        row = c.execute("SELECT * FROM arb_jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise ArbiterError(f"仲裁作业不存在: {job_id}")
        job = dict(row)
        if job["state"] != "claimed":
            raise ArbiterConflict(
                f"作业 #{job_id} 当前状态为 {job['state']}，不接受 claimed "
                f"分片的上报（epoch={claim_epoch}）；作业可能已完成或已被接管",
                code="not_claimed")
        if job["claim_epoch"] != claim_epoch:
            raise ArbiterConflict(
                f"作业 #{job_id} 的 claim_epoch {claim_epoch} 已过期："
                f"当前有效 epoch 为 {job['claim_epoch']}（分片已被重新领取），"
                "旧持有者的上报拒绝",
                code="stale_epoch")
        if job["claim_worker"] != worker_id:
            raise ArbiterConflict(
                f"作业 #{job_id} 当前由 worker {job['claim_worker']} 持有，"
                f"worker {worker_id} 的 epoch={claim_epoch} 上报拒绝",
                code="worker_mismatch")
        if self.now() >= job["lease_expires_at"]:
            raise ArbiterConflict(
                f"作业 #{job_id} 的租约已于 {job['lease_expires_at']} 超时，"
                "分片可被其他执行者接管；请先重新 claim 再上报",
                code="lease_expired")
        return job

    def _check_cursor(self, job: dict, new_cursor: int) -> None:
        if not isinstance(new_cursor, int) or isinstance(new_cursor, bool):
            raise ArbiterError("cursor 必须是整数")
        if new_cursor < 0 or new_cursor > job["shards_total"]:
            raise ArbiterError(
                f"cursor 必须在 0..{job['shards_total']} 之间，收到 {new_cursor}")
        if new_cursor < job["shard_cursor"]:
            raise ArbiterConflict(
                f"分片游标只能前进：当前游标 {job['shard_cursor']}，"
                f"拒绝回退到 {new_cursor}",
                code="cursor_regress")

    def heartbeat(self, job_id: int, claim_epoch: int, worker_id: str,
                  cursor: Optional[int] = None, note: Optional[str] = None) -> dict:
        """领取续期：延长租约；可附带前向推进游标（等价于一次进度提交）。"""
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            job = self._fenced(c, job_id, claim_epoch, worker_id)
            advanced = False
            if cursor is not None:
                self._check_cursor(job, cursor)
                if cursor > job["shard_cursor"]:
                    c.execute(
                        "INSERT INTO arb_progress(job_id,claim_epoch,worker,"
                        "old_cursor,new_cursor,note,at) VALUES(?,?,?,?,?,?,?)",
                        (job_id, claim_epoch, worker_id, job["shard_cursor"],
                         cursor, note or "heartbeat", self.now()))
                    c.execute("UPDATE arb_jobs SET shard_cursor=? WHERE id=?",
                              (cursor, job_id))
                    advanced = True
            c.execute(
                "UPDATE arb_jobs SET lease_expires_at=?, last_heartbeat_at=? "
                "WHERE id=?",
                (self.now() + self.lease_ttl, self.now(), job_id))
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        return self.get_job(job_id)

    def report_progress(self, job_id: int, claim_epoch: int, worker_id: str,
                        cursor: int, note: Optional[str] = None) -> dict:
        """进度提交：fence 校验 epoch，游标只能前进；相等是幂等重放。"""
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            job = self._fenced(c, job_id, claim_epoch, worker_id)
            self._check_cursor(job, cursor)
            advanced = False
            if cursor > job["shard_cursor"]:
                c.execute(
                    "INSERT INTO arb_progress(job_id,claim_epoch,worker,"
                    "old_cursor,new_cursor,note,at) VALUES(?,?,?,?,?,?,?)",
                    (job_id, claim_epoch, worker_id, job["shard_cursor"],
                     cursor, note, self.now()))
                c.execute("UPDATE arb_jobs SET shard_cursor=? WHERE id=?",
                          (cursor, job_id))
                advanced = True
            # cursor 相等：同一 epoch 的重试，幂等成功，不追加台账行
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        out = self.get_job(job_id)
        out["advanced"] = advanced
        return out

    def complete(self, job_id: int, claim_epoch: int, worker_id: str,
                 note: Optional[str] = None) -> dict:
        """完成分片作业：游标落到 shards_total，释放席位（state=succeeded）。"""
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            job = self._fenced(c, job_id, claim_epoch, worker_id)
            if job["shard_cursor"] < job["shards_total"]:
                c.execute(
                    "INSERT INTO arb_progress(job_id,claim_epoch,worker,"
                    "old_cursor,new_cursor,note,at) VALUES(?,?,?,?,?,?,?)",
                    (job_id, claim_epoch, worker_id, job["shard_cursor"],
                     job["shards_total"], note or "complete", self.now()))
            c.execute(
                "UPDATE arb_jobs SET state='succeeded', shard_cursor=?, "
                "finished_at=?, claim_worker=NULL, claimed_at=NULL, "
                "lease_expires_at=NULL, last_heartbeat_at=?, error=NULL "
                "WHERE id=?",
                (job["shards_total"], self.now(), self.now(), job_id))
            self._write_history(
                c, "completed", job["tenant_id"], None, job_id, claim_epoch,
                None, {"worker": worker_id, "shards": job["shards_total"]})
            self._audit(c, "job_succeeded", f"arbjob:{job_id}",
                        f"作业 #{job_id} 完成：{job['shards_total']} 分片"
                        f"（worker={worker_id} epoch={claim_epoch}）")
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        return self.get_job(job_id)

    def fail(self, job_id: int, claim_epoch: int, worker_id: str,
             error: str) -> dict:
        """显式失败：释放席位、保留游标，可 retry 从检查点继续。"""
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            job = self._fenced(c, job_id, claim_epoch, worker_id)
            c.execute(
                "UPDATE arb_jobs SET state='failed', error=?, finished_at=?, "
                "claim_worker=NULL, claimed_at=NULL, lease_expires_at=NULL, "
                "last_heartbeat_at=? WHERE id=?",
                (str(error), self.now(), self.now(), job_id))
            self._write_history(
                c, "failed", job["tenant_id"], None, job_id, claim_epoch,
                None, {"worker": worker_id, "cursor": job["shard_cursor"],
                       "shards": job["shards_total"], "error": str(error)})
            self._audit(c, "job_failed", f"arbjob:{job_id}",
                        f"作业 #{job_id} 失败（游标 {job['shard_cursor']}/"
                        f"{job['shards_total']} 保留，可 retry）：{error}")
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        return self.get_job(job_id)

    def retry_failed(self, job_id: int) -> dict:
        """失败作业重新排队：游标与进度保留，下次 claim 时 epoch 再 +1。"""
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            job = self.get_job(job_id)
            if job["state"] != "failed":
                raise ArbiterError("只能重试 failed 状态的作业")
            c.execute(
                "UPDATE arb_jobs SET state='queued', error=NULL, "
                "finished_at=NULL WHERE id=?",
                (job_id,))
            self._audit(c, "job_retried", f"arbjob:{job_id}",
                        f"作业 #{job_id} 重新排队，从游标 "
                        f"{job['shard_cursor']}/{job['shards_total']} 继续")
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        return self.get_job(job_id)

    # ============================================================== 状态/历史

    def history(self, limit: int = 50) -> list[dict]:
        rows = self.db.conn().execute(
            "SELECT * FROM arb_history ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        out = []
        for r in rows:
            d = dict(r)
            d["detail"] = json.loads(d["detail"] or "{}")
            out.append(d)
        return out

    def tenant_status(self, ref) -> dict:
        """单个租户的仲裁视图：等待时长、欠额、占用席位、近期选择。"""
        c = self.db.conn()
        t = self._tenant_row(c, ref)
        return self._tenant_statuses(c, [t])[0]

    def _tenant_statuses(self, c, tenants: list[dict]) -> list[dict]:
        now = self.now()
        active_min_row = c.execute(
            "SELECT MIN(vruntime) AS m FROM arb_tenants tt WHERE EXISTS ("
            "SELECT 1 FROM arb_jobs jj WHERE jj.tenant_id=tt.id "
            "AND jj.state IN ('queued','claimed'))").fetchone()
        active_min = active_min_row["m"]
        out = []
        for t in tenants:
            q = c.execute(
                "SELECT COUNT(*) n, MIN(enqueued_at) oldest FROM arb_jobs "
                "WHERE tenant_id=? AND state='queued'", (t["id"],)).fetchone()
            claimed = c.execute(
                "SELECT COUNT(*) n FROM arb_jobs WHERE tenant_id=? "
                "AND state='claimed'", (t["id"],)).fetchone()["n"]
            succeeded = c.execute(
                "SELECT COUNT(*) n FROM arb_jobs WHERE tenant_id=? "
                "AND state='succeeded'", (t["id"],)).fetchone()["n"]
            failed = c.execute(
                "SELECT COUNT(*) n FROM arb_jobs WHERE tenant_id=? "
                "AND state='failed'", (t["id"],)).fetchone()["n"]
            waiting = (now - q["oldest"]) if q["oldest"] is not None else 0
            lag = ((active_min - t["vruntime"]) if active_min is not None
                   else 0.0)
            out.append({
                "id": t["id"], "name": t["name"], "weight": t["weight"],
                "seats": t["seats"], "seats_used": claimed,
                "seats_available": max(0, t["seats"] - claimed),
                "paused": bool(t["paused"]),
                "vruntime": round(t["vruntime"], 6),
                "total_claims": t["total_claims"],
                "queued": q["n"], "succeeded": succeeded, "failed": failed,
                "waiting_seconds": waiting,
                "lag": round(lag, 6),
                "deficit": round(max(0.0, lag), 6),
                "updated_at": t["updated_at"],
            })
        return out

    def status(self) -> dict:
        """状态页数据：各租户等待时长/欠额/席位 + 近期选择结果。"""
        c = self.db.conn()
        tenants = [dict(r) for r in c.execute(
            "SELECT * FROM arb_tenants ORDER BY id").fetchall()]
        return {
            "tenants": self._tenant_statuses(c, tenants),
            "history": self.history(30),
            "jobs": self.list_jobs(limit=50),
            "lease_ttl": self.lease_ttl,
            "aging_rate": self.aging_rate,
            "time": self.now(),
        }
