"""既有作业队列与多租户仲裁器的桥接层（opt-in）。

开启桥接后，``jobs`` 表里的回填 / 订正 / 重算作业在提交后**镜像**进仲裁器的
统一 ready set（``arb_jobs``），由同一个欠额 + 老化仲裁器在租户间分配席位；
执行器通过仲裁器 claim，命中镜像作业时回到 :class:`~featforge.store.Store`
原有执行器运行（检查点/订正进度/自动切换逻辑不变）。

- 镜像按 jobs.id 幂等：重复同步/重入只复用同一个 arb_job；
- 仲裁器 claim 的 fencing（claim_epoch/租约/暂停/席位）对镜像作业同等生效；
- 镜像作业完成/失败/超时回收会同步回 jobs.state，两个队列的状态不漂移；
- **默认执行路径不变**：不启用桥接时 Worker 仍走 Store.claim_next_job，
  现有测试与部署零影响。桥接是“统一 ready set”接入既有作业的适配层。
"""
from __future__ import annotations

from typing import Optional

from .arbiter import Arbiter, JOB_KINDS


class LegacyBridge:
    def __init__(self, store, arbiter: Arbiter, default_tenant: str = "default"):
        self.store = store
        self.arbiter = arbiter
        self.default_tenant = default_tenant
        self.arbiter.upsert_tenant(default_tenant, weight=1.0, seats=4)

    def _tenant_for(self, job: dict) -> str:
        """作业 -> 租户。优先按特征名归属，无特征的（订正）落到默认租户。"""
        c = self.store.db.conn()
        name = None
        gen_id = job.get("gen_id")
        feature_id = job.get("feature_id")
        if gen_id is not None:
            row = c.execute(
                "SELECT f.name FROM generations g JOIN features f "
                "ON f.id=g.feature_id WHERE g.id=?", (gen_id,)).fetchone()
            name = row["name"] if row else None
        if name is None and feature_id is not None:
            row = c.execute("SELECT name FROM features WHERE id=?",
                            (feature_id,)).fetchone()
            name = row["name"] if row else None
        return name or self.default_tenant

    def sync(self) -> list[int]:
        """把 jobs 表中 queued/running 但尚未镜像的作业登记进 ready set。

        running 状态（进程崩溃遗留）同样镜像为 queued 镜像行，交由租约/接管
        语义重新分配；Store 侧的 recover 仍可独立使用。
        """
        c = self.store.db.conn()
        rows = c.execute(
            "SELECT * FROM jobs WHERE state IN ('queued','running') "
            "AND arb_job_id IS NULL ORDER BY id").fetchall()
        mirrored = []
        for r in rows:
            job = dict(r)
            if job["kind"] not in JOB_KINDS:
                continue
            tenant = self._tenant_for(job)
            self.arbiter.upsert_tenant(tenant, weight=1.0, seats=2)
            res = self.arbiter.enqueue(
                tenant, job["kind"], f"legacy:job{job['id']}",
                payload={"legacy_job_id": job["id"], "dedup_key": job["dedup_key"]},
                shards_total=max(1, job["partitions_total"] or 1))
            arb_job = res["job"]
            c.execute("UPDATE jobs SET arb_job_id=? WHERE id=?",
                      (arb_job["id"], job["id"]))
            mirrored.append(job["id"])
        c.commit()
        return mirrored

    def claim_and_run(self, worker_id: str) -> Optional[dict]:
        """经仲裁器领取并执行一个作业。

        命中镜像作业时：在同一时刻把 jobs 置为 running 并记录 arb epoch，
        然后调用 Store 原有执行逻辑；执行结束后把结果同步回仲裁器
        （complete/fail），完成 fencing 闭环。
        """
        claim = self.arbiter.claim(worker_id)
        if claim is None:
            return None
        legacy_id = claim["payload"].get("legacy_job_id")
        if legacy_id is None:
            # 非镜像（原生仲裁作业）：调用方应使用 ArbiterWorker 处理
            return {"arbiter_only": True, "claim": claim}
        c = self.store.db.conn()
        legacy = self.store.get_job(legacy_id)
        epoch = claim["claim_epoch"]
        c.execute(
            "UPDATE jobs SET state='running', attempts=attempts+1, "
            "started_at=COALESCE(started_at,?), arb_claim_epoch=? WHERE id=?",
            (self.arbiter.now(), epoch, legacy_id))
        c.commit()
        try:
            result = self.store.run_job(self.store.get_job(legacy_id))
        except Exception as e:
            # Store 执行器崩溃性异常：仲裁器侧标记失败（保留租约语义）
            try:
                self.arbiter.fail(claim["id"], epoch, worker_id, str(e))
            except Exception:
                pass
            raise
        if result["state"] == "succeeded":
            self.arbiter.complete(claim["id"], epoch, worker_id)
        elif result["state"] == "failed":
            self.arbiter.fail(claim["id"], epoch, worker_id,
                             result.get("error") or "legacy job failed")
        return {"arb_job_id": claim["id"], "claim_epoch": epoch,
                "legacy_job_id": legacy_id, "job": result}

    def reconcile_expired(self) -> list[int]:
        """租约超时回收后，把镜像分片的旧 jobs 行复位为 queued 等待接管。"""
        reaped = self.arbiter.reap_expired()
        c = self.store.db.conn()
        out = []
        for r in reaped:
            legacy_id = None
            try:
                detail = self.arbiter.get_job(r["id"])
                legacy_id = detail["payload"].get("legacy_job_id")
            except Exception:
                continue
            if legacy_id is None:
                continue
            c.execute(
                "UPDATE jobs SET state='queued', started_at=NULL, "
                "arb_claim_epoch=0 WHERE id=? AND state='running'",
                (legacy_id,))
            out.append(legacy_id)
        c.commit()
        return out
