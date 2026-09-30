"""HTTP API：定义、事件写入、订正台账、在线/离线读取、代际管理、作业与状态页。"""
from __future__ import annotations

import json
from typing import Optional

from fastapi import FastAPI, HTTPException, Response
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from .config import settings
from .db import Database
from .arbiter import Arbiter, ArbiterConflict, ArbiterError
from .store import (
    AliasConflict,
    CorrectionConflict,
    FeatError,
    SnapshotConflict,
    Store,
)

# 请求模型必须定义在模块级：未来注解（PEP 563）下 FastAPI 才能解析模型类型。


class FeatureIn(BaseModel):
    name: str
    source: str
    join_key: str = "entity_id"
    value_field: str = "value"
    window: str = Field(default="1h", description="如 30m / 6h / 7d / 900s")
    transform: str = "sum"
    transform_version: int = 1
    backfill_start: Optional[int] = None
    partition_seconds: Optional[int] = None
    sample_tick_seconds: Optional[int] = None
    require_online_match: bool = False
    fail_after_partitions: Optional[int] = Field(
        default=None, description="故障注入：物化 N 个分区后失败")


class GenerationIn(BaseModel):
    transform: str
    transform_version: int
    fail_after_partitions: Optional[int] = None


class EventIn(BaseModel):
    source: str
    entity_id: str
    event_ts: int
    value: object = None
    dedup_key: Optional[str] = None
    raw: Optional[dict] = None


class RollbackIn(BaseModel):
    to_gen_no: Optional[int] = None
    reason: str = "人工回滚"


class CorrectionIn(BaseModel):
    source: str
    record_key: str = Field(description="被订正记录的标识（原始事件的 dedup_key）")
    revision: int = Field(description="每条记录单调递增的订正序号")
    op: str = Field(description="replace（替换内容）| retract（撤销记录）")
    event_ts: Optional[int] = Field(default=None, description="replace：新事件时间，缺省不变")
    entity_id: Optional[str] = Field(default=None, description="replace：新关联实体，缺省不变")
    value: object = None
    raw: Optional[dict] = Field(default=None, description="replace：新负载，缺省不变")
    fail_after_batches: Optional[int] = Field(
        default=None, description="故障注入：落地 N 批后失败（演示续跑）")
    crash_after_batches: Optional[int] = Field(
        default=None, description="故障注入：落地 N 批后模拟进程崩溃")


# ------------------------------------------------------------ 多租户仲裁器

class TenantIn(BaseModel):
    name: str
    weight: float = 1.0
    seats: int = 1


class TenantPolicyIn(BaseModel):
    weight: Optional[float] = None
    seats: Optional[int] = None
    paused: Optional[bool] = None


class ArbJobIn(BaseModel):
    tenant: str
    kind: str = Field(description="backfill | correction | recompute | snapshot")
    business_key: str
    payload: dict = {}
    shards_total: int = 1


class ClaimIn(BaseModel):
    worker_id: str


class HeartbeatIn(BaseModel):
    claim_epoch: int
    worker_id: str
    cursor: Optional[int] = None
    note: Optional[str] = None


class ProgressIn(BaseModel):
    claim_epoch: int
    worker_id: str
    cursor: int
    note: Optional[str] = None


class FinishIn(BaseModel):
    claim_epoch: int
    worker_id: str
    note: Optional[str] = None


class FailIn(BaseModel):
    claim_epoch: int
    worker_id: str
    error: str


def create_app(store: Optional[Store] = None,
               arbiter: Optional[Arbiter] = None) -> FastAPI:
    app = FastAPI(title="FeatForge", version="1.0",
                  description="机器学习特征物化与回填服务")
    if store is None:
        store = Store(Database(settings.db_path))
    app.state.store = store
    if arbiter is None:
        arbiter = Arbiter(store.db, clock=store.now)
    app.state.arbiter = arbiter

    def ok(err: Exception) -> HTTPException:
        return HTTPException(status_code=400, detail=str(err))

    def arb_conflict(err: ArbiterConflict) -> HTTPException:
        return HTTPException(status_code=409, detail={
            "error": err.code, "message": str(err)})

    # ------------------------------------------------------- 定义 API

    @app.post("/api/v1/features", status_code=201)
    def create_feature(body: FeatureIn):
        try:
            return store.create_feature(**body.model_dump())
        except FeatError as e:
            raise ok(e)

    @app.get("/api/v1/features")
    def list_features():
        c = store.db.conn()
        rows = c.execute("SELECT * FROM features ORDER BY id").fetchall()
        out = []
        for r in rows:
            f = dict(r)
            f["serving_gen"] = (store.serving_gen(c, f["id"]) or {}).get("gen_no")
            out.append(f)
        return out

    @app.get("/api/v1/features/{name}")
    def get_feature(name: str):
        c = store.db.conn()
        try:
            f = store.get_feature(c, name)
        except FeatError as e:
            raise ok(e)
        gens = c.execute(
            "SELECT * FROM generations WHERE feature_id=? ORDER BY gen_no",
            (f["id"],)).fetchall()
        return {"feature": f,
                "serving_gen": (store.serving_gen(c, f["id"]) or {}).get("gen_no"),
                "generations": [dict(g) for g in gens]}

    @app.post("/api/v1/features/{name}/generations", status_code=201)
    def create_generation(name: str, body: GenerationIn):
        try:
            return store.create_generation(name, body.transform,
                                           body.transform_version,
                                           body.fail_after_partitions)
        except FeatError as e:
            raise ok(e)

    # ------------------------------------------------------- 事件写入

    @app.post("/api/v1/events", status_code=202)
    def ingest_event(body: EventIn):
        try:
            return store.ingest_event(**body.model_dump(exclude_none=True))
        except FeatError as e:
            raise ok(e)

    # ------------------------------------------------------- 事件订正台账

    @app.post("/api/v1/corrections")
    def submit_correction(body: CorrectionIn, response: Response):
        raw = body.raw if body.raw is not None else (
            {"value": body.value} if body.value is not None else None)
        try:
            result = store.submit_correction(
                source=body.source, record_key=body.record_key,
                revision=body.revision, op=body.op, event_ts=body.event_ts,
                entity_id=body.entity_id, raw=raw,
                fail_after_batches=body.fail_after_batches,
                crash_after_batches=body.crash_after_batches)
        except CorrectionConflict as e:
            raise HTTPException(status_code=409, detail=str(e))
        except FeatError as e:
            raise ok(e)
        response.status_code = 200 if result["replayed"] else 201
        return result

    @app.get("/api/v1/corrections")
    def corrections(source: Optional[str] = None,
                    record_key: Optional[str] = None,
                    state: Optional[str] = None):
        return store.list_corrections(source, record_key, state)

    @app.get("/api/v1/corrections/{correction_id}")
    def correction_detail(correction_id: int):
        try:
            return store.get_correction(correction_id)
        except FeatError as e:
            raise ok(e)

    @app.get("/api/v1/records/{source}/{record_key}")
    def record_detail(source: str, record_key: str):
        try:
            return store.get_record(source, record_key)
        except FeatError as e:
            raise ok(e)

    # ------------------------------------------------------- 读取接口

    @app.get("/api/v1/features/{name}/online/{entity_id}")
    def read_online(name: str, entity_id: str):
        try:
            return store.read_online(name, entity_id)
        except FeatError as e:
            raise ok(e)

    @app.get("/api/v1/features/{name}/offline/{entity_id}")
    def read_offline(name: str, entity_id: str, asof_ts: int):
        try:
            return store.read_offline(name, entity_id, asof_ts)
        except FeatError as e:
            raise ok(e)

    # ------------------------------------------- 特征时间旅行 / 代际差异
    # 两个新路由都带静态后缀片段，FastAPI 按声明顺序匹配，不会与上面的
    # {name}/online/{entity_id}、{name}/offline/{entity_id} 冲突。

    @app.get("/api/v1/features/{name}/snapshots/{entity_id}")
    def feature_snapshot(
        name: str, entity_id: str, asof_ts: int,
        gen_id: Optional[int] = None, gen_no: Optional[int] = None,
        limit: int = 100, cursor: Optional[str] = None,
    ):
        """按 (实体, 样本时刻, 代际) 读取可复现的特征快照（时间旅行）。"""
        try:
            return store.feature_snapshot(
                name, entity_id, asof_ts, gen_id=gen_id, gen_no=gen_no,
                limit=limit, cursor=cursor)
        except SnapshotConflict as e:
            raise HTTPException(status_code=409, detail=str(e))
        except FeatError as e:
            raise ok(e)

    @app.get("/api/v1/features/{name}/snapshots/{entity_id}/diff")
    def generation_diff(
        name: str, entity_id: str, asof_ts: int,
        a_gen_id: Optional[int] = None, a_gen_no: Optional[int] = None,
        b_gen_id: Optional[int] = None, b_gen_no: Optional[int] = None,
        limit: int = 100, cursor: Optional[str] = None,
    ):
        """两个代际在同一 (实体, 样本时刻) 的差异查询；分页游标钉住一对快照。"""
        try:
            return store.generation_diff(
                name, entity_id, asof_ts,
                a_gen_id=a_gen_id, a_gen_no=a_gen_no,
                b_gen_id=b_gen_id, b_gen_no=b_gen_no,
                limit=limit, cursor=cursor)
        except SnapshotConflict as e:
            raise HTTPException(status_code=409, detail=str(e))
        except FeatError as e:
            raise ok(e)

    # ------------------------------------------------------- 代际运维

    @app.post("/api/v1/generations/{gen_id}/validate")
    def validate(gen_id: int):
        try:
            return store.validate_generation(gen_id)
        except FeatError as e:
            raise ok(e)

    @app.post("/api/v1/generations/{gen_id}/promote")
    def promote(gen_id: int, force: bool = False):
        try:
            return store.promote_generation(gen_id, force=force)
        except AliasConflict as e:
            raise HTTPException(status_code=409, detail={
                "error": "alias_conflict",
                "message": str(e),
                "generation_id": gen_id,
                "current_serving_gen": (
                    store.serving_gen(
                        store.db.conn(),
                        store.get_generation(store.db.conn(), gen_id)["feature_id"])
                    or {}).get("gen_no"),
            })
        except FeatError as e:
            raise ok(e)

    @app.post("/api/v1/features/{name}/rollback")
    def rollback(name: str, body: RollbackIn):
        try:
            return store.rollback_feature(name, body.to_gen_no, body.reason)
        except AliasConflict as e:
            raise HTTPException(status_code=409, detail={
                "error": "alias_conflict",
                "message": str(e),
            })
        except FeatError as e:
            raise ok(e)

    @app.post("/api/v1/features/{name}/quality-check")
    def quality_check(name: str):
        try:
            return store.post_switch_quality_check(name)
        except AliasConflict as e:
            raise HTTPException(status_code=409, detail={
                "error": "alias_conflict",
                "message": str(e),
            })
        except FeatError as e:
            raise ok(e)

    # ------------------------------------------------------- 作业

    @app.get("/api/v1/jobs")
    def jobs():
        return store.list_jobs()

    @app.get("/api/v1/jobs/{job_id}")
    def job(job_id: int):
        try:
            return store.get_job(job_id)
        except FeatError as e:
            raise ok(e)

    @app.post("/api/v1/jobs/{job_id}/resume", status_code=201)
    def resume(job_id: int):
        try:
            return store.resume_failed_job(job_id)
        except FeatError as e:
            raise ok(e)

    @app.post("/api/v1/worker/tick")
    def worker_tick():
        from .worker import Worker
        return Worker(store, auto_promote=True).tick() or {"idle": True}

    # ----------------------------------------------- 多租户仲裁器

    @app.get("/api/v1/arbiter/tenants")
    def arb_tenants():
        return arbiter.status()["tenants"]

    @app.post("/api/v1/arbiter/tenants", status_code=201)
    def arb_tenant_create(body: TenantIn):
        try:
            return arbiter.upsert_tenant(body.name, body.weight, body.seats)
        except ArbiterError as e:
            raise ok(e)

    @app.get("/api/v1/arbiter/tenants/{name}")
    def arb_tenant_get(name: str):
        try:
            return arbiter.tenant_status(name)
        except ArbiterError as e:
            raise ok(e)

    @app.put("/api/v1/arbiter/tenants/{name}/policy")
    def arb_tenant_policy(name: str, body: TenantPolicyIn):
        try:
            return arbiter.update_policy(
                name, weight=body.weight, seats=body.seats, paused=body.paused)
        except ArbiterError as e:
            raise ok(e)

    @app.post("/api/v1/arbiter/tenants/{name}/pause")
    def arb_tenant_pause(name: str):
        try:
            return arbiter.pause(name)
        except ArbiterError as e:
            raise ok(e)

    @app.post("/api/v1/arbiter/tenants/{name}/resume")
    def arb_tenant_resume(name: str):
        try:
            return arbiter.resume(name)
        except ArbiterError as e:
            raise ok(e)

    @app.post("/api/v1/arbiter/jobs", status_code=201)
    def arb_job_enqueue(body: ArbJobIn, response: Response):
        try:
            result = arbiter.enqueue(
                body.tenant, body.kind, body.business_key,
                payload=body.payload, shards_total=body.shards_total)
        except ArbiterConflict as e:
            raise arb_conflict(e)
        except ArbiterError as e:
            raise ok(e)
        response.status_code = 200 if result["replayed"] else 201
        return result

    @app.get("/api/v1/arbiter/jobs")
    def arb_jobs(tenant: Optional[str] = None, state: Optional[str] = None):
        try:
            return arbiter.list_jobs(tenant, state)
        except ArbiterError as e:
            raise ok(e)

    @app.get("/api/v1/arbiter/jobs/{job_id}")
    def arb_job_get(job_id: int):
        try:
            return arbiter.get_job(job_id)
        except ArbiterError as e:
            raise ok(e)

    @app.get("/api/v1/arbiter/jobs/{job_id}/progress")
    def arb_job_progress(job_id: int):
        try:
            return arbiter.list_progress(job_id)
        except ArbiterError as e:
            raise ok(e)

    @app.post("/api/v1/arbiter/claims")
    def arb_claim(body: ClaimIn):
        try:
            return arbiter.claim(body.worker_id) or {"idle": True}
        except ArbiterError as e:
            raise ok(e)

    @app.post("/api/v1/arbiter/jobs/{job_id}/heartbeat")
    def arb_heartbeat(job_id: int, body: HeartbeatIn):
        try:
            return arbiter.heartbeat(
                job_id, body.claim_epoch, body.worker_id,
                cursor=body.cursor, note=body.note)
        except ArbiterConflict as e:
            raise arb_conflict(e)
        except ArbiterError as e:
            raise ok(e)

    @app.post("/api/v1/arbiter/jobs/{job_id}/progress")
    def arb_progress(job_id: int, body: ProgressIn):
        try:
            return arbiter.report_progress(
                job_id, body.claim_epoch, body.worker_id,
                body.cursor, note=body.note)
        except ArbiterConflict as e:
            raise arb_conflict(e)
        except ArbiterError as e:
            raise ok(e)

    @app.post("/api/v1/arbiter/jobs/{job_id}/complete")
    def arb_complete(job_id: int, body: FinishIn):
        try:
            return arbiter.complete(
                job_id, body.claim_epoch, body.worker_id, note=body.note)
        except ArbiterConflict as e:
            raise arb_conflict(e)
        except ArbiterError as e:
            raise ok(e)

    @app.post("/api/v1/arbiter/jobs/{job_id}/fail")
    def arb_fail(job_id: int, body: FailIn):
        try:
            return arbiter.fail(
                job_id, body.claim_epoch, body.worker_id, body.error)
        except ArbiterConflict as e:
            raise arb_conflict(e)
        except ArbiterError as e:
            raise ok(e)

    @app.post("/api/v1/arbiter/jobs/{job_id}/retry")
    def arb_retry(job_id: int):
        try:
            return arbiter.retry_failed(job_id)
        except ArbiterError as e:
            raise ok(e)

    @app.post("/api/v1/arbiter/reap-expired")
    def arb_reap():
        return {"reaped": arbiter.reap_expired()}

    @app.get("/api/v1/arbiter/history")
    def arb_history(limit: int = 50):
        return arbiter.history(limit)

    @app.get("/api/v1/arbiter/status")
    def arb_status():
        return arbiter.status()

    @app.get("/api/v1/audits")
    def audits(limit: int = 100):
        return store.list_audits(limit)

    # ------------------------------------------------------- 状态页

    @app.get("/", response_class=HTMLResponse)
    def status_page():
        ov = store.overview()
        ov["arbiter"] = arbiter.status()
        return _render_page(ov)

    return app


def _badge(state: str) -> str:
    colors = {
        "serving": "#1a7f37", "candidate": "#0969da", "building": "#9a6700",
        "shadow": "#57606a", "retired": "#cf222e",
        "queued": "#0969da", "running": "#9a6700",
        "succeeded": "#1a7f37", "failed": "#cf222e", "done": "#1a7f37",
        "pending": "#9a6700",
        "accepted": "#0969da", "applying": "#9a6700", "applied": "#1a7f37",
    }
    color = colors.get(state, "#57606a")
    return (f'<span style="background:{color}1a;color:{color};padding:2px 8px;'
            f'border-radius:10px;font-size:12px;font-weight:600">{state}</span>')


# 快照事件排除原因的中文说明与配色（与 store 中的原因码一一对应）
_EXCLUDE_REASONS = {
    "future": ("未来事件（event_ts &gt; 样本时刻，PIT 排除）", "#cf222e"),
    "out_of_window": ("已滑出滚动窗口", "#9a6700"),
    "superseded": ("已被订正 replace 取代", "#57606a"),
    "retracted": ("已被订正 retract 撤销", "#cf222e"),
}


def _esc(s) -> str:
    return (str(s).replace("&", "&amp;").replace("<", "&lt;")
            .replace(">", "&gt;").replace('"', "&quot;"))


def _reason_badge(reason: Optional[str]) -> str:
    if not reason:
        return '<span style="color:#1a7f37;font-weight:600">&#10003; 纳入</span>'
    label, color = _EXCLUDE_REASONS.get(reason, (reason, "#57606a"))
    return (f'<span style="background:{color}1a;color:{color};padding:2px 8px;'
            f'border-radius:10px;font-size:12px;font-weight:600">{label}</span>')


def _render_page(ov: dict) -> str:
    parts = [f"""<!doctype html><html lang="zh"><head><meta charset="utf-8">
<title>FeatForge 作业状态页</title>
<style>
body{{font-family:-apple-system,Segoe UI,Roboto,sans-serif;margin:24px;background:#f6f8fa;color:#1f2328}}
h1{{font-size:22px}} h2{{font-size:16px;margin-top:28px}}
table{{border-collapse:collapse;width:100%;background:#fff;font-size:13px}}
th,td{{border:1px solid #d0d7de;padding:6px 10px;text-align:left}}
th{{background:#f0f3f6}} .muted{{color:#57606a;font-size:12px}}
.card{{background:#fff;border:1px solid #d0d7de;border-radius:8px;padding:14px;margin-bottom:12px}}
code{{background:#eff1f3;padding:1px 5px;border-radius:4px}}
</style></head><body>
<h1>&#128295; FeatForge 特征物化服务 &middot; 作业状态页</h1>
<p class="muted">当前时间 {ov['time']} &middot; 定义 API 见 <code>/docs</code> &middot;
代际状态：building &rarr; candidate &rarr; serving（旧代降为 shadow，可回滚）</p>"""]

    parts.append("<h2>特征与代际</h2>")
    if not ov["features"]:
        parts.append('<p class="muted">暂无特征，POST /api/v1/features 创建。</p>')
    for f in ov["features"]:
        parts.append(f'<div class="card"><b>{f["name"]}</b> '
                     f'<span class="muted">源 <code>{f["source"]}</code> &middot; '
                     f'关联键 <code>{f["join_key"]}</code> &middot; 窗口 '
                     f'{f["window_seconds"]}s &middot; 变换 {f["transform"]} &middot; '
                     f'serving 代: <b>{f["serving_gen"] or "未切换"}</b> &middot; '
                     f'检查点 {f["checkpoints"]} &middot; 离线物化行 '
                     f'{f["offline_rows"]}</span>')
        parts.append('<table style="margin-top:8px"><tr><th>代</th><th>变换</th>'
                     '<th>状态</th><th>回填</th><th>回填末端</th><th>创建时间</th></tr>')
        for g in f["generations"]:
            parts.append(
                f"<tr><td>v{g['gen_no']}</td><td>{g['transform_name']}_v"
                f"{g['transform_version']}</td><td>{_badge(g['status'])}</td>"
                f"<td>{_badge(g['backfill_status'])}</td>"
                f"<td>{g['backfill_end'] or '-'}</td><td>{g['created_at']}</td></tr>")
        parts.append("</table></div>")

    parts.append("<h2>作业队列</h2><table><tr><th>#</th><th>类型</th>"
                 "<th>代际</th><th>状态</th><th>分区进度</th><th>尝试</th>"
                 "<th>错误</th><th>入队</th></tr>")
    for j in ov["jobs"][:50]:
        if j["kind"] == "correction":
            who = f"订正 corr#{j['correction_id']}"
        else:
            who = f"{j['feature_name'] or ''} gen#{j['gen_id']}"
        parts.append(
            f"<tr><td>{j['id']}</td><td>{j['kind']}"
            + (f" ({j['entity_id']})" if j["entity_id"] else "") + "</td>"
            f"<td>{who}</td><td>{_badge(j['state'])}</td>"
            f"<td>{j['partitions_done']}/{j['partitions_total']}</td>"
            f"<td>{j['attempts']}</td><td class='muted'>{j['error'] or ''}</td>"
            f"<td>{j['enqueued_at']}</td></tr>")
    parts.append("</table>")

    # ----------------------------------------------- 多租户仲裁器
    arb = ov.get("arbiter") or {}
    parts.append('<h2>&#9878; 多租户仲裁器（统一 ready set）</h2>')
    parts.append(
        f'<p class="muted">回填 / 订正 / 重算 / 快照同表排队；欠额(vruntime)'
        f"+老化选择（速率 &prop; 权重，老化速率 {arb.get('aging_rate', '-')}）；"
        f"租约 TTL {arb.get('lease_ttl', '-')}s，心跳超时分片回到 ready set，"
        "旧 claim_epoch 上报一律 409；暂停只阻止新 claim。</p>")
    if not arb.get("tenants"):
        parts.append('<p class="muted">暂无租户，POST /api/v1/arbiter/tenants 创建。</p>')
    else:
        parts.append(
            "<table><tr><th>租户</th><th>权重</th><th>席位占用</th>"
            "<th>虚拟运行时间</th><th>欠额</th><th>等待时长</th>"
            "<th>队列</th><th>完成/失败</th><th>总 claim 次数</th>"
            "<th>状态</th></tr>")
        for t in arb["tenants"]:
            wait = f"{t['waiting_seconds']}s" if t["queued"] else "-"
            paused = (' <span style="color:#cf222e;font-weight:600">已暂停'
                      "（不阻新开工分片）</span>") if t["paused"] else ""
            parts.append(
                f"<tr><td><b>{_esc(t['name'])}</b>{paused}</td>"
                f"<td>{t['weight']}</td>"
                f"<td>{t['seats_used']}/{t['seats']}</td>"
                f"<td>{t['vruntime']}</td>"
                f"<td>{t['deficit']}</td>"
                f"<td>{wait}</td>"
                f"<td>{t['queued']}</td>"
                f"<td>{t['succeeded']}/{t['failed']}</td>"
                f"<td>{t['total_claims']}</td>"
                f"<td>{'暂停' if t['paused'] else '正常'}</td></tr>")
        parts.append("</table>")
        parts.append(
            "<h3 style='font-size:14px'>统一 ready set 分片作业</h3><table>"
            "<tr><th>#</th><th>租户</th><th>类型</th><th>业务键</th>"
            "<th>游标</th><th>状态</th><th>epoch</th><th>持有者</th>"
            "<th>租约到期</th><th>尝试</th><th>错误</th></tr>")
        for j in arb.get("jobs", [])[:50]:
            parts.append(
                f"<tr><td>{j['id']}</td><td>{_esc(j['tenant_name'])}</td>"
                f"<td>{j['kind']}</td><td><code>{_esc(j['business_key'])}</code></td>"
                f"<td>{j['shard_cursor']}/{j['shards_total']}</td>"
                f"<td>{_badge(j['state'])}</td><td>{j['claim_epoch']}</td>"
                f"<td>{_esc(j['claim_worker'] or '-')}</td>"
                f"<td>{j['lease_expires_at'] or '-'}</td>"
                f"<td>{j['attempts']}</td>"
                f"<td class='muted'>{_esc(j['error'] or '')}</td></tr>")
        parts.append("</table>")
        parts.append(
            "<h3 style='font-size:14px'>近期仲裁选择结果</h3><table>"
            "<tr><th>时间</th><th>决策</th><th>租户</th><th>作业</th>"
            "<th>epoch</th><th>详情</th></tr>")
        for h in arb.get("history", [])[:30]:
            detail = _esc(json.dumps(h["detail"], ensure_ascii=False))
            parts.append(
                f"<tr><td>{h['at']}</td><td>{h['decision']}</td>"
                f"<td>{_esc(h['tenant_name'] or '-')}</td>"
                f"<td>{h['job_id'] or '-'}</td><td>{h['claim_epoch'] or '-'}</td>"
                f"<td class='muted'>{detail}</td></tr>")
        parts.append("</table>")

    parts.append("<h2>事件订正台账</h2>")
    if not ov["corrections"]:
        parts.append('<p class="muted">暂无订正。POST /api/v1/corrections '
                     '提交 replace / retract。</p>')
    else:
        parts.append('<div class="card"><b>修订链</b>'
                     '<span class="muted">（按记录分组，时间正序）</span>')
        for rec, chain in ov["correction_chains"].items():
            links = " &rarr; ".join(
                f"r{d['revision']} {d['op']} {_badge(d['state'])}"
                for d in chain)
            parts.append(f'<div style="margin:6px 0"><code>{rec}</code>'
                         f'&ensp;{links}</div>')
        parts.append('</div>')
        parts.append("<table><tr><th>#</th><th>记录</th><th>修订</th>"
                     "<th>操作</th><th>状态</th><th>受影响区间</th>"
                     "<th>落地进度</th><th>错误 / 恢复</th></tr>")
        for d in ov["corrections"]:
            scopes = "; ".join(
                f"g{s['gen_id']}:{s['entity_id']}[{s['s']},{s['e']}]"
                for s in d["scopes"]) or "-"
            prog = f"{d['batches_done']}/{d['batches_total']} 批"
            if d["job_state"]:
                prog += f" ({d['job_state']})"
            parts.append(
                f"<tr><td>{d['id']}</td>"
                f"<td><code>{d['source']}/{d['record_key']}</code></td>"
                f"<td>r{d['revision']}</td><td>{d['op']}</td>"
                f"<td>{_badge(d['state'])}</td>"
                f"<td class='muted'>{scopes}</td><td>{prog}</td>"
                f"<td class='muted'>{d['error'] or ''}</td></tr>")
        parts.append("</table>")

    parts.append("<h2>源事件水位（watermark）</h2><table><tr><th>源</th>"
                 "<th>水位时间</th><th>更新于</th></tr>")
    for w in ov["watermarks"]:
        parts.append(f"<tr><td><code>{w['source']}</code></td>"
                     f"<td>{w['watermark_ts']}</td><td>{w['updated_at']}</td></tr>")
    parts.append("</table>")

    parts.append("<h2>&#9201; 特征时间旅行 &amp; 代际差异解释</h2>")
    parts.append('<p class="muted">按 (实体, 样本时刻, 代际) 重建的可复现快照：'
                 "列出窗口边界、纳入事件、被排除的未来/窗外/已订正事件及其来源版本；"
                 "翻页用的 snapshot_id 在数据漂移时返回 409，绝不拼接新旧代清单。</p>")
    tt = ov.get("time_travel") or []
    if not tt:
        parts.append('<p class="muted">尚无 serving 代际或事件，暂无示例快照。</p>')
    for item in tt:
        s = item["snapshot"]
        w = s["window"]
        parts.append(
            f'<div class="card"><b>{item["feature"]}</b> &middot; 实体 '
            f'<code>{item["entity_id"]}</code> &middot; 样本时刻 '
            f'<code>{s["asof_ts"]}</code> &middot; 第 {s["gen_no"]} 代 '
            f'{s["transform"]} &middot; 快照值 <b>{s["value"]}</b>'
            f'<div class="muted" style="margin-top:4px">snapshot_id '
            f'<code style="font-size:11px">{s["snapshot_id"]}</code><br>'
            f'滚动窗口 ({w["window_open_exclusive"]}, {w["window_close_inclusive"]}]'
            f' &middot; 纳入 {s["included_count"]} 条 &middot; 排除 '
            f'{s["excluded_count"]} 条'
            + "".join(f' &middot; {_EXCLUDE_REASONS.get(k, (k, ""))[0]} '
                      f'{v} 条' for k, v in (s["excluded_by_reason"] or {}).items())
            + "</div>")
        parts.append('<table style="margin-top:8px"><tr><th>事件时间</th><th>序号</th>'
                     '<th>记录</th><th>来源版本</th><th>内容</th><th>是否纳入/原因</th></tr>')
        for e in s["events"]:
            parts.append(
                f'<tr><td>{e["event_ts"]}</td><td>{e["seq"]}</td>'
                f'<td><code>{e["record_key"]}</code></td>'
                f'<td>r{e["record_rev"]}</td>'
                f'<td class="muted">{_esc(json.dumps(e["raw"], ensure_ascii=False))}'
                f'</td><td>{_reason_badge(e["exclude_reason"])}</td></tr>')
        parts.append("</table>")
        d = item["diff"]
        if d:
            st = d["diff_stats"]
            parts.append(
                '<div style="margin-top:10px"><b>跨代差异</b> '
                f'第 {d["a"]["gen_no"]} 代（{d["a"]["transform"]}，值 '
                f'{d["a"]["value"]}） &harr; 第 {d["b"]["gen_no"]} 代'
                f'（{d["b"]["transform"]}，值 {d["b"]["value"]}）'
                f'<div class="muted" style="margin-top:2px">快照 '
                f'<code style="font-size:11px">{d["a"]["snapshot_id"]}</code><br>'
                f'<code style="font-size:11px">{d["b"]["snapshot_id"]}</code><br>'
                f'事件合计 {st["total_events"]} &middot; 值不同 '
                f'<b>{st["value_changed"]}</b> &middot; 值相同 {st["value_equal"]}'
                f' &middot; 仅 A 有 {st["a_only"]} &middot; 仅 B 有 {st["b_only"]}'
                "</div>")
            parts.append('<table style="margin-top:6px"><tr><th>事件时间</th>'
                         '<th>记录</th><th>来源版本</th><th>A 值</th><th>B 值</th>'
                         '<th>相对偏差</th><th>是否不同</th></tr>')
            for r in d["rows"]:
                mark = ("&#10007;" if r["different"] else "&#10003;")
                color = "#cf222e" if r["different"] else "#1a7f37"
                parts.append(
                    f'<tr><td>{r["event_ts"]}</td>'
                    f'<td><code>{r["record_key"]}</code></td>'
                    f'<td>r{r["record_rev"]}</td><td>{r["a_value"]}</td>'
                    f'<td>{r["b_value"]}</td><td>{r["relative_bias"]}</td>'
                    f'<td style="color:{color};font-weight:600">{mark}</td></tr>')
            parts.append("</table></div>")
        parts.append("</div>")

    parts.append("<h2>最近质量校验</h2><table><tr><th>#</th><th>代际</th>"
                 "<th>模式</th><th>覆盖率</th><th>p95偏差</th><th>不匹配率</th>"
                 "<th>结果</th></tr>")
    for v in ov["validations"]:
        result = "&#10003; 通过" if v["passed"] else "&#10007; 未通过"
        parts.append(
            f"<tr><td>{v['id']}</td><td>gen#{v['gen_id']}</td><td>{v['mode']}</td>"
            f"<td>{v['coverage']}</td><td>{v['bias_p95']}</td>"
            f"<td>{v['mismatch_rate']}</td><td>{result}</td></tr>")
    parts.append("</table>")

    parts.append("<h2>审计日志（最近 30 条）</h2><table><tr><th>时间</th>"
                 "<th>事件</th><th>对象</th><th>内容</th></tr>")
    for a in ov["audits"]:
        parts.append(f"<tr><td>{a['at']}</td><td>{a['kind']}</td>"
                     f"<td class='muted'>{a['ref']}</td><td>{a['message']}</td></tr>")
    parts.append("</table></body></html>")
    return "".join(parts)


app = create_app()
