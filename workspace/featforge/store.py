"""领域存储层：定义、在线写入、离线回填、代际、校验/切换/回滚。

时间一律为整型 Unix 秒，所有“特征在某时点的值”都严格满足点内时间（PIT）：
asof_ts 处的值只使用 event_ts <= asof_ts 的事件。
"""
from __future__ import annotations

import base64
import hashlib
import json
import time
from typing import Callable, Optional

from .config import settings
from .db import Database
from .transforms import apply_transform, get_transform, parse_window, relative_bias


class FeatError(Exception):
    """可返回给客户端的领域错误（HTTP 4xx 语义）。"""


class CorrectionConflict(Exception):
    """订正序号冲突：同号不同内容或序号回退（HTTP 409 语义）。"""


class SnapshotConflict(Exception):
    """分页游标绑定的完整快照已漂移（别名切换/订正/新事件到达，HTTP 409 语义）。

    翻页必须始终基于首页那一对完整代际快照；检测到漂移时拒绝续页，
    调用方应重新发起查询，绝不把新旧两代的事件清单拼在同一响应里。
    """


class AliasConflict(Exception):
    """服务别名在操作发起后被其他切换/回滚改写（HTTP 409 语义）。

    竞争条件是操作发起时解析到的服务指针（serving 代际 id）。两个并发切换
    由 BEGIN IMMEDIATE 串行化后，后执行的一方必须检测到指针已被先提交的
    一方改写，得到明确冲突，且不写任何成功历史；调用方重新读取当前指针后
    再决定如何操作。
    """


# 有效事件：未被订正取代、未被撤销。所有特征计算只使用有效事件；
# 被取代/撤销的行永久保留在事件日志中供审计，但不再参与计算。
_EFFECTIVE_EVENTS = "superseded_by IS NULL AND retracted_by IS NULL"


def _floor(ts: int, grid: int) -> int:
    return (ts // grid) * grid


def _ceil(ts: int, grid: int) -> int:
    return ((ts + grid - 1) // grid) * grid


class Store:
    def __init__(self, db: Database, clock: Callable[[], int] | None = None):
        self.db = db
        self.now = clock or (lambda: int(time.time()))

    # ------------------------------------------------------------------ 基础

    def _audit(self, c, kind: str, ref: str, message: str) -> None:
        c.execute(
            "INSERT INTO audits(at,kind,ref,message) VALUES(?,?,?,?)",
            (self.now(), kind, ref, message),
        )

    def get_feature(self, c, name_or_id) -> dict:
        row = c.execute(
            "SELECT * FROM features WHERE id=? OR name=?",
            (name_or_id, name_or_id),
        ).fetchone()
        if row is None:
            raise FeatError(f"特征不存在: {name_or_id}")
        return dict(row)

    def get_generation(self, c, gen_id: int) -> dict:
        row = c.execute("SELECT * FROM generations WHERE id=?", (gen_id,)).fetchone()
        if row is None:
            raise FeatError(f"代际不存在: {gen_id}")
        return dict(row)

    def serving_gen(self, c, feature_id: int) -> Optional[dict]:
        row = c.execute(
            "SELECT g.* FROM aliases a JOIN generations g ON g.id=a.serving_gen_id "
            "WHERE a.feature_id=?",
            (feature_id,),
        ).fetchone()
        return dict(row) if row else None

    def list_audits(self, limit: int = 100) -> list[dict]:
        c = self.db.conn()
        rows = c.execute(
            "SELECT * FROM audits ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in rows]

    def list_jobs(self) -> list[dict]:
        c = self.db.conn()
        rows = c.execute(
            "SELECT j.*, f.name AS feature_name FROM jobs j "
            "LEFT JOIN generations g ON g.id=j.gen_id "
            "LEFT JOIN features f ON f.id=COALESCE(j.feature_id,g.feature_id) "
            "ORDER BY j.id DESC"
        ).fetchall()
        return [dict(r) for r in rows]

    def get_job(self, job_id: int) -> dict:
        row = self.db.conn().execute("SELECT * FROM jobs WHERE id=?", (job_id,)).fetchone()
        if row is None:
            raise FeatError(f"作业不存在: {job_id}")
        return dict(row)

    # ------------------------------------------------------------- 特征定义

    def create_feature(
        self,
        name: str,
        source: str,
        join_key: str,
        value_field: str,
        window: str,
        transform: str,
        transform_version: int = 1,
        backfill_start: Optional[int] = None,
        partition_seconds: Optional[int] = None,
        sample_tick_seconds: Optional[int] = None,
        require_online_match: bool = False,
        fail_after_partitions: Optional[int] = None,
    ) -> dict:
        """创建特征定义，同时生成第 1 代（baseline）并入队其全量回填作业。"""
        try:
            t = get_transform(transform, transform_version)
            window_seconds = parse_window(window)
        except ValueError as e:
            raise FeatError(str(e))
        psec = partition_seconds or settings.partition_seconds
        tsec = sample_tick_seconds or settings.sample_tick_seconds
        if psec % tsec != 0:
            raise FeatError("分区长度必须是样本刻度的整数倍")
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            now = self.now()
            bf_start = _floor(backfill_start, tsec) if backfill_start is not None else now
            cur = c.execute(
                "INSERT INTO features(name,source,join_key,value_field,window_seconds,"
                "transform_name,transform_version,kind,require_online_match,backfill_start,"
                "partition_seconds,sample_tick_seconds,created_at,updated_at) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (
                    name, source, join_key, value_field, window_seconds,
                    transform, transform_version, t.kind, int(require_online_match),
                    bf_start, psec, tsec, now, now,
                ),
            )
            fid = cur.lastrowid
            c.execute(
                "INSERT INTO generations(feature_id,gen_no,transform_name,transform_version,"
                "kind,created_at,status,backfill_status,backfill_end,fail_after_partitions) "
                "VALUES(?,1,?,?,?,?,'building','pending',NULL,?)",
                (fid, transform, transform_version, t.kind, now, fail_after_partitions),
            )
            gen_id = c.execute(
                "SELECT id FROM generations WHERE feature_id=? AND gen_no=1", (fid,)
            ).fetchone()[0]
            # baseline：回填区间开放式末端，运行时按当时水位截断
            self._enqueue_job(
                c, "backfill", gen_id=gen_id, dedup_key=f"backfill:gen{gen_id}",
                fail_after=fail_after_partitions,
            )
            self._audit(c, "feature_created", f"feature:{fid}",
                        f"特征 {name} 创建，第 1 代 #{gen_id}({t.key})，回填已入队")
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        return self.get_feature(c, fid)

    def create_generation(
        self,
        feature_ref,
        transform: str,
        transform_version: int,
        fail_after_partitions: Optional[int] = None,
    ) -> dict:
        """定义升级：在旧代并行存在的前提下创建新代际，两代同时接收在线写入。"""
        try:
            t = get_transform(transform, transform_version)
        except ValueError as e:
            raise FeatError(str(e))
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            f = self.get_feature(c, feature_ref)
            if t.kind != f["kind"]:
                raise FeatError(f"值类型不可变更: {f['kind']} -> {t.kind}")
            row = c.execute(
                "SELECT MAX(gen_no) AS m FROM generations WHERE feature_id=?", (f["id"],)
            ).fetchone()
            gen_no = (row["m"] or 0) + 1
            c.execute(
                "INSERT INTO generations(feature_id,gen_no,transform_name,transform_version,"
                "kind,created_at,status,backfill_status,backfill_end,fail_after_partitions) "
                "VALUES(?,?,?,?,?,?,'building','pending',NULL,?)",
                (f["id"], gen_no, transform, transform_version, t.kind,
                 self.now(), fail_after_partitions),
            )
            new_id = c.execute(
                "SELECT id FROM generations WHERE feature_id=? AND gen_no=?",
                (f["id"], gen_no),
            ).fetchone()[0]
            self._enqueue_job(
                c, "backfill", gen_id=new_id, dedup_key=f"backfill:gen{new_id}",
                fail_after=fail_after_partitions,
            )
            c.execute(
                "UPDATE features SET transform_name=?, transform_version=?, updated_at=? "
                "WHERE id=?",
                (transform, transform_version, self.now(), f["id"]),
            )
            self._audit(c, "generation_created", f"gen:{new_id}",
                        f"特征 {f['name']} 定义升级：第 {gen_no} 代 ({t.key})，"
                        f"与旧代并行计算，回填已入队")
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        return self.get_generation(c, new_id)

    def _enqueue_job(
        self, c, kind: str, gen_id: int, dedup_key: str, entity_id=None,
        range_start=None, range_end=None, feature_id=None, fail_after=None,
        correction_id=None,
    ) -> Optional[int]:
        """幂等入队：相同 dedup_key 的排队/运行中作业直接复用。"""
        existing = c.execute(
            "SELECT id FROM jobs WHERE dedup_key=? AND state IN ('queued','running')",
            (dedup_key,),
        ).fetchone()
        if existing:
            return existing["id"]
        cur = c.execute(
            "INSERT INTO jobs(kind,gen_id,feature_id,entity_id,range_start,range_end,"
            "correction_id,dedup_key,state,enqueued_at) "
            "VALUES(?,?,?,?,?,?,?,?,'queued',?)",
            (kind, gen_id, feature_id, entity_id, range_start, range_end,
             correction_id, dedup_key, self.now()),
        )
        jid = cur.lastrowid
        if fail_after is not None and kind == "backfill":
            c.execute(
                "UPDATE generations SET fail_after_partitions=? WHERE id=?",
                (fail_after, gen_id),
            )
        return jid

    # ------------------------------------------------------------- 在线写入

    def _watermark(self, c, source: str) -> Optional[int]:
        row = c.execute(
            "SELECT watermark_ts FROM watermarks WHERE source=?", (source,)
        ).fetchone()
        return row["watermark_ts"] if row else None

    def ingest_event(
        self, source: str, entity_id: str, event_ts: int, value=None,
        dedup_key: Optional[str] = None, raw: Optional[dict] = None,
    ) -> dict:
        """写入一条源事件并把它应用到所有相关代际的在线当前值。

        迟到事件（event_ts < 当前水位）除在线更新外，还会入队“仅受影响范围”的
        重算作业，不触碰无关分区。
        """
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            dkey = dedup_key or f"auto:{source}:{entity_id}:{event_ts}:{json.dumps(value, sort_keys=True, default=str)}"
            dup = c.execute(
                "SELECT 1 FROM events WHERE dedup_key=?", (dkey,)
            ).fetchone()
            if dup:
                c.execute("COMMIT")
                return {"dedup": True, "late": False, "recompute_jobs": []}

            wm = self._watermark(c, source)
            is_late = wm is not None and event_ts < wm
            seq = c.execute(
                "SELECT COALESCE(MAX(seq),0)+1 AS s FROM events WHERE source=?",
                (source,),
            ).fetchone()["s"]
            raw_json = json.dumps(raw if raw is not None else {"value": value},
                                  sort_keys=True, default=str)
            c.execute(
                "INSERT INTO events(source,entity_id,event_ts,ingested_at,seq,value,"
                "raw_json,dedup_key,is_late,record_key,record_rev) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,0)",
                (source, entity_id, event_ts, self.now(), seq,
                 None if value is None else json.dumps(value, default=str),
                 raw_json, dkey, int(is_late), dkey),
            )
            new_wm = max(wm if wm is not None else event_ts, event_ts)
            c.execute(
                "INSERT INTO watermarks(source,watermark_ts,updated_at) VALUES(?,?,?) "
                "ON CONFLICT(source) DO UPDATE SET watermark_ts=excluded.watermark_ts,"
                "updated_at=excluded.updated_at",
                (source, new_wm, self.now()),
            )

            recompute_jobs: list[int] = []
            feats = c.execute("SELECT * FROM features WHERE source=?", (source,)).fetchall()
            for f in feats:
                snap_tick = _ceil(event_ts, f["sample_tick_seconds"])
                # 所有非退役代际都更新在线当前值（影子代也要随时可切）；
                # 但迟到重算只对有读者的代际（serving/shadow）入队，候选代等回填。
                gens = c.execute(
                    "SELECT * FROM generations WHERE feature_id=? AND status!='retired'",
                    (f["id"],),
                ).fetchall()
                for g in gens:
                    refresh_ticks = None
                    if is_late and g["status"] in ("serving", "shadow"):
                        tsec = f["sample_tick_seconds"]
                        last = _floor(event_ts + f["window_seconds"], tsec)
                        refresh_ticks = list(range(snap_tick, last + tsec, tsec))
                    self._apply_online(
                        c, dict(f), dict(g), entity_id,
                        snapshot_tick=snap_tick, recompute_ticks=refresh_ticks,
                    )
                    if is_late and g["status"] in ("serving", "shadow"):
                        jid = self._enqueue_late_recompute(
                            c, dict(f), dict(g), entity_id, event_ts, snap_tick)
                        if jid is not None:
                            recompute_jobs.append(jid)
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        return {
            "dedup": False,
            "late": bool(is_late),
            "watermark": new_wm,
            "recompute_jobs": [j for j in recompute_jobs if j is not None],
        }

    def _window_events(
        self, c, source: str, entity_id: str, window_seconds: int, anchor_ts: int,
        gen_created_at: int,
    ) -> list[tuple[int, int, object]]:
        """点内时间正确地取 (anchor_ts-window, anchor_ts] 滚动窗口内事件。

        左开右闭：恰好在 window 秒前的事件已滑出该窗口（标准滚动窗口语义）。
        """
        rows = c.execute(
            "SELECT event_ts,seq,raw_json FROM events "
            "WHERE source=? AND entity_id=? AND event_ts>? AND event_ts<=? "
            f"AND {_EFFECTIVE_EVENTS} "
            "ORDER BY event_ts,seq",
            (source, entity_id, anchor_ts - window_seconds, anchor_ts),
        ).fetchall()
        out = []
        for r in rows:
            out.append((r["event_ts"], r["seq"], json.loads(r["raw_json"])))
        return out

    def _extract(self, feature: dict, raw: dict):
        vf = feature["value_field"]
        return raw.get(vf) if isinstance(raw, dict) else None

    def _compute_value(self, feature: dict, gen: dict,
                       events: list[tuple[int, int, dict]]) -> object:
        t = get_transform(gen["transform_name"], gen["transform_version"])
        extracted = [(ts, seq, self._extract(feature, raw)) for ts, seq, raw in events]
        return apply_transform(t, extracted)

    def _apply_online(self, c, feature: dict, gen: dict, entity_id: str,
                      snapshot_tick: Optional[int] = None,
                      recompute_ticks: Optional[list[int]] = None) -> None:
        """用该实体最新事件时间作为窗口右端，重算在线当前值。

        在线历史严格按样本刻度做点内时间快照，与离线同口径：事件在 tick 处生效，
        tick 的值只使用 event_ts <= tick 的事件。因此事件 event_ts 写入的是
        ceil(event_ts) 这个刻度。迟到事件可通过 recompute_ticks 批量刷新后续刻度。
        """
        row = c.execute(
            "SELECT MAX(event_ts) AS anchor FROM events WHERE source=? AND entity_id=? "
            f"AND {_EFFECTIVE_EVENTS}",
            (feature["source"], entity_id),
        ).fetchone()
        if row["anchor"] is None:
            return
        anchor = row["anchor"]
        events = self._window_events(
            c, feature["source"], entity_id, feature["window_seconds"], anchor,
            gen["created_at"],
        )
        value = self._compute_value(feature, gen, events)
        num_value, cat_value = self._split_value(value, gen["kind"])
        c.execute(
            "INSERT INTO online_current(gen_id,entity_id,anchor_ts,num_value,cat_value) "
            "VALUES(?,?,?,?,?) ON CONFLICT(gen_id,entity_id) DO UPDATE SET "
            "anchor_ts=excluded.anchor_ts,num_value=excluded.num_value,"
            "cat_value=excluded.cat_value",
            (gen["id"], entity_id, anchor, num_value, cat_value),
        )
        tsec = feature["sample_tick_seconds"]
        ticks = list(dict.fromkeys(
            ([snapshot_tick] if snapshot_tick is not None else [])
            + (recompute_ticks or [])
        ))
        for tick in ticks:
            if tick < feature["backfill_start"]:
                continue
            tick_events = self._window_events(
                c, feature["source"], entity_id, feature["window_seconds"],
                tick, gen["created_at"],
            )
            tv = self._compute_value(feature, gen, tick_events)
            tn, tc = self._split_value(tv, gen["kind"])
            c.execute(
                "INSERT INTO online_history(gen_id,entity_id,asof_ts,num_value,cat_value) "
                "VALUES(?,?,?,?,?) ON CONFLICT(gen_id,entity_id,asof_ts) DO UPDATE SET "
                "num_value=excluded.num_value,cat_value=excluded.cat_value",
                (gen["id"], entity_id, tick, tn, tc),
            )

    @staticmethod
    def _split_value(value, kind: str):
        if value is None:
            return None, None
        if kind == "categorical":
            return None, str(value)
        return float(value), None

    def _rebuild_online_current(self, c, feature: dict, gen: dict) -> int:
        """为该代际重建所有实体的在线当前值（回填完成时调用）。

        新代际可能在历史事件已存在之后才创建，在线写入没有它的状态；
        这里以每个实体的最新事件时间为窗口右端做一次 PIT 重建，
        保证别名一切换到该代，读者立刻看到完整当前值。
        """
        rows = c.execute(
            "SELECT entity_id, MAX(event_ts) AS anchor FROM events "
            f"WHERE source=? AND {_EFFECTIVE_EVENTS} GROUP BY entity_id",
            (feature["source"],),
        ).fetchall()
        n = 0
        for r in rows:
            events = self._window_events(
                c, feature["source"], r["entity_id"], feature["window_seconds"],
                r["anchor"], gen["created_at"],
            )
            value = self._compute_value(feature, gen, events)
            num_value, cat_value = self._split_value(value, gen["kind"])
            c.execute(
                "INSERT INTO online_current(gen_id,entity_id,anchor_ts,num_value,cat_value) "
                "VALUES(?,?,?,?,?) ON CONFLICT(gen_id,entity_id) DO UPDATE SET "
                "anchor_ts=excluded.anchor_ts,num_value=excluded.num_value,"
                "cat_value=excluded.cat_value",
                (gen["id"], r["entity_id"], r["anchor"], num_value, cat_value),
            )
            n += 1
        return n

    def _enqueue_late_recompute(self, c, feature: dict, gen: dict,
                                entity_id: str, event_ts: int,
                                snap_tick: int) -> Optional[int]:
        """迟到事件只重算“受影响范围”：[事件生效刻度, 事件时间+窗口]。

        同一代际/实体已有排队作业则扩展其区间，绝不重复任务。
        """
        start = snap_tick
        # 上界向上对齐：含事件的最后一个窗口是右端为 ceil(event_ts+window) 的刻度
        end = _ceil(event_ts + feature["window_seconds"],
                    feature["sample_tick_seconds"])
        # 不超过该代际已回填覆盖的末端，避免把重算范围扩到回填区间之外
        if gen["backfill_end"] is not None:
            end = min(end, gen["backfill_end"])
        if end < start:
            return None
        dkey = f"recompute:gen{gen['id']}:{entity_id}"
        existing = c.execute(
            "SELECT id,range_start,range_end FROM jobs "
            "WHERE dedup_key=? AND state IN ('queued','running')",
            (dkey,),
        ).fetchone()
        if existing:
            c.execute(
                "UPDATE jobs SET range_start=MIN(range_start,?), "
                "range_end=MAX(range_end,?) WHERE id=?",
                (start, end, existing["id"]),
            )
            return existing["id"]
        # 该代际尚未回填完成时，等回填自然覆盖，不重复入队
        if gen["backfill_status"] != "done":
            return None
        psec = feature["partition_seconds"]
        cp = c.execute(
            "SELECT 1 FROM checkpoints WHERE gen_id=? AND partition_start=?",
            (gen["id"], _floor(start, psec)),
        ).fetchone()
        if not cp:
            return None
        return self._enqueue_job(
            c, "recompute", gen_id=gen["id"], entity_id=entity_id,
            range_start=start, range_end=end, dedup_key=dkey,
        )

    # ------------------------------------------------------------- 离线回填

    def claim_next_job(self) -> Optional[dict]:
        """原子领取一个排队作业（并发执行者只有一个能拿到）。"""
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            row = c.execute(
                "SELECT * FROM jobs WHERE state='queued' ORDER BY id LIMIT 1"
            ).fetchone()
            if row is None:
                c.execute("COMMIT")
                return None
            job = dict(row)
            c.execute(
                "UPDATE jobs SET state='running', started_at=?, attempts=attempts+1 "
                "WHERE id=?",
                (self.now(), job["id"]),
            )
            # 返回更新后的行：attempts 已自增，作业逻辑据此区分首次尝试与重跑
            job = dict(c.execute("SELECT * FROM jobs WHERE id=?",
                                 (job["id"],)).fetchone())
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        return job

    def run_job(self, job: dict) -> dict:
        if job["kind"] == "backfill":
            return self._run_backfill(job)
        if job["kind"] == "correction":
            return self._run_correction(job)
        return self._run_recompute(job)

    def _run_backfill(self, job: dict) -> dict:
        c = self.db.conn()
        gen = self.get_generation(c, job["gen_id"])
        f = self.get_feature(c, gen["feature_id"])
        psec, tsec = f["partition_seconds"], f["sample_tick_seconds"]
        # 开放式末端的 baseline 在开跑时按当时水位确定：回填不会看到未来数据
        wm = self._watermark(c, f["source"])
        end = job["range_end"] if job["range_end"] is not None else (
            wm if wm is not None else self.now()
        )
        end = _floor(end, tsec)
        c.execute(
            "UPDATE generations SET backfill_status='running', backfill_end=? WHERE id=?",
            (end, gen["id"]),
        )
        c.commit()

        start = f["backfill_start"]
        partitions = list(range(_floor(start, psec), end, psec))
        total = len(partitions)
        done = 0
        injected_fail = False
        for pstart in partitions:
            if c.execute(
                "SELECT 1 FROM checkpoints WHERE gen_id=? AND partition_start=?",
                (gen["id"], pstart),
            ).fetchone():
                done += 1
                continue
            # 故障注入只作用于原始回填作业；resume 作业必须能跑完
            fail_after = (gen["fail_after_partitions"]
                          if not job["dedup_key"].startswith("resume:") else None)
            # 成功物化 N 个“新”分区后失败（用于演示检查点续跑）
            if fail_after is not None and done >= fail_after:
                injected_fail = True
                break
            pend = min(pstart + psec, end)
            include_end = pend == end  # 最后一个分区额外物化末端刻度 end
            rows = self._materialize_partition(c, f, gen, pstart, pend, include_end)
            c.execute(
                "INSERT INTO checkpoints(gen_id,partition_start,finished_at,rows_written) "
                "VALUES(?,?,?,?)",
                (gen["id"], pstart, self.now(), rows),
            )
            done += 1
            c.execute(
                "UPDATE jobs SET partitions_total=?, partitions_done=? WHERE id=?",
                (total, done, job["id"]),
            )
            c.commit()

        try:
            c.execute("BEGIN IMMEDIATE")
            if injected_fail:
                c.execute(
                    "UPDATE jobs SET state='failed', error=?, finished_at=? WHERE id=?",
                    (f"注入故障：已完成 {done}/{total} 分区，可从检查点继续",
                     self.now(), job["id"]),
                )
                c.execute(
                    "UPDATE generations SET backfill_status='failed' WHERE id=?",
                    (gen["id"],),
                )
                self._audit(c, "backfill_failed", f"job:{job['id']}",
                            f"第 {gen['id']} 代回填在分区 {done}/{total} 处失败（检查点已保存）")
                c.execute("COMMIT")
            else:
                # 回填成功后，用各实体最新事件时间作为窗口右端重建在线当前值。
                # 这样即使该代际是在历史事件之后才创建，切换/回滚后读者也能立即读到值。
                self._rebuild_online_current(c, f, gen)
                c.execute(
                    "UPDATE jobs SET state='succeeded', partitions_total=?, "
                    "partitions_done=?, finished_at=?, error=NULL WHERE id=?",
                    (total, done, self.now(), job["id"]),
                )
                c.execute(
                    "UPDATE generations SET backfill_status='done', status='candidate', "
                    "backfill_end=? WHERE id=?",
                    (end, gen["id"]),
                )
                self._audit(c, "backfill_done", f"job:{job['id']}",
                            f"第 {gen['id']} 代回填完成：{done}/{total} 分区，"
                            f"区间 [{start},{end})，等待质量校验")
                c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        return self.get_job(job["id"])

    def _entity_universe(self, c, feature: dict, lo: int, hi: int) -> list[str]:
        """分区实体全集：在 (lo-window, hi) 内出现过该源事件的所有实体。

        包含“窗口起点之前刚有事件、分区内无新事件”的实体，否则其跨分区持续
        特征值会丢点（覆盖率漏洞）。
        """
        rows = c.execute(
            "SELECT DISTINCT entity_id FROM events WHERE source=? AND event_ts>? "
            f"AND event_ts<? AND {_EFFECTIVE_EVENTS} ORDER BY entity_id",
            (feature["source"], lo - feature["window_seconds"], hi),
        ).fetchall()
        return [r["entity_id"] for r in rows]

    def _materialize_partition(
        self, c, feature: dict, gen: dict, pstart: int, pend: int,
        include_end: bool = False,
    ) -> int:
        """物化一个分区内全部实体、全部样本刻度的 PIT 值。返回写入行数。

        分区刻度为 [pstart, pend)；最后一个分区额外物化末端刻度 pend。
        """
        tsec = feature["sample_tick_seconds"]
        entities = self._entity_universe(c, feature, pstart, pend)
        ticks = list(range(pstart, pend, tsec)) + ([pend] if include_end else [])
        rows = 0
        for entity_id in entities:
            for tick in ticks:
                events = self._window_events(
                    c, feature["source"], entity_id, feature["window_seconds"],
                    tick, gen["created_at"],
                )
                value = self._compute_value(feature, gen, events)
                num_value, cat_value = self._split_value(value, gen["kind"])
                c.execute(
                    "INSERT INTO offline_values(gen_id,entity_id,asof_ts,num_value,cat_value) "
                    "VALUES(?,?,?,?,?) ON CONFLICT(gen_id,entity_id,asof_ts) DO UPDATE SET "
                    "num_value=excluded.num_value,cat_value=excluded.cat_value",
                    (gen["id"], entity_id, tick, num_value, cat_value),
                )
                rows += 1
        return rows

    def resume_failed_job(self, job_id: int) -> dict:
        """把失败作业重新排队。

        回填作业按分区检查点跳过已完成分区；订正作业按批次进度跳过已落地
        批次。续跑作业不再触发故障注入，保证能跑完。
        """
        c = self.db.conn()
        job = self.get_job(job_id)
        if job["state"] != "failed":
            raise FeatError("只能继续失败的作业")
        if job["kind"] not in ("backfill", "correction"):
            raise FeatError("仅回填/订正作业支持检查点续跑")
        c.execute(
            "INSERT INTO jobs(kind,gen_id,feature_id,entity_id,range_start,range_end,"
            "correction_id,dedup_key,state,enqueued_at) "
            "VALUES(?,?,?,?,?,?,?,?, 'queued', ?)",
            (job["kind"], job["gen_id"], job["feature_id"], job["entity_id"],
             job["range_start"], job["range_end"], job["correction_id"],
             f"resume:job{job['id']}:{int(time.time())}", self.now()),
        )
        jid = c.execute("SELECT last_insert_rowid()").fetchone()[0]
        if job["kind"] == "backfill":
            c.execute(
                "UPDATE generations SET backfill_status='pending' WHERE id=?",
                (job["gen_id"],),
            )
            self._audit(c, "backfill_resumed", f"job:{jid}",
                        f"回填作业 #{job['id']} 从检查点继续为 #{jid}")
        else:
            c.execute(
                "UPDATE corrections SET state='applying', job_id=?, error=NULL "
                "WHERE id=?",
                (jid, job["correction_id"]),
            )
            self._audit(c, "correction_resumed", f"job:{jid}",
                        f"订正作业 #{job['id']} 从批次进度继续为 #{jid}")
        c.commit()
        return self.get_job(jid)

    def _run_recompute(self, job: dict) -> dict:
        """迟到事件的受影响范围重算：只重算指定实体、指定刻度区间。

        离线 PIT 值与在线刻度历史按同一口径刷新，保证二者重算后仍一致。
        """
        c = self.db.conn()
        gen = self.get_generation(c, job["gen_id"])
        f = self.get_feature(c, gen["feature_id"])
        entity_id, tsec = job["entity_id"], f["sample_tick_seconds"]
        start = _floor(job["range_start"], tsec)
        end = _floor(job["range_end"], tsec)
        n = 0
        for tick in range(start, end + tsec, tsec):
            events = self._window_events(
                c, f["source"], entity_id, f["window_seconds"], tick, gen["created_at"]
            )
            value = self._compute_value(f, gen, events)
            num_value, cat_value = self._split_value(value, gen["kind"])
            for table in ("offline_values", "online_history"):
                c.execute(
                    f"INSERT INTO {table}(gen_id,entity_id,asof_ts,num_value,cat_value) "
                    "VALUES(?,?,?,?,?) ON CONFLICT(gen_id,entity_id,asof_ts) DO UPDATE SET "
                    "num_value=excluded.num_value,cat_value=excluded.cat_value",
                    (gen["id"], entity_id, tick, num_value, cat_value),
                )
            n += 1
        c.execute(
            "UPDATE jobs SET state='succeeded', finished_at=?, partitions_total=?, "
            "partitions_done=? WHERE id=?",
            (self.now(), 1, 1, job["id"]),
        )
        self._audit(c, "recompute_done", f"job:{job['id']}",
                    f"迟到事件重算完成：第 {gen['id']} 代 实体 {entity_id} "
                    f"[{start},{end}] 共 {n} 个时点")
        c.commit()
        return self.get_job(job["id"])

    # ------------------------------------------------------------- 事件订正

    def submit_correction(
        self, source: str, record_key: str, revision: int, op: str,
        event_ts: Optional[int] = None, entity_id: Optional[str] = None,
        raw: Optional[dict] = None,
        fail_after_batches: Optional[int] = None,
        crash_after_batches: Optional[int] = None,
    ) -> dict:
        """提交一条事件订正（replace/retract），形成不可变台账条目。

        序号规则（按 source+record_key 单调推进，BEGIN IMMEDIATE 串行化并发）：
        - revision 大于当前序号：接受。同一事务内原子改写事件日志
          （replace 旧行标记取代并插入新版本行；retract 旧行标记撤销），
          计算影响范围并入队可恢复的落地作业——整次订正对外是一个原子版本。
        - revision 已受理且内容相同：幂等重放，返回原结果，不产生新条目。
        - 同号不同内容 / 序号回退：CorrectionConflict（HTTP 409）。
        """
        if op not in ("replace", "retract"):
            raise FeatError("订正操作必须是 replace 或 retract")
        if not isinstance(revision, int) or isinstance(revision, bool) or revision < 1:
            raise FeatError("revision 必须是 >= 1 的整数")
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            cur = c.execute(
                "SELECT * FROM events WHERE source=? AND record_key=? "
                "ORDER BY record_rev DESC, id DESC LIMIT 1",
                (source, record_key),
            ).fetchone()
            if cur is None:
                raise FeatError(f"记录不存在: {source}/{record_key}")
            cur = dict(cur)
            canonical = json.dumps(
                {"op": op, "event_ts": event_ts, "entity_id": entity_id, "raw": raw},
                sort_keys=True, default=str)
            chash = hashlib.sha256(canonical.encode()).hexdigest()
            last = c.execute(
                "SELECT * FROM corrections WHERE source=? AND record_key=? "
                "ORDER BY revision DESC LIMIT 1",
                (source, record_key),
            ).fetchone()
            if last is not None and revision <= last["revision"]:
                prev = c.execute(
                    "SELECT * FROM corrections WHERE source=? AND record_key=? "
                    "AND revision=?",
                    (source, record_key, revision),
                ).fetchone()
                if prev is not None and prev["content_hash"] == chash:
                    c.execute("COMMIT")
                    return {"replayed": True,
                            "correction": self.get_correction(prev["id"]),
                            "job_id": prev["job_id"]}
                if prev is not None:
                    raise CorrectionConflict(
                        f"记录 {source}/{record_key} 的 revision {revision} 已受理且"
                        f"内容不同，同一序号不允许更改内容")
                raise CorrectionConflict(
                    f"记录 {source}/{record_key} 当前 revision={last['revision']}，"
                    f"提交的 revision={revision} 已过期：订正只能按单调序号推进")

            now = self.now()
            new_ts = event_ts if event_ts is not None else cur["event_ts"]
            new_entity = entity_id if entity_id is not None else cur["entity_id"]
            new_raw_json = (json.dumps(raw, sort_keys=True, default=str)
                            if raw is not None else cur["raw_json"])
            effective = cur["superseded_by"] is None and cur["retracted_by"] is None
            cur_corr = c.execute(
                "INSERT INTO corrections(source,record_key,revision,op,entity_id,"
                "new_event_ts,new_entity_id,new_raw_json,content_hash,"
                "supersedes_event_id,state,created_at,"
                "fail_after_batches,crash_after_batches) "
                "VALUES(?,?,?,?,?,?,?,?,?,?,'accepted',?,?,?)",
                (source, record_key, revision, op, cur["entity_id"],
                 new_ts if op == "replace" else None,
                 new_entity if op == "replace" else None,
                 new_raw_json if op == "replace" else None,
                 chash, cur["id"], now, fail_after_batches, crash_after_batches),
            )
            corr_id = cur_corr.lastrowid
            new_event_id = None
            if op == "replace":
                if effective:
                    c.execute("UPDATE events SET superseded_by=? WHERE id=?",
                              (corr_id, cur["id"]))
                seq = c.execute(
                    "SELECT COALESCE(MAX(seq),0)+1 AS s FROM events WHERE source=?",
                    (source,),
                ).fetchone()["s"]
                wm = self._watermark(c, source)
                c.execute(
                    "INSERT INTO events(source,entity_id,event_ts,ingested_at,seq,"
                    "value,raw_json,dedup_key,is_late,record_key,record_rev) "
                    "VALUES(?,?,?,?,?,NULL,?,?,?,?,?)",
                    (source, new_entity, new_ts, now, seq, new_raw_json,
                     f"{record_key}#r{revision}",
                     int(wm is not None and new_ts < wm), record_key, revision),
                )
                new_event_id = c.execute("SELECT last_insert_rowid()").fetchone()[0]
                new_wm = max(wm if wm is not None else new_ts, new_ts)
                if wm is None or new_wm != wm:
                    c.execute(
                        "INSERT INTO watermarks(source,watermark_ts,updated_at) "
                        "VALUES(?,?,?) ON CONFLICT(source) DO UPDATE SET "
                        "watermark_ts=excluded.watermark_ts,"
                        "updated_at=excluded.updated_at",
                        (source, new_wm, now),
                    )
            elif effective:
                c.execute("UPDATE events SET retracted_by=? WHERE id=?",
                          (corr_id, cur["id"]))
            c.execute("UPDATE corrections SET new_event_id=? WHERE id=?",
                      (new_event_id, corr_id))

            # 影响范围：记录旧版本与新版本各自生效的刻度区间，逐 (代际, 实体) 落库。
            # 只刷新这些实体与时间段；所有仍在计算的代际（非 retired）都覆盖。
            points = [(cur["entity_id"], cur["event_ts"])]
            if op == "replace" and (new_entity, new_ts) not in points:
                points.append((new_entity, new_ts))
            feats = [dict(r) for r in c.execute(
                "SELECT * FROM features WHERE source=?", (source,)).fetchall()]
            total_batches = 0
            lo = hi = None
            for f in feats:
                tsec = f["sample_tick_seconds"]
                gens = c.execute(
                    "SELECT id FROM generations WHERE feature_id=? "
                    "AND status!='retired'",
                    (f["id"],),
                ).fetchall()
                for g in gens:
                    for ent, ts in points:
                        start = max(_ceil(ts, tsec), f["backfill_start"])
                        end = _ceil(ts + f["window_seconds"], tsec)
                        if end < start:
                            continue
                        c.execute(
                            "INSERT INTO correction_scopes(correction_id,gen_id,"
                            "entity_id,tick_start,tick_end) VALUES(?,?,?,?,?)",
                            (corr_id, g["id"], ent, start, end),
                        )
                        n_ticks = (end - start) // tsec + 1
                        total_batches += -(-n_ticks // settings.correction_batch_ticks)
                        lo = start if lo is None else min(lo, start)
                        hi = end if hi is None else max(hi, end)
            job_id = None
            if total_batches:
                job_id = self._enqueue_job(
                    c, "correction", gen_id=None,
                    dedup_key=f"correction:corr{corr_id}",
                    entity_id=cur["entity_id"], range_start=lo, range_end=hi,
                    correction_id=corr_id,
                )
                c.execute("UPDATE jobs SET partitions_total=? WHERE id=?",
                          (total_batches, job_id))
                c.execute("UPDATE corrections SET job_id=? WHERE id=?",
                          (job_id, corr_id))
            else:
                # 没有任何特征引用该源：台账条目直接生效，无需落地
                c.execute(
                    "UPDATE corrections SET state='applied', applied_at=? WHERE id=?",
                    (now, corr_id),
                )
            self._audit(
                c, "correction_submitted", f"corr:{corr_id}",
                f"记录 {source}/{record_key} 受理订正 r{revision}({op})：影响 "
                f"{len(feats)} 个特征的区间 [{lo},{hi}]，分 {total_batches} 批落地")
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        return {"replayed": False, "correction": self.get_correction(corr_id),
                "job_id": job_id}

    def get_correction(self, correction_id: int) -> dict:
        """订正详情：台账条目 + 受影响区间 + 批次进度 + 最新落地作业。"""
        c = self.db.conn()
        row = c.execute("SELECT * FROM corrections WHERE id=?",
                        (correction_id,)).fetchone()
        if row is None:
            raise FeatError(f"订正不存在: {correction_id}")
        d = dict(row)
        d["scopes"] = [dict(r) for r in c.execute(
            "SELECT gen_id,entity_id,tick_start,tick_end FROM correction_scopes "
            "WHERE correction_id=? ORDER BY gen_id,entity_id,tick_start",
            (correction_id,)).fetchall()]
        d["progress"] = [dict(r) for r in c.execute(
            "SELECT gen_id,batch_no,entity_id,tick_start,tick_end,rows_written,"
            "finished_at FROM correction_progress WHERE correction_id=? "
            "ORDER BY gen_id,batch_no",
            (correction_id,)).fetchall()]
        d["batches_done"] = len(d["progress"])
        d["job"] = self.get_job(d["job_id"]) if d["job_id"] else None
        return d

    def list_corrections(self, source: Optional[str] = None,
                         record_key: Optional[str] = None,
                         state: Optional[str] = None, limit: int = 200) -> list[dict]:
        """订正台账查询；指定记录时按 revision 升序返回完整修订链。"""
        c = self.db.conn()
        sql, args, conds = "SELECT * FROM corrections", [], []
        if source:
            conds.append("source=?")
            args.append(source)
        if record_key:
            conds.append("record_key=?")
            args.append(record_key)
        if state:
            conds.append("state=?")
            args.append(state)
        if conds:
            sql += " WHERE " + " AND ".join(conds)
        sql += " ORDER BY revision ASC" if record_key else " ORDER BY id DESC"
        sql += " LIMIT ?"
        args.append(limit)
        return [dict(r) for r in c.execute(sql, args).fetchall()]

    def get_record(self, source: str, record_key: str) -> dict:
        """记录视图：全部内容版本（含被取代/撤销的历史行）+ 修订链。

        已撤销的记录始终可在此审计，只是不再参与特征计算。
        """
        c = self.db.conn()
        versions = [dict(r) for r in c.execute(
            "SELECT id,record_rev,entity_id,event_ts,raw_json,dedup_key,seq,is_late,"
            "superseded_by,retracted_by,ingested_at FROM events "
            "WHERE source=? AND record_key=? ORDER BY record_rev, id",
            (source, record_key)).fetchall()]
        if not versions:
            raise FeatError(f"记录不存在: {source}/{record_key}")
        chain = self.list_corrections(source, record_key)
        current = versions[-1]
        effective = (current["superseded_by"] is None
                     and current["retracted_by"] is None)
        return {
            "source": source, "record_key": record_key,
            "current_revision": chain[-1]["revision"] if chain else 0,
            "effective": effective,
            "versions": versions, "corrections": chain,
        }

    def _correction_batches(self, scopes: list[dict], tsec: int) -> list[tuple]:
        """把影响范围切成确定性批次：同一份范围重跑得到同样的批次划分，
        因此崩溃恢复/续跑可以按 (gen_id, batch_no) 精确跳过已处理批次。"""
        size = settings.correction_batch_ticks
        out, bno = [], 0
        for s in sorted(scopes, key=lambda x: (x["entity_id"], x["tick_start"])):
            start = s["tick_start"]
            while start <= s["tick_end"]:
                end = min(start + (size - 1) * tsec, s["tick_end"])
                out.append((bno, s["entity_id"], start, end))
                bno += 1
                start = end + tsec
        return out

    def _refresh_online_current(self, c, feature: dict, gen: dict,
                                entity_id: str) -> None:
        """按有效事件日志重建某实体的在线当前值；无有效事件时删除该行。"""
        row = c.execute(
            "SELECT MAX(event_ts) AS anchor FROM events "
            f"WHERE source=? AND entity_id=? AND {_EFFECTIVE_EVENTS}",
            (feature["source"], entity_id),
        ).fetchone()
        if row["anchor"] is None:
            c.execute("DELETE FROM online_current WHERE gen_id=? AND entity_id=?",
                      (gen["id"], entity_id))
            return
        anchor = row["anchor"]
        events = self._window_events(
            c, feature["source"], entity_id, feature["window_seconds"], anchor,
            gen["created_at"])
        value = self._compute_value(feature, gen, events)
        num_value, cat_value = self._split_value(value, gen["kind"])
        c.execute(
            "INSERT INTO online_current(gen_id,entity_id,anchor_ts,num_value,cat_value) "
            "VALUES(?,?,?,?,?) ON CONFLICT(gen_id,entity_id) DO UPDATE SET "
            "anchor_ts=excluded.anchor_ts,num_value=excluded.num_value,"
            "cat_value=excluded.cat_value",
            (gen["id"], entity_id, anchor, num_value, cat_value),
        )

    def _run_correction(self, job: dict) -> dict:
        """订正落地作业：分批刷新受影响 (代际, 实体, 刻度区间)。

        - 每批一个事务：重算刻度值（在线历史 + 回填范围内的离线值）、
          写进度行、更新作业计数，一起提交——每批完成后进度即落盘。
        - 批次在事务内从**当前**有效事件日志计算，因此较新订正到达后，
          旧作业尚未执行的批次自然按新日志计算；已提交的批次若与新订正
          重叠，由新订正自己的作业重写——旧任务永远不会覆盖新结果。
        - 崩溃/失败后重跑：按 correction_progress 跳过已提交批次。
        """
        c = self.db.conn()
        row = c.execute("SELECT * FROM corrections WHERE id=?",
                        (job["correction_id"],)).fetchone()
        if row is None:
            raise FeatError(f"订正不存在: {job['correction_id']}")
        corr = dict(row)
        scopes = [dict(r) for r in c.execute(
            "SELECT * FROM correction_scopes WHERE correction_id=? "
            "ORDER BY gen_id,entity_id,tick_start",
            (corr["id"],)).fetchall()]
        plans: dict[int, list[tuple]] = {}
        for gen_id in sorted({s["gen_id"] for s in scopes}):
            gen_scopes = [s for s in scopes if s["gen_id"] == gen_id]
            tsec = c.execute(
                "SELECT f.sample_tick_seconds AS tsec FROM generations g "
                "JOIN features f ON f.id=g.feature_id WHERE g.id=?",
                (gen_id,),
            ).fetchone()["tsec"]
            plans[gen_id] = self._correction_batches(gen_scopes, tsec)
        total = sum(len(b) for b in plans.values())
        done = c.execute(
            "SELECT COUNT(*) AS n FROM correction_progress WHERE correction_id=?",
            (corr["id"],)).fetchone()["n"]
        # 故障注入只作用于首次尝试；续跑/崩溃恢复的作业必须能跑完
        inject = job["attempts"] <= 1 and not job["dedup_key"].startswith("resume:")
        c.execute(
            "UPDATE corrections SET state='applying', job_id=?, error=NULL "
            "WHERE id=? AND state!='applied'",
            (job["id"], corr["id"]),
        )
        c.execute("UPDATE jobs SET partitions_total=?, partitions_done=? WHERE id=?",
                  (total, done, job["id"]))
        c.commit()

        for gen_id, batches in plans.items():
            gen = self.get_generation(c, gen_id)
            f = self.get_feature(c, gen["feature_id"])
            tsec = f["sample_tick_seconds"]
            done_nos = {
                r["batch_no"] for r in c.execute(
                    "SELECT batch_no FROM correction_progress "
                    "WHERE correction_id=? AND gen_id=?",
                    (corr["id"], gen_id)).fetchall()
            }
            pending = [b for b in batches if b[0] not in done_nos]
            entities = sorted({s["entity_id"] for s in scopes
                               if s["gen_id"] == gen_id})
            for i, (bno, entity, bstart, bend) in enumerate(pending):
                try:
                    c.execute("BEGIN IMMEDIATE")
                    rows = 0
                    if i == 0:
                        # 每个代际首个待处理批次顺带重建受影响实体的在线当前值
                        for ent in entities:
                            self._refresh_online_current(c, f, gen, ent)
                    tick = bstart
                    while tick <= bend:
                        ev = self._window_events(
                            c, f["source"], entity, f["window_seconds"], tick,
                            gen["created_at"])
                        value = self._compute_value(f, gen, ev)
                        num_value, cat_value = self._split_value(value, gen["kind"])
                        c.execute(
                            "INSERT INTO online_history(gen_id,entity_id,asof_ts,"
                            "num_value,cat_value) VALUES(?,?,?,?,?) "
                            "ON CONFLICT(gen_id,entity_id,asof_ts) DO UPDATE SET "
                            "num_value=excluded.num_value,"
                            "cat_value=excluded.cat_value",
                            (gen_id, entity, tick, num_value, cat_value),
                        )
                        rows += 1
                        # 离线侧只刷新已回填覆盖的范围；未回填部分由回填负责
                        if gen["backfill_end"] is not None and tick <= gen["backfill_end"]:
                            c.execute(
                                "INSERT INTO offline_values(gen_id,entity_id,asof_ts,"
                                "num_value,cat_value) VALUES(?,?,?,?,?) "
                                "ON CONFLICT(gen_id,entity_id,asof_ts) DO UPDATE SET "
                                "num_value=excluded.num_value,"
                                "cat_value=excluded.cat_value",
                                (gen_id, entity, tick, num_value, cat_value),
                            )
                        tick += tsec
                    c.execute(
                        "INSERT INTO correction_progress(correction_id,gen_id,"
                        "batch_no,entity_id,tick_start,tick_end,rows_written,"
                        "finished_at) VALUES(?,?,?,?,?,?,?,?)",
                        (corr["id"], gen_id, bno, entity, bstart, bend, rows,
                         self.now()),
                    )
                    done += 1
                    c.execute("UPDATE jobs SET partitions_done=? WHERE id=?",
                              (done, job["id"]))
                    c.execute("COMMIT")
                except Exception:
                    c.execute("ROLLBACK")
                    raise
                fail_after = corr["fail_after_batches"] if inject else None
                crash_after = corr["crash_after_batches"] if inject else None
                if fail_after is not None and done >= fail_after:
                    msg = (f"注入故障：订正落地 {done}/{total} 批后失败，"
                           f"可从批次进度续跑")
                    try:
                        c.execute("BEGIN IMMEDIATE")
                        c.execute(
                            "UPDATE jobs SET state='failed', error=?, finished_at=? "
                            "WHERE id=?",
                            (msg, self.now(), job["id"]))
                        c.execute(
                            "UPDATE corrections SET state='failed', error=? "
                            "WHERE id=?",
                            (msg, corr["id"]))
                        self._audit(c, "correction_failed", f"corr:{corr['id']}",
                                    f"订正 r{corr['revision']}({corr['op']}) 落地失败："
                                    f"{done}/{total} 批（进度已保存）")
                        c.execute("COMMIT")
                    except Exception:
                        c.execute("ROLLBACK")
                        raise
                    return self.get_job(job["id"])
                if crash_after is not None and done >= crash_after:
                    raise RuntimeError(
                        "模拟进程崩溃：订正作业中断（作业保持 running，"
                        "恢复后从未完成批次继续）")

        try:
            c.execute("BEGIN IMMEDIATE")
            c.execute(
                "UPDATE corrections SET state='applied', applied_at=?, error=NULL "
                "WHERE id=?",
                (self.now(), corr["id"]))
            c.execute(
                "UPDATE jobs SET state='succeeded', finished_at=?, "
                "partitions_done=?, partitions_total=? WHERE id=?",
                (self.now(), done, total, job["id"]))
            self._audit(
                c, "correction_applied", f"corr:{corr['id']}",
                f"订正 r{corr['revision']}({corr['op']}) 落地完成：{done}/{total} 批，"
                f"记录 {corr['source']}/{corr['record_key']} 的受影响区间已刷新"
                f"在线当前值/在线历史/离线值")
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        return self.get_job(job["id"])

    def recover_interrupted_corrections(self) -> int:
        """进程重启恢复：仍处于 running 的订正作业重新排队。

        已提交的批次在 correction_progress 中，重跑时自动跳过；
        返回恢复的作业数。
        """
        c = self.db.conn()
        try:
            c.execute("BEGIN IMMEDIATE")
            rows = c.execute(
                "SELECT id, correction_id FROM jobs "
                "WHERE kind='correction' AND state='running'",
            ).fetchall()
            for r in rows:
                c.execute(
                    "UPDATE jobs SET state='queued', started_at=NULL WHERE id=?",
                    (r["id"],))
                if r["correction_id"] is not None:
                    c.execute(
                        "UPDATE corrections SET state='applying' "
                        "WHERE id=? AND state!='applied'",
                        (r["correction_id"],))
                self._audit(
                    c, "correction_recovered", f"job:{r['id']}",
                    "进程重启：订正作业重新排队，已完成批次将从进度表跳过")
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        return len(rows)

    def _pending_correction_ranges(self, c, gen_id: int, entity_id: str,
                                   tick: Optional[int] = None):
        """该 (代际, 实体) 上尚未完全落地的订正区间；给 tick 时返回是否被覆盖。

        订正在接受时已原子改写事件日志，派生物化值由可恢复作业分批刷新。
        落地完成前，读者对被覆盖区间直接按事件日志实时计算（读穿），
        保证任何时刻读到的都是某个完整订正版本，绝不看到新旧混杂。
        """
        rows = c.execute(
            "SELECT cs.tick_start, cs.tick_end FROM correction_scopes cs "
            "JOIN corrections co ON co.id=cs.correction_id "
            "WHERE cs.gen_id=? AND cs.entity_id=? AND co.state!='applied'",
            (gen_id, entity_id),
        ).fetchall()
        if tick is None:
            return [dict(r) for r in rows]
        return any(r["tick_start"] <= tick <= r["tick_end"] for r in rows)

    # ------------------------------------------------------------- 读取接口

    def read_online(self, feature_ref: str, entity_id: str) -> dict:
        """读取只读别名（serving 代际）的当前值；别名未切换前不暴露半成品。

        若有尚未落地完成的订正覆盖该实体当前值，直接按事件日志实时计算，
        读者看到的始终是完整订正版本。
        """
        c = self.db.conn()
        f = self.get_feature(c, feature_ref)
        g = self.serving_gen(c, f["id"])
        if g is None:
            return {"feature": f["name"], "entity_id": entity_id, "serving_gen": None,
                    "value": None, "reason": "尚无完整代际绑定到读取别名"}
        pending = self._pending_correction_ranges(c, g["id"], entity_id)
        if pending:
            anchor = c.execute(
                "SELECT MAX(event_ts) AS a FROM events "
                f"WHERE source=? AND entity_id=? AND {_EFFECTIVE_EVENTS}",
                (f["source"], entity_id),
            ).fetchone()["a"]
            if anchor is None or any(r["tick_end"] >= anchor for r in pending):
                value = None
                if anchor is not None:
                    ev = self._window_events(
                        c, f["source"], entity_id, f["window_seconds"], anchor,
                        g["created_at"])
                    value = self._compute_value(f, g, ev)
                return {
                    "feature": f["name"], "entity_id": entity_id,
                    "serving_gen": g["gen_no"], "generation_id": g["id"],
                    "anchor_ts": anchor, "value": value,
                    "correction_pending": True,
                }
        row = c.execute(
            "SELECT * FROM online_current WHERE gen_id=? AND entity_id=?",
            (g["id"], entity_id),
        ).fetchone()
        return {
            "feature": f["name"], "entity_id": entity_id,
            "serving_gen": g["gen_no"], "generation_id": g["id"],
            "anchor_ts": row["anchor_ts"] if row else None,
            "value": (row["cat_value"] if g["kind"] == "categorical"
                      else row["num_value"]) if row else None,
        }

    def read_offline(self, feature_ref: str, entity_id: str, asof_ts: int) -> dict:
        """点内时间历史查询：返回 serving 代际在 asof_ts 对齐刻度上的重建值。

        被未落地订正覆盖的刻度按事件日志实时计算（读穿），不返回半成品。
        """
        c = self.db.conn()
        f = self.get_feature(c, feature_ref)
        g = self.serving_gen(c, f["id"])
        if g is None:
            raise FeatError("该特征尚无 serving 代际")
        tick = _floor(asof_ts, f["sample_tick_seconds"])
        if (tick >= f["backfill_start"]
                and (g["backfill_end"] is None or tick <= g["backfill_end"])
                and self._pending_correction_ranges(c, g["id"], entity_id, tick)):
            ev = self._window_events(
                c, f["source"], entity_id, f["window_seconds"], tick,
                g["created_at"])
            return {
                "feature": f["name"], "entity_id": entity_id, "asof_ts": tick,
                "serving_gen": g["gen_no"],
                "value": self._compute_value(f, g, ev),
                "correction_pending": True,
            }
        row = c.execute(
            "SELECT * FROM offline_values WHERE gen_id=? AND entity_id=? AND asof_ts=?",
            (g["id"], entity_id, tick),
        ).fetchone()
        return {
            "feature": f["name"], "entity_id": entity_id, "asof_ts": tick,
            "serving_gen": g["gen_no"],
            "value": (row["cat_value"] if g["kind"] == "categorical"
                      else row["num_value"]) if row else None,
        }

    # --------------------------------------------------- 特征时间旅行 / 差异

    @staticmethod
    def _encode_cursor(payload: dict) -> str:
        raw = json.dumps(payload, sort_keys=True, separators=(",", ":"))
        return base64.urlsafe_b64encode(raw.encode()).decode()

    @staticmethod
    def _decode_cursor(cursor: str) -> dict:
        """解析不透明分页游标；任何损坏/伪造都给明确的客户端错误（400）。"""
        try:
            raw = base64.urlsafe_b64decode(cursor.encode())
            payload = json.loads(raw)
        except Exception:
            raise FeatError("非法分页游标：无法解析，请用首页响应返回的 cursor")
        if not isinstance(payload, dict):
            raise FeatError("非法分页游标：内容格式不正确，请用首页响应返回的 cursor")
        return payload

    @staticmethod
    def _cursor_for_kind(cur: dict, kind: str) -> None:
        """游标必须是本查询契约签发的（snapshot/diff 互不通用）。"""
        if cur.get("kind") != kind:
            raise FeatError(
                f"非法分页游标：该游标不属于{('快照' if kind == 'snapshot' else '差异')}查询，"
                "请重新发起首页请求获取新游标")

    @staticmethod
    def _cursor_offset_type(cur: dict) -> int:
        """校验游标内 offset 的类型：必须是非负整数（越界检查在结果全集上做）。"""
        offset = cur.get("offset")
        if not isinstance(offset, int) or isinstance(offset, bool) or offset < 0:
            raise FeatError("非法分页游标：offset 必须是非负整数")
        return offset

    @staticmethod
    def _check_cursor_offset(offset: int, total: int) -> int:
        if offset >= total:
            raise FeatError(
                "非法分页游标：offset 已超出结果全集（游标已耗尽或结果已收缩），"
                "请重新发起首页查询")
        return offset

    @staticmethod
    def _check_cursor_context(cur: dict, f: dict, entity_id: str,
                              asof_ts: int, serving_gen_id: Optional[int]) -> None:
        """游标与本次请求的固定上下文必须一致，否则拒绝续页。

        - 特征/实体/时刻/显式代际被篡改：400（请求与游标不是同一个查询）。
        - 首页解析到的 serving 别名与当前别名不同（别名在翻页期间切换）：409，
          即使游标来自显式代际查询也拒绝——读取视图必须来自同一个固定视图。
        """
        bound = (cur.get("feature_id"), cur.get("entity_id"), cur.get("asof_ts"))
        if bound != (f["id"], entity_id, asof_ts):
            raise FeatError("分页参数与游标绑定的快照不一致（特征/实体/时刻不可变）")
        if cur.get("serving_gen_id") != serving_gen_id:
            raise SnapshotConflict(
                "游标签发后读取别名已切换到其他代际，旧游标绑定的固定视图不再是"
                "当前服务视图；请重新发起查询，用首页返回的新游标续页")

    @staticmethod
    def _check_no_gen_args_with_cursor(kwargs: dict, names: tuple[str, ...]) -> None:
        """续页请求不得再携带代际参数：代际在首页已钉进游标。"""
        given = {n for n in names if kwargs.get(n) is not None}
        if given:
            raise FeatError(
                "携带分页游标时不能再指定代际参数（"
                + "/".join(sorted(given))
                + "）：续页必须继续读取游标绑定的同一对代际；如需其他代际请重新发起首页查询")

    def _resolve_gen_for_snapshot(
        self, c, f: dict, gen_id: Optional[int], gen_no: Optional[int]
    ) -> dict:
        """解析快照代际：显式 gen_id/gen_no 指定，否则解析当前 serving 别名。

        代际必须是回填完成的完整代际（candidate/serving/shadow/retired 均可，
        retired 仅供审计）；building/半成品绝不作为时间旅行对象。
        """
        if gen_id is not None and gen_no is not None:
            raise FeatError("gen_id 与 gen_no 只能指定其中一个")
        if gen_id is not None:
            g = self.get_generation(c, gen_id)
            if g["feature_id"] != f["id"]:
                raise FeatError(f"代际 {gen_id} 不属于特征 {f['name']}")
        elif gen_no is not None:
            row = c.execute(
                "SELECT * FROM generations WHERE feature_id=? AND gen_no=?",
                (f["id"], gen_no),
            ).fetchone()
            if row is None:
                raise FeatError(f"代际不存在: 第 {gen_no} 代")
            g = dict(row)
        else:
            g = self.serving_gen(c, f["id"])
            if g is None:
                raise FeatError("该特征尚无 serving 代际，无法读取快照")
        if g["backfill_status"] != "done":
            raise FeatError(
                f"第 {g['gen_no']} 代回填状态为 {g['backfill_status']}，"
                "只能对完整代际做时间旅行")
        return g

    @staticmethod
    def _snapshot_event_row(r, included: bool, reason: Optional[str]) -> dict:
        return {
            "event_id": r["id"], "event_ts": r["event_ts"], "seq": r["seq"],
            "entity_id": r["entity_id"], "record_key": r["record_key"],
            "record_rev": r["record_rev"],
            "raw": json.loads(r["raw_json"]),
            "included": included, "exclude_reason": reason,
        }

    def _build_snapshot(
        self, c, f: dict, gen: dict, entity_id: str, asof_ts: int
    ) -> tuple[dict, list[dict]]:
        """在当前事务的一致性读视图上构建一个完整代际快照。

        返回 (header, events)：header 含窗口边界、值、来源版本与快照标识；
        events 为该实体的全部事件行，included=true 为纳入计算的事件，
        其余按 future/out_of_window/superseded/retracted 给出排除原因。
        """
        window = f["window_seconds"]
        win_open, win_close = asof_ts - window, asof_ts
        rows = c.execute(
            "SELECT id,event_ts,seq,entity_id,record_key,record_rev,raw_json,"
            "superseded_by,retracted_by FROM events "
            "WHERE source=? AND entity_id=? ORDER BY event_ts,seq,id",
            (f["source"], entity_id),
        ).fetchall()
        events: list[dict] = []
        included_raw: list[tuple[int, int, dict]] = []
        # 规范化后的原始负载：参与内容哈希，消除 JSON key 顺序差异，保证
        # 同一批事件无论何时重放都得到同一个 snapshot_id
        canonical_raw: dict[int, str] = {}
        counts = {"included": 0, "future": 0, "out_of_window": 0,
                  "superseded": 0, "retracted": 0}
        for r in rows:
            r = dict(r)
            reason: Optional[str] = None
            if r["superseded_by"] is not None:
                reason = "superseded"          # 已被 replace 订正取代（可审计的旧版本）
            elif r["retracted_by"] is not None:
                reason = "retracted"           # 已被 retract 订正撤销
            elif r["event_ts"] > win_close:
                reason = "future"              # 样本时刻之后：点内时间绝不穿越
            elif r["event_ts"] <= win_open:
                reason = "out_of_window"       # 左开窗口外：已滑出 (t-window, t]
            parsed_raw = json.loads(r["raw_json"])
            canonical_raw[r["id"]] = json.dumps(
                parsed_raw, sort_keys=True, default=str, separators=(",", ":"))
            if reason is None:
                counts["included"] += 1
                included_raw.append((r["event_ts"], r["seq"], parsed_raw))
                events.append({
                    **self._snapshot_event_row(r, True, None),
                    "raw": parsed_raw,
                })
            else:
                counts[reason] += 1
                events.append(self._snapshot_event_row(r, False, reason))
        value = self._compute_value(f, gen, included_raw)
        # 来源版本：纳入计算的事件各自的 record_rev（0=原始，N=第 N 次订正）
        source_versions = sorted(
            {(e["record_key"], e["record_rev"]) for e in events if e["included"]})
        content = {
            "feature_id": f["id"], "gen_id": gen["id"], "entity_id": entity_id,
            "asof_ts": asof_ts, "window_open": win_open, "window_close": win_close,
            "value": value,
            "events": [
                [e["event_id"], e["event_ts"], e["seq"], e["record_key"],
                 e["record_rev"], int(e["included"]), e["exclude_reason"],
                 canonical_raw[e["event_id"]]]
                for e in events
            ],
        }
        digest = hashlib.sha256(
            json.dumps(content, sort_keys=True, default=str,
                       separators=(",", ":")).encode()
        ).hexdigest()
        header = {
            "snapshot_id": f"snap_{gen['id']}_{entity_id}_{asof_ts}_{digest[:16]}",
            "content_hash": digest,
            "feature_id": f["id"], "feature": f["name"],
            "entity_id": entity_id,
            "gen_id": gen["id"], "gen_no": gen["gen_no"],
            "gen_status": gen["status"],
            "transform": f"{gen['transform_name']}_v{gen['transform_version']}",
            "asof_ts": asof_ts,
            "window": {"window_seconds": window,
                       "window_open_exclusive": win_open,
                       "window_close_inclusive": win_close},
            "value": value,
            "included_count": counts["included"],
            "excluded_count": sum(v for k, v in counts.items() if k != "included"),
            "excluded_by_reason": {k: v for k, v in counts.items()
                                   if k != "included" and v},
            "source_versions": [
                {"record_key": k, "record_rev": rev} for k, rev in source_versions],
        }
        return header, events

    def _validate_snapshot_tick(self, c, f: dict, tick: int) -> None:
        """时间旅行时刻校验：不早于回填起点，不晚于“可知世界”的末端。

        可知末端优先取源水位（已观测到的最大事件时间）；无任何事件时退回
        当前墙上时钟——PIT 快照不允许读到连上游都尚未观测到的未来。
        """
        if tick < f["backfill_start"]:
            raise FeatError(
                f"非法样本时刻 {tick}：早于特征回填起点 {f['backfill_start']}")
        wm = self._watermark(c, f["source"])
        horizon = wm if wm is not None else self.now()
        if tick > horizon:
            raise FeatError(
                f"非法样本时刻 {tick}：晚于已观测水位 {horizon}"
                "（快照不能包含当时尚未发生的事件）")

    def feature_snapshot(
        self, feature_ref: str, entity_id: str, asof_ts: int,
        gen_id: Optional[int] = None, gen_no: Optional[int] = None,
        limit: int = 100, cursor: Optional[str] = None,
    ) -> dict:
        """按 (实体, 样本时刻, 代际) 读取可复现的特征快照（时间旅行）。

        查询契约（统计、行内容、重复/续页读取始终来自同一个固定视图）：
        - asof_ts 向下对齐到样本刻度；非法时刻（非整数/早于回填起点/晚于已观测
          水位）、不存在/不完整/不属于本特征的代际返回 400。
        - 整个请求在一个只读事务内完成：别名只解析一次，即使期间发生别名切换，
          同一请求也只看到同一个完整代际。
        - 相同参数重复查询返回相同 snapshot_id（事件内容寻址）。
        - 分页游标自包含，钉住（代际 id、当前 serving 别名 id、内容哈希、offset）：
          续页不得再传代际参数；别名在翻页期间切换、订正/新事件改写事件日志导致
          哈希漂移时返回 409 SnapshotConflict；游标损坏/字段缺失/越界返回 400。
        """
        if not isinstance(asof_ts, int) or isinstance(asof_ts, bool):
            raise FeatError("asof_ts 必须是整数 Unix 秒")
        if not isinstance(limit, int) or isinstance(limit, bool) or not (1 <= limit <= 500):
            raise FeatError("limit 必须是 1..500 的整数")
        cur = self._decode_cursor(cursor) if cursor else None
        if cur is not None:
            self._cursor_for_kind(cur, "snapshot")
            self._check_no_gen_args_with_cursor(
                {"gen_id": gen_id, "gen_no": gen_no}, ("gen_id", "gen_no"))
        c = self.db.conn()
        try:
            c.execute("BEGIN")
            f = self.get_feature(c, feature_ref)
            tick = _floor(asof_ts, f["sample_tick_seconds"])
            self._validate_snapshot_tick(c, f, tick)
            serving = self.serving_gen(c, f["id"])
            serving_id = serving["id"] if serving else None
            if cur is not None:
                # 先校验游标自身形状（400：坏游标不能污染后续视图检查），
                # 再做“请求被篡改/别名已切换”的上下文检查（400/409），
                # 最后钉死游标内的代际 id 重建同一个固定视图
                offset = self._cursor_offset_type(cur)
                self._check_cursor_context(cur, f, entity_id, tick, serving_id)
                gen_id, gen_no = cur.get("gen_id"), None
                if not isinstance(gen_id, int) or isinstance(gen_id, bool):
                    raise FeatError("非法分页游标：缺少绑定的代际 id")
            g = self._resolve_gen_for_snapshot(c, f, gen_id, gen_no)
            header, events = self._build_snapshot(c, f, g, entity_id, tick)
            if cur is not None:
                if cur.get("gen_id") != g["id"]:
                    raise FeatError("分页参数与游标绑定的快照不一致（代际不可变）")
                if cur.get("hash") != header["content_hash"]:
                    raise SnapshotConflict(
                        "游标绑定的完整代际快照已漂移（订正落地或新事件写入改写了"
                        "事件内容），为避免新旧事件清单拼接，请重新发起查询")
                offset = self._check_cursor_offset(offset, len(events))
            else:
                offset = 0
            page = events[offset:offset + limit]
            next_offset = offset + limit
            next_cursor = self._encode_cursor({
                "kind": "snapshot", "feature_id": f["id"], "gen_id": g["id"],
                "serving_gen_id": serving_id,
                "entity_id": entity_id, "asof_ts": tick,
                "hash": header["content_hash"], "offset": next_offset,
            }) if next_offset < len(events) else None
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        return {
            **header,
            "events": page,
            "pagination": {"limit": limit, "offset": offset,
                           "total": len(events), "next_cursor": next_cursor},
        }

    def _snapshot_value_for(self, f: dict, gen: dict, events: list[dict]):
        raw = [(e["event_ts"], e["seq"], e["raw"]) for e in events if e["included"]]
        return self._compute_value(f, gen, raw)

    def generation_diff(
        self, feature_ref: str, entity_id: str, asof_ts: int,
        a_gen_id: Optional[int] = None, a_gen_no: Optional[int] = None,
        b_gen_id: Optional[int] = None, b_gen_no: Optional[int] = None,
        limit: int = 100, cursor: Optional[str] = None,
    ) -> dict:
        """两个代际在同一 (实体, 样本时刻) 的代际变更报告（差异解释）。

        查询契约（统计、行内容、重复/续页读取始终来自同一个固定视图）：
        - 同一只读事务内为 A/B 各构建一个完整代际快照再按键对齐：
          同一请求只能使用一对完整代际快照，绝不拼接新旧代事件清单。
        - 对齐顺序稳定：按 (event_ts, seq, event_id) 全外连接；
          行内 a_value/b_value 是“截至该行、窗口内已纳入事件的前缀累积特征值”，
          因此差异在第一个分叉的纳入事件处出现，最后一条纳入事件行的值恰好等于
          头部 a/b 快照值；汇总 value_changed/value_equal 直接数自行内容，
          与头部 a_value/b_value 永远同源，不会出现头部不同却记 value_equal。
        - 被排除事件（future/out_of_window/superseded/retracted）两行的
          a_value/b_value 均为 null 且 different=false，只展示排除原因，
          不计入 value_changed/value_equal。
        - 分页游标自包含，钉住（A/B 代际 id、当前 serving 别名 id、两个内容哈希、
          offset）：续页不得再传代际参数；别名切换/事件版本变化（订正或新事件
          改写事件日志）导致固定视图漂移时返回 409；游标损坏/字段缺失/越界、
          坏代际参数、同代自比返回 400。
        """
        if not isinstance(asof_ts, int) or isinstance(asof_ts, bool):
            raise FeatError("asof_ts 必须是整数 Unix 秒")
        if not isinstance(limit, int) or isinstance(limit, bool) or not (1 <= limit <= 500):
            raise FeatError("limit 必须是 1..500 的整数")
        cur = self._decode_cursor(cursor) if cursor else None
        if cur is not None:
            self._cursor_for_kind(cur, "diff")
            self._check_no_gen_args_with_cursor(
                {"a_gen_id": a_gen_id, "a_gen_no": a_gen_no,
                 "b_gen_id": b_gen_id, "b_gen_no": b_gen_no},
                ("a_gen_id", "a_gen_no", "b_gen_id", "b_gen_no"))
        c = self.db.conn()
        try:
            c.execute("BEGIN")
            f = self.get_feature(c, feature_ref)
            tick = _floor(asof_ts, f["sample_tick_seconds"])
            self._validate_snapshot_tick(c, f, tick)
            serving = self.serving_gen(c, f["id"])
            serving_id = serving["id"] if serving else None
            if cur is not None:
                offset = self._cursor_offset_type(cur)
                self._check_cursor_context(cur, f, entity_id, tick, serving_id)
                for key in ("a_gen_id", "b_gen_id"):
                    if not isinstance(cur.get(key), int) or isinstance(cur.get(key), bool):
                        raise FeatError(f"非法分页游标：缺少绑定的 {key}")
                a_gen_id, b_gen_id = cur["a_gen_id"], cur["b_gen_id"]
                a_gen_no = b_gen_no = None
            ga = self._resolve_gen_for_snapshot(c, f, a_gen_id, a_gen_no)
            gb = self._resolve_gen_for_snapshot(c, f, b_gen_id, b_gen_no)
            if ga["id"] == gb["id"]:
                raise FeatError("差异查询的两个代际必须不同")
            ha, ea = self._build_snapshot(c, f, ga, entity_id, tick)
            hb, eb = self._build_snapshot(c, f, gb, entity_id, tick)
            if cur is not None:
                if (cur.get("a_gen_id"), cur.get("b_gen_id")) != (ga["id"], gb["id"]):
                    raise FeatError("分页参数与游标绑定的差异快照不一致（代际不可变）")
                if cur.get("a_hash") != ha["content_hash"] or \
                        cur.get("b_hash") != hb["content_hash"]:
                    raise SnapshotConflict(
                        "游标绑定的一对完整代际快照已漂移（订正落地或新事件写入改写了"
                        "事件内容），为避免新旧代事件清单拼接，请重新发起差异查询")

            amap = {e["event_id"]: e for e in ea}
            bmap = {e["event_id"]: e for e in eb}
            all_ids = sorted(set(amap) | set(bmap),
                             key=lambda i: (amap.get(i, bmap.get(i))["event_ts"],
                                            amap.get(i, bmap.get(i))["seq"], i))
            # 前缀累积输入：行内值 = 变换作用于“截至该行的全部已纳入事件”。
            # 对 sum/mean 这类聚合变换，单事件重放无法暴露差异（任何单事件的
            # sum 与 mean 都相等），前缀累积才能如实反映代际值的分叉与汇合；
            # 最后一条纳入事件行的累积值即头部快照值，统计与行内容严格同源。
            rows = []
            changed = same = a_only = b_only = 0
            pref_a: list[dict] = []
            pref_b: list[dict] = []
            va = vb = None
            for eid in all_ids:
                ea1, eb1 = amap.get(eid), bmap.get(eid)
                base = ea1 or eb1
                present_in = ("both" if ea1 and eb1
                              else "a_only" if ea1 else "b_only")
                if base["included"]:
                    if ea1 is not None:
                        pref_a.append(ea1)
                        va = self._snapshot_value_for(f, ga, pref_a)
                    if eb1 is not None:
                        pref_b.append(eb1)
                        vb = self._snapshot_value_for(f, gb, pref_b)
                    bias = relative_bias(va, vb, f["kind"])
                    is_diff = va != vb
                    if present_in == "both":
                        if is_diff:
                            changed += 1
                        else:
                            same += 1
                    elif present_in == "a_only":
                        a_only += 1
                    else:
                        b_only += 1
                else:
                    # 排除的事件两代口径一致、均不参与计算：值为 null，
                    # 不计入“值相同/不同”统计，仅展示排除原因供解释
                    va = vb = None
                    bias = None
                    is_diff = False
                rows.append({
                    "event_id": eid, "event_ts": base["event_ts"], "seq": base["seq"],
                    "record_key": base["record_key"], "record_rev": base["record_rev"],
                    "included": base["included"], "exclude_reason": base["exclude_reason"],
                    "present_in": present_in,
                    "a_value": va, "b_value": vb,
                    "relative_bias": bias, "different": is_diff,
                })
            if cur is not None:
                offset = self._check_cursor_offset(offset, len(rows))
            else:
                offset = 0
            page = rows[offset:offset + limit]
            next_offset = offset + limit
            next_cursor = self._encode_cursor({
                "kind": "diff", "feature_id": f["id"],
                "a_gen_id": ga["id"], "b_gen_id": gb["id"],
                "serving_gen_id": serving_id,
                "entity_id": entity_id, "asof_ts": tick,
                "a_hash": ha["content_hash"], "b_hash": hb["content_hash"],
                "offset": next_offset,
            }) if next_offset < len(rows) else None
            c.execute("COMMIT")
        except Exception:
            c.execute("ROLLBACK")
            raise
        return {
            "feature": f["name"], "feature_id": f["id"], "entity_id": entity_id,
            "asof_ts": tick,
            "window": ha["window"],
            "a": {"snapshot_id": ha["snapshot_id"], "gen_id": ga["id"],
                  "gen_no": ga["gen_no"], "transform": ha["transform"],
                  "value": ha["value"]},
            "b": {"snapshot_id": hb["snapshot_id"], "gen_id": gb["id"],
                  "gen_no": gb["gen_no"], "transform": hb["transform"],
                  "value": hb["value"]},
            "excluded_by_reason": ha["excluded_by_reason"],
            "diff_stats": {
                "total_events": len(rows), "value_changed": changed,
                "value_equal": same, "a_only": a_only, "b_only": b_only,
                "a_value": ha["value"], "b_value": hb["value"],
            },
            "rows": page,
            "pagination": {"limit": limit, "offset": offset, "total": len(rows),
                           "next_cursor": next_cursor},
        }

    # ------------------------------------------------- 抽样比对 / 门槛校验

    @staticmethod
    def _sampled(seed: str, items: list, fraction: float, cap: int) -> list:
        """确定性抽样：同一种子与输入永远得到同样的对子（抽样规则可复现）。"""
        chosen = []
        for item in items:
            h = int(hashlib.sha256(f"{seed}:{item}".encode()).hexdigest(), 16)
            if h % 10_000 < int(fraction * 10_000):
                chosen.append(item)
        chosen.sort(key=lambda x: hashlib.sha256(f"{seed}:{x}".encode()).hexdigest())
        return chosen[:cap]

    def validate_generation(self, gen_id: int) -> dict:
        """对一个候选代际执行质量校验：
        1) new_vs_old：与上一代离线值比对覆盖率与偏差（定义升级门槛）
        2) online_vs_offline：同代在线历史与离线 PIT 值抽样自动比对
        """
        c = self.db.conn()
        gen = self.get_generation(c, gen_id)
        if gen["backfill_status"] != "done":
            raise FeatError("代际回填尚未完成，无法校验")
        f = self.get_feature(c, gen["feature_id"])

        # ---- 1) 新代 vs 旧代（离线对离线，按相同实体+时点对齐） ----
        prev = c.execute(
            "SELECT * FROM generations WHERE feature_id=? AND gen_no=? ",
            (f["id"], gen["gen_no"] - 1),
        ).fetchone()
        nv = self._compare_offline(c, f, dict(prev) if prev else None, gen)

        # ---- 2) 在线 vs 离线（同代、同实体、同刻度） ----
        ov = self._compare_online_offline(c, f, gen)

        gate = self._evaluate_gate(gen["id"], f, nv, ov, prev is not None)
        self._audit(c, "validated", f"gen:{gen_id}",
                    f"第 {gen['gen_no']} 代校验：新/旧覆盖率={nv['coverage']:.3f} "
                    f"p95偏差={nv['bias_p95']} 在线/离线匹配率="
                    f"{ov['match_rate']:.3f} -> {'通过' if gate['passed'] else '未通过'}")
        c.commit()
        return {"generation_id": gen_id, "gen_no": gen["gen_no"], **gate,
                "new_vs_old": nv, "online_vs_offline": ov}

    def _compare_offline(self, c, f: dict, prev: Optional[dict], gen: dict) -> dict:
        if prev is None:
            return {"mode": "new_vs_old", "baseline_gen_id": None,
                    "baseline": True, "coverage": 1.0,
                    "bias_p50": 0.0, "bias_p95": 0.0, "bias_max": 0.0,
                    "mismatch_rate": 0.0, "sample_pairs": 0, "compared": 0,
                    "expected": 0, "missing": 0, "passed": True}
        end = min(gen["backfill_end"] or 1 << 62, prev["backfill_end"] or 1 << 62)
        start = f["backfill_start"]
        keys = [f"{e}:{t}" for e in self._all_entities(c, f["id"])
                for t in range(start, end, f["sample_tick_seconds"])]
        # 抽样只用于减少比较量；覆盖率在完整期望集上计算
        seed = f"nvso:g{prev['id']}:g{gen['id']}"
        sampled = set(self._sampled(seed, keys, settings.sample_fraction,
                                    settings.max_sample_pairs))
        expected = missing = 0
        biases: list[float] = []
        mismatches = 0
        prev_rows = c.execute(
            "SELECT entity_id, asof_ts, num_value, cat_value FROM offline_values "
            "WHERE gen_id=? AND asof_ts<? AND (num_value IS NOT NULL OR cat_value IS NOT NULL)",
            (prev["id"], end),
        ).fetchall()
        new_map = {
            (r["entity_id"], r["asof_ts"]): (r["num_value"], r["cat_value"])
            for r in c.execute(
                "SELECT entity_id,asof_ts,num_value,cat_value FROM offline_values "
                "WHERE gen_id=? AND asof_ts<?", (gen["id"], end)).fetchall()
        }
        for r in prev_rows:
            key = (r["entity_id"], r["asof_ts"])
            old_is_null = r["num_value"] is None and r["cat_value"] is None
            if old_is_null:
                continue
            expected += 1
            nv = new_map.get(key)
            if nv is None or (nv[0] is None and nv[1] is None):
                missing += 1
                continue
            if f"{r['entity_id']}:{r['asof_ts']}" not in sampled:
                continue
            b = relative_bias(
                r["cat_value"] if r["cat_value"] is not None else r["num_value"],
                nv[1] if nv[1] is not None else nv[0], f["kind"],
            )
            if b is None:
                continue
            biases.append(b)
            if f["kind"] == "categorical" and b == 1.0:
                mismatches += 1
        compared = len(biases)
        biases.sort()
        return self._stats_dict("new_vs_old", prev["id"], biases, mismatches,
                                compared, expected, missing, len(sampled))

    def _compare_online_offline(self, c, f: dict, gen: dict) -> dict:
        # 在线历史与离线 PIT 值在同一 (实体, 刻度) 上对齐
        rows = c.execute(
            "SELECT o.entity_id AS e, o.asof_ts AS t, "
            "o.num_value AS on_num, o.cat_value AS on_cat, "
            "d.num_value AS off_num, d.cat_value AS off_cat "
            "FROM online_history o JOIN offline_values d "
            "ON d.entity_id=o.entity_id AND d.asof_ts=o.asof_ts AND d.gen_id=o.gen_id "
            "WHERE o.gen_id=?",
            (gen["id"],),
        ).fetchall()
        keys = [f"{r['e']}:{r['t']}" for r in rows]
        keep = set(self._sampled(f"ovo:g{gen['id']}", keys,
                                 settings.sample_fraction, settings.max_sample_pairs))
        expected = missing = compared = mismatches = 0
        biases: list[float] = []
        for r in rows:
            expected += 1
            if r["off_num"] is None and r["off_cat"] is None:
                missing += 1
                continue
            if f"{r['e']}:{r['t']}" not in keep:
                continue
            online_v = r["on_cat"] if r["on_cat"] is not None else r["on_num"]
            offline_v = r["off_cat"] if r["off_cat"] is not None else r["off_num"]
            b = relative_bias(online_v, offline_v, f["kind"])
            if b is None:
                missing += 1
                continue
            biases.append(b)
            compared += 1
            if b > settings.max_online_offline_bias:
                mismatches += 1
        biases.sort()
        d = self._stats_dict("online_vs_offline", gen["id"], biases, mismatches,
                             compared, expected, missing, len(keys))
        d["match_rate"] = 1.0 - d["mismatch_rate"] if compared else 1.0
        return d

    def _all_entities(self, c, feature_id: int) -> list[str]:
        rows = c.execute(
            "SELECT DISTINCT e.entity_id FROM events e JOIN features f ON f.source=e.source "
            "WHERE f.id=? ORDER BY e.entity_id", (feature_id,)).fetchall()
        return [r["entity_id"] for r in rows]

    @staticmethod
    def _pct(sorted_vals: list[float], q: float) -> float:
        if not sorted_vals:
            return 0.0
        idx = min(len(sorted_vals) - 1, int(q * len(sorted_vals)))
        return sorted_vals[idx]

    def _stats_dict(self, mode, baseline_id, biases, mismatches, compared,
                    expected, missing, sample_pairs) -> dict:
        coverage = (expected - missing) / expected if expected else 1.0
        return {
            "mode": mode,
            "baseline_gen_id": baseline_id,
            "coverage": round(coverage, 6),
            "missing": missing,
            "expected": expected,
            "bias_p50": round(self._pct(biases, 0.5), 6),
            "bias_p95": round(self._pct(biases, 0.95), 6),
            "bias_max": round(biases[-1], 6) if biases else 0.0,
            "mismatch_rate": round(mismatches / compared, 6) if compared else 0.0,
            "compared": compared,
            "sample_pairs": sample_pairs,
        }

    def _evaluate_gate(self, gen_id: int, f: dict, nv: dict, ov: dict,
                       has_prev: bool) -> dict:
        reasons = []
        # 新代必须覆盖旧代既有值，且偏差达标
        if has_prev:
            if nv["coverage"] < settings.min_coverage:
                reasons.append(
                    f"覆盖率 {nv['coverage']:.3f} < 门槛 {settings.min_coverage}")
            if f["kind"] == "categorical":
                if nv["mismatch_rate"] > settings.max_bias:
                    reasons.append(
                        f"类别不一致率 {nv['mismatch_rate']:.3f} > 门槛 {settings.max_bias}")
            elif nv["bias_p95"] > settings.max_bias:
                reasons.append(
                    f"p95 偏差 {nv['bias_p95']:.3f} > 门槛 {settings.max_bias}")
        # 在线/离线自动比对
        if ov["expected"] > 0:
            if ov["coverage"] < settings.min_online_offline_coverage:
                reasons.append(
                    f"在线/离线覆盖率 {ov['coverage']:.3f} < "
                    f"{settings.min_online_offline_coverage}")
            if f["require_online_match"] and ov["mismatch_rate"] > settings.max_online_offline_bias:
                reasons.append(
                    f"在线/离线不匹配率 {ov['mismatch_rate']:.3f} 超出门槛")
        passed = not reasons
        result = {"passed": passed, "reasons": reasons}
        c = self.db.conn()
        detail = json.dumps({"new_vs_old": nv, "online_vs_offline": ov,
                             "reasons": reasons}, ensure_ascii=False)
        c.execute(
            "INSERT INTO validations(gen_id,baseline_gen_id,mode,coverage,bias_p50,"
            "bias_p95,bias_max,mismatch_rate,sample_pairs,compared,passed,detail,at) "
            "VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (gen_id, nv["baseline_gen_id"], "gate",
             nv["coverage"], nv["bias_p50"], nv["bias_p95"], nv["bias_max"],
             nv["mismatch_rate"], nv["sample_pairs"], nv["compared"],
             int(passed), detail, self.now()),
        )
        return result

    # ------------------------------------------------------- 切换 / 回滚

    def promote_generation(self, gen_id: int, force: bool = False) -> dict:
        """质量门槛达标后原子切换读取别名。

        - 未达标且非 force：拒绝
        - 别名指向整行替换，读者要么看到旧完整代、要么看到新完整代
        - 并发切换（CAS 服务指针，first-wins）：操作发起时先记下当前 serving
          指针，写事务内重新读取；若指针已被先提交的切换/回滚改写，则本方输掉
          竞争，抛 AliasConflict（HTTP 409），不更新别名/代际状态，也不写
          promote 成功历史或 promoted 审计。同一代重复请求在指针未变时幂等
          返回 already_serving，始终只有一方拿到成功结果。
        """
        # 竞争条件：操作发起时的服务指针。两个并发 promote 都钉住同一个旧指针，
        # BEGIN IMMEDIATE 串行化后只有先提交的一方还能匹配该指针。
        c = self.db.conn()
        gen = self.get_generation(c, gen_id)
        f = self.get_feature(c, gen["feature_id"])
        expected_id = (self.serving_gen(c, f["id"]) or {}).get("id")
        validation = self.validate_generation(gen_id)
        if not validation["passed"] and not force:
            self._audit(c, "promote_blocked", f"gen:{gen_id}",
                        f"第 {gen['gen_no']} 代切换被门槛拦截：{validation['reasons']}")
            c.commit()
            return {"promoted": False, "reason": "quality_gate_failed",
                    "validation": validation}
        try:
            c.execute("BEGIN IMMEDIATE")
            old = self.serving_gen(c, f["id"])
            current_id = old["id"] if old else None
            if current_id == gen_id:
                # 幂等：该代已是 serving（本方成功后的重复请求），不重复写历史
                c.execute("COMMIT")
                return {"promoted": False, "reason": "already_serving",
                        "serving_gen": gen["gen_no"]}
            if current_id != expected_id:
                # 服务指针在操作发起后已被并发切换/回滚改写：本方输掉竞争。
                # 回滚事务，不触碰别名与代际状态，不写任何成功历史。
                c.execute("ROLLBACK")
                expected_label = "无"
                if expected_id is not None:
                    expected_row = c.execute(
                        "SELECT gen_no FROM generations WHERE id=?",
                        (expected_id,)).fetchone()
                    expected_label = f"第 {expected_row['gen_no']} 代"
                raise AliasConflict(
                    f"特征 {f['name']} 的读取别名在切换请求发起后已被其他操作改写："
                    f"发起时服务指针指向{expected_label}，当前已是第 {old['gen_no']} 代；"
                    f"本次切换到第 {gen['gen_no']} 代的请求未生效，请重新读取当前"
                    "服务版本后再决定")
            c.execute(
                "INSERT INTO aliases(feature_id,serving_gen_id,switched_at) "
                "VALUES(?,?,?) ON CONFLICT(feature_id) DO UPDATE SET "
                "serving_gen_id=excluded.serving_gen_id,switched_at=excluded.switched_at",
                (f["id"], gen_id, self.now()),
            )
            c.execute(
                "UPDATE generations SET status='serving' WHERE id=?", (gen_id,)
            )
            if old:
                c.execute(
                    "UPDATE generations SET status='shadow' WHERE id=?", (old["id"],)
                )
            c.execute(
                "INSERT INTO alias_history(feature_id,gen_id,action,reason,at) "
                "VALUES(?,?, 'promote', ?,?)",
                (f["id"], gen_id,
                 "force 强制切换" if force and not validation["passed"]
                 else f"质量校验通过（覆盖率/偏差达标）", self.now()),
            )
            self._audit(c, "promoted", f"gen:{gen_id}",
                        f"特征 {f['name']} 读取别名切换：第 "
                        f"{old['gen_no'] if old else '无'} 代 -> 第 {gen['gen_no']} 代")
            c.execute("COMMIT")
        except AliasConflict:
            raise
        except Exception:
            c.execute("ROLLBACK")
            raise
        return {"promoted": True, "feature": f["name"],
                "serving_gen": gen["gen_no"], "validation": validation}

    def rollback_feature(self, feature_ref, to_gen_no: Optional[int] = None,
                         reason: str = "人工回滚") -> dict:
        """回滚到先前可验证版本。默认回退到上一 serving 代（有校验历史）。

        与切换一样以**操作发起时的服务指针**为竞争条件：发起后若别名已被
        并发切换/回滚改写，抛 AliasConflict（HTTP 409），不写回滚历史。
        重复回滚到当前 serving 代幂等成功且不重复写历史。
        """
        c = self.db.conn()
        f = self.get_feature(c, feature_ref)
        cur = self.serving_gen(c, f["id"])
        if cur is None:
            raise FeatError("尚无 serving 代际，无法回滚")
        expected_id = cur["id"]
        if to_gen_no is not None:
            target = c.execute(
                "SELECT * FROM generations WHERE feature_id=? AND gen_no=?",
                (f["id"], to_gen_no),
            ).fetchone()
        else:
            target = c.execute(
                "SELECT * FROM generations WHERE feature_id=? AND gen_no<? "
                "ORDER BY gen_no DESC LIMIT 1",
                (f["id"], cur["gen_no"]),
            ).fetchone()
        if target is None:
            raise FeatError("找不到可回滚的先前代际")
        target = dict(target)
        if target["backfill_status"] != "done":
            raise FeatError(f"第 {target['gen_no']} 代回填不完整，不能作为回滚目标")
        if target["id"] == expected_id:
            # 幂等：当前 serving 已是回滚目标（重复请求），不重复写历史
            return {"rolled_back": False, "reason": "already_serving",
                    "feature": f["name"], "from_gen": cur["gen_no"],
                    "to_gen": target["gen_no"]}
        try:
            c.execute("BEGIN IMMEDIATE")
            now_serving = self.serving_gen(c, f["id"])
            if now_serving is None or now_serving["id"] != expected_id:
                c.execute("ROLLBACK")
                current_label = (f"第 {now_serving['gen_no']} 代"
                                 if now_serving else "无")
                raise AliasConflict(
                    f"特征 {f['name']} 的读取别名在回滚请求发起后已被其他操作改写："
                    f"发起时服务指针指向第 {cur['gen_no']} 代，当前已是{current_label}；"
                    f"本次回滚到第 {target['gen_no']} 代的请求未生效，请重新读取"
                    "当前服务版本后再决定")
            c.execute(
                "UPDATE aliases SET serving_gen_id=?, switched_at=? WHERE feature_id=?",
                (target["id"], self.now(), f["id"]),
            )
            c.execute("UPDATE generations SET status='shadow' WHERE id=?", (cur["id"],))
            c.execute("UPDATE generations SET status='serving' WHERE id=?", (target["id"],))
            c.execute(
                "INSERT INTO alias_history(feature_id,gen_id,action,reason,at) "
                "VALUES(?,?,'rollback',?,?)",
                (f["id"], target["id"], reason, self.now()),
            )
            self._audit(c, "rolled_back", f"gen:{target['id']}",
                        f"特征 {f['name']} 回滚：第 {cur['gen_no']} 代 -> "
                        f"第 {target['gen_no']} 代（{reason}）")
            c.execute("COMMIT")
        except AliasConflict:
            raise
        except Exception:
            c.execute("ROLLBACK")
            raise
        return {"rolled_back": True, "feature": f["name"],
                "from_gen": cur["gen_no"], "to_gen": target["gen_no"], "reason": reason}

    def post_switch_quality_check(self, feature_ref) -> dict:
        """切换后再校验：用在线/离线同点比对发现质量回退，不达标自动回滚。"""
        c = self.db.conn()
        f = self.get_feature(c, feature_ref)
        g = self.serving_gen(c, f["id"])
        if g is None:
            return {"checked": False, "reason": "no_serving_gen"}
        ov = self._compare_online_offline(c, f, g)
        ok = (ov["expected"] == 0
              or ov["coverage"] >= settings.min_online_offline_coverage)
        if ok and f["kind"] == "categorical":
            ok = ov["mismatch_rate"] <= settings.max_bias
        elif ok:
            # 数值型：p95 偏差超门槛即视为质量回退（例如在线被污染/写入分叉）
            ok = ov["bias_p95"] <= settings.max_bias
        self._audit(c, "post_switch_check", f"gen:{g['id']}",
                    f"切换后质量检查：匹配率={ov['match_rate']:.3f} "
                    f"p95偏差={ov['bias_p95']} 覆盖率={ov['coverage']:.3f} "
                    f"-> {'正常' if ok else '回退！自动回滚'}")
        c.commit()
        if not ok:
            self.rollback_feature(
                f["id"], reason=f"切换后质量回退（在线/离线匹配率 {ov['match_rate']:.3f}）")
        return {"feature": f["name"], "serving_gen": g["gen_no"],
                "healthy": ok, "online_vs_offline": ov}

    # ------------------------------------------------------------- 总览页面

    def overview(self) -> dict:
        c = self.db.conn()
        features = []
        for frow in c.execute("SELECT * FROM features ORDER BY id").fetchall():
            f = dict(frow)
            gens = [dict(r) for r in c.execute(
                "SELECT * FROM generations WHERE feature_id=? ORDER BY gen_no",
                (f["id"],)).fetchall()]
            serving = self.serving_gen(c, f["id"])
            cps = c.execute(
                "SELECT COUNT(*) n, COALESCE(SUM(rows_written),0) rows "
                "FROM checkpoints WHERE gen_id IN "
                "(SELECT id FROM generations WHERE feature_id=?)",
                (f["id"],)).fetchone()
            features.append({
                "id": f["id"], "name": f["name"], "source": f["source"],
                "join_key": f["join_key"], "window_seconds": f["window_seconds"],
                "transform": f"{f['transform_name']}_v{f['transform_version']}",
                "serving_gen": serving["gen_no"] if serving else None,
                "generations": gens,
                "checkpoints": cps["n"], "offline_rows": cps["rows"],
            })
        jobs = self.list_jobs()
        wm = [dict(r) for r in c.execute(
            "SELECT * FROM watermarks ORDER BY source").fetchall()]
        validations = [dict(r) for r in c.execute(
            "SELECT * FROM validations ORDER BY id DESC LIMIT 20").fetchall()]
        corrections = []
        for row in c.execute(
                "SELECT * FROM corrections ORDER BY id DESC LIMIT 50").fetchall():
            d = dict(row)
            d["scopes"] = [dict(s) for s in c.execute(
                "SELECT gen_id, entity_id, MIN(tick_start) AS s, "
                "MAX(tick_end) AS e FROM correction_scopes WHERE correction_id=? "
                "GROUP BY gen_id, entity_id ORDER BY gen_id, entity_id",
                (d["id"],)).fetchall()]
            d["batches_done"] = c.execute(
                "SELECT COUNT(*) AS n FROM correction_progress "
                "WHERE correction_id=?", (d["id"],)).fetchone()["n"]
            if d["job_id"]:
                j = c.execute(
                    "SELECT state, partitions_done, partitions_total, error "
                    "FROM jobs WHERE id=?", (d["job_id"],)).fetchone()
                d["job_state"] = j["state"]
                d["batches_total"] = j["partitions_total"]
            else:
                d["job_state"] = None
                d["batches_total"] = 0
            corrections.append(d)
        # 修订链：按记录分组、时间正序，供状态页展示
        chains: dict[str, list[dict]] = {}
        for d in reversed(corrections):
            chains.setdefault(f"{d['source']}/{d['record_key']}", []).append(d)

        time_travel = self._overview_time_travel(c)
        return {"features": features, "jobs": jobs, "watermarks": wm,
                "validations": validations, "audits": self.list_audits(30),
                "corrections": corrections, "correction_chains": chains,
                "time_travel": time_travel,
                "time": self.now()}

    def _overview_time_travel(self, c) -> list[dict]:
        """状态页示例：每个 serving 特征取一个实体，展示历史快照与跨代差异。

        快照标识、纳入/排除事件明细、排除原因与差异统计全部来自
        feature_snapshot / generation_diff 的同一套只读快照逻辑。
        """
        out: list[dict] = []
        for frow in c.execute("SELECT * FROM features ORDER BY id").fetchall():
            f = dict(frow)
            serving = self.serving_gen(c, f["id"])
            if serving is None or serving["backfill_status"] != "done":
                continue
            ent = c.execute(
                "SELECT entity_id FROM events WHERE source=? "
                f"AND {_EFFECTIVE_EVENTS} ORDER BY event_ts DESC LIMIT 1",
                (f["source"],)).fetchone()
            if ent is None:
                continue
            entity_id = ent["entity_id"]
            wm_row = self._watermark(c, f["source"])
            # 样本时刻取“水位前两个刻度”：窗口内通常既有纳入事件也有未来事件
            asof = max(f["backfill_start"],
                       _floor(wm_row, f["sample_tick_seconds"])
                       - 2 * f["sample_tick_seconds"])
            try:
                snap = self.feature_snapshot(
                    f["name"], entity_id, asof, gen_id=serving["id"], limit=10)
            except FeatError:
                continue
            prev = c.execute(
                "SELECT * FROM generations WHERE feature_id=? AND gen_no<? "
                "AND backfill_status='done' ORDER BY gen_no DESC LIMIT 1",
                (f["id"], serving["gen_no"])).fetchone()
            diff = None
            if prev is not None:
                try:
                    diff = self.generation_diff(
                        f["name"], entity_id, asof,
                        a_gen_id=prev["id"], b_gen_id=serving["id"], limit=10)
                except FeatError:
                    diff = None
            out.append({"feature": f["name"], "entity_id": entity_id,
                        "snapshot": snap, "diff": diff})
        return out
