"""特征时间旅行与差异解释：

- 历史时点读取（可复现快照 + 快照标识）
- 未来事件排除（future/out_of_window/superseded/retracted + 来源版本）
- 跨代差异（同一只读事务内一对完整快照，稳定对齐）
- 分页顺序稳定 + 重复读取同一快照标识 + 漂移 409
- 别名切换并发隔离（同一请求只用一个完整代际，不拼接新旧清单）
- 非法时刻 / 不存在代际 / 非法游标明确拒绝
"""
import json
import threading

import pytest
from fastapi.testclient import TestClient

from conftest import events, make_feature
from featforge.api import create_app
from featforge.db import Database
from featforge.store import FeatError, SnapshotConflict, Store


def _baseline(store, worker, name="tt_1h", events_rows=None):
    make_feature(store, name=name, window="1h")
    events_rows = events_rows or [
        ("u1", 300, 10.0), ("u1", 600, 5.0), ("u1", 3900, 2.0)]
    for i, (e, ts, v) in enumerate(events_rows, start=1):
        store.ingest_event("purchase", e, ts, raw={"amount": v},
                           dedup_key=f"e{i}")
    worker.tick()
    assert store.read_online(name, "u1")["serving_gen"] == 1
    return name


# ----------------------------------------------------------- 1. 历史时点读取

def test_snapshot_reproduces_historical_point(store, worker):
    name = _baseline(store, worker)
    snap = store.feature_snapshot(name, "u1", 600)
    # tick=600 窗口 (-3000,600]：300 与 600 两条 -> 15
    assert snap["value"] == 15.0
    assert snap["asof_ts"] == 600
    # asof 非刻度时向下对齐到样本刻度（与 read_offline 同口径）
    assert store.feature_snapshot(name, "u1", 599)["asof_ts"] == 300
    assert store.feature_snapshot(name, "u1", 650)["asof_ts"] == 600
    # 窗口边界显式列出，左开右闭
    assert snap["window"] == {
        "window_seconds": 3600, "window_open_exclusive": -3000,
        "window_close_inclusive": 600}
    included = [e for e in snap["events"] if e["included"]]
    assert [(e["event_ts"], e["raw"]["amount"]) for e in included] == [
        (300, 10.0), (600, 5.0)]
    # 边界语义：event_ts 恰好在窗口左端（t-window）必须排除
    boundary = store.feature_snapshot(name, "u1", 3900)
    ts300 = next(e for e in boundary["events"] if e["event_ts"] == 300)
    assert ts300["included"] is False
    assert ts300["exclude_reason"] == "out_of_window"


def test_repeated_queries_return_same_snapshot_id(store, worker):
    name = _baseline(store, worker)
    s1 = store.feature_snapshot(name, "u1", 600)
    s2 = store.feature_snapshot(name, "u1", 600, gen_id=s1["gen_id"])
    assert s1["snapshot_id"] == s2["snapshot_id"]
    assert s1["snapshot_id"].startswith(f"snap_{s1['gen_id']}_u1_600_")
    # 不同时刻 -> 不同快照标识
    s3 = store.feature_snapshot(name, "u1", 3900)
    assert s3["snapshot_id"] != s1["snapshot_id"]
    # 快照值与离线 PIT 读取一致
    assert s3["value"] == store.read_offline(name, "u1", 3900)["value"]


# ----------------------------------------------------------- 2. 未来事件排除

def test_future_events_are_excluded_with_reasons_and_source_versions(
        store, worker):
    name = _baseline(store, worker)
    # 样本时刻 600：3900 是未来事件，必须列出且标记来源
    snap = store.feature_snapshot(name, "u1", 600)
    futures = [e for e in snap["events"]
               if e["exclude_reason"] == "future"]
    assert len(futures) == 1
    assert futures[0]["event_ts"] == 3900
    assert futures[0]["record_key"] == "e3"
    assert futures[0]["record_rev"] == 0          # 来源版本：原始版本
    assert snap["included_count"] == 2
    assert snap["excluded_count"] == 1
    assert snap["excluded_by_reason"] == {"future": 1}
    assert snap["value"] == 15.0                  # 未来事件绝不参与计算
    sv = {(x["record_key"], x["record_rev"]) for x in snap["source_versions"]}
    assert sv == {("e1", 0), ("e2", 0)}


def test_corrected_events_show_superseded_retracted_reasons(store, worker):
    name = _baseline(store, worker)
    # replace e1（300=10 -> 100）：旧行标记 superseded，新行 record_rev=1
    store.submit_correction("purchase", "e1", 1, "replace",
                            raw={"amount": 100.0})
    snap = store.feature_snapshot(name, "u1", 600)
    rows300 = [e for e in snap["events"] if e["event_ts"] == 300]
    old = next(e for e in rows300 if e["record_rev"] == 0)
    new = next(e for e in rows300 if e["record_rev"] == 1)
    assert old["included"] is False and old["exclude_reason"] == "superseded"
    assert new["included"] is True and new["record_key"] == "e1"
    assert snap["value"] == 105.0                 # 100 + 5
    sv = {x["record_rev"] for x in snap["source_versions"]
          if x["record_key"] == "e1"}
    assert sv == {1}                              # 来源版本显示为订正后的版本

    # retract e3（未来事件）：原因从 future 变为 retracted，仍可审计
    store.submit_correction("purchase", "e3", 1, "retract")
    snap2 = store.feature_snapshot(name, "u1", 600)
    e3 = next(e for e in snap2["events"] if e["record_key"] == "e3")
    assert e3["included"] is False
    assert e3["exclude_reason"] == "retracted"
    assert snap2["excluded_by_reason"]["retracted"] == 1


# ----------------------------------------------------------- 3. 跨代差异

def test_generation_diff_explains_value_differences(store, worker):
    # gen1 = mean_v1，gen2 = mean_v99（max*10）：差异显著
    make_feature(store, name="diff_1h", transform="mean",
                 transform_version=1, window="1h")
    events(store, [("u1", 300, 10.0), ("u1", 600, 20.0),
                   ("u1", 3900, 5.0)])
    worker.tick()
    g2 = store.create_generation("diff_1h", "mean", 99)
    worker.tick()  # 门槛拦截，gen2 停在 candidate，但回填完整，可显式指定

    d = store.generation_diff("diff_1h", "u1", 600, a_gen_no=1, b_gen_no=2)
    assert d["a"]["gen_id"] != d["b"]["gen_id"]
    assert d["a"]["snapshot_id"] != d["b"]["snapshot_id"]
    # 窗口内两条：mean(10,20)=15 vs max(10,20)*10=200
    assert d["a"]["value"] == 15.0 and d["b"]["value"] == 200.0
    st = d["diff_stats"]
    # 行内值是“截至该行的前缀累积值”：差异在第一个纳入事件（300）即分叉
    assert st["value_changed"] == 2 and st["value_equal"] == 0
    assert st["total_events"] == 3               # 未来事件同样列出（双方一致排除）
    assert st["a_value"] == 15.0 and st["b_value"] == 200.0
    # 事件明细逐行对齐，顺序稳定
    rows = d["rows"]
    assert [(r["event_ts"], r["present_in"]) for r in rows] == [
        (300, "both"), (600, "both"), (3900, "both")]
    # 最后一条纳入事件行的累积值恰好等于头部快照值（统计与行内容严格同源）
    inc = [r for r in rows if r["included"]]
    assert inc[-1]["a_value"] == d["a"]["value"] == 15.0
    assert inc[-1]["b_value"] == d["b"]["value"] == 200.0
    assert [r["different"] for r in inc] == [True, True]
    assert all(r["exclude_reason"] == "future" for r in rows
               if r["event_ts"] == 3900)
    # 被排除事件两行值都为 null，不参与统计
    fut = next(r for r in rows if r["event_ts"] == 3900)
    assert fut["a_value"] is None and fut["b_value"] is None
    assert fut["different"] is False


def test_diff_prefix_values_detect_sum_vs_mean_divergence(store, worker):
    """求和版与平均版：头部 a_value/b_value 已不同，统计不得记 value_equal。

    旧实现对每条事件只放“单事件变换值”，而任何单事件的 sum 与 mean 都相等，
    导致头部 15 vs 7.5 已分叉却把每条都记成值相同。前缀累积语义下，差异从
    第二条纳入事件起出现，汇总计数与行内容、头部值来自同一个固定视图。
    """
    make_feature(store, name="agg_1h", transform="sum",
                 transform_version=1, window="1h")
    events(store, [("u1", 300, 10.0), ("u1", 600, 5.0),
                   ("u1", 3900, 99.0)])
    worker.tick()
    store.create_generation("agg_1h", "mean", 1)
    worker.tick()  # 门槛拦截，但 gen2 回填完整，可显式指定

    d = store.generation_diff("agg_1h", "u1", 600, a_gen_no=1, b_gen_no=2)
    # 头部：sum=15 vs mean=7.5（确已不同）
    assert d["a"]["value"] == 15.0 and d["b"]["value"] == 7.5
    st = d["diff_stats"]
    assert st["a_value"] == 15.0 and st["b_value"] == 7.5
    # 首条纳入事件 sum(10)==mean(10)，第二条起 sum(10,5)=15 != mean=7.5
    assert st["value_changed"] == 1 and st["value_equal"] == 1
    inc = [r for r in d["rows"] if r["included"]]
    assert [(r["event_ts"], r["a_value"], r["b_value"], r["different"])
            for r in inc] == [
        (300, 10.0, 10.0, False), (600, 15.0, 7.5, True)]
    # 末条纳入行与头部严格一致；未来事件不计入相同/不同
    assert inc[-1]["a_value"] == st["a_value"]
    assert inc[-1]["b_value"] == st["b_value"]
    assert st["value_changed"] + st["value_equal"] == len(inc)
    assert st["total_events"] == 3


def test_equivalent_generations_have_empty_diff(store, worker):
    name = _baseline(store, worker)
    store.create_generation(name, "sum", 2)
    worker.tick()
    d = store.generation_diff(name, "u1", 600, a_gen_no=1, b_gen_no=2)
    assert d["diff_stats"]["value_changed"] == 0
    assert d["diff_stats"]["value_equal"] == 2
    assert d["a"]["value"] == d["b"]["value"] == 15.0


# ------------------------------------------------- 4. 分页顺序稳定/重复读取

def test_snapshot_pagination_is_stable_and_resumable(store, worker):
    name = _baseline(
        store, worker,
        events_rows=[("u1", t, float(t)) for t in (100, 200, 300, 400, 500)]
                    + [("u1", 9000, 1.0)])
    all_ids, cursor, pages, first_id = [], None, [], None
    for _ in range(10):
        page = store.feature_snapshot(name, "u1", 600, limit=2, cursor=cursor)
        if first_id is None:
            first_id = page["snapshot_id"]
        pages.append(page)
        ids = [e["event_id"] for e in page["events"]]
        # 同一快照标识贯穿所有翻页
        assert page["snapshot_id"] == first_id
        assert not (set(ids) & set(all_ids))      # 无重复
        all_ids += ids
        cursor = page["pagination"]["next_cursor"]
        if cursor is None:
            break
    # 全集：5 条窗口内 + 1 条未来，顺序按 (event_ts, seq, id)
    assert len(all_ids) == 6
    ts_order = [e["event_ts"] for p in pages for e in p["events"]]
    assert ts_order == sorted(ts_order)
    assert pages[-1]["pagination"]["next_cursor"] is None


def test_diff_pagination_pins_pair_of_snapshots(store, worker):
    name = _baseline(
        store, worker,
        events_rows=[("u1", t, 1.0) for t in (300, 600, 900)]
                    + [("u1", 5000, 9.0)])
    store.create_generation(name, "sum", 2)
    worker.tick()
    first = store.generation_diff(name, "u1", 900, a_gen_no=1, b_gen_no=2,
                                  limit=2)
    a_sid, b_sid = first["a"]["snapshot_id"], first["b"]["snapshot_id"]
    cursor = first["pagination"]["next_cursor"]
    assert cursor is not None
    # 续页请求不得再带代际参数：代际已钉在游标里；固定视图与统计逐页一致
    second = store.generation_diff(name, "u1", 900, limit=2, cursor=cursor)
    assert second["a"]["snapshot_id"] == a_sid
    assert second["b"]["snapshot_id"] == b_sid
    assert second["diff_stats"] == first["diff_stats"]
    p1 = {(r["event_id"]) for r in first["rows"]}
    p2 = {(r["event_id"]) for r in second["rows"]}
    assert not (p1 & p2) and len(p1 | p2) == 4
    # 续页时再传代际参数属于被篡改请求 -> 400
    with pytest.raises(FeatError, match="不能再指定代际参数"):
        store.generation_diff(name, "u1", 900, a_gen_no=1, b_gen_no=2,
                              limit=2, cursor=cursor)


def test_diff_stats_identical_across_pages_and_repeat_reads(store, worker):
    """汇总统计由整份固定视图算出，与翻到第几页无关；重复读取同号。"""
    name = _baseline(
        store, worker,
        events_rows=[("u1", t, float(t)) for t in (100, 200, 300, 400)]
                    + [("u1", 9000, 1.0)])
    store.create_generation(name, "sum", 2)
    worker.tick()
    seen_stats, seen_a, seen_b = set(), set(), set()
    cursor = None
    for _ in range(10):
        # 首页显式指定一对代际；续页只带游标（代际已钉在游标里）
        page = store.generation_diff(
            name, "u1", 600,
            a_gen_no=1 if cursor is None else None,
            b_gen_no=2 if cursor is None else None,
            limit=2, cursor=cursor)
        seen_stats.add(json.dumps(page["diff_stats"], sort_keys=True))
        seen_a.add(page["a"]["snapshot_id"])
        seen_b.add(page["b"]["snapshot_id"])
        cursor = page["pagination"]["next_cursor"]
        if cursor is None:
            break
    assert len(seen_stats) == 1 and len(seen_a) == 1 and len(seen_b) == 1
    # 无游标重复首页：内容寻址，快照标识与统计不变
    again = store.generation_diff(name, "u1", 600, a_gen_no=1, b_gen_no=2)
    assert again["a"]["snapshot_id"] in seen_a
    assert again["b"]["snapshot_id"] in seen_b


def test_pagination_returns_409_when_snapshot_drifts(store, worker):
    name = _baseline(store, worker)
    store.create_generation(name, "sum", 2)
    worker.tick()
    snap_page = store.feature_snapshot(name, "u1", 600, limit=1)
    snap_cursor = snap_page["pagination"]["next_cursor"]
    diff_page = store.generation_diff(
        name, "u1", 600, a_gen_no=1, b_gen_no=2, limit=1)
    diff_cursor = diff_page["pagination"]["next_cursor"]

    # 游标发出后订正落地改写了事件日志（事件版本变化）-> 内容哈希漂移
    store.submit_correction("purchase", "e1", 1, "replace",
                            raw={"amount": 999.0})
    with pytest.raises(SnapshotConflict):
        store.feature_snapshot(name, "u1", 600, limit=1, cursor=snap_cursor)
    with pytest.raises(SnapshotConflict):
        store.generation_diff(name, "u1", 600, limit=1, cursor=diff_cursor)
    # 重新发起首页查询得到新快照标识，可以继续
    again = store.feature_snapshot(name, "u1", 600, limit=1)
    assert again["snapshot_id"] != snap_page["snapshot_id"]
    assert again["pagination"]["next_cursor"] is not None


def test_pagination_rejects_old_cursor_after_alias_switch(store, worker):
    """游标签发后服务别名切换到其他代际：旧游标必须 409 拒绝。

    续页要求读取时刻的别名与签游标时严格一致，绝不允许把切换前的固定
    视图续读进切换后的请求；别名切回后需要重新取首页游标。
    """
    name = _baseline(store, worker)
    store.create_generation(name, "sum", 2)
    worker.tick()
    assert store.read_online(name, "u1")["serving_gen"] == 2

    snap_cursor = store.feature_snapshot(
        name, "u1", 600, limit=1)["pagination"]["next_cursor"]
    diff_cursor = store.generation_diff(
        name, "u1", 600, a_gen_no=1, b_gen_no=2,
        limit=1)["pagination"]["next_cursor"]

    # 别名 2 -> 1：旧游标 409
    store.rollback_feature(name, reason="测试别名切换")
    assert store.read_online(name, "u1")["serving_gen"] == 1
    with pytest.raises(SnapshotConflict, match="别名"):
        store.feature_snapshot(name, "u1", 600, limit=1, cursor=snap_cursor)
    with pytest.raises(SnapshotConflict, match="别名"):
        store.generation_diff(name, "u1", 600, limit=1, cursor=diff_cursor)

    # 别名 1 -> 2 后，服务视图身份与旧游标签发时一致：同一固定视图可续读，
    # 统计/快照标识与首页完全相同（固定视图不因中间发生过切换而变化）
    store.promote_generation(2)
    cont = store.generation_diff(name, "u1", 600, limit=10, cursor=diff_cursor)
    first = store.generation_diff(name, "u1", 600, a_gen_no=1, b_gen_no=2)
    assert cont["a"]["snapshot_id"] == first["a"]["snapshot_id"]
    assert cont["b"]["snapshot_id"] == first["b"]["snapshot_id"]
    assert cont["diff_stats"] == first["diff_stats"]
    # snapshot 端同理：用签游标时的同一视图续页，snapshot_id 不变
    snap_cont = store.feature_snapshot(
        name, "u1", 600, limit=10, cursor=snap_cursor)
    snap_first = store.feature_snapshot(name, "u1", 600, gen_no=2)
    assert snap_cont["snapshot_id"] == snap_first["snapshot_id"]


# ----------------------------------------------- 5. 别名切换并发隔离

def test_concurrent_alias_switch_never_mixes_generations(store, worker):
    """别名在翻页期间发生切换：每页要么完整属于旧代、要么属于新代，
    绝不把两代事件清单拼在一起。"""
    name = _baseline(store, worker)
    store.create_generation(name, "sum", 2)
    worker.tick()
    assert store.read_online(name, "u1")["serving_gen"] == 2

    seen_gen_ids: list[int] = []
    barrier = threading.Barrier(3)
    errors = []

    def paged_reader():
        try:
            barrier.wait()
            # 不带代际：每一页都在自己的只读事务里把 serving 别名解析一次，
            # 同一请求内只可能使用一个完整代际
            for _ in range(2):
                page = store.feature_snapshot(name, "u1", 600, limit=2)
                seen_gen_ids.append(page["gen_id"])
                # 快照标识内嵌 gen_id，整页事件全部与该代的头信息一致
                assert page["snapshot_id"].startswith(
                    f"snap_{page['gen_id']}_")
                assert page["gen_no"] in (1, 2)
        except Exception as e:  # pragma: no cover
            errors.append(e)

    def switcher():
        barrier.wait()
        for _ in range(20):
            cur = store.read_online(name, "u1")["serving_gen"]
            if cur == 2:
                store.rollback_feature(name, reason="并发隔离测试")
            else:
                store.promote_generation(2)

    threads = [threading.Thread(target=paged_reader) for _ in range(2)]
    threads.append(threading.Thread(target=switcher))
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    # 每次请求只使用一个完整代际：没有任何一页能同时来自两代
    assert all(g in (1, 2) for g in seen_gen_ids)


def test_diff_request_uses_one_pair_within_single_transaction(store, worker):
    """显式代际的差异查询：游标绑定一对快照；别名切换后续读旧游标必须 409。"""
    name = _baseline(store, worker)
    store.create_generation(name, "sum", 2)
    worker.tick()
    assert store.read_online(name, "u1")["serving_gen"] == 2
    first = store.generation_diff(name, "u1", 600, a_gen_no=1, b_gen_no=2,
                                  limit=2)
    cursor = first["pagination"]["next_cursor"]
    # 别名在两代间切换后，旧游标绑定的固定视图不再是当前服务视图 -> 拒绝
    store.rollback_feature(name)
    with pytest.raises(SnapshotConflict, match="别名"):
        store.generation_diff(name, "u1", 600, limit=2, cursor=cursor)
    # 不带游标、显式代际重新发起仍是完整一致的一对快照
    redone = store.generation_diff(name, "u1", 600, a_gen_no=1, b_gen_no=2)
    assert redone["a"]["gen_id"] == first["a"]["gen_id"]
    assert redone["b"]["gen_id"] == first["b"]["gen_id"]
    assert redone["a"]["snapshot_id"] == first["a"]["snapshot_id"]
    # 别名只影响“默认代际”解析；回滚后默认 serving 是 gen1
    assert store.feature_snapshot(name, "u1", 600)["gen_no"] == 1


# ----------------------------------------------------------- 6. 非法参数

def test_invalid_asof_ts_rejected(store, worker):
    name = _baseline(store, worker)
    with pytest.raises(FeatError, match="回填起点"):
        store.feature_snapshot(name, "u1", -1)
    with pytest.raises(FeatError, match="回填起点"):
        store.generation_diff(name, "u1", -1, a_gen_no=1, b_gen_no=1)
    with pytest.raises(FeatError, match="晚于已观测水位"):
        store.feature_snapshot(name, "u1", 2_000_000)
    with pytest.raises(FeatError, match="整数"):
        store.feature_snapshot(name, "u1", 3.14)  # type: ignore[arg-type]


def test_missing_or_incomplete_generation_rejected(store, worker):
    name = "inc_1h"
    make_feature(store, name=name, fail_after_partitions=1)
    events(store, [("u1", 100, 1.0), ("u1", 3700, 2.0), ("u1", 7500, 3.0)])
    worker.tick()  # gen1 回填失败，处于不完整状态
    with pytest.raises(FeatError, match="不存在"):
        store.feature_snapshot(name, "u1", 600, gen_no=99)
    # 存在但属于其他特征的代际 id（另一个特征的第 1 代 id=2）
    make_feature(store, name="other_tt", source="click",
                 value_field="n", transform="count")
    with pytest.raises(FeatError, match="不属于特征"):
        store.feature_snapshot(name, "u1", 600, gen_id=2)
    with pytest.raises(FeatError, match="回填状态"):
        store.feature_snapshot(name, "u1", 600, gen_no=1)
    with pytest.raises(FeatError, match="只能指定其中一个"):
        store.feature_snapshot(name, "u1", 600, gen_id=1, gen_no=1)


def test_diff_requires_two_distinct_complete_generations(store, worker):
    name = _baseline(store, worker)
    with pytest.raises(FeatError, match="必须不同"):
        store.generation_diff(name, "u1", 600, a_gen_no=1, b_gen_no=1)
    with pytest.raises(FeatError, match="不存在"):
        store.generation_diff(name, "u1", 600, a_gen_no=1, b_gen_no=99)


def test_bad_cursor_and_limit_rejected(store, worker):
    import base64

    name = _baseline(store, worker)
    store.create_generation(name, "sum", 2)
    worker.tick()

    def b64(obj):
        return base64.urlsafe_b64encode(json.dumps(obj).encode()).decode()

    with pytest.raises(FeatError, match="无法解析"):
        store.feature_snapshot(name, "u1", 600, cursor="not-a-cursor")
    # 可解析但不是游标 JSON 对象 / 缺少 kind
    with pytest.raises(FeatError, match="格式不正确"):
        store.feature_snapshot(name, "u1", 600, cursor=b64([1, 2]))
    with pytest.raises(FeatError, match="不属于快照查询"):
        store.feature_snapshot(name, "u1", 600, cursor=b64({"kind": "diff"}))
    # diff 游标不能用在快照端点
    dcursor = store.generation_diff(
        name, "u1", 600, a_gen_no=1, b_gen_no=2, limit=1
    )["pagination"]["next_cursor"]
    with pytest.raises(FeatError, match="不属于快照查询"):
        store.feature_snapshot(name, "u1", 600, cursor=dcursor)
    # 快照游标不能用在 diff 端点
    scursor = store.feature_snapshot(
        name, "u1", 600, limit=1)["pagination"]["next_cursor"]
    with pytest.raises(FeatError, match="不属于差异查询"):
        store.generation_diff(name, "u1", 600, cursor=scursor)
    # offset 字段损坏/为负
    with pytest.raises(FeatError, match="offset"):
        store.feature_snapshot(
            name, "u1", 600, cursor=b64({"kind": "snapshot", "offset": "2"}))
    with pytest.raises(FeatError, match="offset"):
        store.feature_snapshot(
            name, "u1", 600, cursor=b64({"kind": "snapshot", "offset": -1}))
    # 伪造上下文（特征/实体/时刻不一致）
    real = json.loads(base64.urlsafe_b64decode(scursor))
    tampered = dict(real, entity_id="other")
    with pytest.raises(FeatError, match="不一致"):
        store.feature_snapshot(
            name, "u1", 600, cursor=b64(tampered))
    # 缺少绑定代际 id
    with pytest.raises(FeatError, match="代际 id"):
        store.feature_snapshot(
            name, "u1", 600,
            cursor=b64({"kind": "snapshot", "offset": 0,
                        "feature_id": 1, "entity_id": "u1", "asof_ts": 600,
                        "serving_gen_id": 2}))
    # 续页携带代际参数
    with pytest.raises(FeatError, match="不能再指定代际参数"):
        store.feature_snapshot(name, "u1", 600, gen_no=2, cursor=scursor)
    with pytest.raises(FeatError, match="不能再指定代际参数"):
        store.generation_diff(name, "u1", 600, a_gen_no=1, b_gen_no=2,
                              cursor=dcursor)
    with pytest.raises(FeatError, match="limit"):
        store.feature_snapshot(name, "u1", 600, limit=0)
    with pytest.raises(FeatError, match="limit"):
        store.feature_snapshot(name, "u1", 600, limit=10_000)


def test_snapshot_requires_feature_and_entity_world(store, worker):
    name = _baseline(store, worker)
    with pytest.raises(FeatError, match="特征不存在"):
        store.feature_snapshot("nope", "u1", 600)
    # 实体从未出现：返回明确的空快照（无事件、零纳入），而不是报错或未来数据
    snap = store.feature_snapshot(name, "ghost", 600)
    assert snap["events"] == []
    assert snap["included_count"] == 0 and snap["excluded_count"] == 0


# ----------------------------------------------------------- HTTP 语义

def test_http_snapshot_diff_and_status_page(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "http.db"))
    store = Store(db, clock=lambda: 1_000_000)
    client = TestClient(create_app(store))
    client.post("/api/v1/features", json={
        "name": "h1", "source": "purchase", "join_key": "entity_id",
        "value_field": "amount", "window": "1h", "transform": "sum",
        "backfill_start": 0, "partition_seconds": 3600,
        "sample_tick_seconds": 300})
    for ts, v in [(300, 10.0), (600, 5.0), (3900, 2.0)]:
        client.post("/api/v1/events", json={
            "source": "purchase", "entity_id": "u1",
            "event_ts": ts, "raw": {"amount": v}})
    client.post("/api/v1/worker/tick")
    client.post("/api/v1/features/h1/generations",
                json={"transform": "sum", "transform_version": 2})
    client.post("/api/v1/worker/tick")

    r = client.get("/api/v1/features/h1/snapshots/u1",
                   params={"asof_ts": 600})
    assert r.status_code == 200
    body = r.json()
    assert body["value"] == 15.0
    assert body["excluded_by_reason"] == {"future": 1}
    assert body["window"]["window_close_inclusive"] == 600

    # 分页
    p1 = client.get("/api/v1/features/h1/snapshots/u1",
                    params={"asof_ts": 600, "limit": 1}).json()
    p2 = client.get("/api/v1/features/h1/snapshots/u1",
                    params={"asof_ts": 600, "limit": 1,
                            "cursor": p1["pagination"]["next_cursor"]})
    assert p2.status_code == 200
    assert p1["snapshot_id"] == p2.json()["snapshot_id"]

    # 差异
    r = client.get("/api/v1/features/h1/snapshots/u1/diff",
                   params={"asof_ts": 600, "a_gen_no": 1, "b_gen_no": 2})
    assert r.status_code == 200
    d1 = r.json()
    assert d1["diff_stats"]["total_events"] == 3
    assert d1["diff_stats"]["value_changed"] == 0
    assert d1["diff_stats"]["value_equal"] == 2

    # 差异分页：续页只带游标（不再带代际），固定视图与统计逐页一致
    dp1 = client.get("/api/v1/features/h1/snapshots/u1/diff",
                     params={"asof_ts": 600, "a_gen_no": 1, "b_gen_no": 2,
                             "limit": 1}).json()
    dcursor = dp1["pagination"]["next_cursor"]
    dp2 = client.get("/api/v1/features/h1/snapshots/u1/diff",
                     params={"asof_ts": 600, "limit": 1, "cursor": dcursor})
    assert dp2.status_code == 200
    assert dp2.json()["diff_stats"] == dp1["diff_stats"]
    assert dp2.json()["a"]["snapshot_id"] == dp1["a"]["snapshot_id"]
    # 续页再带代际参数 -> 400
    assert client.get("/api/v1/features/h1/snapshots/u1/diff",
                      params={"asof_ts": 600, "a_gen_no": 1, "b_gen_no": 2,
                              "cursor": dcursor}).status_code == 400

    # 非法参数 -> 400；漂移/坏游标 -> 409/400
    assert client.get("/api/v1/features/h1/snapshots/u1",
                      params={"asof_ts": -1}).status_code == 400
    assert client.get("/api/v1/features/h1/snapshots/u1",
                      params={"asof_ts": 600, "gen_no": 99}).status_code == 400
    assert client.get("/api/v1/features/h1/snapshots/u1/diff",
                      params={"asof_ts": 600, "a_gen_no": 1,
                              "b_gen_no": 1}).status_code == 400
    # 坏游标（不可解析 / 类型错误 / 端点串用）-> 400
    assert client.get("/api/v1/features/h1/snapshots/u1",
                      params={"asof_ts": 600,
                              "cursor": "not-a-cursor"}).status_code == 400
    scursor = p1["pagination"]["next_cursor"]
    assert client.get("/api/v1/features/h1/snapshots/u1/diff",
                      params={"asof_ts": 600,
                              "cursor": scursor}).status_code == 400
    # 事件版本变化（订正落地改写事件日志）后旧游标 -> 409
    first_event_key = store.db.conn().execute(
        "SELECT dedup_key FROM events ORDER BY id LIMIT 1").fetchone()[0]
    store.submit_correction(
        "purchase", first_event_key, 1, "replace", raw={"amount": 999.0})
    assert client.get("/api/v1/features/h1/snapshots/u1",
                      params={"asof_ts": 600, "limit": 1,
                              "cursor": scursor}).status_code == 409
    assert client.get("/api/v1/features/h1/snapshots/u1/diff",
                      params={"asof_ts": 600, "limit": 1,
                              "cursor": dcursor}).status_code == 409
    # 状态页展示快照标识、事件明细、排除原因、差异统计
    page = client.get("/").text
    assert "snap_" in page
    assert "时间旅行" in page
    assert "未来事件" in page
    assert "跨代差异" in page
    db.close()


def test_http_diff_sum_vs_mean_stats_match_rows(tmp_path):
    """HTTP 契约：sum vs mean 头部值不同，汇总计数必须与行内容同源。"""
    db = Database(str(tmp_path / "http2.db"))
    store = Store(db, clock=lambda: 1_000_000)
    client = TestClient(create_app(store))
    client.post("/api/v1/features", json={
        "name": "m1", "source": "purchase", "join_key": "entity_id",
        "value_field": "amount", "window": "1h", "transform": "sum",
        "backfill_start": 0, "partition_seconds": 3600,
        "sample_tick_seconds": 300})
    for ts, v in [(300, 10.0), (600, 5.0), (3900, 99.0)]:
        client.post("/api/v1/events", json={
            "source": "purchase", "entity_id": "u1",
            "event_ts": ts, "raw": {"amount": v}})
    client.post("/api/v1/worker/tick")
    client.post("/api/v1/features/m1/generations",
                json={"transform": "mean", "transform_version": 1})
    client.post("/api/v1/worker/tick")  # 门槛拦截，gen2 仍可显式查询
    r = client.get("/api/v1/features/m1/snapshots/u1/diff",
                   params={"asof_ts": 600, "a_gen_no": 1, "b_gen_no": 2})
    assert r.status_code == 200
    body = r.json()
    st = body["diff_stats"]
    assert st["a_value"] == 15.0 and st["b_value"] == 7.5
    assert st["value_changed"] == 1 and st["value_equal"] == 1
    inc = [row for row in body["rows"] if row["included"]]
    assert inc[-1]["a_value"] == 15.0 and inc[-1]["b_value"] == 7.5
    assert [row["different"] for row in inc] == [False, True]
    db.close()

