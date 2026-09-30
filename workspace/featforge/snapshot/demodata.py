"""演示数据：为导出端实例播种两个版本的特征物化结果。

v1（gmv_1h）：entity_id / value(json) / asof_ts —— 正常搬运与读者隔离用；
v2（gmv_1h）：在列里增加一个 ``bucket`` 列 —— schema_id 不同，用于演示
导入端“schema 不兼容则拒绝激活，旧 bundle 继续服务”。
"""
from __future__ import annotations

from .source import DemoFeatureStore, register_demo_columns

V1_COLUMNS = [
    {"name": "entity_id", "type": "string"},
    {"name": "value", "type": "json"},
    {"name": "asof_ts", "type": "int64"},
]
V2_COLUMNS = V1_COLUMNS + [{"name": "bucket", "type": "string"}]

FEATURE = "gmv_1h"
V1_CUTOFF = 1_000_000
V2_CUTOFF = 2_000_000


def seed_demo(store: DemoFeatureStore, *, feature: str = FEATURE,
              entities: int = 64) -> dict:
    """写入确定性的演示数据，返回数据概况。"""
    register_demo_columns(feature, 1, V1_COLUMNS)
    register_demo_columns(feature, 2, V2_COLUMNS)

    v1_rows = []
    v2_rows = []
    for i in range(entities):
        eid = f"u{i:04d}"
        # 每个实体两条 v1 刻度；全部落在截止时刻前
        v1_rows.append((eid, 100_000 + i * 100, i * 10 + 1))
        v1_rows.append((eid, 200_000 + i * 100, i * 10 + 2))
        # 一条 v2“新 schema”行，截止时刻更晚
        v2_rows.append((eid, 1_500_000 + i * 100,
                        {"score": i * 3, "bucket": "A" if i % 2 else "B"}))
    store.seed(feature, 1, v1_rows, V1_COLUMNS)
    store.seed(feature, 2, v2_rows, V2_COLUMNS)
    return {"feature": feature, "v1_rows": len(v1_rows),
            "v2_rows": len(v2_rows), "entities": entities,
            "v1_cutoff": V1_CUTOFF, "v2_cutoff": V2_CUTOFF}
