import sys
import pathlib

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

import pytest

from featforge.config import settings
from featforge.db import Database
from featforge.store import Store
from featforge.worker import Worker, drain


@pytest.fixture
def store(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    s = Store(db, clock=lambda: 1_000_000)
    # 测试中对所有点做确定性全量抽样（产品默认按比例抽样，规则相同）
    settings.sample_fraction = 1.0
    settings.sample_tick_seconds = 300
    settings.partition_seconds = 3600
    settings.correction_batch_ticks = 12
    yield s
    db.close()


@pytest.fixture
def worker(store):
    return Worker(store, auto_promote=True, poll_seconds=0)


def make_feature(store, **overrides):
    args = dict(
        name="amt_1h", source="purchase", join_key="entity_id",
        value_field="amount", window="1h", transform="sum",
        transform_version=1, backfill_start=0,
        partition_seconds=3600, sample_tick_seconds=300,
    )
    args.update(overrides)
    return store.create_feature(**args)


def events(store, rows):
    """rows: [(entity, ts, value)] 按时间顺序写入（特征 value_field=amount）。"""
    return [store.ingest_event("purchase", e, ts, raw={"amount": v})
            for e, ts, v in rows]
