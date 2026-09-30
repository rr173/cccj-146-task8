"""变换版本与值类型。

每个特征定义带有一个 transform（含版本）。变换接收窗口内按 (事件时间, 事件序号)
排好序的事件数值列表，产出一个特征值（数值或类别）。定义升级若更换 transform，
会创建新代际——变换输出即“代”，代际之间永不互相覆盖。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Optional


def parse_window(spec: str) -> int:
    """把 "1h"/"7d"/"900s" 这样的窗口声明解析为秒数。"""
    spec = spec.strip()
    units = {"s": 1, "m": 60, "h": 3600, "d": 86400}
    if not spec or spec[-1] not in units:
        raise ValueError(f"非法窗口声明: {spec!r}，示例：1h、7d、900s")
    n = int(spec[:-1])
    if n <= 0:
        raise ValueError(f"窗口必须为正: {spec!r}")
    return n * units[spec[-1]]


def to_number(value) -> Optional[float]:
    """从事件 payload 中抽取数值；非数值返回 None。"""
    if isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        try:
            return float(value)
        except ValueError:
            return None
    return None


# --- 具体变换实现 ---------------------------------------------------------

def _mean(xs: list[float]) -> Optional[float]:
    return sum(xs) / len(xs) if xs else None


def _sum(xs: list[float]) -> float:
    return sum(xs)


def _max(xs: list[float]) -> Optional[float]:
    return max(xs) if xs else None


def _min(xs: list[float]) -> Optional[float]:
    return min(xs) if xs else None


def _last(xs: list[float]) -> Optional[float]:
    return xs[-1] if xs else None


def _count(xs: list[float]) -> int:
    return len(xs)


def _stddev(xs: list[float]) -> Optional[float]:
    if not xs:
        return None
    m = _mean(xs)
    var = sum((x - m) ** 2 for x in xs) / len(xs)
    return var ** 0.5


def _last_category(events: list[tuple[int, int, object]]) -> Optional[str]:
    cats = [str(ev[2]) for ev in events if ev[2] is not None]
    return cats[-1] if cats else None


def _most_frequent_category(events: list[tuple[int, int, object]]) -> Optional[str]:
    cats = [str(ev[2]) for ev in events if ev[2] is not None]
    if not cats:
        return None
    counts: dict[str, int] = {}
    for c in cats:
        counts[c] = counts.get(c, 0) + 1
    # 频次最高，平手时取字典序靠前，保证确定性
    return sorted(counts.items(), key=lambda kv: (-kv[1], kv[0]))[0][0]


@dataclass
class Transform:
    name: str
    version: int
    kind: str  # numeric | categorical
    fn: Callable

    @property
    def key(self) -> str:
        return f"{self.name}_v{self.version}"


_REGISTRY: dict[str, Transform] = {}


def register(t: Transform) -> None:
    _REGISTRY[t.key] = t


def get_transform(name: str, version: int) -> Transform:
    key = f"{name}_v{version}"
    if key not in _REGISTRY:
        raise ValueError(f"未知变换版本: {key}")
    return _REGISTRY[key]


def apply_transform(
    t: Transform, events: list[tuple[int, int, object]]
) -> object:
    """events 为 (event_ts, seq, raw_value)，按 (event_ts, seq) 排序。"""
    if t.kind == "numeric":
        xs = [n for _, _, v in events if (n := to_number(v)) is not None]
        return t.fn(xs)
    return t.fn(events)


# 注册内置变换。升级 = 同名下一版本，例如 mean_v1 -> mean_v2。
register(Transform("sum", 1, "numeric", _sum))
# 与 v1 数值等价的新版本：用于演示零偏差的定义升级
register(Transform("sum", 2, "numeric", _sum))
register(Transform("mean", 1, "numeric", _mean))
register(Transform("mean", 2, "numeric", _mean))  # v2 仅作为“定义升级”占位
register(Transform("max", 1, "numeric", _max))
register(Transform("min", 1, "numeric", _min))
register(Transform("last", 1, "numeric", _last))
register(Transform("count", 1, "numeric", _count))
register(Transform("stddev", 1, "numeric", _stddev))
# 与 mean 数值等价但版本不同的变换：v1/v2 偏差应为 0，用于演示达标升级
register(Transform("ewma", 1, "numeric", _mean))
register(Transform("ewma", 2, "numeric", _mean))
# 一个故意与 mean 偏差很大的变换，演示门槛拦截
register(Transform("mean", 99, "numeric", lambda xs: max(xs) * 10 if xs else None))
# 类别型变换
register(Transform("last_cat", 1, "categorical", _last_category))
register(Transform("top_cat", 1, "categorical", _most_frequent_category))
register(Transform("last_cat", 2, "categorical", _most_frequent_category))


# --- 代际间偏差比对 ---------------------------------------------------------

def relative_bias(a: object, b: object, kind: str) -> Optional[float]:
    """返回两个物化值的偏差，范围 [0, +inf)；类别不一致记为 1。

    数值采用相对误差 |a-b| / max(|a|,|b|,1)，量级无关；无法比较返回 None。
    """
    if a is None and b is None:
        return None  # 双方都无值，不参与覆盖率/偏差统计
    if kind == "categorical":
        return 0.0 if str(a) == str(b) else 1.0
    na, nb = to_number(a), to_number(b)
    if na is None or nb is None:
        return None
    denom = max(abs(na), abs(nb), 1.0)
    return abs(na - nb) / denom
