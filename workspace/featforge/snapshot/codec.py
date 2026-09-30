"""内容寻址与确定性序列化原语。

- ``canonical_json``：键排序、无多余空白的确定性 JSON。相同结构的对象在任何
  Python 进程里得到完全相同的字节串，这是幂等参数指纹、schema 标识与 manifest
  内容哈希可复现的前提。
- ``sha256_hex`` / ``schema_id`` / ``params_hash``：三类内容寻址标识。
"""
from __future__ import annotations

import hashlib
import json
from typing import Any


def canonical_json(obj: Any) -> bytes:
    """规范化 JSON 字节串（utf-8，键排序，分隔符紧凑，不转义非 ASCII）。"""
    return json.dumps(
        obj, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def short_hash(data: bytes, prefix: str, length: int = 16) -> str:
    return f"{prefix}_{sha256_hex(data)[:length]}"


def schema_id(feature: str, version: int, columns: list[dict]) -> str:
    """由特征名、版本与列定义（名/类型）计算不可变 schema 标识。

    列顺序是 schema 的一部分：同样名字但列顺序不同视为不同 schema。
    列定义中除 ``name``/``type`` 之外的字段（如说明）不参与标识，
    以免注释变化造成“不兼容”误报。
    """
    payload = {
        "feature": feature,
        "version": version,
        "columns": [
            {"name": c["name"], "type": c["type"]} for c in columns
        ],
    }
    return short_hash(canonical_json(payload), "sch", 20)


def params_hash(params: dict) -> str:
    """导出参数指纹：相同幂等键下用于判定“同参数”。"""
    return short_hash(canonical_json(params), "par", 20)
