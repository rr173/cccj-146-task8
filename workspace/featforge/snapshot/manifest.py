"""快照 bundle 的 manifest：内容寻址、固定块序、schema 标识。

manifest 是 bundle 的“目录 + 合同”，字段一旦发布永不修改::

    {
      "manifest_hash": "man_<sha256 前 24 位>",   # 由其余字段内容寻址
      "feature": "gmv_1h",
      "version": 1,
      "schema_id": "sch_...",                     # 列结构标识
      "columns": [{"name": ..., "type": ...}],
      "cutoff_ts": 1000000,
      "row_count": 10000,
      "created_at": 1700000000,
      "params": {...},                            # 导出参数（不含幂等键）
      "chunks": [
        {"index": 0, "offset": 0, "row_count": ..,
         "size_bytes": .., "sha256": "<完整块字节 sha256>"},
        ...
      ]
    }

块序是 manifest 里写死的（固定块序）：导入端必须按 index 逐块收齐，
块内容哈希是对实际传输字节的承诺；manifest_hash 是对这份合同本身的承诺。
"""
from __future__ import annotations

import time
from typing import Callable, Optional

from .codec import canonical_json, schema_id as compute_schema_id, sha256_hex
from .errors import ChunkCountMismatch, SchemaMismatch

# manifest_hash 与 created_at 是“自我描述字段”，不参与自身内容哈希；
# 其余字段全部参与，保证 bundle id 只由导出语义决定（可复现）。
_MANIFEST_HASH_KEYS = {
    "feature", "version", "schema_id", "columns", "cutoff_ts",
    "row_count", "params", "chunks",
}


def manifest_content(manifest: dict) -> bytes:
    """提取参与内容寻址的规范化字节串。"""
    body = {k: manifest[k] for k in _MANIFEST_HASH_KEYS if k in manifest}
    return canonical_json(body)


def manifest_hash_for(manifest: dict) -> str:
    return f"man_{sha256_hex(manifest_content(manifest))[:24]}"


def bundle_id(manifest: dict) -> str:
    """bundle 标识 = manifest 内容哈希。相同导出语义必然得到同一 bundle。"""
    return manifest_hash_for(manifest)


def build_manifest(
    *,
    feature: str,
    version: int,
    columns: list[dict],
    cutoff_ts: int,
    params: dict,
    chunks: list[dict],
    row_count: int,
    clock: Callable[[], int] = lambda: int(time.time()),
) -> dict:
    """组装 manifest 并补上 schema_id / manifest_hash。"""
    m = {
        "feature": feature,
        "version": version,
        "schema_id": compute_schema_id(feature, version, columns),
        "columns": columns,
        "cutoff_ts": cutoff_ts,
        "row_count": row_count,
        "created_at": clock(),
        "params": params,
        "chunks": [
            {
                "index": c["index"],
                "offset": c["offset"],
                "row_count": c["row_count"],
                "size_bytes": c["size_bytes"],
                "sha256": c["sha256"],
            }
            for c in chunks
        ],
    }
    m["manifest_hash"] = manifest_hash_for(m)
    return m


def verify_manifest(manifest: dict) -> None:
    """对收到的 manifest 做结构、块序、总数与自哈希校验。"""
    required = _MANIFEST_HASH_KEYS | {"manifest_hash"}
    missing = required - set(manifest)
    if missing:
        raise ChunkCountMismatch(f"manifest 缺少字段: {sorted(missing)}")

    chunks = manifest["chunks"]
    indexes = [c["index"] for c in chunks]
    if indexes != list(range(len(chunks))):
        raise ChunkCountMismatch(f"固定块序不连续: {indexes}")

    rows = sum(c["row_count"] for c in chunks)
    if rows != manifest["row_count"]:
        raise ChunkCountMismatch(
            f"块行数合计 {rows} 与 manifest 总行数 {manifest['row_count']} 不一致"
        )
    expected_schema = compute_schema_id(
        manifest["feature"], manifest["version"], manifest["columns"]
    )
    if expected_schema != manifest["schema_id"]:
        raise SchemaMismatch(
            f"schema_id 与列定义不符: 声明 {manifest['schema_id']}，"
            f"实际 {expected_schema}"
        )
    if manifest_hash_for(manifest) != manifest["manifest_hash"]:
        raise ChunkCountMismatch(
            f"manifest_hash 不自洽: 声明 {manifest['manifest_hash']}，"
            f"实际 {manifest_hash_for(manifest)}（manifest 被篡改）"
        )


def check_schema_compatible(
    manifest: dict, expected_schema_id: Optional[str]
) -> None:
    """导入端激活前校验：不兼容 schema 一律拒绝，旧 bundle 继续服务。"""
    if expected_schema_id and manifest["schema_id"] != expected_schema_id:
        raise SchemaMismatch(
            f"bundle schema {manifest['schema_id']} 与导入端要求 "
            f"{expected_schema_id} 不兼容"
        )
