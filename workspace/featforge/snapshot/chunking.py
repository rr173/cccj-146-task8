"""行的确定性 JSONL 编码与分块。

- 每行 ``{"entity_id": ..., "value": ..., "asof_ts": ...}`` 以规范化 JSON 编码
  （键排序、紧凑分隔），UTF-8 + ``\\n``；
- 按行序、每块至多 ``rows_per_chunk`` 行切分，得到固定块序（0..n-1）；
- 对“块字节”整体取 sha256——这正是导出端写入、网络传输、导入端落盘
  三份字节逐字节一致的承诺；行数/偏移/大小随块一起进入 manifest。
"""
from __future__ import annotations

from typing import Iterable

from .codec import canonical_json, sha256_hex

ROW_FIELDS = ("entity_id", "value", "asof_ts")


def encode_row(row: dict) -> bytes:
    return canonical_json({k: row[k] for k in ROW_FIELDS}) + b"\n"


def build_chunks(rows: Iterable[dict], rows_per_chunk: int) -> list[dict]:
    """把物化行切成内容寻址块。返回 manifest 的 chunks 段（带 index/offset）。"""
    if rows_per_chunk <= 0:
        raise ValueError("rows_per_chunk 必须为正整数")
    chunks: list[dict] = []
    buf = bytearray()
    offset = 0
    count = 0
    index = 0

    def flush() -> None:
        nonlocal buf, offset, count, index
        data = bytes(buf)
        chunks.append({
            "index": index,
            "offset": offset,
            "row_count": count,
            "size_bytes": len(data),
            "sha256": sha256_hex(data),
            "_data": data,
        })
        offset += count
        index += 1
        buf = bytearray()
        count = 0

    for row in rows:
        buf.extend(encode_row(row))
        count += 1
        if count >= rows_per_chunk:
            flush()
    if count:
        flush()
    return chunks
