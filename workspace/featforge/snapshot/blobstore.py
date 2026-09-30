"""内容寻址对象存储：块与 manifest 按 sha256 存放，写入即不可变。

目录布局（导出端与导入端的 staging/imports 目录同构）::

    root/
      objects/sha256/<sha>          块字节（同名内容只存一份）
      bundles/<bundle_id>/manifest.json
      staging/<bundle_id>/...       生成中间态（发布前对读取 API 不可见）
      imports/<transfer_id>/<sha>   导入端收到的块（租约内暂存）
      active/<feature>/             激活 bundle 的物化目录（硬链接 + manifest）
"""
from __future__ import annotations

import json
from pathlib import Path

from .atomicfs import ensure_dir, link_or_copy, write_atomic


class BlobStore:
    def __init__(self, root: str):
        self.root = Path(root)
        ensure_dir(self.root / "objects" / "sha256")

    # ---- 不可变对象
    def object_path(self, digest: str) -> Path:
        return self.root / "objects" / "sha256" / digest

    def has_object(self, digest: str) -> bool:
        return self.object_path(digest).exists()

    def put_object(self, data: bytes, digest: str) -> Path:
        """按调用方已验证的 sha256 落盘；已存在则不覆盖（内容寻址天然去重）。"""
        p = self.object_path(digest)
        if not p.exists():
            write_atomic(p, data)
        return p

    def get_object(self, digest: str) -> bytes:
        return self.object_path(digest).read_bytes()

    # ---- 已发布 bundle 的 manifest
    def bundle_dir(self, bundle_id: str) -> Path:
        return self.root / "bundles" / bundle_id

    def write_bundle_manifest(self, bundle_id: str, manifest: dict) -> Path:
        p = self.bundle_dir(bundle_id) / "manifest.json"
        write_atomic(p, json.dumps(manifest, ensure_ascii=False,
                                   sort_keys=True).encode("utf-8"))
        return p

    def published_bundle_exists(self, bundle_id: str) -> bool:
        return (self.bundle_dir(bundle_id) / "manifest.json").exists()

    # ---- 导入端：按搬运会话暂存收到的块
    def import_chunk_path(self, transfer_id: str, digest: str) -> Path:
        return self.root / "imports" / transfer_id / digest

    def put_import_chunk(self, transfer_id: str, data: bytes,
                         digest: str) -> Path:
        p = self.import_chunk_path(transfer_id, digest)
        write_atomic(p, data)
        return p

    def get_import_chunk(self, transfer_id: str, digest: str) -> bytes:
        return self.import_chunk_path(transfer_id, digest).read_bytes()

    def import_chunk_exists(self, transfer_id: str, digest: str) -> bool:
        return self.import_chunk_path(transfer_id, digest).exists()

    def remove_import_chunk(self, transfer_id: str, digest: str) -> None:
        from .atomicfs import remove_if_exists
        remove_if_exists(self.import_chunk_path(transfer_id, digest))

    # ---- 激活目录：把不可变对象硬链接到一起，形成自包含的可服务 bundle
    def materialize_bundle(self, bundle_id: str, manifest: dict,
                           digest_to_object: dict[str, Path]) -> Path:
        out = self.root / "materialized" / bundle_id
        write_atomic(out / "manifest.json", json.dumps(
            manifest, ensure_ascii=False, sort_keys=True).encode("utf-8"))
        chunks_dir = out / "chunks"
        for ch in manifest["chunks"]:
            src = digest_to_object[ch["sha256"]]
            link_or_copy(src, chunks_dir / str(ch["index"]))
        return out

    def materialized_path(self, bundle_id: str) -> Path:
        return self.root / "materialized" / bundle_id

    def active_dir(self, feature: str) -> Path:
        return self.root / "active" / feature

    def staging_dir(self, token: str) -> Path:
        return ensure_dir(self.root / "staging" / token)
