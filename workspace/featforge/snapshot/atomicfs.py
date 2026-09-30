"""原子文件系统原语：临时文件 + rename 发布、硬链接/拷贝、目录切换。"""
from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path


def ensure_dir(path: str | Path) -> Path:
    p = Path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p


def write_atomic(path: str | Path, data: bytes) -> None:
    """同目录写临时文件再原子改名：崩溃只会留下未引用的 .tmp，绝不会出现半成品。"""
    p = Path(path)
    ensure_dir(p.parent)
    fd, tmp = tempfile.mkstemp(prefix=f".{p.name}.", suffix=".tmp", dir=p.parent)
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, p)
    except BaseException:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def link_or_copy(src: str | Path, dst: str | Path) -> None:
    """优先硬链接（内容寻址存储不可变，安全），跨卷时退回拷贝。"""
    src, dst = Path(src), Path(dst)
    ensure_dir(dst.parent)
    tmp = dst.with_name(dst.name + ".linktmp")
    try:
        os.link(src, tmp)
    except OSError:
        shutil.copyfile(src, tmp)
    os.replace(tmp, dst)


def remove_if_exists(path: str | Path) -> None:
    try:
        os.unlink(path)
    except FileNotFoundError:
        pass


def rm_quiet(path: str | Path) -> None:
    shutil.rmtree(path, ignore_errors=True)
