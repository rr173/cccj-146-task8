"""命令行入口：

  python -m featforge api       # 启动定义/读取 API + 状态页
  python -m featforge worker    # 启动容器化执行器（轮询作业）
  python -m featforge all       # 同进程同时运行 API 与执行器（演示用）
"""
from __future__ import annotations

import argparse
import logging
import threading

import uvicorn

from .api import create_app
from .config import settings
from .db import Database
from .store import Store
from .worker import Worker


def main() -> None:
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(prog="featforge")
    parser.add_argument("role", choices=["api", "worker", "all"])
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--no-auto-promote", action="store_true",
                        help="执行器回填后不自动切换，仅保留 candidate 等待人工 promote")
    args = parser.parse_args()

    store = Store(Database(settings.db_path))

    if args.role in ("worker", "all"):
        worker = Worker(store, auto_promote=not args.no_auto_promote)
        t = threading.Thread(target=worker.run_forever, daemon=True)
        t.start()

    if args.role in ("api", "all"):
        app = create_app(store)
        uvicorn.run(app, host=args.host, port=args.port)
    else:
        t.join()


if __name__ == "__main__":
    main()
